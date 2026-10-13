import argparse
import hashlib
import json
import logging
import math
import re
import sys
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from database import PostgresDatabase, execute_sql_directory

TIMEZONE = timezone(timedelta(hours=-3), name='UTC-03:00')

LOGGER = logging.getLogger(__name__)

RAW_DIR = ROOT_DIR / 'data' / 'raw'
BRONZE_DIR = ROOT_DIR / 'data' / 'bronze'
BRONZE_SQL_DIR = ROOT_DIR / 'sql' / 'bronze'

BRONZE_SCHEMA = 'bronze'

BATCH_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]*$')

METADATA_COLUMNS = [
    '_source_path',
    '_source_file',
    '_source_extension',
    '_source_row_number',
    '_ingestion_batch_id',
    '_ingested_at_utc',
    '_record_hash'
]

CURRENCY_FIELDS = [
    'iso_code',
    'iso_numeric',
    'name',
    'symbol',
    'start_date',
    'end_date'
]

RATE_FIELDS = [
    'date',
    'base',
    'quote',
    'rate'
]

CURRENCY_COLUMNS = CURRENCY_FIELDS + METADATA_COLUMNS
RATE_COLUMNS = RATE_FIELDS + METADATA_COLUMNS


def run_bronze(
    batch_id: str | None = None,
    raw_dir: str | Path = RAW_DIR,
    bronze_dir: str | Path = BRONZE_DIR,
    sql_dir: str | Path = BRONZE_SQL_DIR,
    persist_database: bool = True,
    database_factory: Callable[[], PostgresDatabase] = PostgresDatabase
) -> dict[str, str | int]:
    """
    Ingest one raw batch into bronze Parquet files and PostgreSQL tables.

    Args:
        batch_id (str | None): Batch identifier to ingest. When omitted, the
            newest identifier shared by currencies and rates is selected.
        raw_dir (str | Path): Root directory containing the raw
            ``currencies`` and ``rates`` datasets.
        bronze_dir (str | Path): Root directory where bronze Parquet
            files are written.
        sql_dir (str | Path): Directory containing the ordered bronze
            schema and table SQL files.
        persist_database (bool): Whether to create the database objects and
            load the batch into PostgreSQL.
        database_factory (Callable[[], PostgresDatabase]): Factory that returns
            the transactional database facade used by the load.

    Returns:
        dict[str, str | int]: Batch identifier, absolute Parquet paths, and row
            counts for currencies and rates.
    """
    raw_root = Path(raw_dir).resolve()
    bronze_root = Path(bronze_dir).resolve()
    resolved_ingestion_batch_id = _resolve_ingestion_batch_id(raw_root, batch_id)
    ingested_at = datetime.now(TIMEZONE)

    source_paths = {
        dataset: _raw_file(raw_root, dataset, resolved_ingestion_batch_id)
        for dataset in ('currencies', 'rates')
    }
    LOGGER.info('Starting bronze ingestion: batch_id=%s', resolved_ingestion_batch_id)

    currencies = _read_dataset(
        source_path=source_paths['currencies'],
        fields=CURRENCY_FIELDS
    )
    LOGGER.info(
        'Raw dataset read: dataset=currencies batch_id=%s rows=%s path=%s',
        resolved_ingestion_batch_id,
        len(currencies),
        source_paths['currencies']
    )

    rates = _read_dataset(
        source_path=source_paths['rates'],
        fields=RATE_FIELDS
    )
    LOGGER.info(
        'Raw dataset read: dataset=rates batch_id=%s rows=%s path=%s',
        resolved_ingestion_batch_id,
        len(rates),
        source_paths['rates']
    )

    currencies = _as_raw_text(currencies, CURRENCY_FIELDS)
    rates = _as_raw_text(rates, RATE_FIELDS)

    currencies = _add_metadata(
        dataframe=currencies,
        source_path=source_paths['currencies'],
        source_columns=CURRENCY_FIELDS,
        batch_id=resolved_ingestion_batch_id,
        ingested_at=ingested_at
    )
    rates = _add_metadata(
        dataframe=rates,
        source_path=source_paths['rates'],
        source_columns=RATE_FIELDS,
        batch_id=resolved_ingestion_batch_id,
        ingested_at=ingested_at
    )

    output_paths = {
        'currencies': (
            bronze_root
            / 'currencies'
            / resolved_ingestion_batch_id
            / 'currencies.parquet'
        ),
        'rates': (
            bronze_root
            / 'rates'
            / resolved_ingestion_batch_id
            / 'rates.parquet'
        )
    }

    _persist_parquet(currencies, output_paths['currencies'])
    _persist_parquet(rates, output_paths['rates'])

    if persist_database:
        _load_database(
            currencies=currencies,
            rates=rates,
            batch_id=resolved_ingestion_batch_id,
            sql_directory=Path(sql_dir),
            database_factory=database_factory
        )
    else:
        LOGGER.info(
            'PostgreSQL load skipped: batch_id=%s', resolved_ingestion_batch_id
        )

    result = {
        'batch_id': resolved_ingestion_batch_id,
        'currencies_path': str(output_paths['currencies']),
        'rates_path': str(output_paths['rates']),
        'currencies_rows': len(currencies),
        'rates_rows': len(rates)
    }

    LOGGER.info(
        'Bronze ingestion completed: batch_id=%s currencies=%s rates=%s',
        resolved_ingestion_batch_id,
        len(currencies),
        len(rates)
    )
    return result


def _read_dataset(
    source_path: Path,
    fields: list[str]
) -> pl.DataFrame:
    """
    Read a raw JSON array into a DataFrame of source fields.

    Args:
        source_path (Path): JSON file containing the source records.
        fields (list[str]): Source fields copied into the DataFrame.

    Returns:
        pl.DataFrame: Source fields read from the JSON file, without
            semantic conversion of the source values. Object columns preserve the
            decoded JSON values until text conversion.

    Raises:
        ValueError: If the file is invalid JSON or its root is not an array of
            JSON objects.
    """
    LOGGER.info('Reading raw JSON file: path=%s', source_path)
    try:
        payload = json.loads(
            source_path.read_text(encoding='utf-8'),
            parse_float=str,
            parse_int=str
        )
    except json.JSONDecodeError as error:
        raise ValueError(f'Invalid JSON in {source_path}: {error}')
    if not isinstance(payload, list) or not all(
        isinstance(row, dict) for row in payload
    ):
        raise ValueError(f'{source_path} must contain a JSON array of objects')

    return pl.DataFrame({
        column: pl.Series(
            column, [record.get(column) for record in payload], dtype=pl.Object
        )
        for column in fields
    })


def _add_metadata(
    dataframe: pl.DataFrame,
    source_path: Path,
    source_columns: list[str],
    batch_id: str,
    ingested_at: datetime
) -> pl.DataFrame:
    """
    Append ingestion metadata and a deterministic hash to each source row.

    Args:
        dataframe (pl.DataFrame): Source records represented as nullable text.
        source_path (Path): JSON file from which the records were read.
        source_columns (list[str]): Source columns included in the record hash
            and returned before the metadata columns.
        batch_id (str): Identifier assigned to every row in the batch.
        ingested_at (datetime): Timestamp assigned to every row.

    Returns:
        pl.DataFrame: Copy containing source fields followed by all ingestion
            metadata columns, including the SHA-256 record hash.
    """
    LOGGER.info(
        'Adding ingestion metadata and record hashes: batch_id=%s rows=%s path=%s',
        batch_id,
        len(dataframe),
        source_path
    )
    try:
        source_label = source_path.resolve().relative_to(ROOT_DIR).as_posix()
    except ValueError:
        source_label = source_path.resolve().as_posix()

    return dataframe.with_columns(
        pl.lit(source_label).alias('_source_path'),
        pl.lit(source_path.name).alias('_source_file'),
        pl.lit(source_path.suffix.lower()).alias('_source_extension'),
        pl.int_range(1, pl.len() + 1, dtype=pl.Int64).alias('_source_row_number'),
        pl.lit(batch_id).alias('_ingestion_batch_id'),
        pl.lit(ingested_at).cast(pl.Datetime('us', 'UTC')).alias('_ingested_at_utc'),
        pl.struct(source_columns).map_elements(
            lambda row: _calculate_record_hash(row, source_columns),
            return_dtype=pl.String
        ).alias('_record_hash')
    ).select(source_columns + METADATA_COLUMNS)


def _as_raw_text(
    dataframe: pl.DataFrame,
    source_columns: list[str]
) -> pl.DataFrame:
    """
    Represent every source value as text without semantic type coercion.

    Args:
        dataframe (pl.DataFrame): DataFrame containing source columns.
        source_columns (list[str]): Columns originating from the source API and
            therefore stored as nullable text in bronze.

    Returns:
        pl.DataFrame: Selected source columns represented using Polars nullable
            String dtype.
    """
    return dataframe.select(
        pl.col(column).map_elements(
            _raw_value_to_text, return_dtype=pl.String
        ).alias(column)
        for column in source_columns
    )


def _raw_value_to_text(value: Any) -> str | None:
    """
    Convert one decoded JSON value into its text representation.

    Args:
        value (Any): Decoded JSON scalar or composite value.

    Returns:
        str | None: Original string, compact JSON for non-string values, or
            ``None`` for JSON null.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


def _calculate_record_hash(
    row: dict[str, Any],
    source_columns: list[str]
) -> str:
    """
    Calculate a deterministic SHA-256 hash from a source row.

    Args:
        row (dict[str, Any]): Row containing source and metadata values.
        source_columns (list[str]): Ordered source columns included in the hash.

    Returns:
        str: Lowercase hexadecimal SHA-256 digest of the canonical source data.
    """
    source_data = {
        column: (
            None
            if row[column] is None
            else str(row[column])
        )
        for column in source_columns
    }
    encoded_data = json.dumps(
        source_data,
        ensure_ascii=False,
        separators=(',', ':'),
        sort_keys=True
    ).encode('utf-8')
    return hashlib.sha256(encoded_data).hexdigest()


def _persist_parquet(dataframe: pl.DataFrame, destination: Path) -> None:
    """
    Write a DataFrame atomically to a Parquet destination.

    Args:
        dataframe (pl.DataFrame): Bronze rows to persist.
        destination (Path): Final Parquet file path.

    Returns:
        None: The function writes the file and does not return a value.

    Raises:
        Exception: Re-raises an exception caught while writing or replacing the
            Parquet file, after removing the temporary file.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = destination.with_name(f'.{destination.name}.{uuid4().hex}.tmp')
    try:
        dataframe.write_parquet(temporary_path)
        temporary_path.replace(destination)
    except Exception:
        LOGGER.exception('Failed to persist bronze Parquet: path=%s', destination)
        temporary_path.unlink(missing_ok=True)
        raise

    LOGGER.info(
        'Bronze Parquet persisted: dataset=%s rows=%s path=%s',
        destination.stem,
        len(dataframe),
        destination
    )


def _load_database(
    currencies: pl.DataFrame,
    rates: pl.DataFrame,
    batch_id: str,
    sql_directory: Path,
    database_factory: Callable[[], PostgresDatabase]
) -> None:
    """
    Create bronze objects and replace one batch transactionally.

    Args:
        currencies (pl.DataFrame): Currency rows prepared for ``COPY``.
        rates (pl.DataFrame): Exchange-rate rows prepared for ``COPY``.
        batch_id (str): Batch removed and reloaded in both tables.
        sql_directory (Path): Directory containing ordered bronze DDL.
        database_factory (Callable[[], PostgresDatabase]): Factory for the
            transactional database facade.

    Returns:
        None: The database transaction is committed by the facade context.
    """
    LOGGER.info(
        'Starting bronze PostgreSQL load: batch_id=%s schema=%s',
        batch_id,
        BRONZE_SCHEMA
    )
    with database_factory() as database:
        LOGGER.info(
            'Executing bronze DDL files: batch_id=%s directory=%s',
            batch_id,
            sql_directory
        )
        executed_files = execute_sql_directory(database, sql_directory)
        for sql_file in executed_files:
            LOGGER.info(
                'Bronze DDL executed: batch_id=%s file=%s transaction=pending',
                batch_id,
                sql_file
            )
        LOGGER.info(
            'Replacing existing bronze batch: batch_id=%s tables=%s.currencies,%s.rates',
            batch_id,
            BRONZE_SCHEMA,
            BRONZE_SCHEMA
        )
        database.delete(
            table_name='currencies',
            schema=BRONZE_SCHEMA,
            where={
                '_ingestion_batch_id': batch_id
            }
        )
        database.delete(
            table_name='rates',
            schema=BRONZE_SCHEMA,
            where={
                '_ingestion_batch_id': batch_id
            }
        )
        LOGGER.info(
            'Starting COPY: dataset=currencies batch_id=%s table=%s.currencies rows=%s',
            batch_id,
            BRONZE_SCHEMA,
            len(currencies)
        )
        currencies_rows = database.copy_dataframe(
            dataframe=currencies,
            table_name='currencies',
            schema=BRONZE_SCHEMA,
            columns=CURRENCY_COLUMNS
        )
        LOGGER.info(
            'Starting COPY: dataset=rates batch_id=%s table=%s.rates rows=%s',
            batch_id,
            BRONZE_SCHEMA,
            len(rates)
        )
        rates_rows = database.copy_dataframe(
            dataframe=rates,
            table_name='rates',
            schema=BRONZE_SCHEMA,
            columns=RATE_COLUMNS
        )

    LOGGER.info(
        'PostgreSQL load committed: dataset=currencies batch_id=%s table=%s.currencies rows=%s',
        batch_id,
        BRONZE_SCHEMA,
        currencies_rows
    )
    LOGGER.info(
        'PostgreSQL load committed: dataset=rates batch_id=%s table=%s.rates rows=%s',
        batch_id,
        BRONZE_SCHEMA,
        rates_rows
    )


def _resolve_ingestion_batch_id(raw_root: Path, batch_id: str | None) -> str:
    """
    Validate an explicit batch or select the newest shared raw batch.

    Args:
        raw_root (Path): Root directory containing raw datasets.
        batch_id (str | None): Requested batch identifier, or ``None`` to
            discover the newest common identifier.

    Returns:
        str: Validated or discovered batch identifier.

    Raises:
        ValueError: If an explicit batch identifier contains unsafe path
            characters.
        FileNotFoundError: If currencies and rates have no common raw batch.
    """
    if batch_id is not None:
        if not BATCH_PATTERN.fullmatch(batch_id):
            raise ValueError(
                'batch_id may contain only letters, numbers, dots, dashes, '
                'and underscores'
            )
        return batch_id

    batches_by_dataset = []
    for dataset in ('currencies', 'rates'):
        dataset_dir = raw_root / dataset
        batches = (
            {
                child.name.removeprefix('batch_id=')
                for child in dataset_dir.iterdir()
                if child.is_dir() and (child / 'data.json').is_file()
            }
            if dataset_dir.is_dir()
            else set()
        )
        batches_by_dataset.append(batches)
    common_batches = set.intersection(*batches_by_dataset)
    if not common_batches:
        raise FileNotFoundError(
            f'No common currencies/rates batch found below {raw_root}'
        )
    return max(common_batches)


def _raw_file(raw_root: Path, dataset: str, batch_id: str) -> Path:
    """
    Resolve a dataset file in either supported raw batch layout.

    Args:
        raw_root (Path): Root directory containing raw datasets.
        dataset (str): Dataset directory name, such as ``currencies`` or
            ``rates``.
        batch_id (str): Batch identifier whose JSON file is required.

    Returns:
        Path: Existing raw ``data.json`` path.

    Raises:
        FileNotFoundError: If neither supported path exists for the batch.
    """
    candidates = [
        raw_root / dataset / f'batch_id={batch_id}' / 'data.json',
        raw_root / dataset / batch_id / 'data.json'
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f'Raw {dataset} file not found for batch {batch_id}: {candidates}'
    )


def _parse_args() -> argparse.Namespace:
    """
    Parse command-line options for a bronze execution.

    Returns:
        argparse.Namespace: Parsed batch and database persistence options.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--batch-id',
        help='Raw batch to ingest; defaults to newest common batch'
    )
    parser.add_argument(
        '--skip-database',
        action='store_true',
        help='Write Parquet without loading PostgreSQL'
    )
    return parser.parse_args()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    arguments = _parse_args()
    run_bronze(
        batch_id=arguments.batch_id,
        persist_database=not arguments.skip_database
    )
