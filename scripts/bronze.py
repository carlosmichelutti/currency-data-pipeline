import argparse
import json
import logging
import re
import sys
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, ROOT_DIR.as_posix())

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
    '_extension_file',
    '_number_row',
    '_batch_id',
    '_ingested_at'
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
    resolved_batch_id = _resolve_batch_id(raw_root, batch_id)
    ingested_at = datetime.now(TIMEZONE)

    source_paths = {
        dataset: _raw_file(raw_root, dataset, resolved_batch_id)
        for dataset in ('currencies', 'rates')
    }
    LOGGER.info('Starting bronze ingestion: batch_id=%s', resolved_batch_id)

    currencies = _read_dataset(
        source_paths['currencies'],
        fields=CURRENCY_FIELDS,
        batch_id=resolved_batch_id,
        ingested_at=ingested_at
    )

    rates = _read_dataset(
        source_paths['rates'],
        fields=RATE_FIELDS,
        batch_id=resolved_batch_id,
        ingested_at=ingested_at
    )

    currencies = _as_raw_text(currencies, CURRENCY_FIELDS)
    rates = _as_raw_text(rates, RATE_FIELDS)

    output_paths = {
        'currencies': (
            bronze_root
            / 'currencies'
            / resolved_batch_id
            / 'currencies.parquet'
        ),
        'rates': (
            bronze_root
            / 'rates'
            / resolved_batch_id
            / 'rates.parquet'
        )
    }

    _persist_parquet(currencies, output_paths['currencies'])
    _persist_parquet(rates, output_paths['rates'])

    if persist_database:
        _load_database(
            currencies,
            rates,
            batch_id=resolved_batch_id,
            sql_directory=Path(sql_dir),
            database_factory=database_factory
        )

    result = {
        'batch_id': resolved_batch_id,
        'currencies_path': str(output_paths['currencies']),
        'rates_path': str(output_paths['rates']),
        'currencies_rows': len(currencies),
        'rates_rows': len(rates)
    }

    LOGGER.info(
        'Bronze ingestion completed: batch_id=%s currencies=%s rates=%s',
        resolved_batch_id,
        len(currencies),
        len(rates)
    )
    return result


def _read_dataset(
    source_path: Path,
    fields: list[str],
    batch_id: str,
    ingested_at: datetime
) -> pd.DataFrame:
    """
    Read a raw JSON array and append operational lineage metadata.

    Args:
        source_path (Path): JSON file containing the source records.
        fields (list[str]): Source fields copied into the bronze DataFrame.
        batch_id (str): Identifier assigned to every row in the batch.
        ingested_at (datetime): Timestamp assigned to every row.

    Returns:
        pd.DataFrame: Source fields plus ingestion metadata, without
            semantic conversion of the source values.

    Raises:
        ValueError: If the file is invalid JSON or its root is not an array of
            JSON objects.
    """
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

    dataframe = pd.DataFrame(payload, columns=fields)
    try:
        source_label = source_path.resolve().relative_to(ROOT_DIR).as_posix()
    except ValueError:
        source_label = source_path.resolve().as_posix()

    dataframe['_source_path'] = source_label
    dataframe['_extension_file'] = source_path.suffix.lower()
    dataframe['_number_row'] = range(1, len(dataframe) + 1)
    dataframe['_batch_id'] = batch_id
    dataframe['_ingested_at'] = pd.Timestamp(ingested_at)

    return dataframe


def _as_raw_text(
    dataframe: pd.DataFrame,
    source_columns: list[str]
) -> pd.DataFrame:
    """
    Represent every source value as text without semantic type coercion.

    Args:
        dataframe (pd.DataFrame): DataFrame containing source and metadata
            columns.
        source_columns (list[str]): Columns originating from the source API and
            therefore stored as nullable text in bronze.

    Returns:
        pd.DataFrame: Copy whose source columns use pandas' nullable string
            dtype while metadata keeps its operational type.
    """
    result = dataframe.copy()
    for column in source_columns:
        result[column] = result[column].map(_raw_value_to_text).astype('string')
    return result.loc[:, source_columns + METADATA_COLUMNS]


def _raw_value_to_text(value: Any) -> str | None:
    """
    Convert one decoded JSON value into its text representation.

    Args:
        value (Any): Decoded JSON scalar or composite value.

    Returns:
        str | None: Original string, compact JSON for non-string values, or
            ``None`` for JSON null.
    """
    if value is None or value is pd.NA:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


def _persist_parquet(dataframe: pd.DataFrame, destination: Path) -> None:
    """
    Write a DataFrame atomically to a Parquet destination.

    Args:
        dataframe (pd.DataFrame): Bronze rows to persist.
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
        dataframe.to_parquet(temporary_path, engine='pyarrow', index=False)
        temporary_path.replace(destination)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def _load_database(
    currencies: pd.DataFrame,
    rates: pd.DataFrame,
    batch_id: str,
    sql_directory: Path,
    database_factory: Callable[[], PostgresDatabase]
) -> None:
    """
    Create bronze objects and replace one batch transactionally.

    Args:
        currencies (pd.DataFrame): Currency rows prepared for ``COPY``.
        rates (pd.DataFrame): Exchange-rate rows prepared for ``COPY``.
        batch_id (str): Batch removed and reloaded in both tables.
        sql_directory (Path): Directory containing ordered bronze DDL.
        database_factory (Callable[[], PostgresDatabase]): Factory for the
            transactional database facade.

    Returns:
        None: The database transaction is committed by the facade context.
    """
    with database_factory() as database:
        execute_sql_directory(database, sql_directory)
        database.delete(
            'currencies',
            schema=BRONZE_SCHEMA,
            where={
                '_batch_id': batch_id
            }
        )
        database.delete(
            'rates',
            schema=BRONZE_SCHEMA,
            where={
                '_batch_id': batch_id
            }
        )
        database.copy_dataframe(
            currencies,
            'currencies',
            schema=BRONZE_SCHEMA,
            columns=CURRENCY_COLUMNS
        )
        database.copy_dataframe(
            rates,
            'rates',
            schema=BRONZE_SCHEMA,
            columns=RATE_COLUMNS
        )


def _resolve_batch_id(raw_root: Path, batch_id: str | None) -> str:
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
