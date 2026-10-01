import argparse
import logging
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Literal
from uuid import uuid4

import polars as pl

LOGGER = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, ROOT_DIR.as_posix())

from database import PostgresDatabase, execute_sql_directory

BRONZE_DIR = ROOT_DIR / 'data' / 'bronze'
SILVER_DIR = ROOT_DIR / 'data' / 'silver'
SILVER_SQL_DIR = ROOT_DIR / 'sql' / 'silver'

SILVER_SCHEMA = 'silver'

BATCH_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]*$')

DATASETS = (
    'currencies',
    'rates'
)

CURRENCY_FIELDS = (
    'iso_code',
    'iso_numeric',
    'name',
    'symbol',
    'start_date',
    'end_date'
)

RATE_FIELDS = (
    'date',
    'base',
    'quote',
    'rate'
)

METADATA_COLUMNS = (
    '_source_path',
    '_extension_file',
    '_number_row',
    '_batch_id',
    '_ingested_at'
)

CURRENCY_COLUMNS = CURRENCY_FIELDS + METADATA_COLUMNS
RATE_COLUMNS = RATE_FIELDS + METADATA_COLUMNS


def clean_text(
    column: str,
    cast: pl.DataType | type[pl.DataType] = pl.String,
    case: Literal['upper', 'lower'] | None = None,
    collapse_whitespace: bool = False,
    empty_to_null: bool = True,
    strict: bool = True
) -> pl.Expr:
    """
    Build an expression to clean text and cast the result.

    Args:
        column (str): Column to clean.
        cast (pl.DataType | type[pl.DataType]): Destination type.
        case (Literal['upper', 'lower'] | None): Optional letter case conversion.
        collapse_whitespace (bool): Whether to reduce internal whitespace.
        empty_to_null (bool): Whether to convert empty text to null.
        strict (bool): Whether invalid casts raise instead of becoming null.

    Returns:
        pl.Expr: Cleaning expression named after the source column.

    Raises:
        ValueError: If case is not 'upper', 'lower', or None.
    """
    if case not in (None, 'upper', 'lower'):
        raise ValueError('case must be upper, lower, or None')
    expression = pl.col(column).cast(pl.String, strict=strict).str.strip_chars()
    if collapse_whitespace:
        expression = expression.str.replace_all(r'\s+', ' ')
    if case == 'upper':
        expression = expression.str.to_uppercase()
    elif case == 'lower':
        expression = expression.str.to_lowercase()
    if empty_to_null:
        expression = (
            pl.when(expression == '')
            .then(pl.lit(None, dtype=pl.String))
            .otherwise(expression)
        )
    return expression.cast(cast, strict=strict).alias(column)


def cast_columns(
    dataframe: pl.DataFrame,
    schema: Mapping[str, pl.DataType | type[pl.DataType]],
    strict: bool = True
) -> pl.DataFrame:
    """
    Cast selected columns using a type mapping.

    Args:
        dataframe (pl.DataFrame): DataFrame to transform.
        schema (Mapping[str, pl.DataType | type[pl.DataType]]): Column type mapping.
        strict (bool): Whether invalid casts raise instead of becoming null.

    Returns:
        pl.DataFrame: DataFrame with the selected columns cast.
    """
    return dataframe.with_columns(
        pl.col(column).cast(dtype, strict=strict)
        for column, dtype in schema.items()
    )


def parse_dates(
    dataframe: pl.DataFrame,
    columns: Sequence[str],
    format: str = '%Y-%m-%d',
    strict: bool = True
) -> pl.DataFrame:
    """
    Clean and parse selected columns into dates.

    Args:
        dataframe (pl.DataFrame): DataFrame to transform.
        columns (Sequence[str]): Columns to parse.
        format (str): Expected date format.
        strict (bool): Whether invalid dates raise instead of becoming null.

    Returns:
        pl.DataFrame: DataFrame with the selected columns converted to Date.
    """
    return dataframe.with_columns(
        clean_text(column).str.to_date(format, strict=strict, exact=True)
        for column in columns
    )


def require_columns(dataframe: pl.DataFrame, columns: Sequence[str]) -> None:
    """
    Check that the required columns exist.

    Args:
        dataframe (pl.DataFrame): DataFrame to validate.
        columns (Sequence[str]): Required column names.

    Returns:
        None: Validation does not return a value.

    Raises:
        ValueError: If any required column is missing.
    """
    missing = set(columns).difference(dataframe.columns)
    if missing:
        raise ValueError(f'Missing required columns: {sorted(missing)}')


def _reject_rows(dataframe: pl.DataFrame, invalid: pl.Expr, message: str) -> None:
    """
    Reject rows matching an invalidity expression.

    Args:
        dataframe (pl.DataFrame): DataFrame to validate.
        invalid (pl.Expr): Expression identifying invalid rows.
        message (str): Error message describing the rule.

    Returns:
        None: Validation does not return a value.

    Raises:
        ValueError: If any row matches the invalidity expression.
    """
    count = dataframe.filter(invalid.fill_null(False)).height
    if count:
        raise ValueError(f'{message}: {count} invalid row(s)')


def _validate_currency_codes(dataframe: pl.DataFrame, columns: Sequence[str]) -> None:
    """
    Validate that currency codes contain three uppercase letters.

    Args:
        dataframe (pl.DataFrame): DataFrame to validate.
        columns (Sequence[str]): Currency code columns.

    Returns:
        None: Validation does not return a value.
    """
    for column in columns:
        _reject_rows(
            dataframe,
            pl.col(column).is_null() | ~pl.col(column).str.contains(r'^[A-Z]{3}$'),
            f'{column} must contain a three-letter currency code'
        )


def normalize_iso_numeric(column: str = 'iso_numeric') -> pl.Expr:
    """
    Clean and pad numeric currency codes, preserving leading zeros.

    Args:
        column (str): Numeric currency code column.

    Returns:
        pl.Expr: Expression padding text codes to at least three characters.
    """
    return clean_text(column).str.zfill(3).alias(column)


def transform_currencies(dataframe: pl.DataFrame) -> pl.DataFrame:
    """
    Clean, type and validate the currency catalogue.

    Args:
        dataframe (pl.DataFrame): Bronze currency records.

    Returns:
        pl.DataFrame: Silver currency records with bronze metadata preserved.
    """
    require_columns(dataframe, CURRENCY_FIELDS)
    result = dataframe.with_columns(
        clean_text('iso_code', case='upper'),
        normalize_iso_numeric(),
        clean_text('name', case='upper', collapse_whitespace=True),
        clean_text('symbol')
    )
    result = parse_dates(result, ('start_date', 'end_date'))
    _validate_currency_codes(result, ('iso_code',))
    _reject_rows(result, pl.col('name').is_null(), 'name must not be empty')
    _reject_rows(
        result,
        ~pl.col('iso_numeric').str.contains(r'^[0-9]{3}$'),
        'iso_numeric must contain up to three digits'
    )
    _reject_rows(
        result,
        pl.col('start_date') > pl.col('end_date'),
        'start_date must not be after end_date'
    )
    return result


def transform_rates(dataframe: pl.DataFrame) -> pl.DataFrame:
    """
    Clean, type and validate exchange rates.

    Args:
        dataframe (pl.DataFrame): Bronze exchange rate records.

    Returns:
        pl.DataFrame: Silver rate records with bronze metadata preserved.
    """
    require_columns(dataframe, RATE_FIELDS)
    result = dataframe.with_columns(
        clean_text('base', case='upper'),
        clean_text('quote', case='upper'),
        clean_text('rate')
    )
    result = parse_dates(result, ('date',))
    result = cast_columns(result, {'rate': pl.Float64})
    _validate_currency_codes(result, ('base', 'quote'))
    _reject_rows(result, pl.col('date').is_null(), 'date must not be empty')
    _reject_rows(
        result,
        pl.col('rate').is_null()
        | ~pl.col('rate').is_finite()
        | (pl.col('rate') <= 0),
        'rate must be a finite positive number'
    )
    return result


def persist_parquet(dataframe: pl.DataFrame, destination: str | Path) -> Path:
    """
    Write a DataFrame atomically to a Parquet file.

    Args:
        dataframe (pl.DataFrame): Records to persist.
        destination (str | Path): Output Parquet path.

    Returns:
        Path: Absolute path of the persisted file.

    Raises:
        Exception: Re-raises writing or replacement errors after temporary
            file cleanup.
    """
    destination = Path(destination).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = destination.with_name(
        f'.{destination.name}.{uuid4().hex}.tmp'
    )

    try:
        dataframe.write_parquet(temporary_path, compression='zstd')
        temporary_path.replace(destination)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    return destination


def _resolve_batch_id(bronze_root: Path, batch_id: str | None) -> str:
    """
    Validate a requested batch or select the latest shared bronze batch.

    Args:
        bronze_root (Path): Root directory of the bronze datasets.
        batch_id (str | None): Requested batch, or None for automatic selection.

    Returns:
        str: Validated batch identifier or greatest shared identifier.

    Raises:
        ValueError: If the requested identifier contains unsupported characters.
        FileNotFoundError: If no batch has both currencies and rates Parquets.
    """
    if batch_id is not None:
        if not BATCH_PATTERN.fullmatch(batch_id):
            raise ValueError(
                'batch_id may contain only letters, numbers, dots, dashes, '
                'and underscores'
            )
        return batch_id

    batches_by_dataset = []
    for dataset in DATASETS:
        directory = bronze_root / dataset
        batches_by_dataset.append(
            {
                child.name for child in directory.iterdir()
                if child.is_dir()
                and BATCH_PATTERN.fullmatch(child.name)
                and (child / f'{dataset}.parquet').is_file()
            }
            if directory.is_dir() else set()
        )
    common_batches = set.intersection(*batches_by_dataset)
    if not common_batches:
        raise FileNotFoundError(
            f'No common currencies/rates batch found below {bronze_root}'
        )
    return max(common_batches)


def run_silver(
    batch_id: str | None = None,
    bronze_dir: str | Path = BRONZE_DIR,
    silver_dir: str | Path = SILVER_DIR,
    sql_dir: str | Path = SILVER_SQL_DIR,
    persist_database: bool = True,
    database_factory: Callable[[], PostgresDatabase] = PostgresDatabase
) -> dict[str, str | int]:
    """
    Transform a bronze batch into silver Parquets and PostgreSQL tables.

    Args:
        batch_id (str | None): Requested batch, or None for the latest shared batch.
        bronze_dir (str | Path): Root directory containing bronze Parquets.
        silver_dir (str | Path): Root directory for silver Parquets.
        sql_dir (str | Path): Directory containing the ordered silver DDL files.
        persist_database (bool): Whether to load the batch into PostgreSQL.
        database_factory (Callable[[], PostgresDatabase]): Transactional database
            facade factory.

    Returns:
        dict[str, str | int]: Batch identifier, absolute paths and row counts.

    Raises:
        FileNotFoundError: If a required bronze Parquet file is missing.
    """
    bronze_root = Path(bronze_dir).resolve()
    silver_root = Path(silver_dir).resolve()
    resolved_batch_id = _resolve_batch_id(bronze_root, batch_id)
    source_paths = {
        dataset: bronze_root / dataset / resolved_batch_id / f'{dataset}.parquet'
        for dataset in DATASETS
    }
    for source_path in source_paths.values():
        if not source_path.is_file():
            raise FileNotFoundError(f'Bronze Parquet file not found: {source_path}')
    LOGGER.info('Starting silver transformation: batch_id=%s', resolved_batch_id)
    currencies = transform_currencies(pl.read_parquet(source_paths['currencies']))
    rates = transform_rates(pl.read_parquet(source_paths['rates']))
    frames = {'currencies': currencies, 'rates': rates}
    result: dict[str, str | int] = {'batch_id': resolved_batch_id}
    for dataset, dataframe in frames.items():
        destination = silver_root / dataset / resolved_batch_id / f'{dataset}.parquet'
        result[f'{dataset}_path'] = str(persist_parquet(dataframe, destination))
        result[f'{dataset}_rows'] = dataframe.height

    if persist_database:
        _load_database(
            currencies,
            rates,
            batch_id=resolved_batch_id,
            sql_directory=Path(sql_dir),
            database_factory=database_factory
        )

    LOGGER.info(
        'Silver transformation completed: batch_id=%s currencies=%s rates=%s',
        resolved_batch_id, currencies.height, rates.height
    )
    return result


def _load_database(
    currencies: pl.DataFrame,
    rates: pl.DataFrame,
    batch_id: str,
    sql_directory: Path,
    database_factory: Callable[[], PostgresDatabase]
) -> None:
    """
    Execute silver DDL and replace one batch in both tables transactionally.

    Args:
        currencies (pl.DataFrame): Transformed currency records.
        rates (pl.DataFrame): Transformed exchange rate records.
        batch_id (str): Batch identifier to replace.
        sql_directory (Path): Directory containing the ordered silver DDL files.
        database_factory (Callable[[], PostgresDatabase]): Transactional database
            facade factory.

    Returns:
        None: The database context commits or rolls back the load.
    """
    datasets = (
        ('currencies', currencies, CURRENCY_COLUMNS),
        ('rates', rates, RATE_COLUMNS)
    )
    for dataset, dataframe, columns in datasets:
        require_columns(dataframe, columns)
        _reject_rows(
            dataframe,
            pl.col('_batch_id').is_null() | (pl.col('_batch_id') != batch_id),
            f'{dataset} _batch_id must match the requested batch'
        )

    with database_factory() as database:
        execute_sql_directory(database, sql_directory)
        database.delete(
            'currencies',
            schema=SILVER_SCHEMA,
            where={
                '_batch_id': batch_id
            }
        )
        database.delete(
            'rates',
            schema=SILVER_SCHEMA,
            where={
                '_batch_id': batch_id
            }
        )
        database.copy_dataframe(
            currencies.select(CURRENCY_COLUMNS).to_pandas(use_pyarrow_extension_array=True),
            'currencies',
            schema=SILVER_SCHEMA,
            columns=CURRENCY_COLUMNS
        )
        database.copy_dataframe(
            rates.select(RATE_COLUMNS).to_pandas(use_pyarrow_extension_array=True),
            'rates',
            schema=SILVER_SCHEMA,
            columns=RATE_COLUMNS
        )


def _parse_args() -> argparse.Namespace:
    """
    Parse command-line options for a silver execution.

    Returns:
        argparse.Namespace: Batch identifier, directories and persistence option.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--batch-id',
        help='Bronze batch; defaults to newest common batch'
    )
    parser.add_argument(
        '--bronze-dir',
        type=Path,
        default=BRONZE_DIR
    )
    parser.add_argument(
        '--silver-dir',
        type=Path,
        default=SILVER_DIR
    )
    parser.add_argument(
        '--sql-dir',
        type=Path,
        default=SILVER_SQL_DIR
    )
    parser.add_argument(
        '--skip-database',
        action='store_true',
        help='Write Parquet without loading PostgreSQL'
    )
    return parser.parse_args()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s'
    )
    arguments = _parse_args()
    run_silver(
        batch_id=arguments.batch_id,
        bronze_dir=arguments.bronze_dir,
        silver_dir=arguments.silver_dir,
        sql_dir=arguments.sql_dir,
        persist_database=not arguments.skip_database
    )
