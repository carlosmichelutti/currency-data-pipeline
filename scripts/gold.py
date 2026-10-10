"""Build currency/date dimensions and daily exchange-rate facts from silver."""

import argparse
import logging
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

import polars as pl

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, ROOT_DIR.as_posix())

from database import PostgresDatabase, execute_sql_directory
from scripts.silver import (
    BATCH_PATTERN,
    CURRENCY_COLUMNS,
    CURRENCY_FIELDS,
    METADATA_COLUMNS,
    RATE_COLUMNS,
    RATE_FIELDS,
    persist_parquet,
    require_columns,
    transform_currencies,
    transform_rates,
)

LOGGER = logging.getLogger(__name__)
SILVER_DIR = ROOT_DIR / 'data' / 'silver'
GOLD_DIR = ROOT_DIR / 'data' / 'gold'
GOLD_SQL_DIR = ROOT_DIR / 'sql' / 'gold'
GOLD_SCHEMA = 'gold'
SOURCE_DATASETS = ('currencies', 'rates')

CURRENCY_DIMENSION_COLUMNS = ('currency_key',) + CURRENCY_COLUMNS
DATE_DIMENSION_COLUMNS = (
    'date_key', 'date', 'year', 'quarter', 'month', 'day',
    'iso_year', 'iso_week', 'day_of_week', 'is_weekend',
)
FACT_KEY_COLUMNS = ('date_key', 'base_currency_key', 'quote_currency_key')
FACT_COLUMNS = FACT_KEY_COLUMNS + ('rate',) + METADATA_COLUMNS
TABLE_COLUMNS = {
    'dim_currency': CURRENCY_DIMENSION_COLUMNS,
    'dim_date': DATE_DIMENSION_COLUMNS,
    'fact_exchange_rate': FACT_COLUMNS,
}
TABLE_KEYS = {
    'dim_currency': ('currency_key',),
    'dim_date': ('date_key',),
    'fact_exchange_rate': FACT_KEY_COLUMNS,
}


def unique_records(
    dataframe: pl.DataFrame,
    key_columns: Sequence[str],
    value_columns: Sequence[str],
    order_columns: Sequence[str] = ('_source_row_number',),
) -> pl.DataFrame:
    """
    Collapse equal business records and reject conflicting values at one grain.

    Args:
        dataframe (pl.DataFrame): Input records, including optional lineage.
        key_columns (Sequence[str]): Columns defining the business grain.
        value_columns (Sequence[str]): Attributes that must agree at that grain.
        order_columns (Sequence[str]): Tie breakers for identical records.

    Returns:
        pl.DataFrame: One record per key, ordered by key; the first source row
            supplies lineage when equal records appear more than once.

    Raises:
        ValueError: If keys are null or business values conflict at one key.
    """
    keys = list(key_columns)
    if not keys:
        raise ValueError('key_columns must not be empty')
    business_columns = list(dict.fromkeys((*keys, *value_columns)))
    require_columns(dataframe, (*business_columns, *order_columns))
    if dataframe.filter(pl.any_horizontal(pl.col(keys).is_null())).height:
        raise ValueError(f'Null business keys: {keys}')
    conflicts = (
        dataframe.select(business_columns).unique()
        .group_by(keys).len().filter(pl.col('len') > 1)
    )
    if conflicts.height:
        raise ValueError(
            f'Conflicting records for {keys}: {conflicts.head(5).to_dicts()}'
        )
    return (
        dataframe.sort(list(dict.fromkeys((*keys, *order_columns))))
        .unique(subset=keys, keep='first', maintain_order=True)
    )


def currency_key(column: str = 'iso_code', alias: str = 'currency_key') -> pl.Expr:
    """
    Encode a validated three-letter ISO code as a stable, collision-free key.

    Args:
        column (str): Uppercase ISO code column, already validated by silver.
        alias (str): Name of the resulting key column.

    Returns:
        pl.Expr: Int32 expression using A=0 through Z=25 in base 26, plus one.
            Keys range from 1 (AAA) to 17576 (ZZZ), independent of row order.
    """
    letters = [
        pl.col(column).str.slice(index, 1).str.to_integer(base=36) - 10
        for index in range(3)
    ]
    return (letters[0] * 26**2 + letters[1] * 26 + letters[2] + 1).cast(pl.Int32).alias(alias)


def date_key(column: str = 'date', alias: str = 'date_key') -> pl.Expr:
    """
    Encode a date as an integer YYYYMMDD key.

    Args:
        column (str): Date column to encode.
        alias (str): Name of the resulting key column.

    Returns:
        pl.Expr: Stable Int32 date key expression.
    """
    return pl.col(column).dt.strftime('%Y%m%d').cast(pl.Int32).alias(alias)


def build_currency_dimension(currencies: pl.DataFrame) -> pl.DataFrame:
    """
    Build one currency member per ISO code, retaining the source lineage.

    Args:
        currencies (pl.DataFrame): Silver catalogue with all metadata columns.

    Returns:
        pl.DataFrame: Currency dimension with deterministic keys and nullable
            numeric code, symbol and publication coverage dates. Attributes are
            updated in place (SCD type 1) when loaded into PostgreSQL.

    Raises:
        ValueError: If catalogue attributes disagree for the same ISO code.
    """
    require_columns(currencies, CURRENCY_COLUMNS)
    normalized = transform_currencies(currencies)
    members = unique_records(normalized, ('iso_code',), CURRENCY_FIELDS[1:])
    return members.with_columns(currency_key()).select(CURRENCY_DIMENSION_COLUMNS)


def build_date_dimension(
    dataframe: pl.DataFrame,
    column: str = 'date',
) -> pl.DataFrame:
    """
    Build a continuous calendar spanning the observed dates, including gaps.

    Args:
        dataframe (pl.DataFrame): Records containing dates.
        column (str): Date-typed source column defining the inclusive range.

    Returns:
        pl.DataFrame: Calendar with YYYYMMDD keys, ISO week/year and weekdays
            numbered 1 (Monday) through 7 (Sunday). Empty input yields a typed
            empty dimension; nulls and years outside 1..9999 are rejected.
    """
    require_columns(dataframe, (column,))
    if dataframe.schema[column] != pl.Date:
        raise ValueError(f'{column} must have Date type')
    dates = dataframe.get_column(column)
    if dates.null_count() or dataframe.filter(
        ~pl.col(column).dt.year().is_between(1, 9999)
    ).height:
        raise ValueError(f'{column} must contain non-null dates in years 1..9999')
    calendar = (
        pl.date_range(dates.min(), dates.max(), interval='1d', eager=True)
        if len(dates) else pl.Series('date', [], dtype=pl.Date)
    )
    return pl.DataFrame({'date': calendar}).with_columns(
        date_key(),
        pl.col('date').dt.year().alias('year'),
        pl.col('date').dt.quarter().alias('quarter'),
        pl.col('date').dt.month().alias('month'),
        pl.col('date').dt.day().alias('day'),
        pl.col('date').dt.iso_year().alias('iso_year'),
        pl.col('date').dt.week().alias('iso_week'),
        pl.col('date').dt.weekday().alias('day_of_week'),
        (pl.col('date').dt.weekday() >= 6).alias('is_weekend'),
    ).select(DATE_DIMENSION_COLUMNS)


def resolve_dimension_key(
    dataframe: pl.DataFrame,
    dimension: pl.DataFrame,
    source_column: str,
    natural_key: str,
    dimension_key: str,
    target_column: str,
) -> pl.DataFrame:
    """
    Resolve a dimension key using a validated many-to-one left join.

    Args:
        dataframe (pl.DataFrame): Facts or records requiring a dimension key.
        dimension (pl.DataFrame): Dimension containing unique natural/key pairs.
        source_column (str): Natural reference in the source (base, quote, date).
        natural_key (str): Matching natural column in the dimension.
        dimension_key (str): Dimension primary key to retrieve.
        target_column (str): Output role name, such as base_currency_key.

    Returns:
        pl.DataFrame: Source records with the resolved role key appended.

    Raises:
        ValueError: If dimension keys are null/duplicated or a reference is absent.
    """
    require_columns(dataframe, (source_column,))
    require_columns(dimension, (natural_key, dimension_key))
    for column in (natural_key, dimension_key):
        series = dimension.get_column(column)
        if series.null_count() or series.n_unique() != dimension.height:
            raise ValueError(f'Dimension {column} must be non-null and unique')
    lookup = dimension.select(
        pl.col(natural_key).alias(source_column),
        pl.col(dimension_key).alias(target_column),
    )
    result = dataframe.join(lookup, on=source_column, how='left', validate='m:1')
    missing = result.filter(pl.col(target_column).is_null())
    if missing.height:
        raise ValueError(
            f'Unresolved {source_column} references: '
            f'{missing.get_column(source_column).unique().head(5).to_list()}'
        )
    return result


def build_exchange_rate_fact(
    rates: pl.DataFrame,
    currencies: pl.DataFrame,
    dates: pl.DataFrame,
) -> pl.DataFrame:
    """
    Resolve currency/date roles and build facts without aggregated metrics.

    Args:
        rates (pl.DataFrame): Silver daily rates with all lineage columns.
        currencies (pl.DataFrame): Currency dimension for both currency roles.
        dates (pl.DataFrame): Date dimension covering the rate dates.

    Returns:
        pl.DataFrame: One positive finite rate per date/base/quote, with silver
            lineage. One unit of base equals rate units of quote; rates keep
            silver Float64 precision, without rounding or synthetic inverses.

    Raises:
        ValueError: If rates conflict at one grain or dimension references fail.
    """
    require_columns(rates, RATE_COLUMNS)
    facts = unique_records(
        transform_rates(rates), RATE_FIELDS[:3], ('rate',),
    )
    for source, natural, key, target, dimension in (
        ('date', 'date', 'date_key', 'date_key', dates),
        ('base', 'iso_code', 'currency_key', 'base_currency_key', currencies),
        ('quote', 'iso_code', 'currency_key', 'quote_currency_key', currencies),
    ):
        facts = resolve_dimension_key(facts, dimension, source, natural, key, target)
    return facts.select(FACT_COLUMNS).sort(FACT_KEY_COLUMNS)


def resolve_batch_id(source_root: Path, batch_id: str | None = None) -> str:
    """
    Validate a silver batch or select the lexically greatest complete batch.

    Args:
        source_root (Path): Root of the silver datasets.
        batch_id (str | None): Explicit identifier, or None for automatic choice.

    Returns:
        str: Valid batch shared by currencies and rates.

    Raises:
        ValueError: If an explicit batch identifier is unsafe for a path.
        FileNotFoundError: If no complete common batch exists.
    """
    if batch_id is not None:
        if not BATCH_PATTERN.fullmatch(batch_id):
            raise ValueError('Invalid batch_id: use letters, numbers, dots, dashes or underscores')
        return batch_id
    candidates = []
    for dataset in SOURCE_DATASETS:
        directory = source_root / dataset
        candidates.append({
            child.name for child in directory.iterdir()
            if child.is_dir() and BATCH_PATTERN.fullmatch(child.name)
            and (child / f'{dataset}.parquet').is_file()
        } if directory.is_dir() else set())
    common = set.intersection(*candidates)
    if not common:
        raise FileNotFoundError(f'No common currencies/rates batch found below {source_root}')
    return max(common)


def validate_batch(
    dataframe: pl.DataFrame,
    batch_id: str,
    dataset: str,
) -> None:
    """
    Require complete lineage belonging to the requested batch.

    Args:
        dataframe (pl.DataFrame): Source catalogue or rate records.
        batch_id (str): Expected ingestion batch identifier.
        dataset (str): Dataset name included in validation errors.

    Returns:
        None: Raises before any output is written when lineage is invalid.
    """
    require_columns(dataframe, METADATA_COLUMNS)
    invalid = pl.any_horizontal(pl.col(list(METADATA_COLUMNS)).is_null()) | (
        pl.col('_ingestion_batch_id') != batch_id
    )
    if dataframe.filter(invalid).height:
        raise ValueError(f'{dataset} requires non-null metadata and _ingestion_batch_id={batch_id}')


def upsert_table(
    database: PostgresDatabase,
    dataframe: pl.DataFrame,
    table: str,
    columns: Sequence[str],
    key_columns: Sequence[str],
    schema: str = GOLD_SCHEMA,
) -> int:
    """
    Persist any dimension or fact using the shared parameterized upsert writer.

    Args:
        database (PostgresDatabase): Active transactional facade.
        dataframe (pl.DataFrame): Records to persist.
        table (str): Destination table name.
        columns (Sequence[str]): Ordered destination columns.
        key_columns (Sequence[str]): Primary/unique columns for conflict handling.
        schema (str): Destination schema.

    Returns:
        int: Number of records submitted; caller owns commit/rollback.
    """
    require_columns(dataframe, columns)
    return database.upsert(
        table, dataframe.select(columns).iter_rows(named=True),
        conflict_columns=key_columns, schema=schema,
    )


def load_database(
    frames: Mapping[str, pl.DataFrame],
    batch_id: str,
    sql_directory: Path = GOLD_SQL_DIR,
    database_factory: Callable[[], PostgresDatabase] = PostgresDatabase,
) -> None:
    """
    Load dimensions before facts and replace a batch inside one transaction.

    Args:
        frames (Mapping[str, pl.DataFrame]): Gold frames keyed by table name.
        batch_id (str): Batch whose previous fact rows are replaced.
        sql_directory (Path): Ordered idempotent gold DDL scripts.
        database_factory (Callable[[], PostgresDatabase]): Transaction factory.

    Returns:
        None: DDL, dimension upserts, fact deletion and fact upsert commit together.
            Dimensions remain shared across batches. Overlapping fact grains use
            the last successfully executed load, regardless of batch lexical order.
    """
    for table, columns in TABLE_COLUMNS.items():
        require_columns(frames[table], columns)
    for table in ('dim_currency', 'fact_exchange_rate'):
        validate_batch(frames[table], batch_id, table)
    try:
        with database_factory() as database:
            database.execute(
                "SELECT pg_advisory_xact_lock(hashtext('currency-data-pipeline:gold'))"
            )
            execute_sql_directory(database, sql_directory)
            for table in ('dim_currency', 'dim_date'):
                count = upsert_table(database, frames[table], table, TABLE_COLUMNS[table], TABLE_KEYS[table])
                LOGGER.info('Gold upsert submitted: table=%s rows=%s', table, count)
            database.delete(
                'fact_exchange_rate', schema=GOLD_SCHEMA,
                where={'_ingestion_batch_id': batch_id},
            )
            count = upsert_table(
                database, frames['fact_exchange_rate'], 'fact_exchange_rate',
                FACT_COLUMNS, FACT_KEY_COLUMNS,
            )
            LOGGER.info('Gold upsert submitted: table=fact_exchange_rate rows=%s', count)
    except Exception:
        LOGGER.exception('Gold PostgreSQL load failed: batch_id=%s', batch_id)
        raise
    LOGGER.info('Gold PostgreSQL load completed: batch_id=%s', batch_id)


def run_gold(
    batch_id: str | None = None,
    silver_dir: str | Path = SILVER_DIR,
    gold_dir: str | Path = GOLD_DIR,
    sql_dir: str | Path = GOLD_SQL_DIR,
    persist_database: bool = True,
    database_factory: Callable[[], PostgresDatabase] = PostgresDatabase,
) -> dict[str, str | int]:
    """
    Transform a silver batch into gold Parquets and relational PostgreSQL tables.

    Args:
        batch_id (str | None): Explicit batch or latest complete silver batch.
        silver_dir (str | Path): Silver Parquet root.
        gold_dir (str | Path): Gold Parquet root.
        sql_dir (str | Path): Gold DDL directory.
        persist_database (bool): Whether to load PostgreSQL after writing files.
        database_factory (Callable[[], PostgresDatabase]): Transaction factory.

    Returns:
        dict[str, str | int]: Batch, absolute output paths and row counts for XCom.
            Parquets describe this batch; PostgreSQL accumulates shared dimensions
            and unique daily facts across executed batches.

    Raises:
        FileNotFoundError: If source Parquets or requested DDL scripts are absent.
        ValueError: If a grain, business rule, lineage or relationship is invalid.
    """
    source_root = Path(silver_dir).resolve()
    resolved_batch = resolve_batch_id(source_root, batch_id)
    sources = {
        dataset: source_root / dataset / resolved_batch / f'{dataset}.parquet'
        for dataset in SOURCE_DATASETS
    }
    for source in sources.values():
        if not source.is_file():
            raise FileNotFoundError(f'Silver Parquet file not found: {source}')
    LOGGER.info('Gold transformation started: batch_id=%s', resolved_batch)
    try:
        inputs = {dataset: pl.read_parquet(source) for dataset, source in sources.items()}
        for dataset, dataframe in inputs.items():
            validate_batch(dataframe, resolved_batch, dataset)
        currencies = build_currency_dimension(inputs['currencies'])
        normalized_rates = transform_rates(inputs['rates'])
        dates = build_date_dimension(normalized_rates)
        facts = build_exchange_rate_fact(normalized_rates, currencies, dates)
        frames = {'dim_currency': currencies, 'dim_date': dates, 'fact_exchange_rate': facts}
        result: dict[str, str | int] = {'batch_id': resolved_batch}
        for table, dataframe in frames.items():
            destination = Path(gold_dir).resolve() / table / resolved_batch / f'{table}.parquet'
            result[f'{table}_path'] = str(persist_parquet(dataframe, destination))
            result[f'{table}_rows'] = dataframe.height
            LOGGER.info('Gold Parquet persisted: table=%s rows=%s path=%s', table, dataframe.height, destination)
        if persist_database:
            load_database(frames, resolved_batch, Path(sql_dir), database_factory)
    except Exception:
        LOGGER.exception('Gold transformation failed: batch_id=%s', resolved_batch)
        raise
    LOGGER.info('Gold transformation completed: batch_id=%s', resolved_batch)
    return result


def _parse_args() -> argparse.Namespace:
    """
    Parse batch, directories and PostgreSQL persistence options.

    Returns:
        argparse.Namespace: CLI arguments for run_gold.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch-id', help='Silver batch; defaults to latest complete batch')
    parser.add_argument('--silver-dir', type=Path, default=SILVER_DIR)
    parser.add_argument('--gold-dir', type=Path, default=GOLD_DIR)
    parser.add_argument('--sql-dir', type=Path, default=GOLD_SQL_DIR)
    parser.add_argument('--skip-database', action='store_true', help='Write only Parquets')
    return parser.parse_args()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
    arguments = _parse_args()
    run_gold(
        batch_id=arguments.batch_id,
        silver_dir=arguments.silver_dir,
        gold_dir=arguments.gold_dir,
        sql_dir=arguments.sql_dir,
        persist_database=not arguments.skip_database,
    )
