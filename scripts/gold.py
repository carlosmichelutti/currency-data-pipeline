import argparse
import logging
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from time import perf_counter

import polars as pl

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from database import PostgresDatabase, execute_sql_directory
from scripts.silver import (
    BATCH_PATTERN,
    CURRENCY_FIELDS,
    METADATA_COLUMNS,
    RATE_FIELDS,
    persist_parquet,
    require_columns,
)

LOGGER = logging.getLogger(__name__)

SILVER_DIR = ROOT_DIR / 'data' / 'silver'
GOLD_DIR = ROOT_DIR / 'data' / 'gold'
GOLD_SQL_DIR = ROOT_DIR / 'sql' / 'gold'

GOLD_SCHEMA = 'gold'

SOURCE_DATASETS = (
    'currencies',
    'rates'
)

CURRENCY_DIMENSION_COLUMNS = ('currency_key',) + CURRENCY_FIELDS

DATE_DIMENSION_COLUMNS = (
    'date_key',
    'date',
    'year',
    'quarter',
    'month',
    'day',
    'iso_year',
    'iso_week',
    'day_of_week',
    'is_weekend'
)

FACT_KEY_COLUMNS = (
    'date_key',
    'base_currency_key',
    'quote_currency_key'
)

FACT_COLUMNS = FACT_KEY_COLUMNS + ('rate',)

TABLE_COLUMNS = {
    'dim_currency': CURRENCY_DIMENSION_COLUMNS,
    'dim_date': DATE_DIMENSION_COLUMNS,
    'fact_exchange_rate': FACT_COLUMNS
}

TABLE_KEYS = {
    'dim_currency': ('currency_key',),
    'dim_date': ('date_key',),
    'fact_exchange_rate': FACT_KEY_COLUMNS
}


def validate_silver_schema(
    dataframe: pl.DataFrame,
    schema: Mapping[str, pl.DataType | type[pl.DataType]],
    dataset: str
) -> None:
    """
    Check the persisted silver types without cleaning or converting any values.

    Args:
        dataframe (pl.DataFrame): Records read from a silver Parquet.
        schema (Mapping[str, pl.DataType | type[pl.DataType]]): Expected types.
        dataset (str): Dataset name used in validation logs and errors.

    Returns:
        None: The input is inspected without being modified.

    Raises:
        ValueError: If required columns or their silver types do not match.
    """
    require_columns(dataframe, tuple(schema))
    mismatches = {
        column: f'expected {dtype}, received {dataframe.schema[column]}'
        for column, dtype in schema.items()
        if dataframe.schema[column] != dtype
    }
    if mismatches:
        LOGGER.error(
            'Silver schema rejected: dataset=%s mismatches=%s',
            dataset,
            mismatches
        )
        raise ValueError(f'Silver schema mismatch for {dataset}: {mismatches}')
    LOGGER.info(
        'Silver schema validated: dataset=%s rows=%s',
        dataset,
        dataframe.height
    )


def _reject_rows(dataframe: pl.DataFrame, invalid: pl.Expr, message: str) -> None:
    """
    Reject records violating a gold input or relationship contract.

    Args:
        dataframe (pl.DataFrame): Records to inspect without transforming them.
        invalid (pl.Expr): Expression selecting invalid rows.
        message (str): Business rule included in the log and exception.

    Returns:
        None: Successful validation leaves the input unchanged.

    Raises:
        ValueError: If at least one row violates the supplied rule.
    """
    count = dataframe.filter(invalid.fill_null(False)).height
    if count:
        LOGGER.error('Gold rows rejected: reason=%s rows_rejected=%s', message, count)
        raise ValueError(f'{message}: {count} invalid row(s)')


def unique_records(
    dataframe: pl.DataFrame,
    key_columns: Sequence[str],
    value_columns: Sequence[str],
    order_columns: Sequence[str] = ()
) -> pl.DataFrame:
    """
    Collapse equal business records and reject conflicting values at one grain.

    Args:
        dataframe (pl.DataFrame): Input records, including optional lineage.
        key_columns (Sequence[str]): Columns defining the business grain.
        value_columns (Sequence[str]): Attributes that must agree at that grain.
        order_columns (Sequence[str]): Tie breakers for identical records.

    Returns:
        pl.DataFrame: One record per key, ordered by key and optional tie breakers.

    Raises:
        ValueError: If keys are null or business values conflict at one key.
    """
    keys = list(key_columns)
    if not keys:
        raise ValueError('key_columns must not be empty')
    business_columns = list(dict.fromkeys((*keys, *value_columns)))
    require_columns(dataframe, (*business_columns, *order_columns))
    _reject_rows(
        dataframe,
        pl.any_horizontal(pl.col(keys).is_null()),
        f'Null business keys: {keys}'
    )
    conflicts = (
        dataframe.select(business_columns)
        .unique()
        .group_by(keys)
        .len()
        .filter(pl.col('len') > 1)
    )
    if conflicts.height:
        LOGGER.error(
            'Gold business grain rejected: keys=%s conflicting_keys=%s sample=%s',
            keys,
            conflicts.height,
            conflicts.head(5).to_dicts()
        )
        raise ValueError(
            f'Conflicting records for {keys}: {conflicts.head(5).to_dicts()}'
        )
    result = (
        dataframe.sort(list(dict.fromkeys((*keys, *order_columns))))
        .unique(subset=keys, keep='first', maintain_order=True)
    )
    LOGGER.info(
        'Gold records deduplicated: keys=%s rows_before=%s rows_after=%s duplicates_removed=%s',
        keys,
        dataframe.height,
        result.height,
        dataframe.height - result.height
    )
    return result


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
    return (
        (letters[0] * 26**2 + letters[1] * 26 + letters[2] + 1)
        .cast(pl.Int32)
        .alias(alias)
    )


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
    Build one currency member per ISO code using only business attributes.

    Args:
        currencies (pl.DataFrame): Typed silver catalogue. Optional source
            metadata is discarded; business values are preserved.

    Returns:
        pl.DataFrame: Currency dimension with deterministic keys and nullable
            numeric code, symbol and publication coverage dates. Attributes are
            updated in place (SCD type 1) when loaded into PostgreSQL.

    Raises:
        ValueError: If catalogue attributes disagree for the same ISO code.
    """
    LOGGER.info(
        'Gold table build started: table=dim_currency rows_received=%s',
        currencies.height
    )
    require_columns(currencies, CURRENCY_FIELDS)
    validate_silver_schema(
        currencies,
        {
            'iso_code': pl.String,
            'iso_numeric': pl.String,
            'name': pl.String,
            'symbol': pl.String,
            'start_date': pl.Date,
            'end_date': pl.Date
        },
        'currencies'
    )
    _reject_rows(
        currencies,
        pl.col('iso_code').is_null()
        | ~pl.col('iso_code').str.contains(r'^[A-Z]{3}$')
        | pl.col('name').is_null()
        | (pl.col('name') == '')
        | ~pl.col('iso_numeric').str.contains(r'^[0-9]{3}$').fill_null(True)
        | (pl.col('start_date') > pl.col('end_date')).fill_null(False),
        'currencies must satisfy the persisted silver contract'
    )
    members = unique_records(
        currencies.select(CURRENCY_FIELDS),
        ('iso_code',),
        CURRENCY_FIELDS[1:]
    )
    result = members.with_columns(currency_key()).select(CURRENCY_DIMENSION_COLUMNS)
    LOGGER.info(
        'Gold table build completed: table=dim_currency rows_before=%s rows_after=%s',
        currencies.height,
        result.height
    )
    return result


def build_date_dimension(
    dataframe: pl.DataFrame,
    column: str = 'date'
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
    LOGGER.info(
        'Gold table build started: table=dim_date rows_received=%s',
        dataframe.height
    )
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
    result = pl.DataFrame({'date': calendar}).with_columns(
        date_key(),
        pl.col('date').dt.year().alias('year'),
        pl.col('date').dt.quarter().alias('quarter'),
        pl.col('date').dt.month().alias('month'),
        pl.col('date').dt.day().alias('day'),
        pl.col('date').dt.iso_year().alias('iso_year'),
        pl.col('date').dt.week().alias('iso_week'),
        pl.col('date').dt.weekday().alias('day_of_week'),
        (pl.col('date').dt.weekday() >= 6).alias('is_weekend')
    ).select(DATE_DIMENSION_COLUMNS)
    LOGGER.info(
        'Gold table build completed: table=dim_date rows=%s start_date=%s end_date=%s',
        result.height,
        dates.min(),
        dates.max()
    )
    return result


def resolve_dimension_key(
    dataframe: pl.DataFrame,
    dimension: pl.DataFrame,
    source_column: str,
    natural_key: str,
    dimension_key: str,
    target_column: str
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
    LOGGER.info(
        'Gold dimension lookup started: source_column=%s target_column=%s rows=%s members=%s',
        source_column,
        target_column,
        dataframe.height,
        dimension.height
    )
    require_columns(dataframe, (source_column,))
    require_columns(dimension, (natural_key, dimension_key))
    for column in (natural_key, dimension_key):
        series = dimension.get_column(column)
        if series.null_count() or series.n_unique() != dimension.height:
            LOGGER.error('Gold dimension key rejected: column=%s', column)
            raise ValueError(f'Dimension {column} must be non-null and unique')
    lookup = dimension.select(
        pl.col(natural_key).alias(source_column),
        pl.col(dimension_key).alias(target_column)
    )
    result = dataframe.join(lookup, on=source_column, how='left', validate='m:1')
    missing = result.filter(pl.col(target_column).is_null())
    if missing.height:
        LOGGER.error(
            'Gold dimension lookup failed: source_column=%s target_column=%s unresolved_rows=%s',
            source_column,
            target_column,
            missing.height
        )
        raise ValueError(
            f'Unresolved {source_column} references: '
            f'{missing.get_column(source_column).unique().head(5).to_list()}'
        )
    LOGGER.info(
        'Gold dimension lookup completed: source_column=%s target_column=%s rows_resolved=%s',
        source_column,
        target_column,
        result.height
    )
    return result


def build_exchange_rate_fact(
    rates: pl.DataFrame,
    currencies: pl.DataFrame,
    dates: pl.DataFrame
) -> pl.DataFrame:
    """
    Resolve currency/date roles and build facts without aggregated metrics.

    Args:
        rates (pl.DataFrame): Typed daily rates read from silver Parquet.
            Optional metadata is discarded without silver cleaning or conversion.
        currencies (pl.DataFrame): Currency dimension for both currency roles.
        dates (pl.DataFrame): Date dimension covering the rate dates.

    Returns:
        pl.DataFrame: One positive finite rate per date/base/quote, containing
            only dimension keys and the rate. One unit of base equals rate units
            of quote; rates keep
            silver Float64 precision, without rounding or synthetic inverses.

    Raises:
        ValueError: If rates conflict at one grain or dimension references fail.
    """
    LOGGER.info(
        'Gold table build started: table=fact_exchange_rate rows_received=%s',
        rates.height
    )
    require_columns(rates, RATE_FIELDS)
    validate_silver_schema(
        rates,
        {'date': pl.Date, 'base': pl.String, 'quote': pl.String, 'rate': pl.Float64},
        'rates'
    )
    _reject_rows(
        rates,
        pl.any_horizontal(pl.col(list(RATE_FIELDS)).is_null())
        | ~pl.col('base').str.contains(r'^[A-Z]{3}$')
        | ~pl.col('quote').str.contains(r'^[A-Z]{3}$')
        | ~pl.col('rate').is_finite()
        | (pl.col('rate') <= 0),
        'rates must satisfy the persisted silver contract'
    )
    facts = unique_records(
        rates.select(RATE_FIELDS),
        RATE_FIELDS[:3],
        ('rate',)
    )
    for source, natural, key, target, dimension in (
        ('date', 'date', 'date_key', 'date_key', dates),
        ('base', 'iso_code', 'currency_key', 'base_currency_key', currencies),
        ('quote', 'iso_code', 'currency_key', 'quote_currency_key', currencies)
    ):
        facts = resolve_dimension_key(
            facts,
            dimension,
            source,
            natural,
            key,
            target
        )
    result = facts.select(FACT_COLUMNS).sort(FACT_KEY_COLUMNS)
    LOGGER.info(
        'Gold table build completed: table=fact_exchange_rate rows_before=%s rows_after=%s',
        rates.height,
        result.height
    )
    return result


def resolve_batch_id(source_root: Path, batch_id: str | None = None) -> str:
    """
    Validate a silver batch or select the latest complete batch_id directory.

    Args:
        source_root (Path): Silver root with batch_id=<batch_id> directories.
        batch_id (str | None): Identifier without the directory prefix, or None
            for automatic choice of the lexically greatest complete batch.

    Returns:
        str: Valid batch shared by currencies and rates.

    Raises:
        ValueError: If an explicit batch identifier is unsafe for a path.
        FileNotFoundError: If no complete common batch exists.
    """
    if batch_id is not None:
        if not BATCH_PATTERN.fullmatch(batch_id):
            raise ValueError(
                'Invalid batch_id: use letters, numbers, dots, dashes or underscores'
            )
        return batch_id

    candidates = []
    for dataset in SOURCE_DATASETS:
        directory = source_root / dataset
        candidates.append(
            {
                child.name.removeprefix('batch_id=')
                for child in directory.iterdir()
                if child.is_dir()
                and child.name.startswith('batch_id=')
                and BATCH_PATTERN.fullmatch(child.name.removeprefix('batch_id='))
                and (child / f'{dataset}.parquet').is_file()
            }
            if directory.is_dir() else set()
        )
    common = set.intersection(*candidates)
    if not common:
        raise FileNotFoundError(
            f'No common currencies/rates batch found below {source_root}'
        )
    return max(common)


def validate_batch(
    dataframe: pl.DataFrame,
    batch_id: str,
    dataset: str
) -> None:
    """
    Validate silver source lineage before metadata is discarded in gold.

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
    rejected = dataframe.filter(invalid).height
    if rejected:
        LOGGER.error(
            'Gold lineage rejected: dataset=%s batch_id=%s rows_rejected=%s',
            dataset,
            batch_id,
            rejected
        )
        raise ValueError(
            f'{dataset} requires non-null metadata and _ingestion_batch_id={batch_id}'
        )
    LOGGER.info(
        'Gold lineage validated: dataset=%s batch_id=%s rows=%s',
        dataset,
        batch_id,
        dataframe.height
    )


def upsert_table(
    database: PostgresDatabase,
    dataframe: pl.DataFrame,
    table: str,
    columns: Sequence[str],
    key_columns: Sequence[str],
    schema: str = GOLD_SCHEMA
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
    LOGGER.info(
        'Starting gold upsert: table=%s.%s rows=%s conflict_columns=%s',
        schema,
        table,
        dataframe.height,
        list(key_columns)
    )
    return database.upsert(
        table,
        dataframe.select(columns).iter_rows(named=True),
        conflict_columns=key_columns,
        schema=schema
    )


def load_database(
    frames: Mapping[str, pl.DataFrame],
    batch_id: str,
    sql_directory: Path = GOLD_SQL_DIR,
    database_factory: Callable[[], PostgresDatabase] = PostgresDatabase
) -> None:
    """
    Upsert dimensions before facts inside one transaction, without metadata.

    Args:
        frames (Mapping[str, pl.DataFrame]): Gold frames keyed by table name.
        batch_id (str): Batch identifier used only in operational logs.
        sql_directory (Path): Ordered idempotent gold DDL scripts.
        database_factory (Callable[[], PostgresDatabase]): Transaction factory.

    Returns:
        None: DDL and all upserts commit together.
            Dimensions remain shared across batches. Overlapping fact grains use
            the last successfully executed load, regardless of batch lexical order.
            Facts absent from this batch are preserved; gold stores no batch owner.
    """
    started_at = perf_counter()
    stage = 'input_validation'
    LOGGER.info(
        'Starting gold PostgreSQL load: batch_id=%s schema=%s',
        batch_id,
        GOLD_SCHEMA
    )
    try:
        for table, columns in TABLE_COLUMNS.items():
            require_columns(frames[table], columns)
        stage = 'transaction_open'
        with database_factory() as database:
            stage = 'transaction_lock'
            LOGGER.info('Waiting for gold transaction lock: batch_id=%s', batch_id)
            database.execute(
                "SELECT pg_advisory_xact_lock(hashtext('currency-data-pipeline:gold'))"
            )
            LOGGER.info('Gold transaction lock acquired: batch_id=%s', batch_id)
            stage = 'ddl_execution'
            LOGGER.info(
                'Executing gold DDL files: batch_id=%s directory=%s',
                batch_id,
                sql_directory
            )
            executed_files = execute_sql_directory(database, sql_directory)
            for sql_file in executed_files:
                LOGGER.info(
                    'Gold DDL executed: batch_id=%s file=%s transaction=pending',
                    batch_id,
                    sql_file
                )
            for table in ('dim_currency', 'dim_date'):
                stage = f'{table}_upsert'
                count = upsert_table(
                    database,
                    frames[table],
                    table,
                    TABLE_COLUMNS[table],
                    TABLE_KEYS[table]
                )
                LOGGER.info(
                    'Gold upsert submitted: table=%s batch_id=%s rows=%s transaction=pending',
                    table,
                    batch_id,
                    count
                )
            stage = 'fact_exchange_rate_upsert'
            count = upsert_table(
                database,
                frames['fact_exchange_rate'],
                'fact_exchange_rate',
                FACT_COLUMNS,
                FACT_KEY_COLUMNS
            )
            LOGGER.info(
                'Gold upsert submitted: table=fact_exchange_rate batch_id=%s rows=%s transaction=pending',
                batch_id,
                count
            )
            stage = 'transaction_completion'
            LOGGER.info('Gold PostgreSQL transaction completing: batch_id=%s', batch_id)
    except Exception:
        LOGGER.exception(
            'Gold PostgreSQL load failed: batch_id=%s stage=%s elapsed_seconds=%.3f',
            batch_id,
            stage,
            perf_counter() - started_at
        )
        raise
    LOGGER.info(
        'Gold PostgreSQL load completed: batch_id=%s elapsed_seconds=%.3f',
        batch_id,
        perf_counter() - started_at
    )


def run_gold(
    batch_id: str | None = None,
    silver_dir: str | Path = SILVER_DIR,
    gold_dir: str | Path = GOLD_DIR,
    sql_dir: str | Path = GOLD_SQL_DIR,
    persist_database: bool = True,
    database_factory: Callable[[], PostgresDatabase] = PostgresDatabase
) -> dict[str, str | int]:
    """
    Read persisted silver Parquets and build gold dimensions and facts.

    Args:
        batch_id (str | None): Explicit batch or latest complete silver batch.
        silver_dir (str | Path): Root of the already transformed silver Parquets.
            Silver extraction, cleaning and type conversion are never rerun.
        gold_dir (str | Path): Gold Parquet root.
        sql_dir (str | Path): Gold DDL directory.
        persist_database (bool): Whether to load PostgreSQL after writing files.
        database_factory (Callable[[], PostgresDatabase]): Transaction factory.

    Returns:
        dict[str, str | int]: Batch, absolute output paths and row counts for XCom.
            Parquets describe this batch; PostgreSQL accumulates shared dimensions
            and unique daily facts across executed batches. Gold outputs contain
            business attributes and keys only, without silver metadata columns.

    Raises:
        FileNotFoundError: If source Parquets or requested DDL scripts are absent.
        ValueError: If a grain, business rule, lineage or relationship is invalid.
    """
    started_at = perf_counter()
    resolved_batch = batch_id
    stage = 'batch_resolution'
    LOGGER.info('Starting gold transformation: requested_batch_id=%s', batch_id)
    try:
        source_root = Path(silver_dir).resolve()
        resolved_batch = resolve_batch_id(source_root, batch_id)
        LOGGER.info(
            'Gold batch resolved: batch_id=%s silver_root=%s',
            resolved_batch,
            source_root
        )
        sources = {
            dataset: source_root / dataset / f'batch_id={resolved_batch}' / f'{dataset}.parquet'
            for dataset in SOURCE_DATASETS
        }
        stage = 'source_validation'
        for source in sources.values():
            if not source.is_file():
                raise FileNotFoundError(f'Silver Parquet file not found: {source}')
        inputs = {}
        for dataset, source in sources.items():
            stage = f'{dataset}_read'
            LOGGER.info(
                'Reading silver Parquet: dataset=%s batch_id=%s path=%s',
                dataset,
                resolved_batch,
                source
            )
            inputs[dataset] = pl.read_parquet(source)
            LOGGER.info(
                'Silver Parquet read: dataset=%s batch_id=%s rows=%s path=%s',
                dataset,
                resolved_batch,
                inputs[dataset].height,
                source
            )
        for dataset, dataframe in inputs.items():
            stage = f'{dataset}_lineage_validation'
            validate_batch(dataframe, resolved_batch, dataset)
        stage = 'dim_currency_build'
        LOGGER.info(
            'Building gold table: table=dim_currency batch_id=%s',
            resolved_batch
        )
        currencies = build_currency_dimension(inputs['currencies'])
        stage = 'dim_date_build'
        LOGGER.info(
            'Building gold table: table=dim_date batch_id=%s',
            resolved_batch
        )
        dates = build_date_dimension(inputs['rates'])
        stage = 'fact_exchange_rate_build'
        LOGGER.info(
            'Building gold table: table=fact_exchange_rate batch_id=%s',
            resolved_batch
        )
        facts = build_exchange_rate_fact(inputs['rates'], currencies, dates)
        frames = {
            'dim_currency': currencies,
            'dim_date': dates,
            'fact_exchange_rate': facts
        }
        result: dict[str, str | int] = {'batch_id': resolved_batch}
        for table, dataframe in frames.items():
            stage = f'{table}_parquet_write'
            destination = (
                Path(gold_dir).resolve()
                / table
                / f'batch_id={resolved_batch}'
                / f'{table}.parquet'
            )
            LOGGER.info(
                'Writing gold Parquet: table=%s batch_id=%s rows=%s path=%s',
                table,
                resolved_batch,
                dataframe.height,
                destination
            )
            result[f'{table}_path'] = str(persist_parquet(dataframe, destination))
            result[f'{table}_rows'] = dataframe.height
            LOGGER.info(
                'Gold Parquet persisted: table=%s batch_id=%s rows=%s path=%s',
                table,
                resolved_batch,
                dataframe.height,
                destination
            )
        if persist_database:
            stage = 'database_load'
            load_database(
                frames,
                resolved_batch,
                Path(sql_dir),
                database_factory
            )
        else:
            LOGGER.info('Gold PostgreSQL load skipped: batch_id=%s', resolved_batch)
    except Exception:
        LOGGER.exception(
            'Gold transformation failed: batch_id=%s stage=%s elapsed_seconds=%.3f',
            resolved_batch,
            stage,
            perf_counter() - started_at
        )
        raise
    LOGGER.info(
        'Gold transformation completed: batch_id=%s currencies=%s dates=%s facts=%s elapsed_seconds=%.3f',
        resolved_batch,
        currencies.height,
        dates.height,
        facts.height,
        perf_counter() - started_at
    )
    return result


def _parse_args() -> argparse.Namespace:
    """
    Parse batch, directories and PostgreSQL persistence options.

    Returns:
        argparse.Namespace: CLI arguments for run_gold.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--batch-id',
        help='Silver batch; defaults to latest complete batch'
    )
    parser.add_argument(
        '--skip-database',
        action='store_true',
        help='Write only Parquets'
    )
    return parser.parse_args()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    arguments = _parse_args()
    run_gold(
        batch_id=arguments.batch_id,
        persist_database=not arguments.skip_database
    )
