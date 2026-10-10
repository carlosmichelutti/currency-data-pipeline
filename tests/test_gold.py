import itertools
import json
import logging
import string
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, Mock, patch

import polars as pl
from polars.testing import assert_frame_equal

from database import PostgresDatabase
from scripts.bronze import run_bronze
from scripts.gold import (
    FACT_COLUMNS,
    TABLE_COLUMNS,
    TABLE_KEYS,
    build_currency_dimension,
    build_date_dimension,
    build_exchange_rate_fact,
    currency_key,
    date_key,
    load_database,
    resolve_dimension_key,
    run_gold,
    unique_records,
)
from scripts.silver import (
    METADATA_COLUMNS,
    run_silver,
    transform_currencies,
    transform_rates,
)


def with_metadata(frame: pl.DataFrame, dataset: str, batch: str = 'batch-1') -> pl.DataFrame:
    """Attach realistic bronze/silver lineage to a business fixture."""
    return frame.with_columns(
        pl.Series('_source_row_number', range(1, frame.height + 1), dtype=pl.Int64),
        pl.lit(f'raw/{dataset}/{batch}/data.json').alias('_source_path'),
        pl.lit('data.json').alias('_source_file'),
        pl.lit('.json').alias('_source_extension'),
        pl.lit(batch).alias('_ingestion_batch_id'),
        pl.lit(datetime(2026, 10, 9, tzinfo=UTC)).alias('_ingested_at_utc'),
        pl.lit('a' * 64).alias('_record_hash'),
    )


def currency_source(batch: str = 'batch-1') -> pl.DataFrame:
    """Provide three catalogue members with nullable optional attributes."""
    return transform_currencies(with_metadata(pl.DataFrame({
        'iso_code': ['BRL', 'USD', 'EUR'],
        'iso_numeric': ['986', '840', None],
        'name': ['Real brasileiro', 'US dollar', 'Euro'],
        'symbol': ['R$', '$', None],
        'start_date': ['1994-07-01', '1999-01-04', None],
        'end_date': [None, None, None],
    }), 'currencies', batch))


def rate_source(batch: str = 'batch-1') -> pl.DataFrame:
    """Provide different dates and pairs, including one calendar gap."""
    return transform_rates(with_metadata(pl.DataFrame({
        'date': ['2026-10-01', '2026-10-03', '2026-10-03'],
        'base': ['BRL', 'BRL', 'USD'],
        'quote': ['USD', 'EUR', 'EUR'],
        'rate': [0.19, 0.17, 0.89],
    }), 'rates', batch))


def gold_frames(batch: str = 'batch-1', rates: pl.DataFrame | None = None) -> dict[str, pl.DataFrame]:
    """Build a complete relational gold fixture."""
    rates = rate_source(batch) if rates is None else rates
    currencies = build_currency_dimension(currency_source(batch))
    dates = build_date_dimension(rates)
    return {
        'dim_currency': currencies,
        'dim_date': dates,
        'fact_exchange_rate': build_exchange_rate_fact(rates, currencies, dates),
    }


class GoldTests(unittest.TestCase):
    def setUp(self) -> None:
        handler = logging.NullHandler()
        logger = logging.getLogger('scripts.gold')
        logger.addHandler(handler)
        self.addCleanup(logger.removeHandler, handler)

    def write_silver(self, root: Path, batch: str = 'batch-1', rates: pl.DataFrame | None = None) -> None:
        for dataset, frame in (
            ('currencies', currency_source(batch)),
            ('rates', rate_source(batch) if rates is None else rates),
        ):
            destination = root / dataset / batch / f'{dataset}.parquet'
            destination.parent.mkdir(parents=True, exist_ok=True)
            frame.write_parquet(destination)

    def test_currency_keys_are_collision_free_for_every_valid_code(self) -> None:
        codes = [''.join(chars) for chars in itertools.product(string.ascii_uppercase, repeat=3)]
        keys = pl.DataFrame({'code': codes}).select(currency_key('code', 'key'))['key']
        self.assertEqual(keys.n_unique(), 26**3)
        self.assertEqual((keys.min(), keys.max()), (1, 17576))
        self.assertEqual(keys.dtype, pl.Int32)

    def test_currency_keys_survive_catalogue_reordering_and_attribute_updates(self) -> None:
        source = currency_source()
        first = build_currency_dimension(source)
        updated = build_currency_dimension(source.reverse().with_columns(pl.lit('NEW NAME').alias('name')))
        assert_frame_equal(first.select('iso_code', 'currency_key'), updated.select('iso_code', 'currency_key'))
        self.assertEqual(updated['name'].to_list(), ['NEW NAME'] * 3)
        self.assertIsNone(first.filter(pl.col('iso_code') == 'EUR')['iso_numeric'].item())
        assert_frame_equal(source, currency_source())

    def test_calendar_fills_gaps_and_handles_iso_year_boundary_and_leap_day(self) -> None:
        calendar = build_date_dimension(pl.DataFrame({'observed': [date(2020, 12, 31), date(2021, 1, 3)]}), 'observed')
        self.assertEqual(calendar.height, 4)
        jan_first = calendar.filter(pl.col('date_key') == 20210101).row(0, named=True)
        self.assertEqual((jan_first['year'], jan_first['iso_year'], jan_first['iso_week']), (2021, 2020, 53))
        self.assertEqual(calendar['is_weekend'].to_list(), [False, False, True, True])
        leap = build_date_dimension(pl.DataFrame({'date': [date(2024, 2, 28), date(2024, 3, 1)]}))
        self.assertIn(20240229, leap['date_key'].to_list())
        key = pl.DataFrame({'other': [date(2026, 10, 9)]}).select(date_key('other', 'key')).item()
        self.assertEqual(key, 20261009)

    def test_calendar_rejects_null_dates_and_wrong_types(self) -> None:
        for source in (
            pl.DataFrame({'date': [None]}, schema={'date': pl.Date}),
            pl.DataFrame({'date': ['2026-10-01']}),
        ):
            with self.subTest(source=source), self.assertRaises(ValueError):
                build_date_dimension(source)

    def test_facts_have_one_row_per_day_pair_and_preserve_direction_and_lineage(self) -> None:
        frames = gold_frames()
        facts = frames['fact_exchange_rate']
        joined = (
            facts.join(frames['dim_currency'].select('currency_key', 'iso_code'), left_on='base_currency_key', right_on='currency_key')
            .rename({'iso_code': 'base'})
            .join(frames['dim_currency'].select('currency_key', 'iso_code'), left_on='quote_currency_key', right_on='currency_key')
            .rename({'iso_code': 'quote'})
            .join(frames['dim_date'].select('date_key', 'date'), on='date_key')
        )
        assert_frame_equal(
            joined.select('date', 'base', 'quote', 'rate', *METADATA_COLUMNS).sort('_source_row_number'),
            rate_source().select('date', 'base', 'quote', 'rate', *METADATA_COLUMNS),
        )
        self.assertEqual(facts.columns, list(FACT_COLUMNS))
        self.assertEqual(facts.height, 3)
        self.assertEqual(facts['rate'].dtype, pl.Float64)

    def test_equal_duplicates_keep_first_source_lineage_for_dimensions_and_facts(self) -> None:
        currencies = currency_source()
        extra_currency = currencies.head(1).with_columns(pl.lit(99, dtype=pl.Int64).alias('_source_row_number'))
        dim = build_currency_dimension(pl.concat([extra_currency, currencies]))
        self.assertEqual(dim.height, 3)
        self.assertEqual(dim.filter(pl.col('iso_code') == 'BRL')['_source_row_number'].item(), 1)
        rates = rate_source()
        duplicated = pl.concat([rates.head(1).with_columns(pl.lit(99, dtype=pl.Int64).alias('_source_row_number')), rates])
        facts = build_exchange_rate_fact(duplicated, dim, build_date_dimension(rates))
        self.assertEqual(facts.height, 3)
        self.assertEqual(facts.filter(pl.col('date_key') == 20261001)['_source_row_number'].item(), 1)

    def test_conflicting_business_values_are_rejected(self) -> None:
        source = currency_source()
        with self.assertRaisesRegex(ValueError, 'Conflicting records'):
            build_currency_dimension(pl.concat([source, source.head(1).with_columns(pl.lit('OTHER NAME').alias('name'))]))
        rates = rate_source()
        with self.assertRaisesRegex(ValueError, 'Conflicting records'):
            build_exchange_rate_fact(
                pl.concat([rates, rates.head(1).with_columns(pl.lit(2.0).alias('rate'))]),
                build_currency_dimension(source), build_date_dimension(rates),
            )

    def test_missing_currency_roles_and_dates_cannot_drop_facts_silently(self) -> None:
        frames = gold_frames()
        for role in ('base', 'quote'):
            invalid = rate_source().head(1).with_columns(pl.lit('JPY').alias(role))
            with self.subTest(role=role), self.assertRaisesRegex(ValueError, f'Unresolved {role}'):
                build_exchange_rate_fact(invalid, frames['dim_currency'], frames['dim_date'])
        with self.assertRaisesRegex(ValueError, 'Unresolved date'):
            build_exchange_rate_fact(rate_source(), frames['dim_currency'], frames['dim_date'].head(1))

    def test_parameterized_lookup_rejects_duplicate_dimension_keys(self) -> None:
        source = pl.DataFrame({'code': ['BRL']})
        for dimension in (
            pl.DataFrame({'iso': ['BRL', 'BRL'], 'key': [1, 2]}),
            pl.DataFrame({'iso': ['BRL', 'USD'], 'key': [1, 1]}),
            pl.DataFrame({'iso': ['BRL'], 'key': [None]}, schema={'iso': pl.String, 'key': pl.Int32}),
        ):
            with self.subTest(dimension=dimension), self.assertRaisesRegex(ValueError, 'non-null and unique'):
                resolve_dimension_key(source, dimension, 'code', 'iso', 'key', 'role_key')

    def test_reusable_deduplication_supports_other_keys_and_value_columns(self) -> None:
        source = pl.DataFrame({'id': [1, 1], 'value': ['same', 'same'], 'order': [2, 1]})
        result = unique_records(source, ('id',), ('value',), ('order',))
        self.assertEqual(result['order'].item(), 1)

    def test_empty_sources_keep_typed_outputs_and_allow_missing_optional_values(self) -> None:
        currencies = build_currency_dimension(currency_source().head(0))
        dates = build_date_dimension(rate_source().head(0))
        facts = build_exchange_rate_fact(rate_source().head(0), currencies, dates)
        for name, frame in (('dim_currency', currencies), ('dim_date', dates), ('fact_exchange_rate', facts)):
            self.assertEqual(frame.height, 0)
            self.assertEqual(frame.columns, list(TABLE_COLUMNS[name]))
        self.assertEqual(facts.schema['quote_currency_key'], pl.Int32)
        self.assertEqual(dates.schema['date'], pl.Date)

    def test_pipeline_from_raw_through_gold_is_repeatable_and_keeps_silver_unchanged(self) -> None:
        factory = Mock(side_effect=AssertionError('Unexpected connection'))
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for dataset, frame in (('currencies', currency_source()), ('rates', rate_source())):
                fields = [name for name in frame.columns if not name.startswith('_')]
                records = json.loads(frame.select(fields).write_json())
                destination = root / 'raw' / dataset / 'batch_id=batch-1' / 'data.json'
                destination.parent.mkdir(parents=True)
                destination.write_text(json.dumps(records), encoding='utf-8')
            run_bronze('batch-1', root / 'raw', root / 'bronze', persist_database=False)
            silver = run_silver('batch-1', root / 'bronze', root / 'silver', persist_database=False)
            original = Path(silver['rates_path']).read_bytes()
            first = run_gold('batch-1', root / 'silver', root / 'gold', persist_database=False, database_factory=factory)
            again = run_gold('batch-1', root / 'silver', root / 'gold', persist_database=False, database_factory=factory)
            self.assertEqual(first, again)
            self.assertEqual(first['dim_currency_rows'], 3)
            self.assertEqual(first['dim_date_rows'], 3)
            self.assertEqual(first['fact_exchange_rate_rows'], 3)
            for table in TABLE_COLUMNS:
                path = root / 'gold' / table / 'batch-1' / f'{table}.parquet'
                self.assertEqual(first[f'{table}_path'], str(path.resolve()))
                self.assertTrue(path.is_file())
            self.assertEqual(Path(silver['rates_path']).read_bytes(), original)
        factory.assert_not_called()

    def test_latest_batch_requires_both_complete_files_and_explicit_batch_is_respected(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for batch in ('20261001000000', '20261002000000'):
                self.write_silver(root / 'silver', batch)
            incomplete = root / 'silver' / 'currencies' / '20261003000000'
            incomplete.mkdir()
            currency_source('20261003000000').write_parquet(incomplete / 'currencies.parquet')
            latest = run_gold(silver_dir=root / 'silver', gold_dir=root / 'gold', persist_database=False)
            explicit = run_gold('20261001000000', root / 'silver', root / 'gold', persist_database=False)
            self.assertEqual(latest['batch_id'], '20261002000000')
            self.assertEqual(explicit['batch_id'], '20261001000000')

    def test_missing_inputs_and_unsafe_batch_fail_without_outputs(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for batch in ('../escape', '..', '', 'a/b', 'a\\b'):
                with self.subTest(batch=batch), self.assertRaises(ValueError):
                    run_gold(batch, root / 'silver', root / 'gold')
            for batch in (None, 'missing'):
                with self.subTest(batch=batch), self.assertRaises(FileNotFoundError):
                    run_gold(batch, root / 'silver', root / 'gold')
            self.assertFalse((root / 'gold').exists())

    def test_invalid_lineage_and_relationships_fail_before_replacing_any_output(self) -> None:
        factory = Mock(side_effect=AssertionError('Unexpected connection'))
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_silver(root / 'silver')
            first = run_gold('batch-1', root / 'silver', root / 'gold', persist_database=False)
            before = {table: Path(first[f'{table}_path']).read_bytes() for table in TABLE_COLUMNS}
            for invalid in (
                rate_source().with_columns(pl.lit('other').alias('_ingestion_batch_id')),
                rate_source().with_columns(pl.lit(None).alias('_record_hash')),
                rate_source().with_columns(pl.lit('JPY').alias('quote')),
                rate_source().with_columns(pl.lit(-1.0).alias('rate')),
            ):
                self.write_silver(root / 'silver', rates=invalid)
                with self.assertRaises(ValueError):
                    run_gold('batch-1', root / 'silver', root / 'gold', database_factory=factory)
                for table in TABLE_COLUMNS:
                    self.assertEqual(Path(first[f'{table}_path']).read_bytes(), before[table])
        factory.assert_not_called()

    def test_database_load_uses_dimensions_before_facts_and_only_deletes_requested_batch(self) -> None:
        database = MagicMock()
        database.__enter__.return_value = database
        submitted = []

        def capture_upsert(table, rows, **kwargs):
            records = list(rows)
            submitted.append((table, records, kwargs))
            return len(records)

        database.upsert.side_effect = capture_upsert
        load_database(gold_frames(), 'batch-1', database_factory=lambda: database)
        self.assertEqual([entry[0] for entry in submitted], list(TABLE_COLUMNS))
        for table, records, kwargs in submitted:
            self.assertEqual(list(records[0]), list(TABLE_COLUMNS[table]))
            self.assertEqual(kwargs, {'conflict_columns': TABLE_KEYS[table], 'schema': 'gold'})
        database.delete.assert_called_once_with('fact_exchange_rate', schema='gold', where={'_ingestion_batch_id': 'batch-1'})
        statements = [call.args[0] for call in database.execute.call_args_list]
        self.assertTrue(any('CREATE TABLE IF NOT EXISTS gold.fact_exchange_rate' in statement for statement in statements))
        database.__exit__.assert_called_once_with(None, None, None)

    def test_missing_sql_stops_before_any_upsert_or_delete(self) -> None:
        database = MagicMock()
        database.__enter__.return_value = database
        with TemporaryDirectory() as directory, self.assertRaises(FileNotFoundError):
            load_database(gold_frames(), 'batch-1', Path(directory), lambda: database)
        database.upsert.assert_not_called()
        database.delete.assert_not_called()

    def test_database_failure_rolls_back_and_keeps_parquets(self) -> None:
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.executemany.side_effect = [None, None, RuntimeError('fact upsert failed')]
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_silver(root / 'silver')
            with patch('database.session.psycopg.connect', return_value=connection), self.assertRaisesRegex(RuntimeError, 'fact upsert failed'):
                run_gold('batch-1', root / 'silver', root / 'gold', database_factory=PostgresDatabase)
            for table in TABLE_COLUMNS:
                self.assertTrue((root / 'gold' / table / 'batch-1' / f'{table}.parquet').is_file())
        connection.rollback.assert_called_once_with()
        connection.commit.assert_not_called()
        connection.close.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
