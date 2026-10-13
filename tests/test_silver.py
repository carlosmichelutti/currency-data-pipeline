import json
import logging
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, Mock, patch

import polars as pl
from polars.testing import assert_frame_equal

from database import PostgresDatabase
from scripts.bronze import run_bronze
from scripts.silver import (
    CURRENCY_COLUMNS,
    CURRENCY_FIELDS,
    RATE_COLUMNS,
    RATE_FIELDS,
    SILVER_SQL_DIR,
    _load_database,
    cast_columns,
    clean_text,
    parse_dates,
    persist_parquet,
    run_silver,
    transform_currencies,
    transform_rates,
)


class SilverTests(unittest.TestCase):
    def setUp(self) -> None:
        logger = logging.getLogger('scripts.silver')
        handler = logging.NullHandler()
        logger.addHandler(handler)
        self.addCleanup(logger.removeHandler, handler)

    @staticmethod
    def currencies() -> pl.DataFrame:
        return pl.DataFrame({
            'iso_code': [' all ', 'brl'],
            'iso_numeric': [' 8 ', ''],
            'name': [' Albanian\t  Lek ', 'Real brasileiro'],
            'symbol': [' L ', None],
            'start_date': [' 1998-07-07 ', '1994-07-01'],
            'end_date': ['2026-10-01', ' '],
            '_source_row_number': [1, 2],
            '_source_path': ['raw/currencies.json'] * 2,
            '_source_file': ['currencies.json'] * 2,
            '_source_extension': ['.json'] * 2,
            '_ingestion_batch_id': ['batch-1'] * 2,
            '_ingested_at_utc': [datetime(2026, 10, 1, tzinfo=UTC)] * 2,
            '_record_hash': ['a' * 64, 'b' * 64],
        })

    @staticmethod
    def rates() -> pl.DataFrame:
        return pl.DataFrame({
            'date': [' 2026-10-01 '],
            'base': [' brl '],
            'quote': ['usd'],
            'rate': [' 1.9e-1 '],
            '_source_row_number': [1],
            '_source_path': ['raw/rates.json'],
            '_source_file': ['rates.json'],
            '_source_extension': ['.json'],
            '_ingestion_batch_id': ['batch-1'],
            '_ingested_at_utc': [datetime(2026, 10, 1, tzinfo=UTC)],
            '_record_hash': ['c' * 64],
        })

    def test_text_helper_accepts_different_casts_and_case_options(self) -> None:
        source = pl.DataFrame({
            'text': [' São\t  Paulo ', ' ', None],
            'number': [' 008 ', '', None],
            'rate': [' 0.19 ', '', None],
        })
        result = source.with_columns(
            clean_text('text', case='lower', collapse_whitespace=True),
            clean_text('number', cast=pl.Int32),
            clean_text('rate', cast=pl.Float64),
        )
        self.assertEqual(result['text'].to_list(), ['são paulo', None, None])
        self.assertEqual(result['number'].to_list(), [8, None, None])
        self.assertEqual(result['rate'].to_list(), [0.19, None, None])
        self.assertEqual(result.schema['number'], pl.Int32)
        unchanged_case = source.with_columns(clean_text('text', empty_to_null=False))
        self.assertEqual(unchanged_case['text'].to_list(), ['São\t  Paulo', '', None])

    def test_cast_and_date_helpers_allow_explicit_non_strict_mode(self) -> None:
        source = pl.DataFrame({'value': ['12', 'invalid'], 'day': ['01/10/2026', 'bad']})
        result = cast_columns(source, {'value': pl.Int64}, strict=False)
        result = parse_dates(result, ['day'], format='%d/%m/%Y', strict=False)
        self.assertEqual(result['value'].to_list(), [12, None])
        self.assertEqual(result['day'].to_list(), [date(2026, 10, 1), None])
        with self.assertRaises(pl.exceptions.InvalidOperationError):
            cast_columns(source, {'value': pl.Int64})

    def test_currency_rules_preserve_leading_zeros_unicode_and_metadata(self) -> None:
        source = self.currencies()
        result = transform_currencies(source)
        self.assertEqual(result['iso_code'].to_list(), ['ALL', 'BRL'])
        self.assertEqual(result['iso_numeric'].to_list(), ['008', None])
        self.assertEqual(result['name'].to_list(), ['Albanian Lek', 'Real brasileiro'])
        self.assertEqual(result['symbol'].to_list(), ['L', None])
        self.assertEqual(result['start_date'][0], date(1998, 7, 7))
        self.assertEqual(result['end_date'][1], None)
        self.assertEqual(result.schema['start_date'], pl.Date)
        metadata = [column for column in source.columns if column.startswith('_')]
        assert_frame_equal(result.select(metadata), source.select(metadata))
        self.assertEqual(source['iso_numeric'][0], ' 8 ')

    def test_rate_rules_convert_dates_and_numeric_rates(self) -> None:
        result = transform_rates(self.rates())
        self.assertEqual(result['date'][0], date(2026, 10, 1))
        self.assertEqual(result['base'][0], 'BRL')
        self.assertEqual(result['quote'][0], 'USD')
        self.assertAlmostEqual(result['rate'][0], 0.19)
        self.assertEqual(result.schema['rate'], pl.Float64)

    def test_invalid_rates_raise_instead_of_dropping_rows(self) -> None:
        for value in ('0', '-1', 'NaN', 'inf', '-inf', None, ''):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'rate'):
                transform_rates(self.rates().with_columns(pl.lit(value).alias('rate')))
        with self.assertRaises(pl.exceptions.InvalidOperationError):
            transform_rates(self.rates().with_columns(pl.lit('bad').alias('rate')))

    def test_invalid_codes_numeric_identifiers_and_date_ranges_raise(self) -> None:
        for value in ('AB', 'ABCD', '12A', '', None):
            with self.subTest(code=value), self.assertRaisesRegex(ValueError, 'iso_code'):
                transform_currencies(
                    self.currencies().with_columns(pl.lit(value).alias('iso_code'))
                )
        for value in ('1234', 'A1', '-1', '1.5'):
            with self.subTest(numeric=value), self.assertRaisesRegex(ValueError, 'iso_numeric'):
                transform_currencies(
                    self.currencies().with_columns(pl.lit(value).alias('iso_numeric'))
                )
        with self.assertRaisesRegex(ValueError, 'start_date'):
            transform_currencies(
                self.currencies().with_columns(pl.lit('1900-01-01').alias('end_date'))
            )
        for column in ('base', 'quote', 'date'):
            with self.subTest(column=column), self.assertRaisesRegex(ValueError, column):
                transform_rates(self.rates().with_columns(pl.lit(None).alias(column)))
        with self.assertRaises(pl.exceptions.InvalidOperationError):
            transform_rates(self.rates().with_columns(pl.lit('2026-02-30').alias('date')))

    def test_missing_columns_raise_actionable_error(self) -> None:
        with self.assertRaisesRegex(ValueError, 'iso_code'):
            transform_currencies(self.currencies().drop('iso_code'))

    def test_empty_datasets_keep_typed_schema(self) -> None:
        currencies = transform_currencies(pl.DataFrame(schema=dict.fromkeys(CURRENCY_FIELDS, pl.String)))
        rates = transform_rates(pl.DataFrame(schema=dict.fromkeys(RATE_FIELDS, pl.String)))
        self.assertEqual(currencies.height, 0)
        self.assertEqual(currencies.schema['start_date'], pl.Date)
        self.assertEqual(rates.height, 0)
        self.assertEqual(rates.schema['rate'], pl.Float64)

    def test_run_silver_reads_actual_bronze_format_and_is_repeatable(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for dataset, frame in (('currencies', self.currencies()), ('rates', self.rates())):
                records = frame.select(CURRENCY_FIELDS if dataset == 'currencies' else RATE_FIELDS)
                path = root / 'raw' / dataset / 'batch_id=batch-1' / 'data.json'
                path.parent.mkdir(parents=True)
                path.write_text(json.dumps(records.to_dicts()), encoding='utf-8')
            bronze = run_bronze(
                batch_id='batch-1', raw_dir=root / 'raw',
                bronze_dir=root / 'bronze', persist_database=False,
            )
            original = Path(bronze['rates_path']).read_bytes()
            result = run_silver('batch-1', root / 'bronze', root / 'silver', persist_database=False)
            again = run_silver('batch-1', root / 'bronze', root / 'silver', persist_database=False)
            self.assertEqual(result, again)
            for dataset, expected_rows in (('currencies', 2), ('rates', 1)):
                destination = root / 'silver' / dataset / 'batch-1' / f'{dataset}.parquet'
                self.assertEqual(result[f'{dataset}_path'], str(destination.resolve()))
                self.assertEqual(result[f'{dataset}_rows'], expected_rows)
                result_frame = pl.read_parquet(destination)
                source_frame = pl.read_parquet(bronze[f'{dataset}_path'])
                metadata = [column for column in source_frame.columns if column.startswith('_')]
                assert_frame_equal(result_frame.select(metadata), source_frame.select(metadata))
            self.assertEqual(Path(bronze['rates_path']).read_bytes(), original)

    def test_latest_common_batch_requires_both_files(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for batch in ('20260930100000', '20261001100000'):
                self._write_batch(root / 'bronze', batch)
            incomplete = root / 'bronze' / 'rates' / '20261002100000'
            incomplete.mkdir(parents=True)
            persist_parquet(self.currencies(), root / 'bronze' / 'currencies' / incomplete.name / 'currencies.parquet')
            result = run_silver(bronze_dir=root / 'bronze', silver_dir=root / 'silver', persist_database=False)
            self.assertEqual(result['batch_id'], '20261001100000')

    def test_missing_inputs_and_unsafe_batch_do_not_write_outputs(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for batch in (None, 'missing'):
                with self.subTest(batch=batch), self.assertRaises(FileNotFoundError):
                    run_silver(batch, root / 'bronze', root / 'silver')
            for batch in ('../outside', '..', '', 'a/b', 'a\\b'):
                with self.subTest(batch=batch), self.assertRaises(ValueError):
                    run_silver(batch, root / 'bronze', root / 'silver')
            self.assertFalse((root / 'silver').exists())

    def test_invalid_second_dataset_does_not_replace_existing_outputs(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_batch(root / 'bronze', 'batch-1')
            result = run_silver('batch-1', root / 'bronze', root / 'silver', persist_database=False)
            before = {dataset: Path(result[f'{dataset}_path']).read_bytes() for dataset in ('currencies', 'rates')}
            persist_parquet(
                self.rates().with_columns(pl.lit('-1').alias('rate')),
                root / 'bronze' / 'rates' / 'batch-1' / 'rates.parquet',
            )
            with self.assertRaisesRegex(ValueError, 'rate'):
                run_silver('batch-1', root / 'bronze', root / 'silver')
            for dataset, content in before.items():
                self.assertEqual(Path(result[f'{dataset}_path']).read_bytes(), content)

    def test_failed_write_preserves_destination_and_cleans_temporary_file(self) -> None:
        with TemporaryDirectory() as directory:
            destination = Path(directory) / 'rates.parquet'
            persist_parquet(self.rates(), destination)
            original = destination.read_bytes()

            def failed_write(frame, path, **kwargs):
                Path(path).write_bytes(b'partial')
                raise OSError('disk failure')

            with (
                patch.object(pl.DataFrame, 'write_parquet', failed_write),
                self.assertRaisesRegex(OSError, 'disk failure'),
            ):
                persist_parquet(self.rates(), destination)
            self.assertEqual(destination.read_bytes(), original)
            self.assertEqual(list(destination.parent.iterdir()), [destination])

    def _write_batch(self, root: Path, batch: str) -> None:
        for dataset, frame in (('currencies', self.currencies()), ('rates', self.rates())):
            frame = frame.with_columns(pl.lit(batch).alias('_ingestion_batch_id'))
            persist_parquet(frame, root / dataset / batch / f'{dataset}.parquet')

    def test_database_load_executes_ddl_and_replaces_only_requested_batch(self) -> None:
        database = MagicMock()
        database.__enter__.return_value = database
        factory = Mock(return_value=database)
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_batch(root / 'bronze', 'batch-1')
            result = run_silver(
                'batch-1', root / 'bronze', root / 'silver', database_factory=factory,
            )
            self.assertTrue(Path(result['currencies_path']).is_file())
            self.assertTrue(Path(result['rates_path']).is_file())
        factory.assert_called_once_with()
        self.assertEqual(database.execute.call_count, 4)
        statements = [call.args[0] for call in database.execute.call_args_list]
        self.assertIn('CREATE SCHEMA IF NOT EXISTS silver', statements[0])
        self.assertIn('CREATE TABLE IF NOT EXISTS silver.currencies', statements[1])
        self.assertIn('CREATE TABLE IF NOT EXISTS silver.rates', statements[2])
        self.assertEqual(
            [(call.args, call.kwargs) for call in database.delete.call_args_list],
            [((dataset,), {'schema': 'silver', 'where': {'_ingestion_batch_id': 'batch-1'}})
             for dataset in ('currencies', 'rates')],
        )
        copies = database.copy_dataframe.call_args_list
        self.assertEqual(len(copies), 2)
        currencies, rates = copies[0].args[0], copies[1].args[0]
        self.assertIsInstance(currencies, pl.DataFrame)
        self.assertIsInstance(rates, pl.DataFrame)
        self.assertEqual(copies[0].kwargs['columns'], CURRENCY_COLUMNS)
        self.assertEqual(copies[1].kwargs['columns'], RATE_COLUMNS)
        self.assertEqual(copies[0].kwargs['schema'], 'silver')
        self.assertEqual(copies[1].kwargs['schema'], 'silver')
        self.assertEqual(currencies[0, 'iso_numeric'], '008')
        self.assertEqual(currencies[0, 'start_date'], date(1998, 7, 7))
        self.assertIsInstance(currencies[0, 'start_date'], date)
        self.assertEqual(rates[0, 'rate'], 0.19)
        self.assertIsNotNone(currencies[0, '_ingested_at_utc'].tzinfo)
        database.__exit__.assert_called_once_with(None, None, None)

    def test_skip_database_does_not_open_a_connection_or_require_sql(self) -> None:
        factory = Mock(side_effect=AssertionError('Unexpected connection'))
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_batch(root / 'bronze', 'batch-1')
            run_silver(
                'batch-1', root / 'bronze', root / 'silver',
                sql_dir=root / 'missing-sql', persist_database=False,
                database_factory=factory,
            )
        factory.assert_not_called()

    def test_database_load_rejects_mismatched_batch_before_opening_connection(self) -> None:
        factory = Mock(side_effect=AssertionError('Unexpected connection'))
        with self.assertRaisesRegex(ValueError, '_ingestion_batch_id'):
            _load_database(
                transform_currencies(self.currencies()),
                transform_rates(self.rates()),
                'another-batch', SILVER_SQL_DIR, factory,
            )
        factory.assert_not_called()

    def test_missing_sql_stops_before_deleting_or_copying_rows(self) -> None:
        database = MagicMock()
        database.__enter__.return_value = database
        with (
            TemporaryDirectory() as directory,
            self.assertRaises(FileNotFoundError),
        ):
            _load_database(
                transform_currencies(self.currencies()),
                transform_rates(self.rates()),
                'batch-1', Path(directory), lambda: database,
            )
        database.delete.assert_not_called()
        database.copy_dataframe.assert_not_called()

    def test_copy_failure_rolls_back_both_tables_and_keeps_parquets(self) -> None:
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.copy.side_effect = [MagicMock(), RuntimeError('rates COPY failed')]
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_batch(root / 'bronze', 'batch-1')
            with (
                patch('database.session.psycopg.connect', return_value=connection),
                self.assertRaisesRegex(RuntimeError, 'rates COPY failed'),
            ):
                run_silver(
                    'batch-1', root / 'bronze', root / 'silver',
                    database_factory=PostgresDatabase,
                )
            self.assertTrue((root / 'silver' / 'currencies' / 'batch-1' / 'currencies.parquet').is_file())
            self.assertTrue((root / 'silver' / 'rates' / 'batch-1' / 'rates.parquet').is_file())
        connection.rollback.assert_called_once_with()
        connection.commit.assert_not_called()
        connection.close.assert_called_once_with()

    def test_logs_dataset_stages_counts_and_persistence_progress(self) -> None:
        database = MagicMock()
        database.__enter__.return_value = database
        database.delete.return_value = 0
        database.copy_dataframe.side_effect = [2, 1]
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self._write_batch(root / 'bronze', 'batch-1')
            with self.assertLogs('scripts.silver', level='INFO') as captured:
                run_silver(
                    'batch-1', root / 'bronze', root / 'silver',
                    database_factory=lambda: database,
                )
        messages = [record.getMessage() for record in captured.records]
        for dataset, count in (('currencies', 2), ('rates', 1)):
            self.assertTrue(any(f'Dataset processing started: dataset={dataset}' in message for message in messages))
            summary = next(message for message in messages if f'Dataset processing completed: dataset={dataset}' in message)
            self.assertIn(f'rows_received={count} rows_processed={count} rows_rejected=0', summary)
            self.assertIn('elapsed_seconds=', summary)
        for step in ('Bronze read completed', 'stage=text_cleaning', 'stage=date_conversion',
                     'stage=rate_conversion', 'stage=business_validation', 'Parquet write completed',
                     'Silver DDL execution completed', 'Batch deletion completed',
                     'PostgreSQL COPY completed', 'PostgreSQL load completed'):
            self.assertTrue(any(step in message for message in messages), step)
        transaction_completion = next(i for i, message in enumerate(messages) if 'PostgreSQL transaction completing' in message)
        load_completion = next(i for i, message in enumerate(messages) if 'PostgreSQL load completed' in message)
        self.assertLess(transaction_completion, load_completion)

    def test_logs_rejection_reason_source_row_and_failed_summary(self) -> None:
        frame = pl.concat([self.rates(), self.rates()]).with_columns(
            pl.Series('rate', ['-1', '0.19']),
            pl.Series('_source_row_number', [42, 43]),
        )
        with (
            self.assertLogs('scripts.silver', level='INFO') as captured,
            self.assertRaisesRegex(ValueError, 'finite positive'),
        ):
            transform_rates(frame)
        output = '\n'.join(captured.output)
        self.assertIn('reason=rate must be a finite positive number rows_rejected=1', output)
        self.assertIn("'_source_row_number': '42'", output)
        self.assertIn("'rate': '-1.0'", output)
        self.assertIn('rows_received=2 rows_processed=0 rows_rejected=1 status=aborted', output)
        self.assertNotIn('Dataset processing completed', output)

    def test_logs_cast_failures_with_original_values(self) -> None:
        frame = self.rates().with_columns(pl.lit('not-a-number').alias('rate'))
        with (
            self.assertLogs('scripts.silver', level='INFO') as captured,
            self.assertRaises(pl.exceptions.InvalidOperationError),
        ):
            transform_rates(frame)
        output = '\n'.join(captured.output)
        self.assertIn('reason=rate cannot be cast to Float64 rows_rejected=1', output)
        self.assertIn('not-a-number', output)
        self.assertIn('stage=rate_conversion rows_received=1 rows_processed=0 rows_rejected=1', output)

    def test_date_rejection_summary_counts_distinct_rows_and_ignores_empty_dates(self) -> None:
        frame = self.currencies().with_columns(
            pl.Series('start_date', ['invalid', '']),
            pl.Series('end_date', ['also-invalid', None]),
        )
        with (
            self.assertLogs('scripts.silver', level='INFO') as captured,
            self.assertRaises(pl.exceptions.InvalidOperationError),
        ):
            transform_currencies(frame)
        output = '\n'.join(captured.output)
        self.assertIn('reason=start_date does not match date format %Y-%m-%d rows_rejected=1', output)
        self.assertIn('reason=end_date does not match date format %Y-%m-%d rows_rejected=1', output)
        self.assertIn('rows_received=2 rows_processed=0 rows_rejected=1 status=aborted', output)

    def test_rejection_samples_are_bounded_and_context_does_not_leak(self) -> None:
        frame = pl.concat([self.rates()] * 10).with_columns(
            pl.lit('0').alias('rate'),
            pl.Series('_source_row_number', range(1, 11)),
        )
        with (
            self.assertLogs('scripts.silver', level='ERROR') as captured,
            self.assertRaises(ValueError),
        ):
            transform_rates(frame)
        rejection = next(record.getMessage() for record in captured.records if 'Rows rejected:' in record.getMessage())
        self.assertIn('rows_rejected=10 sample_limit=5', rejection)
        self.assertIn("'_source_row_number': '5'", rejection)
        self.assertNotIn("'_source_row_number': '6'", rejection)
        with self.assertLogs('scripts.silver', level='INFO') as next_run:
            transform_currencies(self.currencies())
        self.assertIn('dataset=currencies', next_run.records[0].getMessage())
        self.assertIn('rows_rejected=0', next_run.records[-1].getMessage())

    def test_conversion_diagnostic_failure_preserves_original_exception(self) -> None:
        original = pl.exceptions.InvalidOperationError('original cast error')
        with (
            patch.object(pl.DataFrame, 'with_columns', side_effect=original),
            patch('scripts.silver._log_rejected_rows', side_effect=pl.exceptions.ComputeError('diagnostic error')),
            self.assertLogs('scripts.silver', level='WARNING') as captured,
            self.assertRaises(pl.exceptions.InvalidOperationError) as caught,
        ):
            cast_columns(self.rates(), {'rate': pl.Float64})
        self.assertIs(caught.exception, original)
        self.assertIn('Could not identify rows affected', captured.output[0])

    def test_database_failure_logs_stage_without_success_summary(self) -> None:
        database = MagicMock()
        database.__enter__.return_value = database
        database.copy_dataframe.side_effect = [2, RuntimeError('rates COPY failed')]
        with (
            self.assertLogs('scripts.silver', level='INFO') as captured,
            self.assertRaisesRegex(RuntimeError, 'rates COPY failed'),
        ):
            _load_database(
                transform_currencies(self.currencies()), transform_rates(self.rates()),
                'batch-1', SILVER_SQL_DIR, lambda: database,
            )
        output = '\n'.join(captured.output)
        self.assertIn('PostgreSQL load failed: batch_id=batch-1 stage=rates_copy', output)
        self.assertNotIn('PostgreSQL load completed', output)


if __name__ == '__main__':
    unittest.main()
