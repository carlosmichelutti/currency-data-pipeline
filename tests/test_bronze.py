import hashlib
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Self
from unittest.mock import MagicMock

import polars as pl
from polars.testing import assert_frame_equal

from database.config import DatabaseSettings
from scripts.bronze import (
    BRONZE_SQL_DIR,
    METADATA_COLUMNS,
    RATE_FIELDS,
    _add_metadata,
    _as_raw_text,
    _load_database,
    _read_dataset,
    run_bronze,
)


class FakeDatabase:
    def __init__(self) -> None:
        self.operations: list[tuple[Any, ...]] = []

    def __enter__(self) -> Self:
        self.operations.append(('enter',))
        return self

    def __exit__(self, *args: object) -> None:
        self.operations.append(('exit',))

    def create_schema(self, schema: str) -> None:
        self.operations.append(('create_schema', schema))

    def execute(self, statement: object, parameters: object = None) -> int:
        self.operations.append(('execute', statement, parameters))
        return 0

    def delete(
        self,
        table_name: str,
        *,
        schema: str | None = None,
        where: dict[str, Any] | None = None,
    ) -> int:
        self.operations.append(('delete', schema, table_name, where))
        return 0

    def copy_dataframe(
        self,
        dataframe: pl.DataFrame,
        table_name: str,
        *,
        schema: str | None = None,
        columns: list[str] | None = None,
    ) -> int:
        assert isinstance(dataframe, pl.DataFrame)
        self.operations.append(
            ('copy', schema, table_name, len(dataframe), tuple(columns or ()))
        )
        return len(dataframe)


class BronzeTests(unittest.TestCase):
    def test_polars_preserves_raw_values_and_existing_hash_format(self) -> None:
        fields = ['numeric', 'missing', 'literal_null', 'unicode', 'nested']
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'data.json'
            path.write_text(
                '[{"numeric":0.1900e+00,"missing":null,"literal_null":"null",'
                '"unicode":"São Paulo","nested":{"enabled":true,"codes":["008",null]}}]',
                encoding='utf-8',
            )
            text = _as_raw_text(_read_dataset(path, fields), fields)
            enriched = _add_metadata(
                text, path, fields, 'batch-1',
                datetime(2026, 10, 13, tzinfo=timezone.utc),
            )

        expected_source = {
            'numeric': '0.1900e+00',
            'missing': None,
            'literal_null': 'null',
            'unicode': 'São Paulo',
            'nested': '{"enabled":true,"codes":["008",null]}',
        }
        self.assertEqual(text.row(0, named=True), expected_source)
        expected_hash = hashlib.sha256(json.dumps(
            expected_source, ensure_ascii=False, separators=(',', ':'), sort_keys=True,
        ).encode('utf-8')).hexdigest()
        self.assertEqual(enriched[0, '_record_hash'], expected_hash)
        self.assertEqual(enriched.schema['_ingested_at_utc'], pl.Datetime('us', 'UTC'))
        self.assertEqual(enriched.schema['_source_row_number'], pl.Int64)

    def test_read_convert_and_enrich_keep_separate_responsibilities(self) -> None:
        with TemporaryDirectory() as directory:
            raw = Path(directory)
            self._write_raw(raw, 'rates', 'batch-1', [{
                'date': '2026-10-13',
                'base': 'BRL',
                'quote': 'USD',
                'rate': None,
            }])
            source_path = raw / 'rates' / 'batch_id=batch-1' / 'data.json'
            source = _read_dataset(source_path, RATE_FIELDS)
            text = _as_raw_text(source, RATE_FIELDS)
            self.assertEqual(list(source.columns), RATE_FIELDS)
            self.assertEqual(list(text.columns), RATE_FIELDS)
            self.assertTrue(all(dtype == pl.String for dtype in text.dtypes))

            ingested_at = datetime(2026, 10, 13, tzinfo=timezone.utc)
            enriched = _add_metadata(
                text, source_path, RATE_FIELDS, 'batch-1', ingested_at,
            )
            another_batch = _add_metadata(
                text, raw / 'another.json', RATE_FIELDS, 'batch-2',
                datetime(2026, 10, 14, tzinfo=timezone.utc),
            )
            assert_frame_equal(enriched.select(RATE_FIELDS), text)
            self.assertEqual(list(text.columns), RATE_FIELDS)
            self.assertEqual(list(enriched.columns), RATE_FIELDS + METADATA_COLUMNS)
            self.assertEqual(enriched[0, '_source_file'], 'data.json')
            self.assertEqual(enriched[0, '_ingestion_batch_id'], 'batch-1')
            self.assertEqual(enriched[0, '_ingested_at_utc'], ingested_at)
            self.assertEqual(
                enriched[0, '_record_hash'],
                another_batch[0, '_record_hash'],
            )

    def test_empty_dataset_keeps_source_and_metadata_schema(self) -> None:
        source = pl.DataFrame(schema=dict.fromkeys(RATE_FIELDS, pl.Object))
        text = _as_raw_text(source, RATE_FIELDS)
        result = _add_metadata(
            text, Path('data.json'), RATE_FIELDS, 'empty',
            datetime(2026, 10, 13, tzinfo=timezone.utc),
        )
        self.assertTrue(result.is_empty())
        self.assertEqual(list(result.columns), RATE_FIELDS + METADATA_COLUMNS)
        self.assertEqual(result.schema['_record_hash'], pl.String)

    def test_run_bronze_writes_raw_text_and_replaces_database_batch(self) -> None:
        batch_id = '20260930104015'
        currencies = [
            {
                'iso_code': 'BRL',
                'iso_numeric': '986',
                'name': 'Brazilian Real',
                'symbol': 'R$',
                'start_date': '1994-07-01',
                'end_date': '2026-09-30',
            }
        ]
        rates = [
            {
                'date': '2026-09-30',
                'base': 'BRL',
                'quote': 'USD',
                'rate': 0.19,
            },
            {
                'date': '2026-09-30',
                'base': 'BRL',
                'quote': 'EUR',
            },
            {
                'date': '2026-09-30',
                'base': 'BRL',
                'quote': 'USD',
                'rate': 0.19,
            },
        ]

        with TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / 'raw'
            bronze = root / 'bronze'
            self._write_raw(raw, 'currencies', batch_id, currencies)
            self._write_raw(raw, 'rates', batch_id, rates)
            database = FakeDatabase()

            with self.assertLogs('scripts.bronze', level='INFO') as captured:
                result = run_bronze(
                    batch_id=batch_id,
                    raw_dir=raw,
                    bronze_dir=bronze,
                    database_factory=lambda: database,
                )

            messages = [record.getMessage() for record in captured.records]
            for dataset, rows in (('currencies', 1), ('rates', 3)):
                self.assertTrue(any(
                    f'Raw dataset read: dataset={dataset} batch_id={batch_id} rows={rows}' in message
                    for message in messages
                ))
                self.assertTrue(any(
                    f'Bronze Parquet persisted: dataset={dataset} rows={rows}' in message
                    for message in messages
                ))
                self.assertTrue(any(
                    f'PostgreSQL load committed: dataset={dataset} batch_id={batch_id} '
                    f'table=bronze.{dataset} rows={rows}' in message
                    for message in messages
                ))
            metadata_messages = [
                message for message in messages
                if message.startswith('Adding ingestion metadata and record hashes:')
            ]
            self.assertEqual(len(metadata_messages), 2)
            for sql_file in BRONZE_SQL_DIR.glob('*.sql'):
                self.assertTrue(any(
                    message.startswith('Bronze DDL executed:')
                    and sql_file.name in message
                    for message in messages
                ))

            currency_frame = pl.read_parquet(result['currencies_path'])
            rate_frame = pl.read_parquet(result['rates_path'])
            self.assertEqual(result['currencies_rows'], 1)
            self.assertEqual(result['rates_rows'], 3)
            self.assertEqual(currency_frame[0, '_source_row_number'], 1)
            self.assertEqual(currency_frame[0, '_source_file'], 'data.json')
            self.assertEqual(currency_frame[0, '_source_extension'], '.json')
            self.assertEqual(currency_frame[0, '_ingestion_batch_id'], batch_id)
            self.assertTrue(currency_frame[0, '_source_path'].endswith('data.json'))
            self.assertEqual(rate_frame[0, 'date'], '2026-09-30')
            self.assertEqual(rate_frame[0, 'rate'], '0.19')
            self.assertIsNone(rate_frame[1, 'rate'])
            self.assertEqual(len(rate_frame[0, '_record_hash']), 64)
            self.assertEqual(
                rate_frame[0, '_record_hash'],
                rate_frame[2, '_record_hash'],
            )
            self.assertNotEqual(
                rate_frame[0, '_record_hash'],
                rate_frame[1, '_record_hash'],
            )
            for column in ('date', 'base', 'quote', 'rate'):
                self.assertEqual(rate_frame.schema[column], pl.String)
            self.assertIsNotNone(currency_frame[0, '_ingested_at_utc'].tzinfo)
            self.assertIn(('delete', 'bronze', 'currencies', {'_ingestion_batch_id': batch_id}), database.operations)
            self.assertIn(('delete', 'bronze', 'rates', {'_ingestion_batch_id': batch_id}), database.operations)
            self.assertTrue(
                any(operation[:4] == ('copy', 'bronze', 'currencies', 1) for operation in database.operations)
            )
            self.assertTrue(
                any(operation[:4] == ('copy', 'bronze', 'rates', 3) for operation in database.operations)
            )
            executed_sql = [
                operation for operation in database.operations
                if operation[0] == 'execute'
            ]
            self.assertEqual(len(executed_sql), 5)

    def test_commit_failure_does_not_log_successful_database_load(self) -> None:
        database = MagicMock()
        database.__enter__.return_value = database
        database.__exit__.side_effect = RuntimeError('commit failed')
        database.copy_dataframe.return_value = 0

        with (
            self.assertLogs('scripts.bronze', level='INFO') as captured,
            self.assertRaisesRegex(RuntimeError, 'commit failed'),
        ):
            _load_database(
                pl.DataFrame(), pl.DataFrame(), 'batch-1',
                BRONZE_SQL_DIR, lambda: database,
            )

        self.assertEqual(database.copy_dataframe.call_count, 2)
        self.assertFalse(any(
            'PostgreSQL load committed:' in record.getMessage()
            for record in captured.records
        ))

    def test_run_bronze_selects_latest_common_batch(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            raw = root / 'raw'
            bronze = root / 'bronze'
            for batch_id in ('20260929100000', '20260930100000'):
                self._write_raw(
                    raw,
                    'currencies',
                    batch_id,
                    [{
                        'iso_code': 'BRL',
                        'iso_numeric': '986',
                        'name': 'Brazilian Real',
                        'symbol': 'R$',
                        'start_date': '1994-07-01',
                        'end_date': '2026-09-30',
                    }],
                )
                self._write_raw(
                    raw,
                    'rates',
                    batch_id,
                    [{
                        'date': '2026-09-30',
                        'base': 'BRL',
                        'quote': 'USD',
                        'rate': 0.19,
                    }],
                )

            result = run_bronze(
                raw_dir=raw,
                bronze_dir=bronze,
                persist_database=False,
            )

            self.assertEqual(result['batch_id'], '20260930100000')

    def test_rejects_unsafe_batch_identifier(self) -> None:
        with self.assertRaisesRegex(ValueError, 'batch_id may contain'):
            run_bronze(batch_id='../outside', persist_database=False)

    @staticmethod
    def _write_raw(
        raw_root: Path,
        dataset: str,
        batch_id: str,
        records: list[dict[str, Any]],
    ) -> None:
        destination = raw_root / dataset / f'batch_id={batch_id}' / 'data.json'
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(records), encoding='utf-8')


class DatabaseSettingsTests(unittest.TestCase):
    def test_database_url_encodes_credentials_but_connection_kwargs_are_raw(self) -> None:
        database_settings = DatabaseSettings(
            host='localhost',
            port=5433,
            name='currency_pipeline',
            user='user@example.com',
            password='p@ss word',
        )

        self.assertIn('user%40example.com:p%40ss+word', database_settings.database_url)
        self.assertEqual(database_settings.connection_kwargs['user'], 'user@example.com')
        self.assertEqual(database_settings.connection_kwargs['password'], 'p@ss word')


if __name__ == '__main__':
    unittest.main()
