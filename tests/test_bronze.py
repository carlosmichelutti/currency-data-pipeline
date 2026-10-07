import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Self

import pandas as pd

from database.config import DatabaseSettings
from scripts.bronze import run_bronze


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
        dataframe: pd.DataFrame,
        table_name: str,
        *,
        schema: str | None = None,
        columns: list[str] | None = None,
    ) -> int:
        self.operations.append(
            ('copy', schema, table_name, len(dataframe), tuple(columns or ()))
        )
        return len(dataframe)


class BronzeTests(unittest.TestCase):
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

            result = run_bronze(
                batch_id=batch_id,
                raw_dir=raw,
                bronze_dir=bronze,
                database_factory=lambda: database,
            )

            currency_frame = pd.read_parquet(result['currencies_path'])
            rate_frame = pd.read_parquet(result['rates_path'])
            self.assertEqual(result['currencies_rows'], 1)
            self.assertEqual(result['rates_rows'], 3)
            self.assertEqual(currency_frame.loc[0, '_source_row_number'], 1)
            self.assertEqual(currency_frame.loc[0, '_source_file'], 'data.json')
            self.assertEqual(currency_frame.loc[0, '_source_extension'], '.json')
            self.assertEqual(currency_frame.loc[0, '_ingestion_batch_id'], batch_id)
            self.assertTrue(currency_frame.loc[0, '_source_path'].endswith('data.json'))
            self.assertEqual(rate_frame.loc[0, 'date'], '2026-09-30')
            self.assertEqual(rate_frame.loc[0, 'rate'], '0.19')
            self.assertTrue(pd.isna(rate_frame.loc[1, 'rate']))
            self.assertEqual(len(rate_frame.loc[0, '_record_hash']), 64)
            self.assertEqual(
                rate_frame.loc[0, '_record_hash'],
                rate_frame.loc[2, '_record_hash'],
            )
            self.assertNotEqual(
                rate_frame.loc[0, '_record_hash'],
                rate_frame.loc[1, '_record_hash'],
            )
            for column in ('date', 'base', 'quote', 'rate'):
                self.assertTrue(isinstance(rate_frame.dtypes[column], pd.StringDtype))
            self.assertIsNotNone(currency_frame.loc[0, '_ingested_at_utc'].tzinfo)
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
