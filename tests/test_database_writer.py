import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import polars as pl

from database import PostgresDatabase
from database.writer import copy_dataframe, insert_rows


class CopyDataFrameTests(unittest.TestCase):
    def test_polars_copy_uses_native_rows_in_selected_column_order(self) -> None:
        timestamp = datetime(2026, 10, 13, tzinfo=timezone.utc)
        frame = pl.DataFrame({
            'code': ['BRL', None],
            'name': ['São Paulo', ''],
            'row_number': [1, 2],
            'ingested_at': [timestamp, timestamp],
        })
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        writer = cursor.copy.return_value.__enter__.return_value

        count = copy_dataframe(
            connection, frame, 'rates', schema='bronze',
            columns=['row_number', 'code', 'name', 'ingested_at'],
        )

        self.assertEqual(count, 2)
        self.assertEqual(writer.write_row.call_args_list, [
            call((1, 'BRL', 'São Paulo', timestamp)),
            call((2, None, '', timestamp)),
        ])
        statement = cursor.copy.call_args.args[0].as_string()
        self.assertIn('COPY "bronze"."rates"', statement)
        self.assertIn('("row_number", "code", "name", "ingested_at")', statement)

    def test_non_polars_dataframe_is_rejected_before_cursor_is_opened(self) -> None:
        connection = MagicMock()
        for value in (None, [], {}, object(), pl.Series('code', ['BRL']), pl.DataFrame().lazy()):
            with self.subTest(value_type=type(value)), self.assertRaisesRegex(TypeError, 'Polars DataFrame'):
                copy_dataframe(connection, value, 'currencies')
        connection.cursor.assert_not_called()

    def test_facade_rejects_invalid_dataframe_before_connecting(self) -> None:
        with patch('database.session.psycopg.connect') as connect:
            with PostgresDatabase() as database:
                with self.assertRaisesRegex(TypeError, 'Polars DataFrame'):
                    database.copy_dataframe([], 'currencies')
            connect.assert_not_called()

    def test_insert_and_upsert_mappings_preserve_native_python_values(self) -> None:
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        rows = [{
            'code': '008',
            'date': date(2026, 10, 13),
            'rate': Decimal('0.19'),
            'symbol': None,
        }]
        for conflicts in (None, ['code']):
            with self.subTest(conflict_columns=conflicts):
                count = insert_rows(
                    connection, 'currencies', rows, conflict_columns=conflicts,
                )
                self.assertEqual(count, 1)
                self.assertEqual(cursor.executemany.call_args.args[1], [
                    ('008', date(2026, 10, 13), Decimal('0.19'), None),
                ])

    def test_empty_polars_frame_and_missing_columns(self) -> None:
        connection = MagicMock()
        frame = pl.DataFrame(schema={'code': pl.String})
        self.assertEqual(copy_dataframe(connection, frame, 'currencies'), 0)
        cursor = connection.cursor.return_value.__enter__.return_value
        writer = cursor.copy.return_value.__enter__.return_value
        writer.write_row.assert_not_called()

        with self.assertRaisesRegex(ValueError, 'missing'):
            copy_dataframe(connection, frame, 'currencies', columns=['missing'])


if __name__ == '__main__':
    unittest.main()
