"""Opt-in SQL integration tests against a disposable PostgreSQL database.

Set GOLD_TEST_DSN explicitly. Each test rolls back its DDL/data and skips a
connection that already has a gold schema, protecting an existing pipeline.
"""

import logging
import os
import unittest
from datetime import date

import polars as pl
import psycopg
from test_gold import gold_frames, rate_source

from database import PostgresDatabase
from scripts.gold import load_database


@unittest.skipUnless(os.environ.get('GOLD_TEST_DSN'), 'Set GOLD_TEST_DSN to a disposable PostgreSQL database')
class GoldPostgresTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = psycopg.connect(os.environ['GOLD_TEST_DSN'])
        self.addCleanup(self.connection.close)
        self.addCleanup(self.connection.rollback)
        if self.connection.execute("SELECT to_regnamespace('gold')").fetchone()[0] is not None:
            self.skipTest('Integration tests require a database without an existing gold schema')
        handler = logging.NullHandler()
        logger = logging.getLogger('scripts.gold')
        logger.addHandler(handler)
        self.addCleanup(logger.removeHandler, handler)

    def load(self, frames=None, batch='batch-1') -> None:
        load_database(
            gold_frames(batch) if frames is None else frames, batch,
            database_factory=lambda: PostgresDatabase(connection=self.connection),
        )

    def test_ddl_is_repeatable_and_sql_relationships_preserve_the_business_grain(self) -> None:
        self.load()
        self.load()
        rows = self.connection.execute('''
            SELECT d.date, b.iso_code, q.iso_code, f.rate
            FROM gold.fact_exchange_rate f
            JOIN gold.dim_date d ON d.date_key = f.date_key
            JOIN gold.dim_currency b ON b.currency_key = f.base_currency_key
            JOIN gold.dim_currency q ON q.currency_key = f.quote_currency_key
            ORDER BY d.date, b.iso_code, q.iso_code
        ''').fetchall()
        self.assertEqual(rows, [
            (date(2026, 10, 1), 'BRL', 'USD', 0.19),
            (date(2026, 10, 3), 'BRL', 'EUR', 0.17),
            (date(2026, 10, 3), 'USD', 'EUR', 0.89),
        ])
        self.assertEqual(self.connection.execute('SELECT count(*) FROM gold.dim_currency').fetchone()[0], 3)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM gold.dim_date').fetchone()[0], 3)

    def test_overlapping_batches_update_one_fact_and_preserve_other_grains(self) -> None:
        self.load()
        corrected = rate_source('batch-2').head(1).with_columns(pl.lit(0.2).alias('rate'))
        self.load(gold_frames('batch-2', corrected), 'batch-2')
        self.assertEqual(self.connection.execute('SELECT count(*) FROM gold.fact_exchange_rate').fetchone()[0], 3)
        row = self.connection.execute('SELECT rate, _ingestion_batch_id FROM gold.fact_exchange_rate WHERE date_key = 20261001').fetchone()
        self.assertEqual(row, (0.2, 'batch-2'))
        frames = gold_frames('batch-2', corrected)
        frames['dim_currency'] = frames['dim_currency'].with_columns(pl.lit('UPDATED NAME').alias('name'))
        self.load(frames, 'batch-2')
        self.assertEqual(self.connection.execute('SELECT count(*) FROM gold.dim_currency').fetchone()[0], 3)
        self.assertEqual(self.connection.execute('SELECT DISTINCT name FROM gold.dim_currency').fetchall(), [('UPDATED NAME',)])

    def test_reprocessing_removes_only_facts_still_owned_by_that_batch(self) -> None:
        self.load()
        replacement = rate_source('batch-2').head(1).with_columns(pl.lit(0.2).alias('rate'))
        self.load(gold_frames('batch-2', replacement), 'batch-2')
        empty_batch = gold_frames('batch-1', rate_source().head(0))
        self.load(empty_batch)
        self.assertEqual(self.connection.execute('SELECT date_key, rate, _ingestion_batch_id FROM gold.fact_exchange_rate').fetchall(), [(20261001, 0.2, 'batch-2')])
        self.assertEqual(self.connection.execute('SELECT count(*) FROM gold.dim_date').fetchone()[0], 3)

    def test_foreign_keys_reject_each_orphan_role(self) -> None:
        self.load()
        for column in ('date_key', 'base_currency_key', 'quote_currency_key'):
            with (
                self.subTest(column=column),
                self.assertRaises(psycopg.errors.ForeignKeyViolation),
                self.connection.transaction(),
            ):
                self.connection.execute(f'UPDATE gold.fact_exchange_rate SET {column} = 999999 WHERE date_key = 20261001')
        self.assertEqual(self.connection.execute('SELECT count(*) FROM gold.fact_exchange_rate').fetchone()[0], 3)

    def test_constraints_reject_invalid_rates_and_duplicate_grains(self) -> None:
        self.load()
        for invalid in (0.0, -1.0, float('inf'), float('nan')):
            with (
                self.subTest(rate=invalid),
                self.assertRaises(psycopg.errors.CheckViolation),
                self.connection.transaction(),
            ):
                self.connection.execute('UPDATE gold.fact_exchange_rate SET rate = %s', (invalid,))
        with self.assertRaises(psycopg.errors.UniqueViolation), self.connection.transaction():
            self.connection.execute('INSERT INTO gold.fact_exchange_rate SELECT * FROM gold.fact_exchange_rate LIMIT 1')
        with self.assertRaises(psycopg.errors.CheckViolation), self.connection.transaction():
            self.connection.execute('UPDATE gold.dim_date SET iso_year = 1900')

    def test_fact_failure_rolls_back_dimension_updates_and_batch_deletion(self) -> None:
        self.load()
        original = self.connection.execute('SELECT iso_code, name FROM gold.dim_currency ORDER BY iso_code').fetchall()
        invalid = gold_frames()
        invalid['dim_currency'] = invalid['dim_currency'].with_columns(pl.lit('SHOULD ROLL BACK').alias('name'))
        invalid['fact_exchange_rate'] = invalid['fact_exchange_rate'].with_columns(pl.lit(0.0).alias('rate'))
        with self.assertRaises(psycopg.errors.CheckViolation), self.connection.transaction():
            self.load(invalid)
        self.assertEqual(self.connection.execute('SELECT iso_code, name FROM gold.dim_currency ORDER BY iso_code').fetchall(), original)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM gold.fact_exchange_rate').fetchone()[0], 3)


if __name__ == '__main__':
    unittest.main()
