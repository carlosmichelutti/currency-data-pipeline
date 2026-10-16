import sys
import unittest
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch


@unittest.skipIf(sys.platform == 'win32', 'Run Airflow DAG tests in Linux containers or WSL2')
class CurrencyPipelineDagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from airflow.dag_processing.dagbag import DagBag

        dag_path = Path(__file__).resolve().parents[1] / 'dags' / 'currency_exchange_rates_dag.py'
        dag_bag = DagBag(dag_folder=str(dag_path), safe_mode=False)
        if dag_bag.import_errors:
            raise AssertionError(f'DAG import errors: {dag_bag.import_errors}')
        cls.dag = dag_bag.dags['currency_exchange_rates_dag']

    def test_layers_run_in_order(self) -> None:
        expected_upstream = {
            'generate_batch_id': set(),
            'run_extract': {'generate_batch_id'},
            'run_bronze': {'run_extract'},
            'run_silver': {'run_bronze'},
            'run_gold': {'run_silver'},
        }
        self.assertEqual(set(self.dag.task_ids), set(expected_upstream))
        for task_id, upstream in expected_upstream.items():
            with self.subTest(task=task_id):
                self.assertEqual(self.dag.get_task(task_id).upstream_task_ids, upstream)

    def test_layers_receive_batch_from_previous_task(self) -> None:
        for task_id, previous in (
            ('run_extract', 'generate_batch_id'),
            ('run_bronze', 'run_extract'),
            ('run_silver', 'run_bronze'),
            ('run_gold', 'run_silver'),
        ):
            with self.subTest(task=task_id):
                batch_argument = self.dag.get_task(task_id).op_kwargs['batch_id']
                self.assertEqual(batch_argument.operator.task_id, previous)
                self.assertEqual(batch_argument.key, 'return_value' if task_id == 'run_extract' else 'batch_id')
        self.assertTrue(self.dag.get_task('run_extract').multiple_outputs)
        self.assertTrue(self.dag.get_task('run_bronze').multiple_outputs)
        self.assertTrue(self.dag.get_task('run_silver').multiple_outputs)

    def test_batch_uses_logical_date_in_sao_paulo(self) -> None:
        import pendulum

        dag_run = SimpleNamespace(logical_date=pendulum.datetime(2026, 10, 1, 1, 30, tz='UTC'))
        batch = self.dag.get_task('generate_batch_id').python_callable(dag_run=dag_run)
        self.assertEqual(batch, '20260930223000')

    def test_manual_batch_is_stable_when_generation_is_retried(self) -> None:
        import pendulum

        dag_run = SimpleNamespace(
            logical_date=None,
            run_after=pendulum.datetime(2026, 10, 16, 1, 30, tz='UTC'),
        )
        generate = self.dag.get_task('generate_batch_id').python_callable
        with patch('pendulum.now', side_effect=AssertionError('Batch must not use wall clock')):
            self.assertEqual(generate(dag_run=dag_run), '20261015223000')
            self.assertEqual(generate(dag_run=dag_run), '20261015223000')

    def test_execution_policy(self) -> None:
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(self.dag.timezone.name, 'America/Sao_Paulo')
        for task in self.dag.tasks:
            with self.subTest(task=task.task_id):
                self.assertEqual(task.retries, 3)
                self.assertEqual(task.retry_delay, timedelta(minutes=5))
                self.assertEqual(task.trigger_rule, 'all_success')

    def test_dag_loads_without_importing_pipeline_dependencies(self) -> None:
        import runpy

        dag_path = Path(__file__).resolve().parents[1] / 'dags' / 'currency_exchange_rates_dag.py'
        with patch.dict(sys.modules, {'scripts': None, 'database': None}):
            namespace = runpy.run_path(str(dag_path))
        self.assertEqual(namespace['currency_exchange_rates_dag'].dag_id, self.dag.dag_id)

    def test_tasks_process_one_batch_through_all_file_layers(self) -> None:
        import json

        import polars as pl

        from scripts import bronze, extract, gold, silver

        run_bronze = bronze.run_bronze
        run_silver = silver.run_silver
        run_gold = gold.run_gold
        currencies = [
            {'iso_code': 'BRL', 'name': 'Brazilian Real'},
            {'iso_code': 'USD', 'name': 'US Dollar'},
        ]
        rates = [{'date': '2026-10-15', 'base': 'BRL', 'quote': 'USD', 'rate': 0.19}]
        batch_id = '20261015223000'
        with TemporaryDirectory() as directory:
            root = Path(directory)
            with (
                patch.object(extract, 'RAW_DIR', root / 'raw'),
                patch.object(extract, 'extract_currencies', return_value=currencies),
                patch.object(extract, 'extract_rates', return_value=rates),
                patch.object(bronze, 'run_bronze', wraps=lambda **kwargs: run_bronze(
                    **kwargs, raw_dir=root / 'raw', bronze_dir=root / 'bronze', persist_database=False
                )),
                patch.object(silver, 'run_silver', wraps=lambda **kwargs: run_silver(
                    **kwargs, bronze_dir=root / 'bronze', silver_dir=root / 'silver', persist_database=False
                )),
                patch.object(gold, 'run_gold', wraps=lambda **kwargs: run_gold(
                    **kwargs, silver_dir=root / 'silver', gold_dir=root / 'gold', persist_database=False
                )),
            ):
                result = {'batch_id': batch_id}
                for task_id in ('run_extract', 'run_bronze', 'run_silver', 'run_gold'):
                    result = self.dag.get_task(task_id).python_callable(batch_id=result['batch_id'])
                    self.assertEqual(result['batch_id'], batch_id)
                    json.dumps(result)  # Only JSON-serializable metadata travels through XCom.

            fact = pl.read_parquet(
                root / 'gold' / 'fact_exchange_rate' / f'batch_id={batch_id}' / 'fact_exchange_rate.parquet'
            )
            self.assertEqual(fact.height, 1)
            self.assertEqual(fact['rate'].to_list(), [0.19])


if __name__ == '__main__':
    unittest.main()
