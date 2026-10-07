import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


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
        ):
            with self.subTest(task=task_id):
                batch_argument = self.dag.get_task(task_id).op_kwargs['batch_id']
                self.assertEqual(batch_argument.operator.task_id, previous)
                self.assertEqual(batch_argument.key, 'return_value' if task_id == 'run_extract' else 'batch_id')
        self.assertTrue(self.dag.get_task('run_extract').multiple_outputs)
        self.assertTrue(self.dag.get_task('run_bronze').multiple_outputs)

    def test_batch_uses_logical_date_in_sao_paulo(self) -> None:
        import pendulum

        dag_run = SimpleNamespace(logical_date=pendulum.datetime(2026, 10, 1, 1, 30, tz='UTC'))
        batch = self.dag.get_task('generate_batch_id').python_callable(dag_run=dag_run)
        self.assertEqual(batch, '20260930223000')


if __name__ == '__main__':
    unittest.main()
