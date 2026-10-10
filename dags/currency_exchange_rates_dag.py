import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.types import DagRunProtocol

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, ROOT_DIR.as_posix())

from scripts.bronze import run_bronze
from scripts.extract import run_extract
from scripts.gold import run_gold
from scripts.silver import run_silver

TIMEZONE = pendulum.timezone('America/Sao_Paulo')


@dag(
    dag_id='currency_exchange_rates_dag',
    schedule='@daily',
    start_date=pendulum.datetime(2026, 9, 28, tz='America/Sao_Paulo'),
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    },
    tags=['currency', 'exchange', 'rates', 'dag']
)
def currents_rates_dag() -> None:
    """
    Process currencies and rates through raw, bronze, silver and gold layers.

    Returns:
        None: The function declares the Airflow task dependency graph.
    """

    @task(task_id='generate_batch_id')
    def generate_batch_id(dag_run: DagRunProtocol) -> str:
        """
        Generate a stable batch identifier for the current Airflow DAG run.

        Args:
            dag_run (DagRunProtocol): Current DAG run supplied by the Airflow
                execution context.

        Returns:
            str: Logical run timestamp converted to America/Sao_Paulo and
                formatted as YYYYMMDDHHMMSS.
        """
        logical_date = dag_run.logical_date or pendulum.now(TIMEZONE)
        return logical_date.astimezone(TIMEZONE).strftime('%Y%m%d%H%M%S')

    batch_id = generate_batch_id()
    run_extract_task = task(task_id='run_extract')(run_extract)
    extract_result = run_extract_task(batch_id=batch_id)
    run_bronze_task = task(task_id='run_bronze')(run_bronze)
    bronze_result = run_bronze_task(batch_id=extract_result['batch_id'])
    run_silver_task = task(task_id='run_silver')(run_silver)
    silver_result = run_silver_task(batch_id=bronze_result['batch_id'])
    run_gold_task = task(task_id='run_gold')(run_gold)
    run_gold_task(batch_id=silver_result['batch_id'])


currents_rates_dag()
