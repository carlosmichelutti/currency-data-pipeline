import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.types import DagRunProtocol

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

TIMEZONE = pendulum.timezone('America/Sao_Paulo')


@dag(
    dag_id='currency_exchange_rates_dag',
    schedule='@daily',
    start_date=pendulum.datetime(2026, 10, 15, tz='America/Sao_Paulo'),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    },
    tags=['currency', 'exchange', 'rates', 'dag'],
    description='Extract Frankfurter data and load bronze, silver and gold layers.'
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
            str: Logical date, or the fixed run_after timestamp for manual runs,
                converted to America/Sao_Paulo and formatted as YYYYMMDDHHMMSS.
        """
        batch_date = dag_run.logical_date or dag_run.run_after
        return batch_date.astimezone(TIMEZONE).strftime('%Y%m%d%H%M%S')

    @task(task_id='run_extract', multiple_outputs=True)
    def extract(batch_id: str) -> dict[str, str]:
        """
        Extract currencies and rates from Frankfurter and persist raw JSON files.

        Args:
            batch_id (str): Ingestion batch identifier formatted as YYYYMMDDHHMMSS,
                supplied by the generate_batch_id task.

        Returns:
            dict[str, str]: Batch identifier and absolute paths of the currencies
                and rates JSON files, shared with downstream tasks through XCom.
        """
        from scripts.extract import run_extract

        return run_extract(batch_id=batch_id)

    @task(task_id='run_bronze', multiple_outputs=True)
    def bronze(batch_id: str) -> dict[str, str | int]:
        """
        Ingest the raw batch into bronze Parquet files and PostgreSQL tables.

        Args:
            batch_id (str): Ingestion batch identifier returned by the extract
                task, used to locate the currencies and rates raw JSON files.

        Returns:
            dict[str, str | int]: Batch identifier, absolute bronze Parquet paths
                and row counts for currencies and rates, shared through XCom.
        """
        from scripts.bronze import run_bronze

        return run_bronze(batch_id=batch_id)

    @task(task_id='run_silver', multiple_outputs=True)
    def silver(batch_id: str) -> dict[str, str | int]:
        """
        Transform and validate the bronze batch, then persist the silver layer.

        Args:
            batch_id (str): Ingestion batch identifier returned by the bronze
                task, used to locate the currencies and rates bronze Parquets.

        Returns:
            dict[str, str | int]: Batch identifier, absolute silver Parquet paths
                and row counts for currencies and rates after transformation
                and loading into PostgreSQL, shared through XCom.
        """
        from scripts.silver import run_silver

        return run_silver(batch_id=batch_id)

    @task(task_id='run_gold', multiple_outputs=True)
    def gold(batch_id: str) -> dict[str, str | int]:
        """
        Build gold dimensions and facts from the persisted silver batch.

        Args:
            batch_id (str): Ingestion batch identifier returned by the silver
                task, used to locate the currencies and rates silver Parquets.

        Returns:
            dict[str, str | int]: Batch identifier, absolute gold Parquet paths
                and row counts for dim_currency, dim_date and fact_exchange_rate
                after loading into PostgreSQL, available through XCom.
        """
        from scripts.gold import run_gold

        return run_gold(batch_id=batch_id)

    batch_id = generate_batch_id()
    extract_result = extract(batch_id=batch_id)
    bronze_result = bronze(batch_id=extract_result['batch_id'])
    silver_result = silver(batch_id=bronze_result['batch_id'])
    gold_result = gold(batch_id=silver_result['batch_id'])


currency_exchange_rates_dag = currents_rates_dag()
