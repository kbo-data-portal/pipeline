from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

DBT_DIR = '/opt/airflow/analytics_dbt'

with DAG(
    dag_id="dbt_run",
    schedule_interval="@once",
    start_date=datetime(2025, 1, 27),
    catchup=False,
    tags=["dbt"],
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} &&" \
                     f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
    )

    dbt_run
