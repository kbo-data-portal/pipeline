import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/collector"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

from plugins.storage import upload_to_gcs

from collector.config import OUTPUT_DIR, FILENAMES
from collector.config import Scraper
from collector.scrapers import schedule_scraper

PROJECT_NAME = "kob-data-project"
BUCKET_NAME = "kbo-data"
BUCKET_DIR = "schedules/weekly"

SCHEDULE_FILE = f"{FILENAMES[Scraper.SCHEDULE]}.parquet"

def run_scraper(**kwargs):
    """
    Runs the KBO scraper and checks if the task completes successfully.
    """
    execution_date = kwargs['execution_date']
    start_date = execution_date + timedelta(days=1)
    end_date = execution_date + timedelta(days=7)

    is_running = schedule_scraper.run(start_date, end_date)
    if not is_running:
        raise AirflowSkipException("No games scheduled for the next 7 days.")

with DAG(
    dag_id="fetch_kbo_schedules_weekly",
    description="Fetches and uploads weekly KBO game schedules to GCS",
    schedule_interval="@weekly",
    start_date=datetime(1982, 4, 10),
    catchup=False,
    tags=["kbo", "schedule", "etl", "gcs", "weekly"],
) as dag:

    run_scraper_task = PythonOperator(
        task_id="run_schedule_scraper",
        python_callable=run_scraper,
        dag=dag
    )

    upload_schedules_task = PythonOperator(
        task_id="upload_schedules_to_gcs",
        python_callable=upload_to_gcs,
        op_args=[BUCKET_NAME, BUCKET_DIR, OUTPUT_DIR, SCHEDULE_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    run_scraper_task >> upload_schedules_task
