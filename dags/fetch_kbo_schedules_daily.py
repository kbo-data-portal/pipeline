import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/collector"))

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from collector.scrapers import schedule_scraper

OUTPUT_DIR = Variable.get("output_dir", default_var="/opt/airflow/collector/output")
SCHEDULE_FILE = Variable.get("schedules_filename", default_var="game_schedules.parquet")

def upload_to_gcs(bucket_dir, filename, **kwargs):
    """
    Uploads the KBO data to Google Cloud Storage (GCS).
    """
    bucket_name = "kbo-data"
    execution_date = kwargs['execution_date']

    hook = GCSHook(gcp_conn_id="google_cloud_default")

    file_path = os.path.join(OUTPUT_DIR, execution_date.strftime("%Y-%m-%d"), filename)
    gcs_path = os.path.join(bucket_dir, execution_date.strftime("%Y/%m/%d"), filename)

    hook.upload(bucket_name=bucket_name, object_name=gcs_path, filename=file_path)

def run_scraper(**kwargs):
    """
    Runs the KBO scraper and checks if the task completes successfully.
    """
    execution_date = kwargs['execution_date']
    execution_date = datetime(2009, 4, 10) #Test
    is_running = schedule_scraper.run(execution_date, execution_date)

    if not is_running:
        raise Exception("Scraper condition is False, failing the task!")

with DAG(
    dag_id="fetch_kbo_schedules_daily",
    description="Fetches and uploads daily KBO game schedules to GCS",
    schedule_interval="@weekly",
    start_date=datetime(1982, 4, 10),
    catchup=False,
    tags=["kbo", "schedule", "etl", "gcs", "daily"],
) as dag:

    run_scraper_task = PythonOperator(
        task_id="run_schedule_scraper",
        python_callable=run_scraper,
        dag=dag
    )

    upload_schedules_task = PythonOperator(
        task_id="upload_schedules_to_gcs",
        python_callable=upload_to_gcs,
        op_args=["schedules/weekly", SCHEDULE_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    run_scraper_task >> upload_schedules_task
