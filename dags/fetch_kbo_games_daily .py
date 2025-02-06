import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/collector"))

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from collector.scrapers import game_scraper

OUTPUT_DIR = Variable.get("output_dir", default_var="/opt/airflow/collector/output")
SCHEDULE_FILE = Variable.get("schedules_filename", default_var="game_schedules.parquet")
DETAIL_FILE = Variable.get("game_detail_filename", default_var="game_details.parquet")
HITTER_FILE = Variable.get("game_stat_hitter_filename", default_var="batting_stats_game.parquet")
PITCHER_FILE = Variable.get("game_stat_pitcher_filename", default_var="pitching_stats_game.parquet")

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
    file_path = os.path.join(OUTPUT_DIR, execution_date.strftime("%Y-%m-%d"), SCHEDULE_FILE)
    
    is_running = game_scraper.run(file_path)
    if not is_running:
        raise Exception("Scraper condition is False, failing the task!")

with DAG(
    dag_id="fetch_kbo_games_daily",
    description="Fetches and uploads daily KBO game details and stats to GCS",
    schedule_interval="@daily",
    start_date=datetime(1982, 4, 10),
    catchup=False,
    tags=["kbo", "game", "etl", "gcs", "daily"],
) as dag:

    run_scraper_task = PythonOperator(
        task_id="run_game_scraper",
        python_callable=run_scraper,
        dag=dag
    )

    upload_game_details_task = PythonOperator(
        task_id="upload_game_details_to_gcs",
        python_callable=upload_to_gcs,
        op_args=["games/daily", DETAIL_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    upload_hitter_stats_task = PythonOperator(
        task_id="upload_hitter_stats_to_gcs",
        python_callable=upload_to_gcs,
        op_args=["players/daily", HITTER_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    upload_pitcher_stats_task = PythonOperator(
        task_id="upload_pitcher_stats_to_gcs",
        python_callable=upload_to_gcs,
        op_args=["players/daily", PITCHER_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    run_scraper_task >> [upload_game_details_task, upload_hitter_stats_task, upload_pitcher_stats_task]
