import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/collector"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

from plugins.storage import upload_to_gcs

from collector.config import OUTPUT_DIR, FILENAMES
from collector.config import Scraper, Player
from collector.scrapers import player_scraper

PROJECT_NAME = "kob-data-project"
BUCKET_NAME = "kbo-data"
BUCKET_DIR = "players/weekly"

HITTER_FILE = f"{FILENAMES[Scraper.PLAYER][Player.HITTER]}.parquet"
PITCHER_FILE = f"{FILENAMES[Scraper.PLAYER][Player.PITCHER]}.parquet"
#FIELDER_FILE = f"{FILENAMES[Scraper.PLAYER][Player.FIELDER]}.parquet"
#RUNNER_FILE = f"{FILENAMES[Scraper.PLAYER][Player.RUNNER]}.parquet"

def run_scraper(**kwargs):
    """
    Runs the KBO scraper and checks if the task completes successfully.
    """
    execution_date = kwargs['execution_date']
    season = execution_date.year - 1

    for player_type in Player:
        is_running = player_scraper.run(player_type.value, season)
        if not is_running:
            raise AirflowFailException("Scraper condition is False, failing the task!")

with DAG(
    dag_id="fetch_kbo_players_weekly",
    description="Fetches and uploads weekly KBO game players to GCS",
    schedule_interval="@weekly",
    start_date=datetime(1982, 4, 10),
    catchup=False,
    tags=["kbo", "player", "hitter", "pitcher", "etl", "gcs", "weekly"],
) as dag:

    run_scraper_task = PythonOperator(
        task_id="run_schedule_scraper",
        python_callable=run_scraper,
        dag=dag
    )

    upload_hitter_stats_task = PythonOperator(
        task_id="upload_hitter_stats_to_gcs",
        python_callable=upload_to_gcs,
        op_args=[BUCKET_NAME, BUCKET_DIR, OUTPUT_DIR, HITTER_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    upload_pitcher_stats_task = PythonOperator(
        task_id="upload_pitcher_stats_to_gcs",
        python_callable=upload_to_gcs,
        op_args=[BUCKET_NAME, BUCKET_DIR, OUTPUT_DIR, PITCHER_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    run_scraper_task >> [upload_hitter_stats_task, upload_pitcher_stats_task]
