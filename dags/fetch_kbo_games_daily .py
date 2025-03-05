import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/collector"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException, AirflowSkipException

from plugins.storage import upload_to_gcs

from collector.config import OUTPUT_DIR, FILENAMES
from collector.config import Scraper, Game, Player
from collector.scrapers import game_scraper, schedule_scraper

PROJECT_NAME = "kbo-data-project"
BUCKET_NAME = "kbo-data"
BUCKET_DIR = "games/daily"
BUCKET_PLAYER_DIR = "players/daily"

SCHEDULE_FILE = f"{FILENAMES[Scraper.GAME][Game.DETAIL]}.parquet"
DETAIL_FILE = f"{FILENAMES[Scraper.GAME][Game.DETAIL]}.parquet"
HITTER_FILE = f"{FILENAMES[Scraper.GAME][Game.STAT][Player.HITTER]}.parquet"
PITCHER_FILE = f"{FILENAMES[Scraper.GAME][Game.STAT][Player.PITCHER]}.parquet"

def run_scraper(**kwargs):
    """
    Runs the KBO scraper and checks if the task completes successfully.
    """
    execution_date = kwargs['execution_date']

    if schedule_scraper.run(execution_date, execution_date):
        file_path = os.path.join(OUTPUT_DIR, SCHEDULE_FILE)
        
        is_running = game_scraper.run(file_path)
        if not is_running:
            raise AirflowFailException("Scraper condition is False, failing the task!")
    else:
        raise AirflowSkipException("No games scheduled today.")

with DAG(
    dag_id="fetch_kbo_games_daily",
    description="Fetches and uploads daily KBO game details and stats to GCS",
    schedule_interval="@daily",
    start_date=datetime(1982, 4, 10),
    catchup=False,
    tags=["kbo", "game", "hitter", "pitcher", "etl", "gcs", "daily"],
) as dag:

    run_scraper_task = PythonOperator(
        task_id="run_game_scraper",
        python_callable=run_scraper,
        dag=dag
    )

    upload_game_details_task = PythonOperator(
        task_id="upload_game_details_to_gcs",
        python_callable=upload_to_gcs,
        op_args=[BUCKET_NAME, BUCKET_DIR, OUTPUT_DIR, DETAIL_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    upload_hitter_stats_task = PythonOperator(
        task_id="upload_hitter_stats_to_gcs",
        python_callable=upload_to_gcs,
        op_args=[BUCKET_NAME, BUCKET_PLAYER_DIR, OUTPUT_DIR, HITTER_FILE],
        trigger_rule='all_success',
        dag=dag
    )


    upload_pitcher_stats_task = PythonOperator(
        task_id="upload_pitcher_stats_to_gcs",
        python_callable=upload_to_gcs,
        op_args=[BUCKET_NAME, BUCKET_PLAYER_DIR, OUTPUT_DIR, PITCHER_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    run_scraper_task >> [upload_game_details_task, upload_hitter_stats_task, upload_pitcher_stats_task]
