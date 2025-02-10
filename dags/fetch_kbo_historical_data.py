import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/collector"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException

from plugins.storage import upload_to_historical_gcs

from collector.config import OUTPUT_DIR, FILENAMES
from collector.config import Scraper, Game, Player
from collector.scrapers import game_scraper, schedule_scraper, player_scraper

PROJECT_NAME = "kob-data-project"
BUCKET_NAME = "kbo-data"

GAME_BUCKET_DIR = "games/historical"
SCHEDULE_BUCKET_DIR = "schedules/historical"
PLAYER_BUCKET_DIR = "players/historical"

GAME_DETAIL_FILE = f"{FILENAMES[Scraper.GAME][Game.DETAIL]}.parquet"
GAME_HITTER_FILE = f"{FILENAMES[Scraper.GAME][Game.STAT][Player.HITTER]}.parquet"
GAME_PITCHER_FILE = f"{FILENAMES[Scraper.GAME][Game.STAT][Player.PITCHER]}.parquet"
SCHEDULE_FILE = f"{FILENAMES[Scraper.SCHEDULE]}.parquet"

PLAYER_HITTER_FILE = f"{FILENAMES[Scraper.PLAYER][Player.HITTER]}.parquet"
PLAYER_PITCHER_FILE = f"{FILENAMES[Scraper.PLAYER][Player.PITCHER]}.parquet"
#PLAYER_FIELDER_FILE = f"{FILENAMES[Scraper.PLAYER][Player.FIELDER]}.parquet"
#PLAYER_RUNNER_FILE = f"{FILENAMES[Scraper.PLAYER][Player.RUNNER]}.parquet"

def run_player_scraper(**kwargs):
    """
    Scrapes player data for the KBO season based on the execution date.
    Ensures the scraper completes successfully before proceeding.
    """
    execution_date = kwargs['execution_date']
    target_season = execution_date.year

    for player_type in Player:
        is_running = player_scraper.run(player_type.value, target_season)
        if not is_running:
            raise AirflowFailException(f"Player scraper failed for season {target_season} and player type {player_type.value}. Failing the task!")


def run_schedule_scraper(**kwargs):
    """
    Scrapes the KBO game schedule for the specified season.
    Ensures the scraper completes successfully before proceeding.
    """
    execution_date = kwargs['execution_date']
    target_season = execution_date.year

    if target_season < 2001:
        raise AirflowSkipException(f"No games scheduled for the KBO season {target_season}.")

    start_date = datetime(target_season, 1, 1)
    end_date = datetime(target_season, 12, 31)

    is_running = schedule_scraper.run(start_date, end_date, target_season)
    if not is_running:
        raise AirflowFailException(f"Schedule scraper failed for season {target_season}. Failing the task!")


def run_game_scraper(**kwargs):
    """
    Scrapes KBO game data based on the execution date.
    Ensures the scraper completes successfully before proceeding.
    """
    execution_date = kwargs['execution_date']
    target_season = execution_date.year

    file_path = os.path.join(OUTPUT_DIR, f"{target_season}_{SCHEDULE_FILE}")
    
    is_running = game_scraper.run(file_path, target_season)
    if not is_running:
        raise AirflowFailException(f"Game scraper failed for season {target_season}. Failing the task!")
    
with DAG(
    dag_id="fetch_kbo_historical_data",
    description="Fetches and uploads historical KBO game data to GCS",
    schedule_interval="@yearly",
    start_date=datetime(1982, 4, 10),
    catchup=True,
    tags=["kbo", "historical", "etl", "gcs"],
) as dag:
    
    run_schedule_scraper_task = PythonOperator(
        task_id="run_schedule_scraper",
        python_callable=run_schedule_scraper,
        dag=dag
    )
    
    run_game_scraper_task = PythonOperator(
        task_id="run_game_scraper",
        python_callable=run_game_scraper,
        trigger_rule='all_success',
        dag=dag
    )

    run_player_scraper_task = PythonOperator(
        task_id="run_player_scraper",
        python_callable=run_player_scraper,
        dag=dag
    )

    upload_schedules_task = PythonOperator(
        task_id="upload_schedules_to_gcs",
        python_callable=upload_to_historical_gcs,
        op_args=[BUCKET_NAME, SCHEDULE_BUCKET_DIR, OUTPUT_DIR, SCHEDULE_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    upload_game_details_task = PythonOperator(
        task_id="upload_game_details_to_gcs",
        python_callable=upload_to_historical_gcs,
        op_args=[BUCKET_NAME, GAME_BUCKET_DIR, OUTPUT_DIR, GAME_DETAIL_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    upload_game_hitter_stats_task = PythonOperator(
        task_id="upload_game_hitter_stats_to_gcs",
        python_callable=upload_to_historical_gcs,
        op_args=[BUCKET_NAME, PLAYER_BUCKET_DIR, OUTPUT_DIR, GAME_HITTER_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    upload_game_pitcher_stats_task = PythonOperator(
        task_id="upload_game_pitcher_stats_to_gcs",
        python_callable=upload_to_historical_gcs,
        op_args=[BUCKET_NAME, PLAYER_BUCKET_DIR, OUTPUT_DIR, GAME_PITCHER_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    upload_player_hitter_stats_task = PythonOperator(
        task_id="upload_player_hitter_stats_to_gcs",
        python_callable=upload_to_historical_gcs,
        op_args=[BUCKET_NAME, PLAYER_BUCKET_DIR, OUTPUT_DIR, PLAYER_HITTER_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    upload_player_pitcher_stats_task = PythonOperator(
        task_id="upload_player_pitcher_stats_to_gcs",
        python_callable=upload_to_historical_gcs,
        op_args=[BUCKET_NAME, PLAYER_BUCKET_DIR, OUTPUT_DIR, PLAYER_PITCHER_FILE],
        trigger_rule='all_success',
        dag=dag
    )

    run_schedule_scraper_task >> upload_schedules_task >> run_game_scraper_task >> [upload_game_details_task, 
                                                                                    upload_game_hitter_stats_task, 
                                                                                    upload_game_pitcher_stats_task]
    run_player_scraper_task >> [upload_player_hitter_stats_task, 
                                upload_player_pitcher_stats_task]
