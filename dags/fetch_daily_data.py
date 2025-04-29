import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/collector"))

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from collector.scrapers import game, player, schedule, team
from plugins.database import upload_to_database

PROJECT_NAME = "kbo-data-project"
BUCKET_NAME = "kbo-data"
BUCKET_DIR = "players/weekly"
ANALYTICS_DIR = '/opt/airflow/analytics'


def load_data():
    """
    Load data from the specified bucket and directory.
    """
    upload_to_database("collector/output/game/*/*/summary.parquet", "game_summary")
    upload_to_database("collector/output/game/*/*/hitter.parquet", "game_hitter_summary")
    upload_to_database("collector/output/game/*/*/pitcher.parquet", "game_pitcher_summary")

    upload_to_database("collector/output/player/*/hitter.parquet", "player_hitter_stats")
    upload_to_database("collector/output/player/*/pitcher.parquet", "player_pitcher_stats")
    upload_to_database("collector/output/player/*/runner.parquet", "player_runner_stats")
    upload_to_database("collector/output/player/*/fielder.parquet", "player_fielder_stats")

    upload_to_database("collector/output/schedule/*.parquet", "game_schedule")


def run_season_scraper(**kwargs):
    """
    Run the season scraper to fetch data for the current season.
    """
    execution_date = kwargs['execution_date']
    season = execution_date.year

    player.run(target_season=season)
    schedule.run(target_season=season)


def run_date_scraper(**kwargs):
    """
    Run the date scraper to fetch data for the specified date.
    """
    execution_date = kwargs['execution_date']
    date = execution_date.strftime("%Y%m%d")

    game.run(target_date=date)


with DAG(
    dag_id="fetch_daily_data",
    description="Fetches and uploads daily KBO data",
    schedule_interval="@daily",
    start_date=datetime(1982, 4, 10),
    catchup=False,
    tags=["kbo", "etl", "gcs", "daily"],
) as dag:

    run_season_scraper_task = PythonOperator(
        task_id="run_season_scraper",
        python_callable=run_season_scraper,
        dag=dag
    )

    run_date_scraper_task = PythonOperator(
        task_id="run_date_scraper",
        python_callable=run_date_scraper,
        dag=dag
    )

    load_data_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        dag=dag
    )

    analytics_data_task = BashOperator(
        task_id="analytics_data",
        bash_command=f"cd {ANALYTICS_DIR} &&" \
                     f"dbt run --project-dir {ANALYTICS_DIR} --profiles-dir {ANALYTICS_DIR}"
    )

    [run_season_scraper_task, run_date_scraper_task] >> load_data_task >> analytics_data_task