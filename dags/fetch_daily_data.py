import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/collector"))

from airflow import DAG
from airflow.operators.python import PythonOperator

from collector.scrapers import game, player, schedule, team

PROJECT_NAME = "kbo-data-project"
BUCKET_NAME = "kbo-data"
BUCKET_DIR = "players/weekly"


def run_season_scraper(**kwargs):
    execution_date = kwargs['execution_date']
    season = execution_date.year

    player.run(target_season=season)
    schedule.run(target_season=season)


def run_date_scraper(**kwargs):
    execution_date = kwargs['execution_date']
    date = execution_date.strftime("%Y%m%d")

    game.run(target_date=date)


with DAG(
    dag_id="fetch_daily_data",
    description="Fetches and uploads daily KBO data to GCS",
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

    run_season_scraper_task >> run_date_scraper_task
