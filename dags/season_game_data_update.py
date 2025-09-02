import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/collector"))

from airflow import DAG
from airflow.operators.python import PythonOperator

from collector.scrapers import game, player
from plugins.factory import KBOOperatorFactory


SERIES = {
    0: "Regular Season",
    1: "Preseason Game",
    3: "Semi-Playoffs",
    4: "Wild Card Round",
    5: "Playoffs",
    7: "Korean Series",
    8: "International Competitions",
    9: "All-ytar Game",
}
FORMAT = "parquet"


def run_scraping_and_saving(**kwargs):
    execution_date = kwargs["execution_date"]
    year = execution_date.year

    series = list(SERIES.keys())
    game.GameResultScraper(FORMAT, series).run(year)

    for pt in ["hitter", "pitcher", "fielder", "runner"]:
        player.PlayerSeasonStatsScraper(FORMAT, series, pt).run(year)

    for pt in ["hitter", "pitcher"]:
        player.PlayerDetailStatsScraper(FORMAT, series, pt, "daily").run(year)
        player.PlayerDetailStatsScraper(FORMAT, series, pt, "situation").run(year)


with DAG(
    dag_id=f"season_game_data_update",
    description=f"Updates season data for the upcoming season, including schedules, and player data.",
    schedule_interval="@weekly",
    start_date=datetime(1982, 4, 10),
    catchup=False,
    tags=["kbo", "baseball", "airflow", "python", "dbt", "elt", "season-start"],
) as dag:

    factory = KBOOperatorFactory(dag=dag)

    fetch_and_save_data = PythonOperator(
        task_id="fetch_and_save_data", python_callable=run_scraping_and_saving
    )

    (
        fetch_and_save_data
        >> factory.upload_to_cloud_storage
        >> factory.insert_data_into_db
        >> factory.run_dbt_model
    )
