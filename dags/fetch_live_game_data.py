import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/collector"))

from airflow import DAG
from airflow.operators.python import PythonOperator

from collector.scrapers import game
from plugins.factory import KBOOperatorFactory

SERIES = {
    0: "Regular_Season",
    1: "Preseason_Game",
    3: "Semi_Playoffs",
    4: "Wild_Card_Round",
    5: "Playoffs",
    7: "Korean_Series",
    8: "International_Competitions",
    9: "All_star_Game",
}
FORMAT = "parquet"


def run_scraping_and_saving(**kwargs):
    execution_date = kwargs["execution_date"]
    year = execution_date.year
    today_str = execution_date.strftime("%Y%m%d")

    series = list(SERIES.keys())
    game.GameResultScraper(FORMAT, series).run(year, today_str)


with DAG(
    dag_id="fetch_live_game_data",
    description="Fetches and processes live game data every 5 minutes.",
    schedule_interval="0/5 * * * *",
    start_date=datetime(1982, 4, 10),
    catchup=False,
    tags=["kbo", "baseball", "airflow", "python", "dbt", "elt", "real-time"],
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
