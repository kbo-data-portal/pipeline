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
    0: "Regular_Season",
    1: "Preseason_Game",
    3: "Semi_Playoffs",
    4: "Wild_Card_Round",
    5: "Playoffs",
    7: "Korean_Series",
    8: "International_Competitions",
    9: "All_star_Game"
}
FORMAT = "parquet"


def run_scraping_and_saving(series, **kwargs):
    execution_date = kwargs['execution_date']
    season = execution_date.year
    date = execution_date.strftime("%Y%m%d")

    game.GameResultScraper(FORMAT, SERIES).run(season, date)
    
    for pt in ["hitter", "pitcher", "fielder", "runner"]:
        player.PlayerSeasonStatsScraper(FORMAT, series, pt).run(season)
        
    for pt in ["hitter", "pitcher"]:
        player.PlayerDetailStatsScraper(FORMAT, series, pt, "daily").run(season)
        player.PlayerDetailStatsScraper(FORMAT, series, pt, "situation").run(season)


for id, series in SERIES.items():
    with DAG(
        dag_id=f"{id}_post_game_data_update_{series.lower()}",
        description=f"Updates game results and processes player statistics for {series} after a game ends.",
        schedule_interval="@daily",
        start_date=datetime(1982, 4, 10),
        catchup=False,
        tags=["kbo", "baseball", "airflow", "python", "dbt", "elt", "post-game", series.lower()]
    ) as dag:
    
        factory = KBOOperatorFactory(dag=dag)

        fetch_and_save_data = PythonOperator(
            task_id="fetch_and_save_data",
            python_callable=run_scraping_and_saving,
            op_args=[id]
        )

        fetch_and_save_data >> factory.upload_to_cloud_storage >> factory.insert_data_into_db >> factory.run_dbt_model
