from plugins.database import insert_game_data_to_db
from plugins.storage import upload_data_to_cloud_storage

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

ANALYTICS_DIR = "/opt/airflow/analytics"


class KBOOperatorFactory:
    def __init__(self, dag):
        self.insert_data_into_db = PythonOperator(
            task_id="insert_data_into_db",
            python_callable=insert_game_data_to_db,
            dag=dag
        )

        self.upload_to_cloud_storage = PythonOperator(
            task_id="upload_to_cloud_storage",
            python_callable=upload_data_to_cloud_storage,
            dag=dag
        )

        self.run_dbt_model = BashOperator(
            task_id="run_dbt_model",
            bash_command=f"cd {ANALYTICS_DIR} &&" \
                         f"dbt run --project-dir {ANALYTICS_DIR} --profiles-dir {ANALYTICS_DIR}",
            dag=dag
        )