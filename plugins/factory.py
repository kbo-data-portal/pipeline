from plugins.database import insert_game_data_to_db
from plugins.storage import upload_data_to_cloud_storage

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection

DBT_PATH = '/home/airflow/.local/bin'
ANALYTICS_DIR = "/opt/airflow/analytics"


class KBOOperatorFactory:
    def __init__(self, dag, dbt_schema="analytics"):
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
        
        connection = Connection.get_connection_from_secrets("postgres_default")
        self.run_dbt_model = BashOperator(
            task_id="run_dbt_model",
            env={
                "DB_NAME": connection.schema,
                "DB_HOST": connection.host,
                "DB_USER": connection.login,
                "DB_PASSWORD": connection.password,
                "DB_SCHEMA": dbt_schema,
            },
            bash_command=f"""
                cd {ANALYTICS_DIR} &&
                {DBT_PATH}/dbt run --project-dir {ANALYTICS_DIR} --profiles-dir {ANALYTICS_DIR}
            """,
            dag=dag
        )