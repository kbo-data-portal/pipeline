import sys
import os
import joblib
from datetime import datetime

sys.path.insert(0, os.path.abspath("/opt/airflow"))
sys.path.insert(0, os.path.abspath("/opt/airflow/collector"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

from learning.train import train_game_model
from learning.utils import load_game_data

from plugins.database import insert_prediction_data_to_db

RANDOM_STATE = 1982


def train_model_pipeline(**kwargs):
    _, X, y = load_game_data("ml_game_dataset")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=RANDOM_STATE)

    model_path = os.path.join("output", "model", "best_model.joblib")
    os.makedirs(os.path.dirname(model_path), exist_ok=True)

    if os.path.exists(model_path):
        model = joblib.load(model_path)
    else:
        model = LogisticRegression(max_iter=10000, solver="liblinear", random_state=RANDOM_STATE)

    train_model, metrics = train_game_model(model, X_train, X_test, y_train, y_test)

    metrics["name"] = 'best_model'
    joblib.dump(train_model, model_path)


def make_model_predictions(**kwargs):
    raw_df, X, _ = load_game_data("ml_game_predict")

    model_path = os.path.join("output", "model", "best_model.joblib")
    if os.path.exists(model_path):
        model = joblib.load(model_path)
    else:
        raise AirflowException("Model file not found")

    predictions = model.predict(X)
    proba = model.predict_proba(X)
    raw_df["HOME_WIN"] = predictions
    raw_df["HOME_WIN_PROB"] = proba[:, 1]
    raw_df["AWAY_WIN_PROB"] = proba[:, 0]
    
    data_path = os.path.join("output", "prediction", "match.parquet")
    os.makedirs(os.path.dirname(data_path), exist_ok=True)

    raw_df.to_parquet(data_path, engine="pyarrow", index=False)


with DAG(
    dag_id="daily_model_predictions",
    description="Trains the model and performs game predictions daily using the latest data.",
    schedule_interval="@daily",
    start_date=datetime(1982, 4, 10),
    catchup=False,
    tags=["kbo", "baseball", "airflow", "python", "model", "training", "prediction", "daily"]
) as dag:
    
    execute_model_training = PythonOperator(
        task_id="execute_model_training",
        python_callable=train_model_pipeline
    )
    
    execute_model_prediction = PythonOperator(
        task_id="execute_model_prediction",
        python_callable=make_model_predictions
    )

    insert_data_into_db = PythonOperator(
        task_id="insert_prediction_data_to_db",
        python_callable=insert_prediction_data_to_db,
        dag=dag
    )

    execute_model_training >> execute_model_prediction >> insert_data_into_db