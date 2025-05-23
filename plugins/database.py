import glob
import pandas as pd
from sqlalchemy import engine, create_engine
from airflow.models.connection import Connection

def _get_engine():
    """Create and return the SQLAlchemy engine."""
    connection = Connection.get_connection_from_secrets("postgres_default")
    url_object = engine.URL.create(
        drivername="postgresql+psycopg2",
        username=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port,
        database=connection.schema
    )
    
    return create_engine(url_object)

def _insert_to_db(file_list:list, table: str, schema: str = "public"):
    """Insert data from files into the specified database table."""
    engine = _get_engine()
    with engine.connect() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        df_list = [pd.read_parquet(file_path) for file_path in file_list]
        if not df_list:
            print(f"No parquet files found for {schema}.{table}. Skipping...")
            return
        
        conn.execute(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE;")
        
        df = pd.concat(df_list, ignore_index=True)
        df.to_sql(table, engine, schema=schema, if_exists="append", index=False)

        print(f"Inserted data into {schema}.{table} from {len(df_list)} parquet files.")


def insert_prediction_data_to_db():
    """Insert game prediction data into the database."""
    predictions = glob.glob("output/prediction/*.parquet")
    _insert_to_db(predictions, "prediction", "game")


def insert_game_data_to_db():
    """Insert various game and player data into the database."""
    schedules = glob.glob("output/processed/game/schedule/*/*.parquet")
    _insert_to_db(schedules, "schedule", "game")
    
    results = glob.glob("output/processed/game/result/*/*.parquet")
    _insert_to_db(results, "result", "game")

    for pt in ["hitter", "pitcher", "fielder", "runner"]:
        summaries = glob.glob(f"output/processed/player/*/{pt}/season_summary.parquet")
        _insert_to_db(summaries, f"{pt}_season_summary", "player")

        dailies = glob.glob(f"output/processed/player/*/{pt}/*/daily.parquet")
        _insert_to_db(dailies, f"{pt}_daily_stats", "player")

        situations = glob.glob(f"output/processed/player/*/{pt}/*/situation.parquet")
        _insert_to_db(situations, f"{pt}_situation_stats", "player")


def select_data(table: str, schema: str = "public"):
    """Load data from the specified database table."""
    return pd.read_sql(f"SELECT * FROM {schema}.{table};", _get_engine())
        
