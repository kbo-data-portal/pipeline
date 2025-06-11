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

def _insert_to_db(file_list:list, table: str, schema: str = "public", season_id: int = None):
    """Insert data from files into the specified database table."""
    engine = _get_engine()
    with engine.connect() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

        df_list = [pd.read_parquet(file_path) for file_path in file_list]
        if not df_list:
            print(f"No parquet files found for {schema}.{table}. Skipping...")
            return
        
        if season_id:
            conn.execute(f'DELETE FROM {schema}.{table} WHERE "SEASON_ID" = {season_id};')
        else:
            conn.execute(f"DROP TABLE IF EXISTS {schema}.{table} CASCADE;")
        
        df = pd.concat(df_list, ignore_index=True)
        df.to_sql(table, engine, schema=schema, if_exists="append", index=False)

        print(f"Inserted data into {schema}.{table} from {len(df_list)} parquet files.")


def insert_prediction_data_to_db(**kwargs):
    """Insert game prediction data into the database."""
    engine = _get_engine()
    execution_date = kwargs['execution_date']
    game_date = execution_date.strftime("%Y-%m-%d")

    with engine.connect() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS game;")
        conn.execute(f'DELETE FROM game.prediction WHERE "G_DT" >= \'{game_date}\';')
        
        df = pd.read_parquet("output/prediction/match.parquet")
        df["G_DT"] = pd.to_datetime(df["G_DT"])

        filtered_df = df[df["G_DT"] >= game_date]
        filtered_df.to_sql("prediction", engine, schema="game", if_exists="append", index=False)

        print(f"Inserted data into game.prediction from matches since {game_date}")

def insert_game_data_to_db(**kwargs):
    """Insert various game and player data into the database."""
    execution_date = kwargs['execution_date']
    year = execution_date.year

    schedules = glob.glob(f"output/processed/game/schedule/{year}/*.parquet")
    _insert_to_db(schedules, "schedule", "game", year)
    
    results = glob.glob(f"output/processed/game/result/{year}/*.parquet")
    _insert_to_db(results, "result", "game", year)

    for pt in ["hitter", "pitcher", "fielder", "runner"]:
        summaries = glob.glob(f"output/processed/player/{year}/{pt}/season_summary.parquet")
        _insert_to_db(summaries, f"{pt}_season_summary", "player", year)

        dailies = glob.glob(f"output/processed/player/{year}/{pt}/*/daily.parquet")
        _insert_to_db(dailies, f"{pt}_daily_stats", "player", year)

        situations = glob.glob(f"output/processed/player/{year}/{pt}/*/situation.parquet")
        _insert_to_db(situations, f"{pt}_situation_stats", "player", year)


def select_data(table: str, schema: str = "public"):
    """Load data from the specified database table."""
    return pd.read_sql(f"SELECT * FROM {schema}.{table};", _get_engine())
        
