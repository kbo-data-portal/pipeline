import glob
import pandas as pd
from sqlalchemy import create_engine

def upload_to_database(pathname, table_name, schema_name="public"):
    """
    Load data from the specified bucket and directory into a PostgreSQL database.
    """
    engine = create_engine("postgresql://postgres:postgres@host.docker.internal:5432/postgres") 

    df_list = []
    for file_path in glob.glob(pathname):
        print(f"File Path: {file_path}")
        df_list.append(pd.read_parquet(file_path))

    df = pd.concat(df_list, ignore_index=True)
    df.to_sql(table_name, engine, schema=schema_name, if_exists="replace", index=False)