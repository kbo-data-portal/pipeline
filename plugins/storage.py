import os
import pandas as pd

from airflow.providers.google.cloud.hooks.gcs import GCSHook

def upload_to_gcs_helper(bucket_name, file_path, gcs_path):
    """
    Helper function to upload KBO data to Google Cloud Storage (GCS).
    This function handles the common logic for both historical and regular data uploads.
    """
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    df = pd.read_parquet(file_path)

    print("Column Types:")
    print(df.dtypes)
    
    print("Preview Rows:")
    print(df.head())

    print(f"GCS Path:  {bucket_name}/{gcs_path}")
    print(f"File Path: {file_path}")

    hook.upload(bucket_name=bucket_name, object_name=gcs_path, filename=file_path)

def upload_to_historical_gcs(bucket_name, bucket_dir, output_dir, filename, **kwargs):
    """
    Uploads the historical KBO data to Google Cloud Storage (GCS).
    """
    execution_date = kwargs['execution_date']
    targer_season = str(execution_date.year)

    file_path = os.path.join(output_dir, f"{targer_season}_{filename}")
    gcs_path = os.path.join(bucket_dir, targer_season, filename)

    upload_to_gcs_helper(bucket_name, file_path, gcs_path)

def upload_to_gcs(bucket_name, bucket_dir, output_dir, filename, **kwargs):
    """
    Uploads the KBO data to Google Cloud Storage (GCS).
    """
    execution_date = kwargs['execution_date']

    file_path = os.path.join(output_dir, filename)
    gcs_path = os.path.join(bucket_dir, execution_date.strftime("%Y/%m/%d"), filename)

    upload_to_gcs_helper(bucket_name, file_path, gcs_path)
