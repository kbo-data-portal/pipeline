import os
import pandas as pd

from airflow.providers.google.cloud.hooks.gcs import GCSHook


def upload_to_gcs(bucket_name, bucket_dir, output_dir, filename, **kwargs):
    """
    Uploads the KBO data to Google Cloud Storage (GCS).
    """
    execution_date = kwargs['execution_date']

    hook = GCSHook(gcp_conn_id="google_cloud_default")

    file_path = os.path.join(output_dir, filename)
    gcs_path = os.path.join(bucket_dir, execution_date.strftime("%Y/%m/%d"), filename)
    df = pd.read_parquet(file_path)

    print(df.head())

    print(f"Total rows: {df.shape[0]}")
    print(f"Total columns: {df.shape[1]}")

    print(f"Bucket Name: {bucket_name}")
    print(f"GCS Path: {gcs_path}")
    print(f"File Path: {file_path}")

    hook.upload(bucket_name=bucket_name, object_name=gcs_path, filename=file_path)