import glob
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import gcsfs


def _upload_to_cloud_storage(bucket_name, pathname):
    """Upload files to Google Cloud Storage if they do not already exist."""
    hook = GCSHook(gcp_conn_id="google_cloud_default")

    file_list = [file_path for file_path in glob.glob(pathname)]
    if not file_list:
        print(f"No parquet files found for {pathname}. Skipping...")
        return
        
    for filename in file_list:
        object_name = filename.split("output/processed/")[1]
        hook.upload(bucket_name=bucket_name, object_name=object_name, filename=filename)

    print(f"Uploaded file into {bucket_name} from {len(file_list)} parquet files.")


def upload_data_to_cloud_storage():
    """Upload various game and player data to Google Cloud Storage."""
    bucket_name = "kbo-data"
    _upload_to_cloud_storage(bucket_name, "output/processed/game/schedule/*/*.parquet")
    _upload_to_cloud_storage(bucket_name, "output/processed/game/result/*/*.parquet")

    for pt in ["hitter", "pitcher", "fielder", "runner"]:
        _upload_to_cloud_storage(bucket_name, f"output/processed/player/*/{pt}/season_summary.parquet")
        _upload_to_cloud_storage(bucket_name, f"output/processed/player/*/{pt}/*/daily.parquet")
        _upload_to_cloud_storage(bucket_name, f"output/processed/player/*/{pt}/*/situation.parquet")

