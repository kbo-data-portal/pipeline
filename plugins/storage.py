import glob
from concurrent.futures import ThreadPoolExecutor
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def _upload_to_cloud_storage(bucket_name: str, pathname: str):
    """Upload files to Google Cloud Storage if they do not already exist."""
    hook = GCSHook(gcp_conn_id="google_cloud_default")

    file_list = [file_path for file_path in glob.glob(pathname)]
    if not file_list:
        print(f"No parquet files found for {pathname}. Skipping...")
        return

    def upload_file(filename):
        object_name = filename.split("output/processed/")[1]
        hook.upload(bucket_name=bucket_name, object_name=object_name, filename=filename)

    with ThreadPoolExecutor(max_workers=24) as executor:
        executor.map(upload_file, file_list)

    print(f"Uploaded file into {bucket_name} from {len(file_list)} parquet files.")


def upload_data_to_cloud_storage(**kwargs):
    """Upload various game and player data to Google Cloud Storage."""
    execution_date = kwargs["execution_date"]
    year = execution_date.year

    bucket_name = "kbo-data"
    _upload_to_cloud_storage(
        bucket_name, f"output/processed/game/schedule/{year}/*.parquet"
    )
    _upload_to_cloud_storage(
        bucket_name, f"output/processed/game/result/{year}/*.parquet"
    )

    for pt in ["hitter", "pitcher", "fielder", "runner"]:
        _upload_to_cloud_storage(
            bucket_name, f"output/processed/player/{year}/{pt}/season_summary.parquet"
        )
        _upload_to_cloud_storage(
            bucket_name, f"output/processed/player/{year}/{pt}/*/daily.parquet"
        )
        _upload_to_cloud_storage(
            bucket_name, f"output/processed/player/{year}/{pt}/*/situation.parquet"
        )


def get_list_from_cloud_storage(prefix: str, filename: str):
    """Get a list of files from Google Cloud Storage."""
    hook = GCSHook(gcp_conn_id="google_cloud_default")

    bucket_name = "kbo-data"
    object_names = hook.list(bucket_name=bucket_name, prefix=prefix)

    return [
        f"gs://{bucket_name}/{obj}" for obj in object_names if obj.endswith(filename)
    ]
