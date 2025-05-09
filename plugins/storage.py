from airflow.providers.google.cloud.hooks.gcs import GCSHook

def _upload_to_cloud_storage(bucket_name, file_path, gcs_path):
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    hook.upload(bucket_name=bucket_name, object_name=gcs_path, filename=file_path)


def upload_data_to_cloud_storage():
    pass