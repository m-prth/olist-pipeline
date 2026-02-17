from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
import os

BUCKET_NAME = "olist-lake"

LANDING_ZONE = "/opt/airflow/data/input"

def upload_to_minio():
    client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="password",
        secure=False
        )
    
    if not os.path.exists(LANDING_ZONE):
        print("Landing zone is empty")
        return

    for date_folder in os.listdir(LANDING_ZONE):
        folder_path = os.path.join(LANDING_ZONE, date_folder)
        if os.path.isdir(folder_path):
            for file_name in os.listdir(folder_path):
                # We define the destination path in MinIO
                # e.g., bronze/orders/date=2017-01-01/orders.parquet
                table_name = file_name.replace(".parquet", "")
                minio_path = f"bronze/{table_name}/date={date_folder}/{file_name}"
                local_file = os.path.join(folder_path, file_name)
                
                client.fput_object(BUCKET_NAME, minio_path, local_file)
                print(f"Uploaded {file_name} to {minio_path}")


with DAG(
    '01_injest_bronze',
    start_date = datetime(2026, 1, 1),
    schedule_interval= None,
    catchup=False
) as dag:
    ingest_task = PythonOperator(
        task_id='ingest_files_to_minio',
        python_callable=upload_to_minio
    )