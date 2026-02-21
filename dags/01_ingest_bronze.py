from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
import os
import shutil

# Configuration
LANDING_ZONE = "/opt/airflow/data/input"
ARCHIVE_ZONE = "/opt/airflow/data/archive"  # New folder for processed files
BUCKET_NAME = "olist-lake"

def ingest_and_archive():
    client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="password",
        secure=False
    )
    
    if not os.path.exists(LANDING_ZONE):
        print("Landing zone empty.")
        return

    # Ensure archive directory exists
    os.makedirs(ARCHIVE_ZONE, exist_ok=True)

    count = 0
    for date_folder in os.listdir(LANDING_ZONE):
        folder_path = os.path.join(LANDING_ZONE, date_folder)
        
        if os.path.isdir(folder_path):
            # 1. Upload all files in this date folder
            for file_name in os.listdir(folder_path):
                table_name = file_name.replace(".parquet", "")
                minio_path = f"bronze/{table_name}/date={date_folder}/{file_name}"
                local_file = os.path.join(folder_path, file_name)
                
                client.fput_object(BUCKET_NAME, minio_path, local_file)
                print(f"Uploaded {file_name}")
            
            # 2. Move the whole date folder to Archive
            shutil.move(folder_path, os.path.join(ARCHIVE_ZONE, date_folder))
            print(f"Archived {date_folder}")
            count += 1
            
    print(f"Total folders processed: {count}")

with DAG(
    '01_ingest_bronze',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_and_archive',
        python_callable=ingest_and_archive
    )