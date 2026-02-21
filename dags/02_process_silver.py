from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
import polars as pl
import io

def promote_all_to_silver():
    client = Minio("minio:9000", access_key="admin", secret_key="password", secure=False)
    
    # List all objects in the Bronze layer
    objects = client.list_objects("olist-lake", prefix="bronze/", recursive=True)
    
    # Set of processed directories to avoid redundant logs
    processed_paths = set()

    for obj in objects:
        if not obj.object_name.endswith(".parquet"):
            continue
            
        # 1. Download from Bronze
        response = client.get_object("olist-lake", obj.object_name)
        df = pl.read_parquet(io.BytesIO(response.read()))
        response.close()
        
        # 2. CLEANING: Deduplicate (Universal for all tables)
        clean_df = df.unique()
        
        # 3. Upload to Silver (preserving the table structure)
        silver_path = obj.object_name.replace("bronze/", "silver/")
        
        out_buffer = io.BytesIO()
        clean_df.write_parquet(out_buffer)
        out_buffer.seek(0)
        
        client.put_object(
            "olist-lake", silver_path, out_buffer, length=out_buffer.getbuffer().nbytes
        )
        
        table_name = silver_path.split('/')[1]
        if table_name not in processed_paths:
            print(f"✅ Now processing table: {table_name}")
            processed_paths.add(table_name)

with DAG(
    '02_process_silver',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    PythonOperator(task_id='promote_all_to_silver', python_callable=promote_all_to_silver)