from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator
from datetime import datetime

# Path to the dbt project inside the Airflow container
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

with DAG(
    '03_process_gold',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    doc_md="""
    ## Gold Layer Pipeline (dbt)

    Runs the dbt project to build all Gold layer models:
    - `dim_customers`, `dim_sellers` (dimensions)
    - `fact_orders`, `fact_order_lifecycle`, `fact_shipping_network` (facts)

    Models are materialized as Parquet files in MinIO (`s3://olist-lake/gold/`).
    """
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir . --target airflow',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir . --target airflow',
    )

    dbt_run >> dbt_test
