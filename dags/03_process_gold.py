from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import duckdb

def get_duckdb_connection():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        CREATE SECRET minio_secret (
            TYPE S3,
            KEY_ID 'admin',
            SECRET 'password',
            REGION 'us-east-1',
            ENDPOINT 'minio:9000',
            URL_STYLE 'path',
            USE_SSL false
        );
    """)
    return con

def build_dim_customers():
    print("Building dim_customers with DuckDB SQL...")
    con = get_duckdb_connection()
    
    sql = """
        COPY (
            SELECT 
                ROW_NUMBER() OVER () AS customer_sk,
                customer_unique_id,
                customer_city,
                customer_state
            FROM (
                -- Deduplicate to ensure 1 row per unique customer
                SELECT DISTINCT ON(customer_unique_id) 
                    customer_unique_id, 
                    customer_city, 
                    customer_state
                FROM read_parquet('s3://olist-lake/silver/customers/**/*.parquet')
            )
        ) TO 's3://olist-lake/gold/dim_customers/dim_customers.parquet' (FORMAT PARQUET);
    """
    con.execute(sql)
    print("dim_customers complete!")

def build_dim_sellers():
    print("Building dim_sellers with SQL...")
    con = get_duckdb_connection()
    
    sql = """
        COPY (
            SELECT 
                ROW_NUMBER() OVER () AS seller_sk,
                seller_id,
                seller_city,
                seller_state
            FROM (
                SELECT DISTINCT ON(seller_id) 
                    seller_id, 
                    seller_city, 
                    seller_state
                FROM read_parquet('s3://olist-lake/silver/sellers/**/*.parquet')
            )
        ) TO 's3://olist-lake/gold/dim_sellers/dim_sellers.parquet' (FORMAT PARQUET);
    """
    con.execute(sql)
    print("dim_sellers complete!")

def build_fact_orders():
    print("Building fact_orders with  SQL...")
    con = get_duckdb_connection()
    
    sql = """
        COPY (
            SELECT 
                order_id,
                customer_id,
                -- Cast strings to actual Date and Timestamp objects
                CAST(order_purchase_timestamp AS DATE) AS date_key,
                order_status,
                -- Calculate if the order was delivered late (Boolean)
                CAST(order_delivered_customer_date AS TIMESTAMP) > CAST(order_estimated_delivery_date AS TIMESTAMP) AS is_late
            FROM read_parquet('s3://olist-lake/silver/orders/**/*.parquet')
        ) TO 's3://olist-lake/gold/fact_orders/fact_orders.parquet' (FORMAT PARQUET);
    """
    con.execute(sql)
    print(" fact_orders complete!")

def build_fact_order_lifecycle():
    print("Building fact_order_lifecycle with DuckDB SQL...")
    con = get_duckdb_connection()
    
    sql = """
        COPY (
            SELECT 
                order_id,
                -- Process Mining: Calculate the lag between stages in hours
                date_diff('hour', 
                    CAST(order_purchase_timestamp AS TIMESTAMP), 
                    CAST(order_approved_at AS TIMESTAMP)
                ) AS approval_lag_hours,
                
                date_diff('day', 
                    CAST(order_purchase_timestamp AS TIMESTAMP), 
                    CAST(order_delivered_customer_date AS TIMESTAMP)
                ) AS total_delivery_days,
                
                -- Flag orders that took more than 48 hours to approve
                CASE 
                    WHEN date_diff('hour', CAST(order_purchase_timestamp AS TIMESTAMP), CAST(order_approved_at AS TIMESTAMP)) > 48 
                    THEN 'Slow Approval' 
                    ELSE 'Fast Approval' 
                END AS approval_efficiency
            FROM read_parquet('s3://olist-lake/silver/orders/**/*.parquet')
            WHERE order_status = 'delivered'
        ) TO 's3://olist-lake/gold/fact_order_lifecycle/fact_order_lifecycle.parquet' (FORMAT PARQUET);
    """
    con.execute(sql)
    print("fact_order_lifecycle complete!")

def build_fact_shipping_network():
    print("Building fact_shipping_network with Coordinates included...")
    con = get_duckdb_connection()
    
    con.execute("PRAGMA memory_limit='1GB';")
    con.execute("PRAGMA threads=2;")
    
    sql = """
        COPY (
            -- Step 1: Deduplicate
            WITH geo_dedup AS (
                SELECT 
                    geolocation_zip_code_prefix, 
                    AVG(geolocation_lat) as lat, 
                    AVG(geolocation_lng) as lng
                FROM read_parquet('s3://olist-lake/silver/geolocation/**/*.parquet')
                GROUP BY geolocation_zip_code_prefix
            ),
            
            -- Step 2: Safe Joins
            order_locations AS (
                SELECT 
                    o.order_id,
                    gc.lat AS cust_lat,
                    gc.lng AS cust_lng,
                    gs.lat AS sell_lat,
                    gs.lng AS sell_lng
                FROM read_parquet('s3://olist-lake/silver/orders/**/*.parquet') o
                JOIN read_parquet('s3://olist-lake/silver/customers/**/*.parquet') c ON o.customer_id = c.customer_id
                JOIN read_parquet('s3://olist-lake/silver/order_items/**/*.parquet') oi ON o.order_id = oi.order_id
                JOIN read_parquet('s3://olist-lake/silver/sellers/**/*.parquet') s ON oi.seller_id = s.seller_id
                JOIN geo_dedup gc ON c.customer_zip_code_prefix = gc.geolocation_zip_code_prefix
                JOIN geo_dedup gs ON s.seller_zip_code_prefix = gs.geolocation_zip_code_prefix
            )
            
            -- Step 3: Math (Now including all the location columns with *)
            SELECT 
                *, 
                6371 * acos(
                    least(greatest(
                        cos(radians(sell_lat)) * cos(radians(cust_lat)) * cos(radians(cust_lng) - radians(sell_lng)) + 
                        sin(radians(sell_lat)) * sin(radians(cust_lat))
                    , -1.0), 1.0)
                ) AS distance_km
            FROM order_locations
        ) TO 's3://olist-lake/gold/fact_shipping_network/fact_shipping_network.parquet' (FORMAT PARQUET);
    """
    con.execute(sql)
    print("✅ fact_shipping_network complete!")

# --- DAG DEFINITION ---
with DAG(
    '03_process_gold',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # Visual Gateways
    start_pipeline = EmptyOperator(task_id='start_gold_pipeline')
    dimensions_complete = EmptyOperator(task_id='dimensions_complete')
    end_pipeline = EmptyOperator(task_id='end_gold_pipeline')

    # Processing Tasks
    task_dim_customers = PythonOperator(task_id='build_dim_customers', python_callable=build_dim_customers)
    task_dim_sellers = PythonOperator(task_id='build_dim_sellers', python_callable=build_dim_sellers)
    task_fact_orders = PythonOperator(task_id='build_fact_orders', python_callable=build_fact_orders)
    task_lifecycle = PythonOperator(task_id='build_fact_order_lifecycle', python_callable=build_fact_order_lifecycle)
    task_shipping = PythonOperator(task_id='build_fact_shipping_network', python_callable=build_fact_shipping_network)

    # Graph Wiring
    start_pipeline >> [task_dim_customers, task_dim_sellers]
    [task_dim_customers, task_dim_sellers] >> dimensions_complete
    dimensions_complete >> [task_fact_orders, task_lifecycle, task_shipping] >> end_pipeline
