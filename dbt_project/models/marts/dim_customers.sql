{{ config(
    materialized='external', 
    location='s3://olist-lake/gold/dim_customers/dim_customers.parquet'
) }}

SELECT
    ROW_NUMBER() OVER () AS customer_sk,
    customer_unique_id,
    customer_city,
    customer_state
FROM (
    SELECT DISTINCT ON(customer_unique_id)
        customer_unique_id, customer_city, customer_state
    FROM {{ ref('stg_customers') }}
)