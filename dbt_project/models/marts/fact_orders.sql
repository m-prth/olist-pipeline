{{ config(
    materialized='external',
    location='s3://olist-lake/gold/fact_orders/fact_orders.parquet'
) }}

SELECT
    order_id,
    customer_id,
    CAST(order_purchase_timestamp AS DATE) AS date_key,
    order_status,
    order_delivered_customer_date > order_estimated_delivery_date AS is_late
FROM {{ ref('stg_orders') }}
