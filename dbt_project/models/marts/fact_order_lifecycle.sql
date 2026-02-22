{{ config(
    materialized='external',
    location='s3://olist-lake/gold/fact_order_lifecycle/fact_order_lifecycle.parquet'
) }}

SELECT
    order_id,

    -- Process Mining: Calculate the lag between stages
    date_diff('hour',
        order_purchase_timestamp,
        order_approved_at
    ) AS approval_lag_hours,

    date_diff('day',
        order_purchase_timestamp,
        order_delivered_customer_date
    ) AS total_delivery_days,

    -- Flag orders that took more than 48 hours to approve
    CASE
        WHEN date_diff('hour', order_purchase_timestamp, order_approved_at) > 48
        THEN 'Slow Approval'
        ELSE 'Fast Approval'
    END AS approval_efficiency

FROM {{ ref('stg_orders') }}
WHERE order_status = 'delivered'
