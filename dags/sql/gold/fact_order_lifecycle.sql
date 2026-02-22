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
