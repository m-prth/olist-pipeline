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
