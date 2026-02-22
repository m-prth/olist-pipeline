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
