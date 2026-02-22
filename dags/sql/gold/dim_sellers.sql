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
