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
