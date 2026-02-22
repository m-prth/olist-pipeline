{{ config(
    materialized='external',
    location='s3://olist-lake/gold/fact_shipping_network/fact_shipping_network.parquet'
) }}

-- Step 1: Average coordinates per zip code (deduplicate geolocation)
WITH geo_dedup AS (
    SELECT
        geolocation_zip_code_prefix,
        AVG(geolocation_lat) AS lat,
        AVG(geolocation_lng) AS lng
    FROM {{ ref('stg_geolocation') }}
    GROUP BY geolocation_zip_code_prefix
),

-- Step 2: Join orders → customers → sellers → geo coordinates
order_locations AS (
    SELECT
        o.order_id,
        gc.lat AS cust_lat,
        gc.lng AS cust_lng,
        gs.lat AS sell_lat,
        gs.lng AS sell_lng
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
    JOIN {{ ref('stg_order_items') }} oi ON o.order_id = oi.order_id
    JOIN {{ ref('stg_sellers') }} s ON oi.seller_id = s.seller_id
    JOIN geo_dedup gc ON c.customer_zip_code_prefix = gc.geolocation_zip_code_prefix
    JOIN geo_dedup gs ON s.seller_zip_code_prefix = gs.geolocation_zip_code_prefix
)

-- Step 3: Calculate Haversine distance in km
SELECT
    *,
    6371 * acos(
        least(greatest(
            cos(radians(sell_lat)) * cos(radians(cust_lat)) * cos(radians(cust_lng) - radians(sell_lng)) +
            sin(radians(sell_lat)) * sin(radians(cust_lat))
        , -1.0), 1.0)
    ) AS distance_km
FROM order_locations
