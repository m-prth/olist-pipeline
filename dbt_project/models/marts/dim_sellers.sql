SELECT
    ROW_NUMBER() OVER () AS seller_sk,
    seller_id,
    seller_city,
    seller_state
FROM (
    SELECT DISTINCT ON(seller_id)
        seller_id, seller_city, seller_state
    FROM {{ ref('stg_sellers') }}
)
