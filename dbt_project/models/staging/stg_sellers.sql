SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM {{ source('silver', 'sellers') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY seller_id) = 1
