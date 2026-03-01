SELECT
    ROW_NUMBER() OVER () AS product_sk,
    product_id,
    product_category_name,
    CAST(product_length_cm * product_width_cm * product_height_cm AS DOUBLE) AS volume_cm3,
    product_weight_g
FROM {{ ref('stg_products') }}
