SELECT
    p.product_id,
    COALESCE(ct.product_category_name_english, 'Unknown') AS product_category_name,
    CAST(p.product_weight_g AS INT) AS product_weight_g,
    CAST(p.product_length_cm AS DOUBLE) AS product_length_cm,
    CAST(p.product_height_cm AS DOUBLE) AS product_height_cm,
    CAST(p.product_width_cm AS DOUBLE) AS product_width_cm
FROM {{ source('silver', 'products') }} p
LEFT JOIN {{ source('silver', 'product_category_name_translation') }} ct
    ON p.product_category_name = ct.product_category_name
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.product_id ORDER BY p.product_id) = 1
