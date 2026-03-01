SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.price AS item_price,
    oi.freight_value,
    oi.price + oi.freight_value AS total_line_value
FROM {{ ref('stg_order_items') }} oi
