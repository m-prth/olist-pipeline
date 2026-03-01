SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.item_price,
    oi.freight_value,
    oi.total_line_value,
    o.customer_id,
    o.order_status,
    o.date_key AS order_date,
    o.is_late,
    cust.customer_unique_id,
    cust.customer_city,
    cust.customer_state,
    p.product_category_name,
    p.volume_cm3,
    p.product_weight_g,
    s.seller_city,
    s.seller_state
FROM {{ ref('fact_order_items') }} oi
JOIN {{ ref('fact_orders') }} o ON oi.order_id = o.order_id
JOIN {{ ref('stg_customers') }} cust ON o.customer_id = cust.customer_id
JOIN {{ ref('dim_products') }} p ON oi.product_id = p.product_id
JOIN {{ ref('dim_sellers') }} s ON oi.seller_id = s.seller_id
