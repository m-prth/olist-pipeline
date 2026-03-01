SELECT
    p.product_category_name,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COUNT(*) AS total_items_sold,
    ROUND(SUM(oi.total_line_value), 2) AS total_revenue,
    ROUND(AVG(oi.item_price), 2) AS avg_price,
    ROUND(AVG(r.review_score), 2) AS avg_review_score,
    COUNT(CASE WHEN r.review_score <= 2 THEN 1 END) AS low_reviews,
    COUNT(r.review_id) AS total_reviews,
    ROUND(
        COUNT(CASE WHEN r.review_score <= 2 THEN 1 END) * 100.0 / NULLIF(COUNT(r.review_id), 0),
        2
    ) AS low_review_pct
FROM {{ ref('fact_order_items') }} oi
JOIN {{ ref('dim_products') }} p ON oi.product_id = p.product_id
LEFT JOIN {{ ref('fact_reviews') }} r ON oi.order_id = r.order_id
GROUP BY p.product_category_name
