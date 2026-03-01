WITH seller_revenue AS (
    SELECT
        oi.seller_id,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        COUNT(*) AS total_items_sold,
        ROUND(SUM(oi.total_line_value), 2) AS total_revenue,
        ROUND(AVG(oi.item_price), 2) AS avg_item_price
    FROM {{ ref('fact_order_items') }} oi
    GROUP BY oi.seller_id
),

seller_delivery AS (
    SELECT
        oi.seller_id,
        AVG(ol.total_delivery_days) AS avg_delivery_days,
        SUM(CASE WHEN ol.approval_efficiency = 'Slow Approval' THEN 1 ELSE 0 END) AS slow_approvals
    FROM {{ ref('fact_order_items') }} oi
    JOIN {{ ref('fact_order_lifecycle') }} ol ON oi.order_id = ol.order_id
    GROUP BY oi.seller_id
),

seller_reviews AS (
    SELECT
        oi.seller_id,
        ROUND(AVG(r.review_score), 2) AS avg_review_score,
        COUNT(r.review_id) AS total_reviews
    FROM {{ ref('fact_order_items') }} oi
    JOIN {{ ref('fact_reviews') }} r ON oi.order_id = r.order_id
    GROUP BY oi.seller_id
)

SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    sr.total_orders,
    sr.total_items_sold,
    sr.total_revenue,
    sr.avg_item_price,
    ROUND(sd.avg_delivery_days, 1) AS avg_delivery_days,
    sd.slow_approvals,
    srev.avg_review_score,
    srev.total_reviews
FROM seller_revenue sr
JOIN {{ ref('dim_sellers') }} s ON sr.seller_id = s.seller_id
LEFT JOIN seller_delivery sd ON sr.seller_id = sd.seller_id
LEFT JOIN seller_reviews srev ON sr.seller_id = srev.seller_id
