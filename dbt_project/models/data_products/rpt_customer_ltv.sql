WITH customer_metrics AS (
    SELECT
        cust.customer_unique_id AS customer_id,
        MIN(o.date_key) AS first_order_date,
        MAX(o.date_key) AS last_order_date,
        COUNT(DISTINCT o.order_id) AS total_orders,
        ROUND(SUM(oi.total_line_value), 2) AS total_spend,
        ROUND(AVG(oi.total_line_value), 2) AS avg_order_value,
        DATE_DIFF('month', MIN(o.date_key), MAX(o.date_key)) AS tenure_months
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_order_items') }} oi ON o.order_id = oi.order_id
    JOIN {{ ref('stg_customers') }} cust ON o.customer_id = cust.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY cust.customer_unique_id
),

customer_reviews AS (
    SELECT
        cust.customer_unique_id AS customer_id,
        ROUND(AVG(r.review_score), 2) AS avg_review_given
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_reviews') }} r ON o.order_id = r.order_id
    JOIN {{ ref('stg_customers') }} cust ON o.customer_id = cust.customer_id
    GROUP BY cust.customer_unique_id
)

SELECT
    cm.customer_id,
    c.customer_city,
    c.customer_state,
    cm.first_order_date,
    cm.last_order_date,
    cm.tenure_months,
    cm.total_orders,
    cm.total_spend,
    cm.avg_order_value,
    ROUND(cm.total_spend / NULLIF(cm.tenure_months, 0), 2) AS monthly_spend_rate,
    cr.avg_review_given,
    CASE
        WHEN cm.total_spend >= 500 AND cm.total_orders >= 3 THEN 'High Value'
        WHEN cm.total_spend >= 200 OR cm.total_orders >= 2 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS ltv_tier,
    NTILE(10) OVER (ORDER BY cm.total_spend ASC) AS ltv_decile
FROM customer_metrics cm
JOIN {{ ref('dim_customers') }} c ON cm.customer_id = c.customer_unique_id
LEFT JOIN customer_reviews cr ON cm.customer_id = cr.customer_id
