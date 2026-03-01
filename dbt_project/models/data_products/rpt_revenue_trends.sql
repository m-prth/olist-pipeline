WITH monthly_metrics AS (
    SELECT
        DATE_TRUNC('month', o.date_key) AS month,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT o.customer_id) AS unique_customers,
        ROUND(SUM(oi.total_line_value), 2) AS total_revenue,
        ROUND(AVG(oi.total_line_value), 2) AS avg_order_value,
        COUNT(DISTINCT oi.product_id) AS unique_products_sold
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_order_items') }} oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY DATE_TRUNC('month', o.date_key)
)

SELECT
    month,
    total_orders,
    unique_customers,
    total_revenue,
    avg_order_value,
    unique_products_sold,
    LAG(total_revenue) OVER (ORDER BY month) AS prev_month_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (ORDER BY month))
        * 100.0 / NULLIF(LAG(total_revenue) OVER (ORDER BY month), 0),
        2
    ) AS revenue_growth_pct,
    ROUND(
        AVG(total_revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
        2
    ) AS rolling_3m_avg_revenue,
    SUM(total_revenue) OVER (ORDER BY month) AS cumulative_revenue
FROM monthly_metrics
ORDER BY month
