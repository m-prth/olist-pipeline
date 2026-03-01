WITH customer_orders AS (
    SELECT
        cust.customer_unique_id AS customer_id,
        COUNT(DISTINCT o.order_id) AS frequency,
        MAX(o.date_key) AS last_order_date,
        SUM(oi.total_line_value) AS monetary
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('fact_order_items') }} oi ON o.order_id = oi.order_id
    JOIN {{ ref('stg_customers') }} cust ON o.customer_id = cust.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY cust.customer_unique_id
),

rfm_scores AS (
    SELECT
        customer_id,
        frequency,
        monetary,
        last_order_date,
        DATE_DIFF('day', last_order_date, (SELECT MAX(last_order_date) FROM customer_orders)) AS recency_days,
        NTILE(5) OVER (ORDER BY DATE_DIFF('day', last_order_date, (SELECT MAX(last_order_date) FROM customer_orders)) DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
    FROM customer_orders
)

SELECT
    r.customer_id,
    c.customer_city,
    c.customer_state,
    r.recency_days,
    r.frequency,
    ROUND(r.monetary, 2) AS monetary,
    r.r_score,
    r.f_score,
    r.m_score,
    ROUND((r.r_score + r.f_score + r.m_score) / 3.0, 2) AS rfm_avg,
    CASE
        WHEN r.r_score >= 4 AND r.f_score >= 4 AND r.m_score >= 4 THEN 'Champions'
        WHEN r.r_score >= 3 AND r.f_score >= 3 THEN 'Loyal Customers'
        WHEN r.r_score >= 4 AND r.f_score <= 2 THEN 'New Customers'
        WHEN r.r_score <= 2 AND r.f_score >= 3 THEN 'At Risk'
        WHEN r.r_score <= 2 AND r.f_score <= 2 THEN 'Lost'
        ELSE 'Other'
    END AS rfm_segment
FROM rfm_scores r
JOIN {{ ref('dim_customers') }} c ON r.customer_id = c.customer_unique_id
