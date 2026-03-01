WITH customer_cohort AS (
    SELECT
        cust.customer_unique_id AS customer_id,
        DATE_TRUNC('month', MIN(o.date_key)) AS cohort_month
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('stg_customers') }} cust ON o.customer_id = cust.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY cust.customer_unique_id
),

customer_activity AS (
    SELECT
        cust.customer_unique_id AS customer_id,
        DATE_TRUNC('month', o.date_key) AS activity_month
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('stg_customers') }} cust ON o.customer_id = cust.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY cust.customer_unique_id, DATE_TRUNC('month', o.date_key)
),

cohort_data AS (
    SELECT
        cc.cohort_month,
        ca.activity_month,
        DATE_DIFF('month', cc.cohort_month, ca.activity_month) AS months_since_first,
        COUNT(DISTINCT ca.customer_id) AS active_customers
    FROM customer_cohort cc
    JOIN customer_activity ca ON cc.customer_id = ca.customer_id
    GROUP BY cc.cohort_month, ca.activity_month
),

cohort_size AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id) AS cohort_customers
    FROM customer_cohort
    GROUP BY cohort_month
)

SELECT
    cd.cohort_month,
    cd.months_since_first,
    cs.cohort_customers,
    cd.active_customers,
    ROUND(cd.active_customers * 100.0 / cs.cohort_customers, 2) AS retention_pct
FROM cohort_data cd
JOIN cohort_size cs ON cd.cohort_month = cs.cohort_month
ORDER BY cd.cohort_month, cd.months_since_first
