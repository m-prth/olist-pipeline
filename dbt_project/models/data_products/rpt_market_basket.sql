WITH order_categories AS (
    SELECT
        oi.order_id,
        p.product_category_name
    FROM {{ ref('fact_order_items') }} oi
    JOIN {{ ref('dim_products') }} p ON oi.product_id = p.product_id
    WHERE p.product_category_name IS NOT NULL
),

category_pairs AS (
    SELECT
        a.product_category_name AS category_a,
        b.product_category_name AS category_b,
        COUNT(DISTINCT a.order_id) AS co_occurrence_count
    FROM order_categories a
    JOIN order_categories b
        ON a.order_id = b.order_id
        AND a.product_category_name < b.product_category_name
    GROUP BY a.product_category_name, b.product_category_name
),

category_totals AS (
    SELECT
        product_category_name,
        COUNT(DISTINCT order_id) AS category_order_count
    FROM order_categories
    GROUP BY product_category_name
)

SELECT
    cp.category_a,
    cp.category_b,
    cp.co_occurrence_count,
    ct_a.category_order_count AS category_a_orders,
    ct_b.category_order_count AS category_b_orders,
    ROUND(cp.co_occurrence_count * 100.0 / ct_a.category_order_count, 2) AS pct_of_category_a,
    ROUND(cp.co_occurrence_count * 100.0 / ct_b.category_order_count, 2) AS pct_of_category_b,
    ROUND(
        cp.co_occurrence_count * 1.0 /
        (ct_a.category_order_count + ct_b.category_order_count - cp.co_occurrence_count),
        4
    ) AS jaccard_similarity
FROM category_pairs cp
JOIN category_totals ct_a ON cp.category_a = ct_a.product_category_name
JOIN category_totals ct_b ON cp.category_b = ct_b.product_category_name
WHERE cp.co_occurrence_count >= 5
ORDER BY cp.co_occurrence_count DESC
