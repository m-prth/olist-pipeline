
WITH delivered_orders AS (
    SELECT
        oi.seller_id,
        CAST(o.order_purchase_timestamp AS DATE) AS order_date,
        CAST(o.order_delivered_customer_date AS DATE) AS delivered_date,
        oi.price + oi.freight_value AS line_value
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_order_items') }} oi ON o.order_id = oi.order_id
    WHERE o.order_status IN ('shipped', 'delivered', 'invoiced', 'processing')
),

date_spine AS (
    SELECT UNNEST(generate_series(
        (SELECT MIN(order_date) FROM delivered_orders),
        (SELECT MAX(order_date) FROM delivered_orders),
        INTERVAL 1 DAY
    ))::DATE AS snapshot_date
),

daily_snapshot AS (
    SELECT
        d.seller_id,
        ds.snapshot_date,
        COUNT(*) AS open_orders_count,
        SUM(d.line_value) AS revenue_in_transit
    FROM delivered_orders d
    CROSS JOIN date_spine ds
    WHERE ds.snapshot_date >= d.order_date
      AND (d.delivered_date IS NULL OR ds.snapshot_date <= d.delivered_date)
    GROUP BY d.seller_id, ds.snapshot_date
)

SELECT
    seller_id,
    snapshot_date,
    open_orders_count,
    ROUND(revenue_in_transit, 2) AS revenue_in_transit
FROM daily_snapshot
