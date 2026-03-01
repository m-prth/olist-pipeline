SELECT
    o.order_id,
    o.order_status,
    o.date_key AS order_date,
    o.is_late,
    ol.total_delivery_days,
    ol.approval_lag_hours,
    ol.approval_efficiency,
    sn.distance_km,
    ROUND(sn.distance_km / NULLIF(ol.total_delivery_days, 0), 2) AS km_per_day,
    CASE
        WHEN ol.total_delivery_days <= 7 THEN '0-7 days'
        WHEN ol.total_delivery_days <= 14 THEN '8-14 days'
        WHEN ol.total_delivery_days <= 21 THEN '15-21 days'
        ELSE '22+ days'
    END AS delivery_bucket,
    CASE
        WHEN sn.distance_km <= 100 THEN 'Local (<100km)'
        WHEN sn.distance_km <= 500 THEN 'Regional (100-500km)'
        WHEN sn.distance_km <= 1000 THEN 'Long Distance (500-1000km)'
        ELSE 'Very Long Distance (>1000km)'
    END AS distance_bucket
FROM {{ ref('fact_orders') }} o
JOIN {{ ref('fact_order_lifecycle') }} ol ON o.order_id = ol.order_id
JOIN {{ ref('fact_shipping_network') }} sn ON o.order_id = sn.order_id
