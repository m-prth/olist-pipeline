-- Ensure all Gold layer models are not empty (contain at least 1 row).
-- Fails if any model has 0 records.

{% set gold_models = [
    'dim_customers',
    'dim_sellers',
    'dim_products',
    'dim_geolocation',
    'dim_date',
    'fact_orders',
    'fact_order_items',
    'fact_payments',
    'fact_reviews',
    'fact_order_lifecycle',
    'fact_shipping_network',
    'snapshot_daily_seller_backlog',
    'obt_sales_analytics',
    'rpt_customer_rfm',
    'rpt_seller_performance',
    'rpt_product_category_analysis',
    'rpt_shipping_efficiency',
    'rpt_cohort_retention',
    'rpt_revenue_trends',
    'rpt_customer_ltv',
    'rpt_market_basket',
] %}

{% for model_name in gold_models %}
SELECT '{{ model_name }}' AS model_name, COUNT(*) AS row_count
FROM {{ ref(model_name) }}
HAVING COUNT(*) = 0
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
