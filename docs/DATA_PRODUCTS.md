# Data Products & Transformation Plan

This document outlines the Medallion Architecture (Bronze -> Silver -> Gold) for the Olist E-Commerce pipeline.

## 1. Bronze Layer (Raw Ingestion)
**Goal:** Ingest data "as-is" from CSV source to the Data Lake (MinIO).
**Format:** Parquet (snappy compressed)
**Update Strategy:** Append-only / Overwrite daily partition.

| Table Name | Description | Columns (Key) |
| :--- | :--- | :--- |
| `bronze_orders` | Order status and timestamps | `order_id`, `customer_id`, `status`, `purchase_timestamp`... |
| `bronze_order_items` | Items within an order | `order_id`, `order_item_id`, `product_id`, `seller_id`, `price`, `freight`... |
| `bronze_order_payments` | Payment methods and values | `order_id`, `payment_sequential`, `payment_type`, `installments`, `value` |
| `bronze_order_reviews` | Customer reviews | `review_id`, `order_id`, `score`, `comment_title`, `comment_message`... |
| `bronze_customers` | Customer location info | `customer_id`, `unique_id`, `zip_code`, `city`, `state` |
| `bronze_products` | Product attributes | `product_id`, `category_name`, `weight`, `length`, `height`, `width` |
| `bronze_sellers` | Seller location info | `seller_id`, `zip_code`, `city`, `state` |
| `bronze_geolocation` | Zip code lat/long mapping | `zip_code`, `lat`, `lng`, `city`, `state` |
| `bronze_category_translation` | PT -> EN translation | `category_name_pt`, `category_name_english` |

---

## 2. Silver Layer (Cleansed & Conformed)
**Goal:** Clean, deduplicate, and join reference data.
**Format:** Parquet

### Key Transformations:
1.  **Translation:** Join `products` with `product_category_name_translation` to replace Portuguese category names with English.
2.  **Date Standardization:** Convert all timestamp strings to `TIMESTAMP` objects.
3.  **Geolocation Deduplication:** The raw geolocation dataset has multiple lat/long points for the same zip code. We take the centroid (average lat/long) per zip code.
4.  **Customer De-anonymization:** Standardize on `customer_unique_id` for customer identity.

### dbt Staging Models (8 models):
| Table Name | Transformation Logic |
| :--- | :--- |
| `stg_orders` | Cast dates, handle null delivered dates for in-flight orders. |
| `stg_order_items` | Calculate `total_line_value` (price + freight). |
| `stg_customers` | Deduplicate on `customer_id`. |
| `stg_sellers` | Deduplicate on `seller_id` across date partitions. |
| `stg_products` | Join with `product_category_name_translation` for English category names, fill nulls with 'Unknown', deduplicate. |
| `stg_geolocation` | `GROUP BY zip_code` → `AVG(lat), AVG(lng)` to resolve duplicates (applied in dim_geolocation). |
| `stg_payments` | Standardize payment types (title case), cast `payment_value` to DOUBLE. |
| `stg_reviews` | Cast `review_score` to INT, date fields to TIMESTAMP, deduplicate on `review_id`. |

---

## 3. Gold Layer (Dimensional Modeling & Marts)
**Goal:** Business-ready tables for reporting and dashboards.
**Schema:** Star Schema
**Format:** Parquet (external materialization)
**Engine:** dbt + DuckDB (`dbt-duckdb`)

### 3.1 Dimension Tables (5 tables)

| Table | Grain | Key Columns |
|---|---|---|
| `dim_customers` | 1 row per unique customer | `customer_sk`, `customer_unique_id`, `customer_city`, `customer_state` |
| `dim_sellers` | 1 row per seller | `seller_sk`, `seller_id`, `seller_city`, `seller_state` |
| `dim_products` | 1 row per product | `product_sk`, `product_id`, `product_category_name`, `volume_cm3`, `product_weight_g` |
| `dim_geolocation` | 1 row per zip code | `zip_code_prefix`, `latitude`, `longitude` (centroid) |
| `dim_date` | 1 row per day (2016–2018) | `date_key`, `year`, `month`, `quarter`, `day_of_week`, `is_weekend` |

### 3.2 Fact Tables (7 tables)

| Table | Grain | Key Metrics |
|---|---|---|
| `fact_orders` | 1 row per order | `order_id`, `customer_id`, `date_key`, `order_status`, `is_late` |
| `fact_order_items` | 1 row per item in order | `order_id`, `product_id`, `seller_id`, `item_price`, `freight_value`, `total_line_value` |
| `fact_payments` | 1 row per payment attempt | `order_id`, `payment_sequential`, `payment_type`, `installments`, `payment_value` |
| `fact_reviews` | 1 row per review | `review_id`, `order_id`, `review_score`, `response_time_hours` |
| `fact_order_lifecycle` | 1 row per delivered order | `order_id`, `approval_lag_hours`, `total_delivery_days`, `approval_efficiency` |
| `fact_shipping_network` | 1 row per shipping route | `order_id`, `cust_lat/lng`, `sell_lat/lng`, `distance_km` (Haversine) |
| `snapshot_daily_seller_backlog` | 1 row per seller/day | `seller_id`, `snapshot_date`, `open_orders_count`, `revenue_in_transit` |

### 3.3 Data Products (9 tables)

#### Core Reports
| Table | Description | Use Case |
|---|---|---|
| `obt_sales_analytics` | One Big Table: order_items + orders + products + customers + sellers | Ad-hoc exploration, Streamlit filters |
| `rpt_customer_rfm` | RFM segmentation: quintile R/F/M scores + segment labels | Customer segmentation dashboard |
| `rpt_seller_performance` | Revenue, delivery speed, review scores per seller | Seller ranking and partner management |
| `rpt_product_category_analysis` | Sales, revenue, avg review score, low-review % per category | Category performance dashboard |
| `rpt_shipping_efficiency` | Delivery time vs distance with bucketed analysis | Logistics optimization |

#### Advanced KPIs
| Table | Description | Use Case |
|---|---|---|
| `rpt_cohort_retention` | Monthly retention rates by customer acquisition cohort | Retention heatmap dashboard |
| `rpt_revenue_trends` | Monthly revenue, MoM growth %, rolling 3M average, cumulative | Executive revenue dashboard |
| `rpt_customer_ltv` | Total spend, tenure, monthly spend rate, LTV tier & decile | Customer value analysis |
| `rpt_market_basket` | Category co-occurrence pairs with Jaccard similarity | Cross-sell strategy |

## 4. Pipeline Flow
1.  **Simulate Data**: `simulate_stream.py` drops a day's Parquet files into `data/input/`.
2.  **DAG 01 — Ingest**: Upload Parquet batches to `Bronze` (MinIO).
3.  **DAG 02 — Silver**: Polars deduplication → `Silver` (MinIO).
4.  **DAG 03 — Gold (dbt)**:
    - `dbt run`: Builds 8 staging views, 12 mart tables, and 9 data products.
    - `dbt test`: Validates `unique` / `not_null` constraints (56 tests total).

## 5. Model Count Summary

| Layer | Models | Materialization |
|---|---|---|
| Staging | 8 | View (on Silver Parquet) |
| Marts | 12 (5 dim + 7 fact) | External Parquet |
| Data Products | 9 | External Parquet |
| **Total** | **29** | |
