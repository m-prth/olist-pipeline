Here is the comprehensive architecture plan, formatted exactly as requested. It incorporates the granularity fixes (splitting Header/Line items) and adds the advanced "Level 2" tables to demonstrate senior engineering capabilities.

# Olist Data Architecture Plan

This document outlines the data lineage from raw ingestion (Bronze) to cleansed data (Silver) and final analytical models (Gold).

## 1. Bronze Layer (Raw Ingestion)

* **Source:** Olist E-Commerce Public Dataset (Kaggle)
* **Format:** Parquet (converted from CSV)
* **Storage:** MinIO `processing-zone/bronze/`

The raw data consists of 9 relational tables. We will ingest these as-is, adding only metadata columns (`ingestion_timestamp`, `source_file`).

| Table Name | Description | Key Columns |
| --- | --- | --- |
| `orders` | Core order registry | `order_id`, `customer_id`, `order_status`, `order_purchase_timestamp` |
| `order_items` | Items within an order | `order_id`, `order_item_id`, `product_id`, `seller_id`, `price`, `freight_value` |
| `order_payments` | Payment methods & values | `order_id`, `payment_sequential`, `payment_type`, `payment_installments`, `payment_value` |
| `order_reviews` | Customer reviews | `review_id`, `order_id`, `review_score`, `review_comment_message` |
| `products` | Product catalog | `product_id`, `product_category_name`, `product_weight_g`, `product_length_cm` |
| `sellers` | Merchant registry | `seller_id`, `seller_zip_code_prefix`, `seller_city`, `seller_state` |
| `customers` | Customer registry (anonymous) | `customer_id`, `customer_unique_id`, `customer_zip_code_prefix`, `customer_city` |
| `geolocation` | Zip code lat/long coordinates | `geolocation_zip_code_prefix`, `geolocation_lat`, `geolocation_lng` |
| `category_translation` | PT to EN translation | `product_category_name`, `product_category_name_english` |

---

## 2. Silver Layer (Cleansed & Conformed)

* **Transformation Engine:** dbt / Polars
* **Storage:** MinIO `processing-zone/silver/`
* **Format:** Iceberg (for ACID compliance) or Parquet

**Transformations:**

* **Standardization:** Rename columns to English (if needed), standardize casing (snake_case).
* **Type Casting:** Convert string dates to `Timestamp`, numeric strings to `Float/Int`.
* **Deduplication:** Ensure unique keys (especially for `geolocation` which often has duplicates per zip code).
* **Null Handling:** Impute or flag missing critical values.

| Table (Model) | Transformations |
| --- | --- |
| `stg_orders` | Cast dates (`purchase`, `approved`, `delivered`). Filter test orders. Calculate `estimated_delivery_days`. |
| `stg_order_items` | Calculate `total_line_value` (price + freight). Do NOT aggregate yet (preserve line granularity). |
| `stg_payments` | Standardize payment types (e.g., 'credit_card'  'Credit'). |
| `stg_products` | Join with `category_translation` to get English category names. Fill null categories with 'Unknown'. |
| `stg_customers` | Clean city names (Title Case, remove special chars). Deduplicate `customer_unique_id`. |
| `stg_geolocation` | Group by `zip_code` and take the centroid (avg lat/long) to ensure 1 row per zip code. |

---

## 3. Gold Layer (Dimensional Modeling)

* **Purpose:** Business Analytics & Reporting
* **Storage:** MinIO `processing-zone/gold/`
* **Format:** Iceberg
* **Serving:** Trino  Metabase

We will use a **Star Schema** approach, with distinct fact tables for different grains to avoid fan-out errors.

### Fact Tables (Business Processes)

**1. `fact_orders` (Header Grain)**

* *Grain: One row per unique Order.*
* **Keys:** `order_id`, `customer_id`, `date_key`.
* **Metrics:** `total_order_value`, `total_freight`, `count_items`.
* **Attributes:** `order_status`, `is_late` (Boolean), `days_actual_vs_estimated`.

**2. `fact_order_items` (Line Grain)**

* *Grain: One row per Item in an Order.*
* **Keys:** `order_id`, `product_id`, `seller_id`.
* **Metrics:** `item_price`, `freight_value`.
* **Attributes:** `order_item_id`.

**3. `fact_payments` (Transaction Grain)**

* *Grain: One row per Payment Attempt.*
* **Keys:** `order_id`, `payment_sequential`.
* **Metrics:** `payment_value`, `installments`.
* **Attributes:** `payment_type` (Credit, Boleto, Voucher).

**4. `fact_reviews` (Feedback Grain)**

* *Grain: One row per Review.*
* **Keys:** `review_id`, `order_id`.
* **Metrics:** `review_score` (1-5).

### Advanced Fact Tables (Level 2 Engineering)

**5. `fact_shipping_network` (Geospatial)**

* *Grain: One row per shipping route.*
* **Keys:** `seller_id`, `customer_id`.
* **Metrics:** `distance_km` (Haversine calculation), `cost_per_km`.
* **Attributes:** `origin_lat/long`, `dest_lat/long`.

**6. `fact_order_lifecycle` (Process Mining)**

* *Grain: One row per Order.*
* **Metrics:** `approval_lag_hours`, `packaging_lag_hours`, `transit_lag_hours`.
* **Purpose:** Identify bottlenecks in the supply chain.

**7. `snapshot_daily_seller_backlog` (Accumulating Snapshot)**

* *Grain: One row per Seller per Day.*
* **Metrics:** `open_orders_count`, `revenue_in_transit`.
* **Purpose:** Time-series analysis of seller operational load.

### Dimension Tables (Context)

1. `dim_customers`

* `customer_sk` (Surrogate Key)
* `customer_unique_id` (Natural Key)
* `customer_city`
* `customer_state`
* `first_order_date`

2. `dim_products`

* `product_sk`
* `product_id`
* `category_name` (English)
* `volume_cm3` (L  W  H)
* `weight_g`

3. `dim_sellers`

* `seller_sk`
* `seller_id`
* `seller_city`
* `seller_state`

4. `dim_geolocation`

* `zip_code_prefix`
* `latitude` (Centroid)
* `longitude` (Centroid)
* `city`, `state`

5. `dim_date` (Auto-generated)

* `date_key`
* `year`, `month`, `quarter`, `day_of_week`
* `is_holiday_br`, `is_weekend`

---

## 4. Final Data Products (Views/Aggregates)

These will be created as views in Trino or final tables in Gold for Metabase dashboards to ensure sub-second performance.

1. **`obt_sales_analytics`**: One Big Table joining `fact_order_items` + `orders` + `products` + `customers` + `sellers` for ad-hoc exploration.
2. **`rpt_customer_rfm`**: Pre-calculated segmentation (Recency, Frequency, Monetary) for every customer.
3. **`rpt_seller_performance`**: Top sellers by revenue, delivery speed, and review score.
4. **`rpt_product_category_analysis`**: Best-selling categories and highest return rates.
5. **`rpt_shipping_efficiency`**: Analysis of `estimated_delivery` vs `actual_delivery` overlaid with `distance_km`.