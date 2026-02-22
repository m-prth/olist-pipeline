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
**Format:** Iceberg / Parquet

### Key Transformations:
1.  **Translation:** Join `products` with `category_translation` to replace Portuguese category names with English.
2.  **Date Standardization:** Convert all timestamp strings to `TIMESTAMP` objects.
3.  **Geolocation Deduplication:** The raw geolocation dataset has multiple lat/long points for the same zip code. We will take the centroid (average lat/long) per zip code to create a unique mapping.
4.  **Customer De-anonymization:** The dataset has `customer_id` (per order) and `customer_unique_id` (real person). We will standardize on `customer_unique_id`.

### Silver Tables:
| Table Name | Transformation Logic |
| :--- | :--- |
| `silver_orders` | Cast dates, handle null delivered dates (for in-flight orders). |
| `silver_order_items` | Calculate `total_item_value` (price + freight). |
| `silver_customers` | Deduped list of unique customers with their latest location. |
| `silver_sellers` | Cleaned seller location data. |
| `silver_products` | English category names, fill null dimensions with 0. |
| `silver_geolocation` | `GROUP BY zip_code` -> `AVG(lat), AVG(lng)` to resolve duplicates. |
| `silver_payments` | Standardize payment types (e.g., merge similar types if needed). |

---

## 3. Gold Layer (Dimensional Modeling & Marts)
**Goal:** Business-ready tables for reporting and dashboards.
**Schema:** Star Schema
**Format:** Iceberg

### 3.1 Fact Tables
**`fact_orders`** (Grain: One row per order)
- **Keys:** `order_id`, `customer_key`, `date_key`
- **Metrics:** `total_order_value`, `freight_value`, `count_items`
- **Attributes:** `order_status`, `delivery_delay_days` (actual - estimated)

**`fact_order_items`** (Grain: One row per item)
- **Keys:** `order_id`, `product_key`, `seller_key`, `customer_key`
- **Metrics:** `price`, `freight_value`

### 3.2 Dimension Tables
- **`dim_customers`**: `customer_key`, `unique_id`, `city`, `state`, `first_purchase_date`
- **`dim_products`**: `product_key`, `category`, `size_bucket` (Small/Medium/Large based on dimensions)
- **`dim_sellers`**: `seller_key`, `city`, `state`
- **`dim_date`**: Standard calendar dimension (Year, Month, Quarter, Weekday, Holiday flag)

### 3.3 Data Marts (Aggregated Views)
**`dm_executive_daily_sales`**
- Daily aggregation of revenue, total orders, AOV (Average Order Value).
- **Use Case:** Executive Dashboard line charts.

**`dm_seller_performance`**
- Aggregated by `seller_id`.
- Metrics: `total_sales`, `avg_review_score`, `delivery_speed_avg`.
- **Use Case:** Identifying top/bottom performing partners.

**`dm_product_category_performance`**
- Aggregated by `category`.
- Metrics: `sales_volume`, `revenue`, `return_rate` (implied by low review scores).

## 4. Pipeline Flow
1.  **Simulate Data**: `simulate_stream.py` drops a day's Parquet files into `data/input/`.
2.  **DAG 01 — Ingest**: Upload Parquet batches to `Bronze` (MinIO).
3.  **DAG 02 — Silver**: Polars deduplication → `Silver` (MinIO).
4.  **DAG 03 — Gold (dbt)**:
    - `dbt run`: Builds staging views on Silver, then materializes mart Parquet to Gold.
    - `dbt test`: Validates `unique` / `not_null` constraints on mart models.
