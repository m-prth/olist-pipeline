# 🏛️ Data Architecture

This document describes the Medallion Architecture of the Olist pipeline in detail, including table schemas, technology choices, and data lineage.

---

## Overview

The pipeline is divided into three layers stored in MinIO under the `olist-lake` bucket:

```
olist-lake/
├── bronze/          # Raw, as-ingested Parquet files
├── silver/          # Deduplicated, conformed Parquet files
└── gold/            # Dimensional models (Star Schema)
```

---

## 🥉 Bronze Layer (Raw Ingestion)

**Goal:** Ingest data exactly as-is from the local landing zone to the Data Lake.
**Format:** Parquet (snappy compressed)
**Path:** `s3://olist-lake/bronze/<table>/date=<YYYY-MM-DD>/<table>.parquet`

Data is ingested from daily Parquet batches produced by `simulate_stream.py` and stored with a Hive-style date partition.

### Source Tables

| Table | Description | Key Columns |
|---|---|---|
| `orders` | Core order header | `order_id`, `customer_id`, `order_status`, timestamps |
| `order_items` | Line items per order | `order_id`, `product_id`, `seller_id`, `price`, `freight_value` |
| `order_payments` | Payment transactions | `order_id`, `payment_type`, `payment_value` |
| `order_reviews` | Customer feedback | `review_id`, `order_id`, `review_score` |
| `customers` | Customer registry | `customer_id`, `customer_unique_id`, `zip_code_prefix` |
| `products` | Product catalog | `product_id`, `category_name`, `weight_g` |
| `sellers` | Seller registry | `seller_id`, `zip_code_prefix`, `city`, `state` |
| `geolocation` | Zip → lat/lng map | `zip_code_prefix`, `lat`, `lng` |
| `product_category_name_translation` | PT → EN category names | `category_name_pt`, `category_name_english` |

---

## 🥈 Silver Layer (Cleansed & Conformed)

**Goal:** Remove duplicates and ensure data consistency.
**Engine:** Polars (`02_process_silver` DAG)
**Format:** Parquet
**Path:** `s3://olist-lake/silver/<table>/date=<YYYY-MM-DD>/<table>.parquet`

### Transformation Logic

The Silver layer currently applies **universal deduplication** using `polars.DataFrame.unique()` across all tables. The path structure is preserved — only the prefix changes from `bronze/` to `silver/`.

| Silver Table | Key Transformation |
|---|---|
| `silver_orders` | Deduplicated on all columns |
| `silver_customers` | Deduplicated, standardizing `customer_unique_id` |
| `silver_sellers` | Deduplicated |
| `silver_products` | Deduplicated |
| `silver_order_items` | Deduplicated |
| `silver_geolocation` | Deduplicated (note: centroid logic is a planned enhancement) |
| `silver_order_payments` | Deduplicated |
| `silver_order_reviews` | Deduplicated |

> **Planned Enhancement:** Apply table-specific cleansing (date casting, geolocation centroid, translation joins) using dbt.

---

## 🥇 Gold Layer (Dimensional Modeling)

**Goal:** Business-ready Star Schema tables for analytics and reporting.
**Engine:** dbt + DuckDB (`03_process_gold` DAG, using `dbt-duckdb`)
**Format:** Parquet (external materialization)
**Path:** `s3://olist-lake/gold/<model>/<model>.parquet`

dbt reads Silver Parquet files from MinIO via DuckDB’s `httpfs` extension and materializes mart models as external Parquet back to MinIO.

### Dimension Tables

| Table | Grain | Key Columns |
|---|---|---|
| `dim_customers` | 1 row per unique customer | `customer_sk`, `customer_unique_id`, `city`, `state` |
| `dim_sellers` | 1 row per seller | `seller_sk`, `seller_id`, `city`, `state` |

### Fact Tables

| Table | Grain | Key Metrics |
|---|---|---|
| `fact_orders` | 1 row per order | `order_id`, `customer_id`, `date_key`, `order_status`, `is_late` (bool) |
| `fact_order_lifecycle` | 1 row per delivered order | `order_id`, `approval_lag_hours`, `total_delivery_days`, `approval_efficiency` |
| `fact_shipping_network` | 1 row per order route | `order_id`, `cust_lat/lng`, `sell_lat/lng`, `distance_km` (Haversine) |

### Gold DAG Task Graph

```
dbt_run  →  dbt_test
```

| Task ID | Description |
|---|---|
| `dbt_run` | Runs `dbt run` — builds all staging views and mart Parquet files |
| `dbt_test` | Runs `dbt test` — validates `unique` and `not_null` constraints on mart models |

### dbt Model Layers

| Layer | Models | Materialization |
|---|---|---|
| **Staging** (`models/staging/`) | `stg_orders`, `stg_customers`, `stg_sellers`, `stg_order_items`, `stg_geolocation` | View (on Silver Parquet) |
| **Marts** (`models/marts/`) | `dim_customers`, `dim_sellers`, `fact_orders`, `fact_order_lifecycle`, `fact_shipping_network` | External Parquet (to `s3://olist-lake/gold/`) |

---

## Technology Rationale

| Decision | Reason |
|---|---|
| **Polars over Pandas** | ~5–10x faster for deduplication on large Parquet datasets; lower memory footprint |
| **dbt-duckdb** | Declarative SQL models with lineage, testing, and documentation; DuckDB provides zero-overhead SQL on remote Parquet via httpfs |
| **DuckDB over Spark** | Dataset size (~100k rows) does not justify Spark's overhead |
| **MinIO** | S3-compatible, runs locally in Docker, no cloud costs |
| **Parquet format** | Columnar, compressed, ideal for analytical queries |

---

## Planned Extensions

- **Trino** configured in `config/trino/catalog/minio.properties` as a future SQL serving layer
- **Metabase** planned as BI tool connecting to Trino
- Additional dbt models planned: `dim_products`, `dim_date`, `fact_order_items`, `fact_payments`
