# Data Lineage Diagram

Full end-to-end lineage from Kaggle source files through the Medallion Architecture (Bronze → Silver → Gold).

```mermaid
flowchart LR


%% ── Kaggle Source Files ──────────────────────
subgraph SRC["Source (Kaggle CSVs)"]
    direction TB
    src_orders["orders"]
    src_customers["customers"]
    src_sellers["sellers"]
    src_products["products"]
    src_order_items["order_items"]
    src_payments["order_payments"]
    src_reviews["order_reviews"]
    src_geo["geolocation"]
    src_cat_trans["category_translation"]
end

%% ── Bronze Layer ─────────────────────────────
subgraph BRZ["Bronze (Raw Parquet)"]
    direction TB
    brz_orders["orders"]
    brz_customers["customers"]
    brz_sellers["sellers"]
    brz_products["products"]
    brz_order_items["order_items"]
    brz_payments["order_payments"]
    brz_reviews["order_reviews"]
    brz_geo["geolocation"]
    brz_cat_trans["category_translation"]
end

%% ── Silver Layer ─────────────────────────────
subgraph SLV["Silver (Deduplicated)"]
    direction TB
    slv_orders["orders"]
    slv_customers["customers"]
    slv_sellers["sellers"]
    slv_products["products"]
    slv_order_items["order_items"]
    slv_payments["order_payments"]
    slv_reviews["order_reviews"]
    slv_geo["geolocation"]
    slv_cat_trans["category_translation"]
end

%% ── Gold: Staging ────────────────────────────
subgraph STG["Gold: Staging Views"]
    direction TB
    stg_orders["stg_orders"]
    stg_customers["stg_customers"]
    stg_sellers["stg_sellers"]
    stg_products["stg_products"]
    stg_order_items["stg_order_items"]
    stg_payments["stg_payments"]
    stg_reviews["stg_reviews"]
    stg_geo["stg_geolocation"]
end

%% ── Gold: Marts ──────────────────────────────
subgraph MARTS["Gold: Dimensions & Facts"]
    direction TB
    dim_customers["dim_customers"]
    dim_sellers["dim_sellers"]
    dim_products["dim_products"]
    dim_geo["dim_geolocation"]
    dim_date["dim_date"]
    fact_orders["fact_orders"]
    fact_lifecycle["fact_order_lifecycle"]
    fact_order_items["fact_order_items"]
    fact_payments["fact_payments"]
    fact_reviews["fact_reviews"]
    fact_shipping["fact_shipping_network"]
    snap_backlog["snapshot_daily_seller_backlog"]
end

%% ── Gold: Data Products ──────────────────────
subgraph DP["Gold: Data Products"]
    direction TB
    obt["obt_sales_analytics"]
    rpt_rfm["rpt_customer_rfm"]
    rpt_ltv["rpt_customer_ltv"]
    rpt_cohort["rpt_cohort_retention"]
    rpt_rev["rpt_revenue_trends"]
    rpt_seller["rpt_seller_performance"]
    rpt_product["rpt_product_category_analysis"]
    rpt_ship["rpt_shipping_efficiency"]
    rpt_basket["rpt_market_basket"]
end

%% ── Source → Bronze (DAG 01: Ingest) ─────────
src_orders --> brz_orders
src_customers --> brz_customers
src_sellers --> brz_sellers
src_products --> brz_products
src_order_items --> brz_order_items
src_payments --> brz_payments
src_reviews --> brz_reviews
src_geo --> brz_geo
src_cat_trans --> brz_cat_trans

%% ── Bronze → Silver (DAG 02: Deduplicate) ────
brz_orders --> slv_orders
brz_customers --> slv_customers
brz_sellers --> slv_sellers
brz_products --> slv_products
brz_order_items --> slv_order_items
brz_payments --> slv_payments
brz_reviews --> slv_reviews
brz_geo --> slv_geo
brz_cat_trans --> slv_cat_trans

%% ── Silver → Staging (dbt source) ────────────
slv_orders --> stg_orders
slv_customers --> stg_customers
slv_sellers --> stg_sellers
slv_products --> stg_products
slv_cat_trans --> stg_products
slv_order_items --> stg_order_items
slv_payments --> stg_payments
slv_reviews --> stg_reviews
slv_geo --> stg_geo

%% ── Staging → Dimensions ─────────────────────
stg_customers --> dim_customers
stg_sellers --> dim_sellers
stg_products --> dim_products
stg_geo --> dim_geo

%% ── Staging → Facts ──────────────────────────
stg_orders --> fact_orders
stg_orders --> fact_lifecycle
stg_order_items --> fact_order_items
stg_payments --> fact_payments
stg_reviews --> fact_reviews

%% ── Staging → fact_shipping_network ──────────
stg_orders --> fact_shipping
stg_customers --> fact_shipping
stg_sellers --> fact_shipping
stg_geo --> fact_shipping
stg_order_items --> fact_shipping

%% ── Staging → snapshot ───────────────────────
stg_orders --> snap_backlog
stg_order_items --> snap_backlog

%% ── Marts → Data Products ────────────────────
fact_orders --> obt
fact_order_items --> obt
dim_products --> obt
dim_sellers --> obt
stg_customers --> obt

fact_orders --> rpt_rfm
fact_order_items --> rpt_rfm
dim_customers --> rpt_rfm
stg_customers --> rpt_rfm

fact_orders --> rpt_ltv
fact_order_items --> rpt_ltv
fact_reviews --> rpt_ltv
dim_customers --> rpt_ltv
stg_customers --> rpt_ltv

fact_orders --> rpt_cohort
stg_customers --> rpt_cohort

fact_order_items --> rpt_rev
fact_orders --> rpt_rev

fact_order_items --> rpt_seller
fact_lifecycle --> rpt_seller
fact_reviews --> rpt_seller
dim_sellers --> rpt_seller

fact_order_items --> rpt_product
fact_reviews --> rpt_product
dim_products --> rpt_product

fact_orders --> rpt_ship
fact_lifecycle --> rpt_ship
fact_shipping --> rpt_ship

fact_order_items --> rpt_basket
dim_products --> rpt_basket

%% ── Styles ───────────────────────────────────
style SRC fill:#4a5568,stroke:#2d3748,color:#fff
style BRZ fill:#c05621,stroke:#9c4221,color:#fff
style SLV fill:#2b6cb0,stroke:#2c5282,color:#fff
style STG fill:#276749,stroke:#22543d,color:#fff
style MARTS fill:#276749,stroke:#22543d,color:#fff
style DP fill:#276749,stroke:#22543d,color:#fff
```

## Layer Summary

| Layer | Airflow DAG | Storage | Count | Description |
|-------|------------|---------|-------|-------------|
| **Source** | - | `data/raw_kaggle/` | 9 tables | Kaggle CSV dataset |
| **Bronze** | `01_ingest_bronze` | `s3://olist-lake/bronze/` | 9 tables | Raw Parquet upload to MinIO |
| **Silver** | `02_process_silver` | `s3://olist-lake/silver/` | 9 tables | Polars deduplication |
| **Gold: Staging** | `03_process_gold` | DuckDB views | 8 views | dbt staging (type casting, renaming) |
| **Gold: Marts** | `03_process_gold` | `s3://olist-lake/gold/` | 12 tables | Dimensions, facts, snapshot |
| **Gold: Products** | `03_process_gold` | `s3://olist-lake/gold/` | 9 tables | Analytics-ready reports & OBT |
