# Project TODO

This is the active development checklist for the Olist Data Pipeline.

---

## Phase 1: Infrastructure & Environment ✅
- [x] Create project directory and Git repository
- [x] Create standard folder structure (`dags/`, `scripts/`, `data/`, `config/`, `docs/`)
- [x] Create `.env` file for credentials
- [x] Create `requirements.txt`
- [x] Create `docker-compose.yaml` (MinIO, Postgres, Airflow)
- [x] Configure DuckDB as query engine

---

## Phase 2: Data Ingestion (Bronze Layer) ✅
- [x] Download Olist dataset from Kaggle → `data/raw_kaggle/`
- [x] Write `scripts/simulate_stream.py` (daily + setup modes, state management)
- [x] Write `scripts/backfill_data.py` (loops simulate_stream.py over 2016–2018)
- [x] Create Airflow DAG `01_ingest_bronze` (upload Parquet → MinIO, archive processed files)

---

## Phase 3: Transformation & Modeling ✅
- [x] Create Airflow DAG `02_process_silver` (Polars deduplication)
- [x] Create Airflow DAG `03_process_gold` (DuckDB Star Schema)
  - [x] `dim_customers`
  - [x] `dim_sellers`
  - [x] `fact_orders`
  - [x] `fact_order_lifecycle` (process mining)
  - [x] `fact_shipping_network` (Haversine geospatial)

---

## Phase 4: Dashboard & Visualization ✅
- [x] Build Streamlit dashboard (`scripts/dashboard.py`)
  - [x] KPI cards (orders, late rate, avg delivery time)
  - [x] Daily order volume line chart
  - [x] Top states bar chart
  - [x] 3D shipping arc map (PyDeck) with distance filter
  - [x] Raw data explorer (tabbed, Gold layer preview)

---

## Phase 5: Documentation ✅
- [x] Write `README.md`
- [x] Write `docs/setup.md`
- [x] Write `docs/architecture.md`
- [x] Write `docs/dag_reference.md`
- [x] Write `docs/data_simulation.md`
- [x] Write `docs/dashboard.md`

---

## Phase 6: dbt Implementation ✅
- [x] Implement dbt project (`dbt_project/`) for Gold layer
  - [x] Configure `profiles.yml` for DuckDB + MinIO (dev & airflow targets)
  - [x] Staging models (8): `stg_orders`, `stg_order_items`, `stg_customers`, `stg_sellers`, `stg_geolocation`, `stg_payments`, `stg_products`, `stg_reviews`
  - [x] Dimension models (5): `dim_customers`, `dim_sellers`, `dim_products`, `dim_geolocation`, `dim_date`
  - [x] Fact models (7): `fact_orders`, `fact_order_items`, `fact_payments`, `fact_reviews`, `fact_order_lifecycle`, `fact_shipping_network`, `snapshot_daily_seller_backlog`
  - [x] Data products (9): `obt_sales_analytics`, `rpt_customer_rfm`, `rpt_seller_performance`, `rpt_product_category_analysis`, `rpt_shipping_efficiency`, `rpt_cohort_retention`, `rpt_revenue_trends`, `rpt_customer_ltv`, `rpt_market_basket`
  - [x] `schema.yml` with `unique` / `not_null` tests (56 tests, all passing)
  - [x] DAG `03_process_gold` rewritten to use `dbt run` + `dbt test` via BashOperator
  - [x] Centralized model configs in `schema.yml` (no inline `{{ config() }}` in SQL)

---

## Phase 7: Planned Enhancements 🔜
- [ ] Update Streamlit dashboard to visualize new data products (RFM, cohort heatmap, revenue trends)
- [ ] Add data quality checks / alerting in Airflow
- [ ] Add idempotency guard (skip re-ingesting already-uploaded partitions)
