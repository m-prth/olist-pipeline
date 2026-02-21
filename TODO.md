# Project TODO

This is the active development checklist for the Olist Data Pipeline.

---

## Phase 1: Infrastructure & Environment ✅
- [x] Create project directory and Git repository
- [x] Create standard folder structure (`dags/`, `scripts/`, `data/`, `config/`, `docs/`)
- [x] Create `.env` file for credentials
- [x] Create `requirements.txt`
- [x] Create `docker-compose.yaml` (MinIO, Postgres, Airflow)
- [x] Configure Trino catalog skeleton (`config/trino/catalog/minio.properties`)

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

## Phase 6: Planned Enhancements 🔜
- [ ] Add Trino service to `docker-compose.yaml`
- [ ] Add Metabase service to `docker-compose.yaml`
- [ ] Implement full dbt project (`dbt_project/`) for Silver/Gold
  - [ ] `dbt init` + configure `profiles.yml` for Trino
  - [ ] Silver staging models (date casting, type coercion, geolocation centroid)
  - [ ] Gold mart models (mirroring current DuckDB logic)
  - [ ] `schema.yml` with `unique` / `not_null` tests
- [ ] Add `dim_products` to Gold layer
- [ ] Add `dim_date` (calendar dimension) to Gold layer
- [ ] Implement `fact_order_items` (line-item grain)
- [ ] Implement `fact_payments` (transaction grain)
- [ ] Add data quality checks / alerting in Airflow
- [ ] Add idempotency guard (skip re-ingesting already-uploaded partitions)