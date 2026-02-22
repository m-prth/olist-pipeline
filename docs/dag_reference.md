# ✈️ Airflow DAG Reference

This document describes each of the three Airflow DAGs in the pipeline, their purpose, how to trigger them, and what they produce.

All DAGs are located in the `dags/` directory and are available in the Airflow UI at **http://localhost:8081**.

---

## DAG 01: `01_ingest_bronze`

**File:** `dags/01_ingest_bronze.py`
**Purpose:** Picks up daily Parquet batches from the local landing zone and uploads them to the Bronze layer in MinIO.
**Schedule:** Manual (`schedule_interval=None`)

### How to Trigger
1. Ensure data exists in `data/input/` (run `simulate_stream.py` first).
2. In Airflow UI, find `01_ingest_bronze` and click **▶ Trigger DAG**.

### Task Details

| Task ID | Action |
|---|---|
| `ingest_and_archive` | Iterates over date-stamped folders in `data/input/`, uploads each Parquet file to `s3://olist-lake/bronze/<table>/date=<date>/`, then moves the folder to `data/archive/`. |

### Inputs & Outputs

| | Path |
|---|---|
| **Input** | `data/input/<YYYY-MM-DD>/<table>.parquet` (local) |
| **Output** | `s3://olist-lake/bronze/<table>/date=<YYYY-MM-DD>/<table>.parquet` |

---

## DAG 02: `02_process_silver`

**File:** `dags/02_process_silver.py`
**Purpose:** Reads all Parquet files from the Bronze layer, deduplicates them using Polars, and writes the clean results to the Silver layer.
**Schedule:** Manual (`schedule_interval=None`)

### How to Trigger
Run **after** `01_ingest_bronze` has completed successfully.

### Task Details

| Task ID | Action |
|---|---|
| `promote_all_to_silver` | Lists all `.parquet` files under `s3://olist-lake/bronze/`, applies `polars.DataFrame.unique()` on each, and writes the result to the equivalent `s3://olist-lake/silver/` path. |

### Inputs & Outputs

| | Path |
|---|---|
| **Input** | `s3://olist-lake/bronze/**/*.parquet` |
| **Output** | `s3://olist-lake/silver/**/*.parquet` (mirrored structure) |

---

## DAG 03: `03_process_gold`

**File:** `dags/03_process_gold.py`
**Purpose:** Runs the dbt project to build all Gold layer models (staging views + mart Parquet files) and validate data quality.
**Schedule:** Manual (`schedule_interval=None`)

### How to Trigger
Run **after** `02_process_silver` has completed successfully.

### Task Graph

```
dbt_run  →  dbt_test
```

### Task Details

| Task ID | Type | Command |
|---|---|---|
| `dbt_run` | `BashOperator` | `cd /opt/airflow/dbt_project && dbt run --profiles-dir . --target airflow` |
| `dbt_test` | `BashOperator` | `cd /opt/airflow/dbt_project && dbt test --profiles-dir . --target airflow` |

### What `dbt run` Builds

**Staging models** (materialized as views):
- `stg_orders` — Casts timestamp columns from Silver orders
- `stg_customers` — Selects customer fields from Silver
- `stg_sellers` — Selects seller fields from Silver
- `stg_order_items` — Selects order item fields from Silver
- `stg_geolocation` — Selects geolocation fields from Silver

**Mart models** (materialized as external Parquet to `s3://olist-lake/gold/`):
- `dim_customers` — `DISTINCT ON(customer_unique_id)` + surrogate key
- `dim_sellers` — `DISTINCT ON(seller_id)` + surrogate key
- `fact_orders` — Order status, date key, `is_late` boolean
- `fact_order_lifecycle` — `date_diff()` between purchase → approval → delivery timestamps
- `fact_shipping_network` — Joins orders + customers + sellers + geolocation, computes Haversine `distance_km`

### What `dbt test` Validates

| Model | Tests |
|---|---|
| `dim_customers` | `customer_sk`: unique, not_null |
| `dim_sellers` | `seller_sk`: unique, not_null |
| `fact_orders` | `order_id`: not_null |
| `fact_order_lifecycle` | `order_id`: not_null |
| `fact_shipping_network` | `order_id`: not_null, `distance_km`: not_null |

### DuckDB Configuration

Connection settings are defined in `dbt_project/profiles.yml` under the `airflow` target:

```yaml
type: duckdb
path: '/tmp/dbt.duckdb'
extensions: [httpfs, parquet]
secrets:
  - type: s3
    key_id: "admin"
    secret: "password"
    endpoint: "minio:9000"
    url_style: "path"
    use_ssl: false
external_root: "s3://olist-lake/gold"
```

> The endpoint `minio:9000` is the internal Docker hostname. The `dbt_project/` directory is volume-mounted into the Airflow container at `/opt/airflow/dbt_project`.

---

## Running the Full Pipeline End-to-End

```bash
# 1. Generate a daily batch
python scripts/simulate_stream.py --mode daily

# 2. In Airflow UI (http://localhost:8081), trigger in order:
#    01_ingest_bronze  →  02_process_silver  →  03_process_gold
```

Each DAG must **fully succeed** before triggering the next.
