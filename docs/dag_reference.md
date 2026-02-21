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
**Purpose:** Reads Silver Parquet files via DuckDB (using httpfs) and builds the Gold Star Schema dimensional model.
**Schedule:** Manual (`schedule_interval=None`)

### How to Trigger
Run **after** `02_process_silver` has completed successfully.

### Task Graph

```
start_gold_pipeline
    ├── build_dim_customers ─┐
    └── build_dim_sellers ───┤
                             ▼
                    dimensions_complete
                    ├── build_fact_orders
                    ├── build_fact_order_lifecycle
                    └── build_fact_shipping_network
                             ▼
                     end_gold_pipeline
```

Dimension tasks run in **parallel**. Fact tasks run in **parallel** after dimensions complete.

### Task Details

| Task ID | SQL Logic | Output Path |
|---|---|---|
| `build_dim_customers` | `DISTINCT ON(customer_unique_id)` + `ROW_NUMBER()` for surrogate key | `gold/dim_customers/dim_customers.parquet` |
| `build_dim_sellers` | `DISTINCT ON(seller_id)` + `ROW_NUMBER()` for surrogate key | `gold/dim_sellers/dim_sellers.parquet` |
| `build_fact_orders` | Selects `order_id`, date, status, computes `is_late` boolean | `gold/fact_orders/fact_orders.parquet` |
| `build_fact_order_lifecycle` | `date_diff()` between purchase → approval → delivery timestamps | `gold/fact_order_lifecycle/fact_order_lifecycle.parquet` |
| `build_fact_shipping_network` | Joins orders + customers + sellers + geolocation (deduped centroids), computes Haversine `distance_km` | `gold/fact_shipping_network/fact_shipping_network.parquet` |

### DuckDB Configuration

All tasks connect to MinIO using the DuckDB `httpfs` extension with:

```python
TYPE S3, KEY_ID 'admin', SECRET 'password',
ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false
```

> The endpoint `minio:9000` is the internal Docker hostname. DuckDB is not running in a separate container — it runs in-process inside the Airflow Python task.

---

## Running the Full Pipeline End-to-End

```bash
# 1. Generate a daily batch
python scripts/simulate_stream.py --mode daily

# 2. In Airflow UI (http://localhost:8081), trigger in order:
#    01_ingest_bronze  →  02_process_silver  →  03_process_gold
```

Each DAG must **fully succeed** before triggering the next.
