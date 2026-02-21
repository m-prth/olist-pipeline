# 🎲 Data Simulation Guide

Because the Olist dataset is a historical snapshot (not a live feed), the `scripts/simulate_stream.py` script is used to replay data day-by-day — mimicking the behavior of a real production pipeline.

---

## How It Works

The script reads all 9 raw CSV files from `data/raw_kaggle/`, filters records by `order_purchase_timestamp`, and exports the matching rows as Parquet batches into `data/input/<YYYY-MM-DD>/`.

### State Management

The script maintains state in `data/pipeline_state.json`:
```json
{"last_processed_time": "2016-09-01"}
```
Each successful run advances this date by one day, ensuring no date is processed twice.

---

## Modes

### `daily` Mode (Default)

Advances the state by one day and processes that date.

```bash
# Process the next chronological day (auto-increments from state file)
python scripts/simulate_stream.py --mode daily

# Process a specific date
python scripts/simulate_stream.py --mode daily --date 2017-06-15
```

Use `daily` mode to simulate new data arriving each day — run it before triggering the Airflow pipeline.

### `setup` Mode

Intended for running an initial bulk backfill. Use this to process a wide range of historical dates rapidly.

```bash
python scripts/simulate_stream.py --mode setup
```

---

## What Gets Exported Per Day

For a given `target_date`, the script identifies all orders placed on that date, then filters and exports all related records across all 9 tables:

| File | Filter Logic |
|---|---|
| `orders.parquet` | `order_purchase_timestamp` == target_date |
| `order_items.parquet` | Joined on active `order_id` |
| `order_payments.parquet` | Joined on active `order_id` |
| `order_reviews.parquet` | Joined on active `order_id` |
| `customers.parquet` | Joined on active `customer_id` |
| `sellers.parquet` | Joined on active `seller_id` (from items) |
| `products.parquet` | Joined on active `product_id` (from items) |
| `geolocation.parquet` | Full table (no filter — reference data) |
| `product_category_name_translation.parquet` | Full table (no filter — reference data) |

---

## Output Structure

```
data/input/
└── 2017-06-15/
    ├── orders.parquet
    ├── order_items.parquet
    ├── order_payments.parquet
    ├── order_reviews.parquet
    ├── customers.parquet
    ├── sellers.parquet
    ├── products.parquet
    ├── geolocation.parquet
    └── product_category_name_translation.parquet
```

This structure is what the `01_ingest_bronze` Airflow DAG expects.

---

## Running a Full Historical Backfill

To load all historical data (2016-09-01 to present):

```bash
# Method: run daily mode in a loop from a shell script
for date in $(seq ...) ; do
    python scripts/simulate_stream.py --mode daily --date $date
done
# Then trigger 01_ingest_bronze, 02_process_silver, 03_process_gold in Airflow
```

Alternatively, use `backfill_data.py` if available for bulk operations.

---

## Notes

- Dates with **no orders** are automatically skipped (script prints a "No orders found" message and does not advance state).
- The dataset covers orders from **September 2016 to October 2018**.
- Running the same date twice will **overwrite** the output folder in `data/input/`.
