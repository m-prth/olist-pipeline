# ⚙️ Environment Setup Guide

This guide walks you through setting up the full local data platform on your machine from scratch.

---

## Prerequisites

Ensure the following are installed before you begin:

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop | Latest | Must be running before any step |
| Python | 3.9+ | For running scripts outside Docker |
| Git | Any | For cloning the repo |
| Kaggle account | — | To download the dataset |

---

## Step 1: Clone the Repository

```bash
git clone <your-repo-url>
cd olist-pipeline
```

---

## Step 2: Configure Credentials

Create a `.env` file in the project root. **Do not commit this file.**

```env
# MinIO (Data Lake)
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password

# PostgreSQL (Airflow Metadata)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
```

> The default credentials in the Docker Compose file match these values. Change them for any non-local deployment.

---

## Step 3: Download the Kaggle Dataset

1. Go to the [Olist Dataset on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
2. Download the ZIP and extract the CSVs.
3. Place **all CSV files** into `data/raw_kaggle/`:

```
data/raw_kaggle/
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_customers_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
├── olist_geolocation_dataset.csv
└── product_category_name_translation.csv
```

---

## Step 4: Start Infrastructure

```bash
docker-compose up -d
```

This command starts four services:
- `olist_postgres` — Airflow metadata database
- `olist_minio` — S3-compatible data lake storage
- `minio-init` — One-shot container that creates the `olist-lake` bucket
- `airflow-init` — One-shot container that initializes the Airflow DB and creates an admin user
- `airflow-webserver` — Airflow web UI
- `airflow-scheduler` — Airflow task runner

> **First startup takes 2–3 minutes** as Airflow installs dependencies and runs database migrations.

---

## Step 5: Verify Services

Once `docker-compose up -d` completes, confirm all services are healthy:

```bash
docker-compose ps
```

All services should show `running` or `healthy`.

Then open each UI:

| Service | URL | Default Credentials |
|---|---|---|
| Airflow | http://localhost:8081 | admin / admin |
| MinIO | http://localhost:9001 | admin / password |

---

## Step 6: Install Python Dependencies (Local)

For running scripts like `simulate_stream.py` and `dashboard.py` on your local machine (outside Docker):

```bash
python -m venv .venv
.venv\Scripts\activate   # Windows
pip install -r requirements.txt
pip install streamlit plotly pydeck   # Dashboard extras
```

---

## Stopping Services

```bash
docker-compose down
```

To also **delete all data** (volumes):
```bash
docker-compose down -v
```
