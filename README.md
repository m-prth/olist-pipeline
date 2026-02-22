# 🇧🇷 Olist E-Commerce Data Pipeline

A fully containerized, end-to-end **Data Engineering Pipeline** built on the [Olist E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). It implements a **Medallion Architecture** (Bronze → Silver → Gold) to ingest, clean, and serve analytics-ready data — a "Data Platform in a Box."

---

## 🏗️ Architecture

```mermaid
graph LR
    A[📁 data/input\nLanding Zone] -->|DAG 01| B
    subgraph B["🥉 Bronze (MinIO)"]
        B1[orders.parquet]
        B2[customers.parquet]
        B3[...]
    end
    B -->|DAG 02 · Polars| C
    subgraph C["🥈 Silver (MinIO)"]
        C1[Deduplicated\nParquet files]
    end
    C -->|DAG 03 · dbt| D
    subgraph D["🥇 Gold (MinIO)"]
        D1[fact_orders]
        D2[dim_customers]
        D3[fact_shipping_network]
        D4[...]
    end
    D --> E[📊 Streamlit Dashboard]
```

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Orchestration | Apache Airflow 2.8.1 | DAG scheduling & task management |
| Storage | MinIO (S3-compatible) | Data Lake for all layers |
| Processing (Silver) | Polars | Fast dataframe deduplication |
| Processing (Gold) | dbt + DuckDB | SQL dimensional modeling via dbt-duckdb |
| Visualization | Streamlit + PyDeck + Plotly | BI Dashboard with 3D shipping map |
| Infrastructure | Docker Compose | Containerized local environment |
| Metadata DB | PostgreSQL 13 | Airflow backend |

---

## 🚀 Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Python 3.9+](https://www.python.org/)
- Olist dataset CSVs placed in `data/raw_kaggle/` (download from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce))

### 1. Clone & Configure
```bash
git clone <your-repo-url>
cd olist-pipeline
```

Create a `.env` file in the project root:
```env
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
```

### 2. Start Infrastructure
```bash
docker-compose up -d
```

Wait ~60 seconds for all services to initialize, then verify:

| Service | URL |
|---|---|
| Airflow | http://localhost:8081 |
| MinIO | http://localhost:9001 |

### 3. Simulate & Run the Pipeline

**Step 1:** Generate the first batch of daily data:
```bash
python scripts/simulate_stream.py --mode daily
```

**Step 2:** In the Airflow UI, trigger the DAGs in order:
1. `01_ingest_bronze`
2. `02_process_silver`
3. `03_process_gold`

**Step 3:** Run the dashboard:
```bash
pip install streamlit plotly pydeck duckdb
streamlit run scripts/dashboard.py
```

---

## 📁 Project Structure

```
olist-pipeline/
├── dags/                        # Airflow DAGs
│   ├── 01_ingest_bronze.py      # Raw CSV → MinIO Bronze
│   ├── 02_process_silver.py     # Deduplicate → MinIO Silver
│   └── 03_process_gold.py       # dbt run → MinIO Gold
├── dbt_project/                 # dbt models (Gold layer)
│   ├── models/
│   │   ├── staging/             # Views on Silver Parquet
│   │   │   ├── sources.yml
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_sellers.sql
│   │   │   ├── stg_order_items.sql
│   │   │   └── stg_geolocation.sql
│   │   └── marts/               # External Parquet tables
│   │       ├── schema.yml
│   │       ├── dim_customers.sql
│   │       ├── dim_sellers.sql
│   │       ├── fact_orders.sql
│   │       ├── fact_order_lifecycle.sql
│   │       └── fact_shipping_network.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── scripts/
│   ├── simulate_stream.py       # Daily/backfill data generator
│   ├── backfill_data.py         # Bulk historical data loader
│   └── dashboard.py             # Streamlit BI dashboard
├── config/
│   └── trino/catalog/           # Trino connector configs (future)
├── data/
│   ├── raw_kaggle/              # Source CSVs from Kaggle
│   ├── input/                   # Daily landing zone (watched by Airflow)
│   └── archive/                 # Processed files archive
├── docker-compose.yaml
├── requirements.txt
└── .env                         # Credentials (not committed)
```

---

## 📚 Documentation

| Doc | Description |
|---|---|
| [Setup Guide](docs/setup.md) | Detailed environment setup walkthrough |
| [Architecture](docs/architecture.md) | Medallion layers, schemas, and technology rationale |
| [DAG Reference](docs/dag_reference.md) | All Airflow DAGs explained |
| [Data Simulation](docs/data_simulation.md) | How to use `simulate_stream.py` |
| [Dashboard Guide](docs/dashboard.md) | How to run and use the Streamlit dashboard |

---

## 🗺️ Roadmap

- [x] Implement dbt project for Gold transformations (dbt-duckdb)
- [x] Add dbt tests (`unique`, `not_null`) on mart models
- [ ] Add Trino as a SQL query layer over Gold Parquet files
- [ ] Add Metabase for BI dashboards connected to Trino
- [ ] Add `dim_products` and `dim_date` to the Gold layer
- [ ] Add `fact_order_items` and `fact_payments` fact tables
