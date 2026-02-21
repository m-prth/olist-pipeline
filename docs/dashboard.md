# 📊 Streamlit Dashboard Guide

The project includes a real-time BI dashboard built with **Streamlit**, **Plotly**, and **PyDeck**. It connects directly to the Gold Data Lake in MinIO via DuckDB and requires no additional infrastructure beyond what's already running.

---

## Prerequisites

Before running the dashboard, ensure:
1. Docker services are running (`docker-compose up -d`)
2. The Gold layer has been populated by running all three DAGs at least once
3. Dashboard Python dependencies are installed:

```bash
pip install streamlit plotly pydeck duckdb
```

---

## Running the Dashboard

From the project root:

```bash
streamlit run scripts/dashboard.py
```

Streamlit will open the dashboard at **http://localhost:8501** automatically.

---

## Dashboard Sections

### 1. KPI Cards (Top Row)

Four at-a-glance metrics derived from the Gold layer:

| Metric | Source Table | Description |
|---|---|---|
| Total Orders | `fact_orders` | Count of distinct order IDs |
| Late Delivery Rate | `fact_orders` | % of orders where `is_late = True` |
| Avg Delivery Time | `fact_order_lifecycle` | Average `total_delivery_days` for delivered orders |
| System Status | Static | Always "Healthy / Online" |

---

### 2. Daily Order Volume (Line Chart)

A time-series chart showing the number of orders placed per day, pulled from `fact_orders` grouped by `date_key`.

---

### 3. Top States by Orders (Bar Chart)

A horizontal bar chart showing the top 10 Brazilian states by order count, queried from `dim_customers`.

---

### 4. 3D Shipping Network Map

The centrepiece visualization — a **PyDeck ArcLayer** rendering a sample of 2,000 shipping routes across Brazil in a tilted 3D view.

- 🔴 **Red arcs** originate at the seller's location
- 🔵 **Blue arcs** terminate at the customer's location
- Data sourced from `fact_shipping_network` (which includes pre-computed lat/lng for both endpoints)

> The map is limited to 2,000 routes to keep browser rendering performance fast.

**Map Controls:**
- Drag to pan
- Scroll to zoom
- Right-click drag to rotate/tilt

---

### 5. Raw Data Explorer (Tabbed)

A section at the bottom of the dashboard that previews the first 10 rows of each Gold table in a separate tab. Useful for quick data validation.

**Tabs:**
- `Fact Orders`
- `Fact Order Lifecycle`
- `Fact Shipping Network`
- `Dim Customers`
- `Dim Sellers`

Each tab shows the exact SQL query executed and displays an interactive table with sortable columns.

---

## Configuration

The dashboard connects to MinIO using DuckDB's `httpfs` extension. Connection settings are hardcoded at the top of `scripts/dashboard.py`:

```python
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='admin';
SET s3_secret_access_key='password';
SET s3_use_ssl=false;
SET s3_url_style='path';
```

> If you've changed your MinIO credentials in `.env`, update these values accordingly.

---

## Caching

- `@st.cache_resource` is used for the DuckDB connection (created once per session)
- `@st.cache_data` is used for KPI metrics (cached until the app is restarted)

To force a data refresh, use the **"Clear cache"** option in the Streamlit menu (top-right `⋮` icon).
