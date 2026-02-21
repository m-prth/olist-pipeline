import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import pydeck as pdk

st.set_page_config(page_title="Olist E-Commerce Intelligence", layout="wide")

# --- 1. MINIO CONNECTION ---
@st.cache_resource
def get_db_connection():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='admin';
        SET s3_secret_access_key='password';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con

con = get_db_connection()

st.title("🇧🇷 Olist Business Intelligence Dashboard")
st.markdown("### Real-time Insights from the Gold Data Lake")

# --- 2. DATA LOADING ---
@st.cache_data
def load_metrics():
    # Base KPIs
    kpis = con.execute("""
        SELECT 
            COUNT(DISTINCT order_id) as total_orders,
            CAST(SUM(is_late::INT) AS FLOAT) / COUNT(*) * 100 as late_rate
        FROM read_parquet('s3://olist-lake/gold/fact_orders/*.parquet')
    """).df()
    
    # Process Mining KPI
    lifecycle = con.execute("""
        SELECT AVG(total_delivery_days) as avg_delivery_days
        FROM read_parquet('s3://olist-lake/gold/fact_order_lifecycle/*.parquet')
    """).df()
    
    return kpis, lifecycle

kpis, lifecycle = load_metrics()

# --- 3. ROW 1: TOP LEVEL KPIS ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Orders", f"{int(kpis['total_orders'][0]):,}")
col2.metric("Late Delivery Rate", f"{kpis['late_rate'][0]:.2f}%")
col3.metric("Avg Delivery Time", f"{lifecycle['avg_delivery_days'][0]:.1f} Days")
col4.metric("System Status", "Healthy", delta="Online")

st.divider()

# --- 4. ROW 2: TEMPORAL & SPATIAL CHARTS ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Daily Order Volume")
    # Time Series from Fact Orders
    trend_df = con.execute("""
        SELECT date_key, COUNT(order_id) as daily_orders 
        FROM read_parquet('s3://olist-lake/gold/fact_orders/*.parquet')
        GROUP BY date_key
        ORDER BY date_key
    """).df()
    
    fig_trend = px.line(trend_df, x="date_key", y="daily_orders", template="plotly_white")
    st.plotly_chart(fig_trend, use_container_width=True)

with col_right:
    st.subheader("Top States by Orders")
    state_df = con.execute("""
        SELECT customer_state, COUNT(*) as count 
        FROM read_parquet('s3://olist-lake/gold/dim_customers/*.parquet')
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).df()
    fig_bar = px.bar(state_df, x='customer_state', y='count', color='count', color_continuous_scale="Blues")
    st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# --- 5. ROW 3: THE 3D SHIPPING MAP ---
st.subheader("Live Shipping Network")

# 1. Get the maximum distance to set the slider limits dynamically
max_dist = con.execute("""
    SELECT MAX(distance_km) 
    FROM read_parquet('s3://olist-lake/gold/fact_shipping_network/*.parquet')
""").fetchone()[0]

# 2. Add the Streamlit Slider
selected_distance = st.slider(
    "Filter by Maximum Shipping Distance (km):", 
    min_value=0, 
    max_value=int(max_dist), 
    value=int(max_dist)
)

# 3. Query the data using the slider's value
map_data = con.execute(f"""
    SELECT 
        cust_lng, cust_lat, 
        sell_lng, sell_lat 
    FROM read_parquet('s3://olist-lake/gold/fact_shipping_network/*.parquet')
    WHERE distance_km <= {selected_distance}
    LIMIT 2000
""").df()

# 4. Render the PyDeck Map
arc_layer = pdk.Layer(
    "ArcLayer",
    data=map_data,
    get_source_position=["sell_lng", "sell_lat"],
    get_target_position=["cust_lng", "cust_lat"],
    get_source_color=[200, 30, 0, 160], 
    get_target_color=[0, 150, 200, 160], 
    get_width=2,
)

st.pydeck_chart(pdk.Deck(
    map_style="dark",
    initial_view_state=pdk.ViewState(
        latitude=-15.0,
        longitude=-50.0,
        zoom=3.5,
        pitch=45, 
    ),
    layers=[arc_layer]
))

# --- 6. ROW 4: DATA EXPLORER ---
st.subheader("🗄️ Raw Data Explorer (Gold Layer)")
st.markdown("Preview the first 10 rows of models stored in the Gold lake.")

gold_tables = {
    "Fact Orders": "s3://olist-lake/gold/fact_orders/*.parquet",
    "Fact Order Lifecycle": "s3://olist-lake/gold/fact_order_lifecycle/*.parquet",
    "Fact Shipping Network": "s3://olist-lake/gold/fact_shipping_network/*.parquet",
    "Dim Customers": "s3://olist-lake/gold/dim_customers/*.parquet",
    "Dim Sellers": "s3://olist-lake/gold/dim_sellers/*.parquet"
}

tabs = st.tabs(list(gold_tables.keys()))

for tab, (name, path) in zip(tabs, gold_tables.items()):
    with tab:
        st.write(f"Query: `SELECT * FROM read_parquet('{path}') LIMIT 10`")
        try:
            df_preview = con.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 10").df()
            st.dataframe(df_preview, use_container_width=True)
        except Exception as e:
            st.error(f"Error loading {name}: {e}")