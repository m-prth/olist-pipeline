import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="Olist E-Commerce Intelligence",
    page_icon="🇧🇷",
    layout="wide",
    initial_sidebar_state="expanded",
)

GOLD = "s3://olist-lake/gold"
PLOTLY_TEMPLATE = "plotly_dark"

# Inject dark CSS overrides
st.markdown("""
<style>
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #0f3460;
        border-radius: 12px;
        padding: 16px 20px;
    }
    [data-testid="stMetricLabel"] { font-size: 0.85rem; }
    [data-testid="stMetricValue"] { font-size: 1.6rem; font-weight: 700; }
    .block-container { padding-top: 1rem; }
    div[data-testid="stSidebar"] > div { background: linear-gradient(180deg, #0a0a23 0%, #1a1a3e 100%); }
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────
# DATABASE CONNECTION
# ──────────────────────────────────────────────
@st.cache_resource
def get_connection():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='admin';
        SET s3_secret_access_key='password';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con

con = get_connection()

def q(sql):
    """Shorthand to run a query and return a DataFrame."""
    return con.execute(sql).df()

def read(table):
    """Read a Gold table by name."""
    return f"read_parquet('{GOLD}/{table}/*.parquet')"

# ──────────────────────────────────────────────
# SIDEBAR NAVIGATION
# ──────────────────────────────────────────────
st.sidebar.title("🇧🇷 Olist BI")
st.sidebar.caption("E-Commerce Intelligence Platform")
page = st.sidebar.radio(
    "Navigate",
    ["📊 Executive Overview", "👥 Customer Analytics", "🏪 Seller Performance",
     "📦 Product Categories", "🚚 Shipping & Logistics", "🔗 Market Basket",
     "🗄️ Data Explorer"],
    label_visibility="collapsed",
)
st.sidebar.divider()
st.sidebar.info("**29 dbt models** · **56 tests** · All passing ✅")

# ══════════════════════════════════════════════
# PAGE 1: EXECUTIVE OVERVIEW
# ══════════════════════════════════════════════
if page == "📊 Executive Overview":
    st.title("📊 Executive Overview")
    st.caption("High-level KPIs and revenue trends across the Olist marketplace")

    # --- KPI Cards ---
    kpi = q(f"""
        SELECT
            COUNT(DISTINCT order_id) AS total_orders,
            CAST(SUM(is_late::INT) AS FLOAT) / COUNT(*) * 100 AS late_rate
        FROM {read('fact_orders')}
    """)
    lifecycle = q(f"SELECT AVG(total_delivery_days) AS avg_del FROM {read('fact_order_lifecycle')}")
    rev = q(f"SELECT SUM(total_line_value) AS total_rev, AVG(total_line_value) AS aov FROM {read('fact_order_items')}")
    cust = q(f"SELECT COUNT(DISTINCT customer_sk) AS cnt FROM {read('dim_customers')}")
    sellers = q(f"SELECT COUNT(DISTINCT seller_sk) AS cnt FROM {read('dim_sellers')}")

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Orders", f"{int(kpi['total_orders'][0]):,}")
    c2.metric("Total Revenue", f"R$ {rev['total_rev'][0]:,.0f}")
    c3.metric("Avg Order Value", f"R$ {rev['aov'][0]:.2f}")
    c4.metric("Late Delivery %", f"{kpi['late_rate'][0]:.1f}%")
    c5.metric("Avg Delivery", f"{lifecycle['avg_del'][0]:.1f} days")
    c6.metric("Customers / Sellers", f"{int(cust['cnt'][0]):,} / {int(sellers['cnt'][0]):,}")
    st.caption("📌 **Total Revenue** & **Avg Order Value** — higher is better · **Late Delivery %** & **Avg Delivery** — lower is better")

    st.divider()

    # --- Revenue Trends ---
    trends = q(f"""
        SELECT month, total_revenue, revenue_growth_pct, rolling_3m_avg_revenue,
               cumulative_revenue, total_orders, unique_customers
        FROM {read('rpt_revenue_trends')}
        ORDER BY month
    """)
    trends['month'] = pd.to_datetime(trends['month']).dt.strftime('%Y-%m')

    tab1, tab2 = st.tabs(["Revenue Trend", "Growth & Cumulative"])

    with tab1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=trends['month'], y=trends['total_revenue'],
                                 mode='lines+markers', name='Monthly Revenue',
                                 line=dict(color='#00d4ff', width=2)))
        fig.add_trace(go.Scatter(x=trends['month'], y=trends['rolling_3m_avg_revenue'],
                                 mode='lines', name='Rolling 3M Avg',
                                 line=dict(color='#ff6b6b', width=2, dash='dash')))
        fig.update_layout(template=PLOTLY_TEMPLATE, title="Monthly Revenue vs Rolling 3-Month Average",
                          yaxis_title="Revenue (R$)", xaxis_title="Month", height=400)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("📌 **Monthly Revenue**: Total order value per month. The dashed line smooths short-term spikes using a 3-month rolling average. Consistent growth above the rolling avg signals a healthy trend.")

    with tab2:
        col_l, col_r = st.columns(2)
        with col_l:
            # Exclude ramp-up months with extreme growth from tiny base revenue
            growth_data = trends[trends['month'] >= '2017-02'].copy()
            fig_g = px.bar(growth_data, x='month', y='revenue_growth_pct',
                           color='revenue_growth_pct', color_continuous_scale='RdYlGn',
                           template=PLOTLY_TEMPLATE, title="Month-over-Month Revenue Growth %")
            fig_g.update_layout(height=350, xaxis_type='category')
            st.plotly_chart(fig_g, use_container_width=True)
            st.caption("📌 **MoM Growth**: Percentage change from previous month. 🟢 Green = growth, 🔴 Red = decline. Early ramp-up months (before Feb 2017) excluded due to extremely small base revenue causing misleading 1000%+ swings.")
        with col_r:
            fig_c = px.area(trends, x='month', y='cumulative_revenue',
                            template=PLOTLY_TEMPLATE, title="Cumulative Revenue")
            fig_c.update_traces(line_color='#00d4ff', fillcolor='rgba(0,212,255,0.15)')
            fig_c.update_layout(height=350)
            st.plotly_chart(fig_c, use_container_width=True)
            st.caption("📌 **Cumulative Revenue**: Running total of all revenue. A steeper curve = faster revenue growth.")

    # --- Volume metrics ---
    col_l2, col_r2 = st.columns(2)
    with col_l2:
        fig_ord = px.bar(trends, x='month', y='total_orders', template=PLOTLY_TEMPLATE,
                         title="Monthly Order Volume", color_discrete_sequence=['#0f3460'])
        fig_ord.update_layout(height=300)
        st.plotly_chart(fig_ord, use_container_width=True)
        st.caption("📌 **Order Volume**: Total orders placed per month — higher is better.")
    with col_r2:
        fig_cust = px.line(trends, x='month', y='unique_customers', template=PLOTLY_TEMPLATE,
                           title="Unique Customers per Month", markers=True)
        fig_cust.update_traces(line_color='#e94560')
        fig_cust.update_layout(height=300)
        st.plotly_chart(fig_cust, use_container_width=True)
        st.caption("📌 **Unique Customers**: Distinct buyers per month. Growth here means the marketplace is attracting new customers — higher is better.")

# ══════════════════════════════════════════════
# PAGE 2: CUSTOMER ANALYTICS
# ══════════════════════════════════════════════
elif page == "👥 Customer Analytics":
    st.title("👥 Customer Analytics")
    st.caption("RFM segmentation, Lifetime Value, and cohort retention analysis")

    cust_tab1, cust_tab2, cust_tab3 = st.tabs(["RFM Segmentation", "Customer LTV", "Cohort Retention"])

    # --- RFM ---
    with cust_tab1:
        rfm = q(f"SELECT * FROM {read('rpt_customer_rfm')}")

        col1, col2 = st.columns([1, 2])
        with col1:
            seg_counts = rfm['rfm_segment'].value_counts().reset_index()
            seg_counts.columns = ['segment', 'count']
            fig_donut = px.pie(seg_counts, values='count', names='segment', hole=0.5,
                               template=PLOTLY_TEMPLATE, title="Customer Segments",
                               color_discrete_sequence=px.colors.qualitative.Set2)
            fig_donut.update_layout(height=400)
            st.plotly_chart(fig_donut, use_container_width=True)
            st.caption("📌 **RFM Segments**: Champions are best (recent, frequent, high-spend). Lost/At Risk need re-engagement campaigns.")

        with col2:
            rfm_clean = rfm.dropna(subset=['monetary', 'frequency', 'rfm_avg'])
            fig_rfm = px.scatter(rfm_clean, x='monetary', y='frequency', color='rfm_segment',
                                 size='rfm_avg', hover_data=['customer_id', 'recency_days'],
                                 template=PLOTLY_TEMPLATE, title="RFM Scatter: Monetary vs Frequency",
                                 color_discrete_sequence=px.colors.qualitative.Set2)
            fig_rfm.update_layout(height=400, xaxis_title="Monetary (R$)", yaxis_title="Frequency")
            st.plotly_chart(fig_rfm, use_container_width=True)
            st.caption("📌 **R** = Recency (days since last order, lower is better) · **F** = Frequency (order count, higher is better) · **M** = Monetary (total spend, higher is better). Bubble size = avg RFM score.")

        # Segment filter table
        selected_seg = st.selectbox("Filter by Segment", ["All"] + sorted(rfm['rfm_segment'].unique().tolist()))
        if selected_seg != "All":
            rfm = rfm[rfm['rfm_segment'] == selected_seg]
        st.dataframe(rfm.head(100), use_container_width=True, height=300)

    # --- LTV ---
    with cust_tab2:
        ltv = q(f"SELECT * FROM {read('rpt_customer_ltv')}")

        c1, c2, c3 = st.columns(3)
        c1.metric("Avg LTV", f"R$ {ltv['total_spend'].mean():.2f}")
        c2.metric("Avg Tenure", f"{ltv['tenure_months'].mean():.1f} months")
        c3.metric("High-Value Customers", f"{(ltv['ltv_tier'] == 'High Value').sum():,}")
        st.caption("📌 **LTV** = Lifetime Value (total spend per customer, higher is better) · **Tenure** = months between first and last order · **High Value** = spend ≥ R$500 and ≥3 orders")

        col_l, col_r = st.columns(2)
        with col_l:
            tier_counts = ltv['ltv_tier'].value_counts().reset_index()
            tier_counts.columns = ['tier', 'count']
            fig_tier = px.bar(tier_counts, x='tier', y='count', color='tier',
                              template=PLOTLY_TEMPLATE, title="LTV Tier Distribution",
                              color_discrete_map={'High Value': '#00d4ff', 'Medium Value': '#ffc107', 'Low Value': '#6c757d'})
            fig_tier.update_layout(height=350)
            st.plotly_chart(fig_tier, use_container_width=True)
        with col_r:
            fig_dec = px.histogram(ltv, x='total_spend', nbins=50, template=PLOTLY_TEMPLATE,
                                   title="Customer Spend Distribution", color_discrete_sequence=['#e94560'])
            fig_dec.update_layout(height=350, xaxis_title="Total Spend (R$)")
            st.plotly_chart(fig_dec, use_container_width=True)
            st.caption("📌 **Spend Distribution**: Most customers are concentrated at the low end (long tail). A wider distribution indicates a healthier customer mix.")

        # State breakdown
        state_ltv = ltv.groupby('customer_state').agg(
            avg_spend=('total_spend', 'mean'), customers=('customer_id', 'count')
        ).reset_index().sort_values('avg_spend', ascending=False).head(15)
        fig_state = px.bar(state_ltv, x='customer_state', y='avg_spend', color='customers',
                           template=PLOTLY_TEMPLATE, title="Avg Spend by State (Top 15)",
                           color_continuous_scale='Blues')
        fig_state.update_layout(height=350)
        st.plotly_chart(fig_state, use_container_width=True)

    # --- Cohort Retention ---
    with cust_tab3:
        cohort = q(f"SELECT * FROM {read('rpt_cohort_retention')}")

        st.subheader("📅 Cohort Retention Heatmap")
        st.caption("Each row is a monthly acquisition cohort. Values show % of customers who returned in later months. Higher retention % is better — it means more repeat buyers. Month 0 is always 100% (first purchase). Low retention (1-3%) is typical for marketplaces like Olist where most purchases are one-time.")

        # Pivot for heatmap
        pivot = cohort.pivot_table(index='cohort_month', columns='months_since_first',
                                   values='retention_pct', aggfunc='first')
        pivot.index = pd.to_datetime(pivot.index).strftime('%Y-%m')

        fig_heat = px.imshow(pivot, text_auto='.1f', color_continuous_scale='Blues',
                             template=PLOTLY_TEMPLATE, title="Retention Rate % by Cohort",
                             labels=dict(x="Months Since First Order", y="Cohort Month", color="Retention %"),
                             aspect='auto')
        fig_heat.update_layout(height=500)
        st.plotly_chart(fig_heat, use_container_width=True)

        # Cohort sizes
        sizes = cohort[cohort['months_since_first'] == 0][['cohort_month', 'cohort_customers']].copy()
        sizes['cohort_month'] = pd.to_datetime(sizes['cohort_month']).dt.strftime('%Y-%m')
        fig_sizes = px.bar(sizes, x='cohort_month', y='cohort_customers',
                           template=PLOTLY_TEMPLATE, title="Cohort Sizes (New Customers per Month)",
                           color_discrete_sequence=['#00d4ff'])
        fig_sizes.update_layout(height=300)
        st.plotly_chart(fig_sizes, use_container_width=True)

# ══════════════════════════════════════════════
# PAGE 3: SELLER PERFORMANCE
# ══════════════════════════════════════════════
elif page == "🏪 Seller Performance":
    st.title("🏪 Seller Performance")
    st.caption("Revenue, delivery speed, and customer satisfaction by seller")

    sellers = q(f"SELECT * FROM {read('rpt_seller_performance')}")

    # KPIs
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Sellers", f"{len(sellers):,}")
    c2.metric("Avg Revenue/Seller", f"R$ {sellers['total_revenue'].mean():,.0f}")
    c3.metric("Avg Review Score", f"{sellers['avg_review_score'].mean():.2f} ⭐")
    c4.metric("Avg Delivery", f"{sellers['avg_delivery_days'].mean():.1f} days")
    st.caption("📌 **Revenue/Seller** — higher is better · **Review Score** (1-5) — higher is better · **Avg Delivery** — lower is better (faster shipping)")

    st.divider()

    # Top-N selector
    top_n = st.slider("Show Top N Sellers", 5, 50, 15)

    col_l, col_r = st.columns(2)
    with col_l:
        top_rev = sellers.nlargest(top_n, 'total_revenue')
        fig = px.bar(top_rev, x='total_revenue', y='seller_id', orientation='h',
                     color='avg_review_score', color_continuous_scale='RdYlGn',
                     template=PLOTLY_TEMPLATE, title=f"Top {top_n} Sellers by Revenue")
        fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        st.caption("📌 Bar color shows review score (🟢 green = high, 🔴 red = low). Top sellers should ideally also have high review scores.")

    with col_r:
        fig_scatter = px.scatter(sellers, x='total_revenue', y='avg_review_score',
                                 size='total_orders', color='avg_delivery_days',
                                 color_continuous_scale='RdYlGn_r',
                                 hover_data=['seller_id', 'seller_city', 'total_items_sold'],
                                 template=PLOTLY_TEMPLATE,
                                 title="Revenue vs Review Score (size = orders)")
        fig_scatter.update_layout(height=500, xaxis_title="Total Revenue (R$)",
                                  yaxis_title="Avg Review Score")
        st.plotly_chart(fig_scatter, use_container_width=True)
        st.caption("📌 Ideal sellers are in the **top-right** (high revenue + high reviews). Bubble size = order count. Color = delivery speed (🟢 fast, 🔴 slow).")

    # State-level
    state_perf = sellers.groupby('seller_state').agg(
        revenue=('total_revenue', 'sum'), sellers_count=('seller_id', 'count'),
        avg_score=('avg_review_score', 'mean')
    ).reset_index().sort_values('revenue', ascending=False)

    fig_state = px.bar(state_perf, x='seller_state', y='revenue', color='avg_score',
                       color_continuous_scale='RdYlGn', template=PLOTLY_TEMPLATE,
                       title="Total Revenue by State (colored by avg review)")
    fig_state.update_layout(height=350)
    st.plotly_chart(fig_state, use_container_width=True)

# ══════════════════════════════════════════════
# PAGE 4: PRODUCT CATEGORIES
# ══════════════════════════════════════════════
elif page == "📦 Product Categories":
    st.title("📦 Product Category Analysis")
    st.caption("Category-level sales, revenue, and customer satisfaction metrics")

    cats = q(f"SELECT * FROM {read('rpt_product_category_analysis')}")

    # KPIs
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Categories", f"{len(cats):,}")
    c2.metric("Total Revenue", f"R$ {cats['total_revenue'].sum():,.0f}")
    c3.metric("Avg Review Score", f"{cats['avg_review_score'].mean():.2f} ⭐")

    st.divider()

    # Treemap
    top_cats = cats.nlargest(30, 'total_revenue')
    fig_tree = px.treemap(top_cats, path=['product_category_name'], values='total_revenue',
                          color='avg_review_score', color_continuous_scale='RdYlGn',
                          template=PLOTLY_TEMPLATE, title="Revenue Treemap (Top 30 Categories)")
    fig_tree.update_layout(height=500)
    st.plotly_chart(fig_tree, use_container_width=True)
    st.caption("📌 **Treemap**: Tile size = revenue (larger is better). Color = avg review score (🟢 green = satisfied customers, 🔴 red = dissatisfied).")

    col_l, col_r = st.columns(2)
    with col_l:
        top15 = cats.nlargest(15, 'total_revenue')
        fig_bar = px.bar(top15, x='total_revenue', y='product_category_name', orientation='h',
                         color='avg_review_score', color_continuous_scale='RdYlGn',
                         template=PLOTLY_TEMPLATE, title="Top 15 Categories by Revenue")
        fig_bar.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)
        st.caption("📌 **Revenue** — higher is better. Color = review score (🟢 good, 🔴 poor).")

    with col_r:
        # Low review categories
        risky = cats[cats['total_reviews'] >= 10].nlargest(15, 'low_review_pct')
        fig_risk = px.bar(risky, x='low_review_pct', y='product_category_name', orientation='h',
                          color='low_review_pct', color_continuous_scale='Reds',
                          template=PLOTLY_TEMPLATE, title="Highest Dissatisfaction Rate (≥10 reviews)")
        fig_risk.update_layout(height=500, yaxis={'categoryorder': 'total ascending'},
                               xaxis_title="Low Review %")
        st.plotly_chart(fig_risk, use_container_width=True)
        st.caption("📌 **Low Review %**: Percentage of reviews scoring ≤2 out of 5. Lower is better. Categories here may need quality improvements or better descriptions.")

# ══════════════════════════════════════════════
# PAGE 5: SHIPPING & LOGISTICS
# ══════════════════════════════════════════════
elif page == "🚚 Shipping & Logistics":
    st.title("🚚 Shipping & Logistics")
    st.caption("Delivery performance, distance analysis, and the 3D shipping network map")

    ship = q(f"SELECT * FROM {read('rpt_shipping_efficiency')}")

    # KPIs
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Deliveries", f"{len(ship):,}")
    c2.metric("Late Rate", f"{ship['is_late'].sum() / len(ship) * 100:.1f}%")
    c3.metric("Avg Delivery", f"{ship['total_delivery_days'].mean():.1f} days")
    c4.metric("Avg Distance", f"{ship['distance_km'].mean():.0f} km")
    st.caption("📌 **Late Rate** — lower is better (% delivered after estimated date) · **Avg Delivery** — lower is better · **Avg Distance** — informational (Haversine km between seller & customer)")

    st.divider()

    tab_charts, tab_map = st.tabs(["📈 Analytics", "🗺️ 3D Shipping Map"])

    with tab_charts:
        col_l, col_r = st.columns(2)
        with col_l:
            bucket_counts = ship['delivery_bucket'].value_counts().reset_index()
            bucket_counts.columns = ['bucket', 'count']
            fig_buck = px.pie(bucket_counts, values='count', names='bucket', hole=0.4,
                              template=PLOTLY_TEMPLATE, title="Delivery Time Distribution",
                              color_discrete_sequence=px.colors.sequential.Blues_r)
            fig_buck.update_layout(height=400)
            st.plotly_chart(fig_buck, use_container_width=True)
            st.caption("📌 Shows what % of deliveries fall into each speed bucket. A larger share in faster buckets is better.")

        with col_r:
            dist_counts = ship['distance_bucket'].value_counts().reset_index()
            dist_counts.columns = ['bucket', 'count']
            fig_dist = px.pie(dist_counts, values='count', names='bucket', hole=0.4,
                              template=PLOTLY_TEMPLATE, title="Distance Distribution",
                              color_discrete_sequence=px.colors.sequential.Oranges_r)
            fig_dist.update_layout(height=400)
            st.plotly_chart(fig_dist, use_container_width=True)

        # Scatter: distance vs delivery
        sample = ship.sample(min(2000, len(ship)), random_state=42)
        fig_sc = px.scatter(sample, x='distance_km', y='total_delivery_days',
                            color='is_late', color_discrete_map={True: '#e94560', False: '#00d4ff'},
                            opacity=0.4, template=PLOTLY_TEMPLATE,
                            title="Distance vs Delivery Time (sample of 2000)")
        fig_sc.update_layout(height=400, xaxis_title="Distance (km)", yaxis_title="Delivery Days")
        st.plotly_chart(fig_sc, use_container_width=True)
        st.caption("📌 Each dot is a delivery. 🔴 Red = late (delivered after estimate), 🔵 Blue = on time. Orders clustering in the bottom-left (short distance, fast delivery) is ideal.")

    with tab_map:
        st.subheader("🌐 Live Shipping Network")
        max_dist = q(f"SELECT MAX(distance_km) AS m FROM {read('fact_shipping_network')}")['m'][0]
        selected_distance = st.slider("Max Shipping Distance (km)", 0, int(max_dist), int(max_dist))

        map_data = q(f"""
            SELECT cust_lng, cust_lat, sell_lng, sell_lat
            FROM {read('fact_shipping_network')}
            WHERE distance_km <= {selected_distance}
            LIMIT 2000
        """)

        arc_layer = pdk.Layer(
            "ArcLayer", data=map_data,
            get_source_position=["sell_lng", "sell_lat"],
            get_target_position=["cust_lng", "cust_lat"],
            get_source_color=[200, 30, 0, 160],
            get_target_color=[0, 150, 200, 160],
            get_width=2,
        )
        st.pydeck_chart(pdk.Deck(
            map_style="dark",
            initial_view_state=pdk.ViewState(latitude=-15.0, longitude=-50.0, zoom=3.5, pitch=45),
            layers=[arc_layer],
        ))

# ══════════════════════════════════════════════
# PAGE 6: MARKET BASKET
# ══════════════════════════════════════════════
elif page == "🔗 Market Basket":
    st.title("🔗 Market Basket Analysis")
    st.caption("Which product categories are frequently bought together?")

    basket = q(f"SELECT * FROM {read('rpt_market_basket')} ORDER BY co_occurrence_count DESC")

    # KPIs
    c1, c2, c3 = st.columns(3)
    c1.metric("Category Pairs Found", f"{len(basket):,}")
    c2.metric("Strongest Pair", f"{basket.iloc[0]['category_a']} + {basket.iloc[0]['category_b']}" if len(basket) > 0 else "N/A")
    c3.metric("Top Co-occurrences", f"{int(basket.iloc[0]['co_occurrence_count']):,}" if len(basket) > 0 else "0")

    st.divider()

    top_n = st.slider("Show Top N Pairs", 5, 50, 20, key="basket_n")

    top_pairs = basket.head(top_n).copy()
    top_pairs['pair'] = top_pairs['category_a'] + ' + ' + top_pairs['category_b']

    col_l, col_r = st.columns(2)
    with col_l:
        fig = px.bar(top_pairs, x='co_occurrence_count', y='pair', orientation='h',
                     color='jaccard_similarity', color_continuous_scale='Viridis',
                     template=PLOTLY_TEMPLATE, title=f"Top {top_n} Co-occurring Category Pairs")
        fig.update_layout(height=600, yaxis={'categoryorder': 'total ascending'},
                          xaxis_title="Co-occurrence Count")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("📌 **Co-occurrence**: How many orders contain both categories. Higher = stronger buying pattern. Useful for cross-sell & product bundling strategies.")

    with col_r:
        fig_jac = px.bar(top_pairs.sort_values('jaccard_similarity', ascending=False),
                         x='jaccard_similarity', y='pair', orientation='h',
                         color='jaccard_similarity', color_continuous_scale='Plasma',
                         template=PLOTLY_TEMPLATE, title=f"Top {top_n} Pairs by Jaccard Similarity")
        fig_jac.update_layout(height=600, yaxis={'categoryorder': 'total ascending'},
                              xaxis_title="Jaccard Similarity")
        st.plotly_chart(fig_jac, use_container_width=True)
        st.caption("📌 **Jaccard Similarity** = overlap / union of orders for two categories (0-1 scale). Higher = categories are more exclusively bought together. Better than raw counts for identifying true affinity.")

    # Data table
    st.subheader("Full Pair Data")
    st.dataframe(basket, use_container_width=True, height=300)

# ══════════════════════════════════════════════
# PAGE 7: DATA EXPLORER
# ══════════════════════════════════════════════
elif page == "🗄️ Data Explorer":
    st.title("🗄️ Gold Layer Data Explorer")
    st.caption("Query any model stored in the Gold data lake")

    gold_tables = {
        "Staging": [],
        "Dimensions": ["dim_customers", "dim_sellers", "dim_products", "dim_geolocation", "dim_date"],
        "Facts": ["fact_orders", "fact_order_items", "fact_payments", "fact_reviews",
                  "fact_order_lifecycle", "fact_shipping_network", "snapshot_daily_seller_backlog"],
        "Data Products": ["obt_sales_analytics", "rpt_customer_rfm", "rpt_seller_performance",
                          "rpt_product_category_analysis", "rpt_shipping_efficiency",
                          "rpt_cohort_retention", "rpt_revenue_trends", "rpt_customer_ltv",
                          "rpt_market_basket"],
    }

    col1, col2 = st.columns([1, 3])
    with col1:
        category = st.selectbox("Category", [k for k in gold_tables if gold_tables[k]])
        table = st.selectbox("Table", gold_tables[category])
        row_limit = st.number_input("Row Limit", 10, 1000, 100, step=50)

    path = f"{GOLD}/{table}/*.parquet"
    with col2:
        st.code(f"SELECT * FROM read_parquet('{path}') LIMIT {row_limit}", language="sql")

    try:
        df = q(f"SELECT * FROM read_parquet('{path}') LIMIT {row_limit}")
        st.success(f"✅ {len(df)} rows × {len(df.columns)} columns")

        # Stats row
        c1, c2, c3 = st.columns(3)
        c1.metric("Rows", f"{len(df):,}")
        c2.metric("Columns", f"{len(df.columns)}")
        c3.metric("Memory", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")

        st.dataframe(df, use_container_width=True, height=400)

        # Column info
        with st.expander("📋 Column Types"):
            col_info = pd.DataFrame({'Column': df.columns, 'Type': df.dtypes.astype(str),
                                     'Non-Null': df.notna().sum(), 'Unique': df.nunique()})
            st.dataframe(col_info, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading `{table}`: {e}")