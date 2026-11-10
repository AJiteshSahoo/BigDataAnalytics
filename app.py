"""
Streamlit Real-Time Payment Fraud Detection Dashboard with Distributed Spark & Kafka Analytics.

Features:
- Live updating metrics cards (throughput, fraud intercepted, fraud rate, latency).
- Streaming transaction table with color-coded risk status badges.
- Dedicated Fraud Alert Center displaying violated rules and explainability.
- Interactive controls for streaming speed, anomaly injection, and threshold tuning.
- Real-time Plotly charts for risk distribution and anomaly breakdowns.
- Distributed Big Data & MLlib Analytics tab:
  * Apache Kafka KRaft cluster telemetry and key-based partitioning metrics.
  * Apache Parquet partitioned Data Lake sink inspector.
  * Distributed PySpark Window analytics & Spark SQL regional aggregations.
  * PySpark MLlib Unsupervised K-Means clustering for behavioral user segmentation.
- Architecture & Theory tab for major project viva and technical defense.
"""

import os
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from models import RiskStatus
from pipeline import get_pipeline

# Configure Streamlit page layout and appearance
st.set_page_config(
    page_title="StreamGuard | Distributed Fraud Engine",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for sleek executive dashboard aesthetics
st.markdown("""
<style>
    .main { background-color: #0e1117; }
    .metric-card {
        background: linear-gradient(135deg, #1e222d 0%, #151821 100%);
        border: 1px solid #2d3345;
        border-radius: 10px;
        padding: 16px 20px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
    }
    .metric-label { font-size: 0.85rem; color: #8b949e; text-transform: uppercase; font-weight: 600; letter-spacing: 0.5px; }
    .metric-val { font-size: 1.8rem; font-weight: 700; color: #f0f6fc; margin: 4px 0; }
    .metric-sub { font-size: 0.78rem; color: #58a6ff; font-weight: 500; }
    .status-badge-declined { background-color: #f85149; color: white; padding: 3px 8px; border-radius: 4px; font-weight: 600; font-size: 0.8rem; }
    .status-badge-review { background-color: #d29922; color: black; padding: 3px 8px; border-radius: 4px; font-weight: 600; font-size: 0.8rem; }
    .status-badge-approved { background-color: #238636; color: white; padding: 3px 8px; border-radius: 4px; font-weight: 600; font-size: 0.8rem; }
    .alert-card {
        background-color: #1c1417;
        border-left: 4px solid #f85149;
        border-radius: 6px;
        padding: 12px 16px;
        margin-bottom: 10px;
    }
    .alert-card-review {
        background-color: #1f1b13;
        border-left: 4px solid #d29922;
        border-radius: 6px;
        padding: 12px 16px;
        margin-bottom: 10px;
    }
    .cluster-badge-connected {
        background-color: rgba(35, 134, 54, 0.2);
        color: #3fb950;
        border: 1px solid #238636;
        padding: 6px 12px;
        border-radius: 6px;
        font-weight: 600;
        display: inline-block;
    }
    .cluster-badge-fallback {
        background-color: rgba(210, 153, 34, 0.2);
        color: #d29922;
        border: 1px solid #d29922;
        padding: 6px 12px;
        border-radius: 6px;
        font-weight: 600;
        display: inline-block;
    }
</style>
""", unsafe_allow_html=True)

# Retrieve global singleton streaming pipeline
pipeline = get_pipeline()

# -------------------------------------------------------------
# SIDEBAR CONTROLS & STREAM CONFIGURATION
# -------------------------------------------------------------
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/shield.png", width=64)
    st.title("StreamGuard")
    st.caption("Distributed Big Data & Real-Time Fraud Engine")
    st.divider()

    # Stream Run/Pause Toggle
    st.subheader("⚡ Stream Controls")
    is_streaming = pipeline.is_running
    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("▶️ Resume" if not is_streaming else "⏸️ Pause", use_container_width=True):
            if is_streaming:
                pipeline.stop()
            else:
                pipeline.start()
            st.rerun()

    with col_btn2:
        if st.button("🔄 Reset", use_container_width=True):
            pipeline.reset()
            st.rerun()

    tps_slider = st.slider(
        "Ingest Rate (txns / sec)",
        min_value=0.5,
        max_value=15.0,
        value=float(pipeline.simulator.transactions_per_sec),
        step=0.5,
        help="Rate at which synthetic transactions are produced and published to the Kafka streaming bus"
    )
    pipeline.set_speed(tps_slider)

    fraud_rate_slider = st.slider(
        "Synthetic Fraud Rate (%)",
        min_value=0,
        max_value=50,
        value=int(pipeline.simulator.fraud_probability * 100),
        step=5,
        help="Probability of injecting anomalous patterns (Velocity, Geo-Hop, Whale Drain)"
    )
    pipeline.set_fraud_rate(fraud_rate_slider / 100.0)

    st.divider()
    st.subheader("🚨 Live Demo Injections")
    st.caption("Click to trigger immediate anomalous behavior:")

    if st.button("💥 Velocity Burst (5 txns / 2s)", use_container_width=True):
        pipeline.manual_inject_velocity()
        st.toast("Injected Velocity Burst!", icon="⚡")

    if st.button("🌍 Impossible Geo-Hop (US ✈️ SG)", use_container_width=True):
        pipeline.manual_inject_geo()
        st.toast("Injected Geo-Velocity Anomaly!", icon="✈️")

    if st.button("💰 Whale Drain (>10x Avg)", use_container_width=True):
        pipeline.manual_inject_drain()
        st.toast("Injected High-Value Drain!", icon="💎")

    st.divider()
    st.subheader("⚙️ Engine Sensitivity")
    with st.expander("Tuning Parameters", expanded=False):
        vel_thresh = st.number_input("Velocity Threshold (txns/window)", min_value=2, max_value=10, value=pipeline.engine.velocity_threshold)
        drain_mult = st.slider("Drain Multiplier (x avg)", min_value=2.0, max_value=10.0, value=float(pipeline.engine.drain_multiplier), step=0.5)
        decline_cutoff = st.slider("Decline Score Cutoff", min_value=50, max_value=95, value=int(pipeline.engine.decline_score_threshold))
        review_cutoff = st.slider("Review Score Cutoff", min_value=20, max_value=60, value=int(pipeline.engine.review_score_threshold))
        pipeline.update_thresholds(
            velocity_threshold=vel_thresh,
            drain_multiplier=drain_mult,
            decline_threshold=float(decline_cutoff),
            review_threshold=float(review_cutoff),
        )


# -------------------------------------------------------------
# MAIN DASHBOARD INTERFACE
# -------------------------------------------------------------
st.title("🛡️ StreamGuard: Distributed Fraud Stream & Big Data Engine")
st.markdown(
    "Hybrid stream processor combining **Apache Kafka** partitioned ingestion, **Apache Spark / PySpark** "
    "Window SQL analytics, **Apache Parquet** partitioned data lakes, and **PySpark MLlib K-Means** behavioral clustering."
)

tab_dashboard, tab_alerts, tab_spark, tab_architecture = st.tabs([
    "📊 Live Operations Center",
    "🚨 Fraud Alert Center",
    "🔥 Distributed Spark & Kafka Analytics",
    "🎓 Architecture & Streaming Theory"
])


def get_animated_metric(key: str, target_value: float, step_speed: float = 0.3) -> float:
    """
    Stateful Linear Interpolation (LERP) helper tracking previous values across UI refresh cycles.
    Smooths data fluctuations and ensures values organically animate up or down incrementally.
    """
    if "anim_states" not in st.session_state:
        st.session_state.anim_states = {}
    if key not in st.session_state.anim_states:
        st.session_state.anim_states[key] = float(target_value)
        return float(target_value)
    
    current_val = float(st.session_state.anim_states[key])
    if abs(target_value - current_val) < 0.01:
        st.session_state.anim_states[key] = float(target_value)
        return float(target_value)
        
    new_val = current_val + (target_value - current_val) * step_speed
    st.session_state.anim_states[key] = float(new_val)
    return float(new_val)


@st.fragment(run_every="1s")
def render_live_operations():
    """Auto-refreshing fragment rendering real-time metrics and streaming feed."""
    metrics = pipeline.get_metrics()

    # Stateful Interpolated Metrics for Organic Visual Transitions
    anim_saved = get_animated_metric("amount_saved", metrics["total_amount_prevented"], step_speed=0.35)
    anim_fraud_rate = get_animated_metric("fraud_rate", metrics["fraud_rate_pct"], step_speed=0.25)
    anim_f1 = get_animated_metric("f1_score", metrics["f1_score"], step_speed=0.25)
    anim_latency = get_animated_metric("latency_ms", metrics["avg_latency_ms"], step_speed=0.30)

    # KPI METRIC CARDS
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Total Ingested</div>
            <div class="metric-val">{metrics['total_processed']:,}</div>
            <div class="metric-sub">⚡ {metrics['current_tps']:.1f} txns/sec</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Fraud Intercepted</div>
            <div class="metric-val" style="color: #f85149;">{metrics['total_declined']:,}</div>
            <div class="metric-sub">🛡️ ${anim_saved:,.2f} saved</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Flagged / Review</div>
            <div class="metric-val" style="color: #d29922;">{metrics['total_review']:,}</div>
            <div class="metric-sub">⚠️ Needs human audit</div>
        </div>
        """, unsafe_allow_html=True)
    with c4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Fraud Rate</div>
            <div class="metric-val">{anim_fraud_rate:.1f}%</div>
            <div class="metric-sub">🎯 F1: {anim_f1:.1f}%</div>
        </div>
        """, unsafe_allow_html=True)
    with c5:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Scoring Latency</div>
            <div class="metric-val" style="color: #3fb950;">{anim_latency:.2f} ms</div>
            <div class="metric-sub">⚡ Sub-millisecond SLA</div>
        </div>
        """, unsafe_allow_html=True)

    st.write("")

    # REAL-TIME VISUALIZATIONS ROW
    col_chart1, col_chart2 = st.columns([1, 1])

    with col_chart1:
        st.subheader("Triggered Anomaly Distribution")
        anomaly_data = metrics["anomaly_counts"]
        total_anomalies = sum(anomaly_data.values())
        if total_anomalies > 0:
            anim_pie_counts = [
                get_animated_metric(f"pie_{k}", float(v), step_speed=0.25)
                for k, v in anomaly_data.items()
            ]
            df_pie = pd.DataFrame({
                "Anomaly": list(anomaly_data.keys()),
                "Count": [max(0.01, c) for c in anim_pie_counts]
            })
            df_pie = df_pie[df_pie["Count"] > 0.05]
            fig = px.pie(
                df_pie,
                names="Anomaly",
                values="Count",
                hole=0.45,
                color="Anomaly",
                color_discrete_map={
                    "VELOCITY_SPIKE": "#f85149",
                    "GEO_ANOMALY": "#a371f7",
                    "HIGH_VALUE_DRAIN": "#d29922",
                    "STATISTICAL_ZSCORE": "#58a6ff",
                    "HIGH_RISK_CATEGORY": "#3fb950",
                }
            )
        else:
            df_pie = pd.DataFrame({
                "Anomaly": ["Awaiting Injections"],
                "Count": [1.0]
            })
            fig = px.pie(
                df_pie,
                names="Anomaly",
                values="Count",
                hole=0.45,
                color="Anomaly",
                color_discrete_map={"Awaiting Injections": "#21262d"}
            )

        fig.update_layout(
            margin=dict(t=20, b=20, l=10, r=10),
            height=260,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
            font=dict(color="#c9d1d9"),
            uirevision="anomaly_pie_uirevision",
            transition={
                'duration': 1000,          # 400ms smooth transition window
                'easing': 'cubic-in-out'  # Smooth acceleration/deceleration curve
            },
        )
        st.plotly_chart(fig, use_container_width=True, key="anomaly_pie_chart")

    with col_chart2:
        st.subheader("Evaluation Pipeline Accuracy")
        anim_precision = get_animated_metric("chart_prec", metrics["precision"], step_speed=0.25)
        anim_recall = get_animated_metric("chart_rec", metrics["recall"], step_speed=0.25)
        anim_f1_bar = get_animated_metric("chart_f1", metrics["f1_score"], step_speed=0.25)

        acc_df = pd.DataFrame({
            "Metric": ["Precision", "Recall", "F1 Score"],
            "Percentage (%)": [round(anim_precision, 1), round(anim_recall, 1), round(anim_f1_bar, 1)],
        })
        fig_bar = px.bar(
            acc_df,
            x="Metric",
            y="Percentage (%)",
            text_auto=".1f",
            color="Metric",
            color_discrete_sequence=["#58a6ff", "#3fb950", "#a371f7"],
            range_y=[0, 105],
        )
        fig_bar.update_layout(
            margin=dict(t=20, b=20, l=10, r=10),
            height=260,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
            font=dict(color="#c9d1d9"),
            uirevision="accuracy_bar_uirevision",
            transition_duration=350,
            transition_easing="cubic-in-out",
        )
        st.plotly_chart(fig_bar, use_container_width=True, key="accuracy_bar_chart")

    # STREAMING TRANSACTION FEED
    st.subheader("📡 Live Ingest Stream Feed (Keyed Sliding Window Snapshot)")
    recent_df = pipeline.get_recent_df()

    if not recent_df.empty:
        def highlight_status(val):
            if val == "DECLINED":
                return "background-color: rgba(248, 81, 73, 0.25); color: #ff7b72; font-weight: bold;"
            elif val == "REVIEW":
                return "background-color: rgba(210, 153, 34, 0.25); color: #e3b341; font-weight: bold;"
            return "color: #7ee787;"

        styled_df = recent_df.style.map(highlight_status, subset=["Status"])
        st.dataframe(styled_df, use_container_width=True, height=350)
    else:
        st.info("Streaming producer starting up...")


with tab_dashboard:
    render_live_operations()


@st.fragment(run_every="2s")
def render_fraud_alerts():
    """Fraud Alert Center panel detailing flagged suspicious transactions."""
    alerts = pipeline.get_alerts_list()
    st.subheader(f"🚨 Flagged Transactions Queue ({len(alerts)} events)")
    st.caption("Transactions flagged by the multi-factor scoring engine requiring manual review or automatic decline.")

    if not alerts:
        st.success("✅ No suspicious transactions detected in the current buffer.")
        return

    for alert in alerts[:15]:
        txn = alert.transaction
        css_class = "alert-card" if alert.status == RiskStatus.DECLINED else "alert-card-review"
        badge_class = "status-badge-declined" if alert.status == RiskStatus.DECLINED else "status-badge-review"

        with st.container():
            st.markdown(f"""
            <div class="{css_class}">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <span class="{badge_class}">{alert.status.value}</span>
                        <strong style="margin-left: 10px; font-size: 1.05rem; color: #f0f6fc;">Txn: {txn.transaction_id}</strong>
                        <span style="color: #8b949e; margin-left: 8px;">(User: {txn.user_id})</span>
                    </div>
                    <div style="text-align: right;">
                        <span style="font-size: 1.2rem; font-weight: 700; color: #f0f6fc;">${txn.amount:,.2f}</span>
                        <span style="color: #f85149; font-weight: 600; margin-left: 8px;">Risk Score: {alert.risk_score:.0f}/100</span>
                    </div>
                </div>
                <div style="margin-top: 8px; color: #c9d1d9; font-size: 0.9rem;">
                    <strong>Triggered Rules:</strong> {alert.primary_reason}
                </div>
                <div style="margin-top: 4px; color: #8b949e; font-size: 0.8rem;">
                    📍 Country: {txn.location} | 🏪 Category: {txn.merchant_category} | 🌐 IP: {txn.ip_address} | ⏱️ Latency: {alert.latency_ms:.2f}ms
                </div>
            </div>
            """, unsafe_allow_html=True)


with tab_alerts:
    render_fraud_alerts()


# -------------------------------------------------------------
# TAB 3: DISTRIBUTED SPARK & KAFKA ANALYTICS
# -------------------------------------------------------------
with tab_spark:
    st.header("🔥 Distributed Big Data Architecture: Spark, Kafka & Parquet")
    st.markdown(
        "Demonstrates production-scale distributed components: **Apache Kafka** partitioned stream ingest, "
        "**Apache Parquet** columnar data lake storage, and **Apache Spark (PySpark)** SQL Windowing & MLlib clustering."
    )

    # Cluster Telemetry Cards
    metrics = pipeline.get_metrics()
    k_status = metrics["kafka_status"]
    s_mode = metrics["spark_mode"]

    col_k, col_s = st.columns(2)
    with col_k:
        is_k_online = k_status["is_connected"]
        k_badge = "cluster-badge-connected" if is_k_online else "cluster-badge-fallback"
        k_label = "🟢 DISTRIBUTED KAFKA (ONLINE)" if is_k_online else "🟡 LOCAL MEMORY FALLBACK"
        st.markdown(f"""
        <div class="metric-card">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <span class="metric-label">Apache Kafka Ingest Bus</span>
                <span class="{k_badge}">{k_label}</span>
            </div>
            <div style="margin-top: 8px; font-size: 0.9rem; color: #c9d1d9;">
                <strong>Topic:</strong> <code>{k_status['topic']}</code> | <strong>Broker:</strong> <code>{k_status['bootstrap_servers']}</code><br/>
                <strong>Partitioning Key:</strong> <code>hash(user_id) % 4 partitions</code><br/>
                <span style="color:#8b949e; font-size:0.8rem;">Published: {k_status['published_count']} msgs | Status: {k_status['mode']}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col_s:
        is_s_online = "DISTRIBUTED" in s_mode
        s_badge = "cluster-badge-connected" if is_s_online else "cluster-badge-fallback"
        s_label = "🟢 DISTRIBUTED PYSPARK (ONLINE)" if is_s_online else "🟡 PARQUET ARROW SINK (ZERO-CRASH)"
        st.markdown(f"""
        <div class="metric-card">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <span class="metric-label">Apache Spark / PySpark Engine</span>
                <span class="{s_badge}">{s_label}</span>
            </div>
            <div style="margin-top: 8px; font-size: 0.9rem; color: #c9d1d9;">
                <strong>Data Lake Sink:</strong> <code>data/transactions_parquet/location=*</code><br/>
                <strong>Engine Mode:</strong> <code>{s_mode}</code><br/>
                <span style="color:#8b949e; font-size:0.8rem;">Supports distributed Windowing, SQL & MLlib KMeans</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.write("")

    # Execution Action Trigger
    col_run1, col_run2 = st.columns([1, 3])
    with col_run1:
        run_spark_btn = st.button("🚀 Run Distributed PySpark & MLlib Job", type="primary", use_container_width=True)

    with col_run2:
        st.caption("Materializes the accumulated transaction buffer (seeds to 500 records), partitions Parquet by location, runs PySpark SQL windowing, and trains a PySpark MLlib K-Means model.")

    if run_spark_btn:
        with st.spinner("Executing Distributed PySpark, Parquet Sink, and MLlib Clustering..."):
            pipeline.run_spark_batch_job(target_sample_size=500)
            st.success("✅ Spark Batch & MLlib Job executed successfully!")

    spark_res = pipeline.latest_spark_results

    if spark_res:
        st.divider()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Batch Records Processed", f"{spark_res['records_processed']:,}")
        m2.metric("Execution Time", f"{spark_res['execution_time_s']} s")
        m3.metric("Data Lake Sink", spark_res['parquet_dir'])
        m4.metric("Job Timestamp", spark_res['timestamp'])

        # 1. Spark MLlib Unsupervised Anomaly Clustering
        st.subheader("1. 🤖 Spark MLlib K-Means Behavioral Anomaly Clustering (k=3)")
        st.markdown(
            "Extracts behavioral feature vectors: `[txn_count, avg_amount, max_amount, std_amount, avg_risk_score]`. "
            "Normalizes features with `StandardScaler` and clusters user cohorts to detect automated velocity bots and whale drains."
        )

        users_clustered = spark_res["clustered_users_df"]
        if not users_clustered.empty:
            col_scat, col_sum = st.columns([3, 2])
            with col_scat:
                fig_km = px.scatter(
                    users_clustered,
                    x="txn_count",
                    y="avg_amount",
                    size="max_amount",
                    color="Behavioral_Profile",
                    hover_name="user_id",
                    hover_data=["max_amount", "avg_risk_score"],
                    title="User Behavioral Clusters (Spark MLlib KMeans)",
                    labels={"txn_count": "Window Transaction Count (Velocity)", "avg_amount": "Average Spend ($USD)"},
                    color_discrete_map={
                        "🟢 Normal Baseline User": "#3fb950",
                        "⚡ High-Velocity Spike / Bot": "#f85149",
                        "💰 Whale Drain Outlier": "#d29922",
                    }
                )
                fig_km.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#c9d1d9"),
                    height=360,
                    margin=dict(t=35, b=20, l=10, r=10),
                    uirevision="kmeans_scatter_uirevision",
                    transition_duration=300,
                    transition_easing="cubic-in-out",
                )
                st.plotly_chart(fig_km, use_container_width=True, key="kmeans_scatter_chart")

            with col_sum:
                st.markdown("**Cluster Centroids & Profiles**")
                cluster_sum_df = spark_res["cluster_summary_df"]
                st.dataframe(cluster_sum_df, use_container_width=True, height=320)

        # 2. Regional Spark SQL Aggregations
        st.subheader("2. 🌍 Distributed Spark SQL Aggregations by Region & Merchant")
        st.markdown("Groups across partitioned country datasets (`location=US`, `location=GB`, etc.) computing regional fraud rates and volumes:")
        reg_df = spark_res["regional_df"]
        st.dataframe(reg_df, use_container_width=True, height=260)

        # 3. Distributed Window Analytics
        st.subheader("3. 🪟 PySpark Windowing Features (`Window.partitionBy('user_id').orderBy('timestamp')`)")
        st.markdown("Calculates temporal lag deltas (`F.lag()`), rolling 5-transaction average spend (`rowsBetween(-4, 0)`), and spend surge ratio:")
        win_df = spark_res["window_df"]
        st.dataframe(win_df[["user_id", "transaction_id", "amount", "location", "inter_txn_delta_s", "rolling_5_avg", "spend_surge_ratio", "status"]].head(25), use_container_width=True, height=280)
    else:
        st.info("👉 Click **'🚀 Run Distributed PySpark & MLlib Job'** above to execute the Big Data pipeline and inspect results.")


# -------------------------------------------------------------
# TAB 4: ARCHITECTURE & THEORY
# -------------------------------------------------------------
with tab_architecture:
    st.header("🎓 Distributed Systems Architecture & Viva Defense Guide")
    st.markdown("""
    This project is engineered to demonstrate real-world Big Data and distributed streaming patterns
    utilizing **Apache Kafka**, **Apache Spark / PySpark**, **Apache Parquet**, and **Sliding-Window Stateful Stream Processing**.
    """)

    st.markdown("""
    ### 1. Kafka Key-Based Hash Partitioning
    - In high-throughput distributed messaging, transactions are partitioned across multiple Kafka partition logs:
      $$\\text{Partition} = \\text{hash}(\\text{user\\_id}) \\pmod{\\text{num\\_partitions}}$$
    - **Why this matters for Fraud Detection:** By routing transactions for a specific `user_id` to the *exact same partition*, we guarantee **strict FIFO temporal ordering** and eliminate cross-partition race conditions during sliding-window state evaluation.

    ### 2. Columnar Partitioned Apache Parquet Data Lake
    - Transactions are stored in a columnar data lake partitioned by directory:
      `data/transactions_parquet/location=US/`, `data/transactions_parquet/location=GB/`, etc.
    - **Partition Pruning:** When executing Spark SQL queries filtered by country (e.g. `WHERE location = 'US'`), the query planner skips reading unrelated country partitions entirely, slashing disk I/O by up to 90%.

    ### 3. PySpark Distributed Window Analytics
    - Using PySpark Window specifications:
      ```python
      Window.partitionBy("user_id").orderBy("timestamp").rowsBetween(-4, 0)
      ```
    - Computes rolling moving averages and inter-transaction time deltas without costly self-joins, running parallelized across worker executor partitions.

    ### 4. Spark MLlib Unsupervised Anomaly Clustering
    - Combines multiple behavioral dimensions into normalized vectors via `VectorAssembler` and `StandardScaler`.
    - `pyspark.ml.clustering.KMeans` ($k=3$) automatically groups user accounts into distinct behavioral profiles:
      - **Normal Baseline:** Consistent transaction counts and modest amounts.
      - **Velocity Spike / Bot:** High volume of events with near-zero inter-transaction deltas.
      - **Whale Drain Outliers:** Anomalous spikes in single-transaction expenditure.
    """)

    st.info("💡 **Viva Talking Point:** StreamGuard features a zero-crash architecture: If Docker or external Kafka/Spark clusters are running, it seamlessly connects to genuine distributed brokers. If running barebones on a laptop, it transparently degrades to in-memory queues, PyArrow columnar Parquet sinks, and Scikit-Learn KMeans without breaking or crashing.")
