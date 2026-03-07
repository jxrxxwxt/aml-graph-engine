import streamlit as st
import pandas as pd
import plotly.express as px
from neo4j import GraphDatabase
from datetime import datetime

# --- System Configurations ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "aml_graph_pass"

# Set Page Config (Must be the first Streamlit command)
st.set_page_config(page_title="AML Enterprise Command Center", layout="wide", initial_sidebar_state="expanded")

# --- Custom CSS for Enterprise Look ---
st.markdown("""
    <style>
    /* Main Background */
    .stApp { background-color: #0E1117; }
    
    /* Custom KPI Cards */
    .kpi-card {
        background-color: #1E293B;
        border-radius: 4px;
        padding: 20px;
        border-left: 4px solid #3B82F6;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.12);
    }
    .kpi-alert { border-left: 4px solid #EF4444; }
    .kpi-title { color: #94A3B8; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
    .kpi-value { color: #F8FAFC; font-size: 32px; font-weight: 400; }
    .kpi-value-alert { color: #EF4444; font-size: 32px; font-weight: 600; }
    </style>
    """, unsafe_allow_html=True)

class AMLAnalytics:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run_query(self, query):
        with self.driver.session() as session:
            return [dict(record) for record in session.run(query)]

# Helper function to render HTML cards
def render_kpi(title, value, is_alert=False):
    card_class = "kpi-card kpi-alert" if is_alert else "kpi-card"
    val_class = "kpi-value-alert" if is_alert else "kpi-value"
    html = f"""
    <div class="{card_class}">
        <div class="kpi-title">{title}</div>
        <div class="{val_class}">{value}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

# --- Initialization ---
analytics = AMLAnalytics(NEO4J_URI, NEO4J_USER, NEO4J_PASS)

# --- Sidebar Controls ---
with st.sidebar:
    st.title("System Parameters")
    st.markdown("Adjust parameters for real-time monitoring.")
    
    st.subheader("High-Value Alert Rules")
    alert_threshold = st.number_input("Threshold (THB)", min_value=50000, value=400000, step=50000)
    
    st.divider()
    st.caption("Neo4j Database: Connected")
    st.caption("Kafka Stream: Active")
    st.caption(f"Session: {datetime.now().strftime('%H:%M')} UTC")

# --- Main Dashboard Header ---
st.title("Anti-Money Laundering Command Center")
st.markdown("Enterprise Transaction Monitoring and Risk Detection Interface")
st.write("") 

# --- Layer 1: Executive KPI Cards ---
metrics = analytics.run_query("""
    MATCH (a:Account) WITH count(a) AS accounts
    MATCH ()-[r:TRANSFERRED_TO]->() RETURN accounts, count(r) AS txns, sum(r.amount) AS vol
""")[0]

c1, c2, c3, c4 = st.columns(4)
with c1:
    render_kpi("Total Exposure (THB)", f"{metrics['vol']:,.0f}")
with c2:
    render_kpi("Monitored Accounts", f"{metrics['accounts']:,}")
with c3:
    render_kpi("Processed Transactions", f"{metrics['txns']:,}")
with c4:
    high_risk_count = analytics.run_query(f"MATCH ()-[r:TRANSFERRED_TO]->() WHERE r.amount > {alert_threshold} RETURN count(r) AS c")[0]['c']
    render_kpi("Critical Alerts", f"{high_risk_count:,}", is_alert=True)

st.write("")
st.write("")

# --- Layer 2: Visual Analytics ---
st.markdown("### Risk Distribution & Outlier Detection")
col_chart1, col_chart2 = st.columns([1, 2])

channel_data_raw = analytics.run_query("""
    MATCH ()-[r:TRANSFERRED_TO]->() 
    WHERE r.channel IS NOT NULL AND r.channel <> ""
    RETURN r.channel AS channel, sum(r.amount) AS total_value, count(r) AS tx_count
""")

if channel_data_raw:
    df_channel = pd.DataFrame(channel_data_raw)
    
    with col_chart1:
        fig_tree = px.treemap(
            df_channel, path=['channel'], values='total_value',
            title="Exposure by Channel",
            color='total_value', color_continuous_scale='Blues',
            template="plotly_dark"
        )
        fig_tree.update_layout(margin=dict(t=40, l=10, r=10, b=10))
        st.plotly_chart(fig_tree, use_container_width=True)

    with col_chart2:
        scatter_data = analytics.run_query("MATCH ()-[r:TRANSFERRED_TO]->() WHERE r.channel IS NOT NULL RETURN r.timestamp AS time, r.amount AS amount, r.channel AS channel ORDER BY r.timestamp DESC LIMIT 500")
        if scatter_data:
            df_scatter = pd.DataFrame(scatter_data)
            fig_scatter = px.scatter(
                df_scatter, x='time', y='amount', color='channel', size='amount',
                title="Transaction Outlier Detection (Last 500 Txns)",
                labels={'amount': 'Amount (THB)', 'time': 'Timestamp'},
                template="plotly_dark",
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            fig_scatter.add_hline(y=alert_threshold, line_dash="dash", line_color="red", annotation_text="Alert Threshold")
            st.plotly_chart(fig_scatter, use_container_width=True)

# --- Layer 3: Actionable Intelligence (Tables) ---
st.markdown("### Priority Investigation Taskforce")
tab1, tab2 = st.tabs(["Circular Laundering (Integration)", "High-Value Anomalies (Placement)"])

with tab1:
    st.markdown("Detection of multi-hop closed-loop transfers attempting to obscure fund origins.")
    circles = analytics.run_query("""
        MATCH (a:Account) WITH a LIMIT 1000
        MATCH path = (a)-[:TRANSFERRED_TO*2..4]->(a)
        RETURN a.account_id AS Subject_Account, length(path) AS Hop_Depth, [n IN nodes(path) | n.account_id] AS Trace_Path
        LIMIT 5
    """)
    if circles:
        st.dataframe(pd.DataFrame(circles), use_container_width=True, hide_index=True)
    else:
        st.success("System Clear: No circular money laundering networks detected.")

with tab2:
    st.markdown("Immediate review required for transactions exceeding the defined threshold.")
    alerts = analytics.run_query(f"""
        MATCH (s:Account)-[r:TRANSFERRED_TO]->(d:Account)
        WHERE r.amount > {alert_threshold}
        RETURN r.transaction_id AS Transaction_ID, s.account_id AS Sender, d.account_id AS Receiver, r.amount AS Amount, r.channel AS Channel, r.timestamp AS Timestamp
        ORDER BY Amount DESC LIMIT 15
    """)
    if alerts:
        df_alerts = pd.DataFrame(alerts)
        # Using Streamlit's native column config for professional formatting
        st.dataframe(
            df_alerts,
            column_config={
                "Amount": st.column_config.NumberColumn(
                    "Amount (THB)",
                    format="%,.2f"
                ),
            },
            use_container_width=True, 
            hide_index=True
        )
    else:
        st.info("System Clear: No transactions currently breach the defined threshold.")

analytics.close()