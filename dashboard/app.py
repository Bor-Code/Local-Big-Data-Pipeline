import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide")

engine = create_engine("postgresql+psycopg2://postgres:mysecretpassword@postgres:5432/bigdata_db")

st.title("NYC Yellow Taxi Data Pipeline Dashboard")

query_daily = "SELECT * FROM daily_summary ORDER BY pickup_date"
df_daily = pd.read_sql(query_daily, engine)

query_hourly = "SELECT * FROM hourly_revenue ORDER BY pickup_hour"
df_hourly = pd.read_sql(query_hourly, engine)

st.subheader("Daily Total Trips")
fig_daily_trips = px.line(df_daily, x="pickup_date", y="total_trips", markers=True)
st.plotly_chart(fig_daily_trips, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Daily Average Distance")
    fig_daily_dist = px.bar(df_daily, x="pickup_date", y="avg_distance")
    st.plotly_chart(fig_daily_dist, use_container_width=True)

with col2:
    st.subheader("Hourly Total Revenue")
    fig_hourly_rev = px.bar(df_hourly, x="pickup_hour", y="total_revenue")
    st.plotly_chart(fig_hourly_rev, use_container_width=True)