import pandas as pd
import streamlit as st
import plotly.express as px
from .data_helpers import build_order_level_dataset

def render_sales_trends(df: pd.DataFrame):
    st.subheader("Sales Trends")
    df = df[df["order_date"].notna()].copy()
    if df.empty: return
    trend_df = df.copy()
    trend_df["order_day"] = trend_df["order_date"].dt.date
    trend_df["day_name"] = trend_df["order_date"].dt.day_name()
    trend_df["day_num"] = trend_df["order_date"].dt.dayofweek
    trend_df["hour"] = trend_df["order_date"].dt.hour
    order_trend_df = build_order_level_dataset(trend_df)
    daily = order_trend_df.groupby("order_day", as_index=False).agg(Revenue=("order_total", "sum"), Orders=("order_id", "nunique"))
    fig1 = px.line(daily, x="order_day", y="Revenue", title="Daily Revenue", markers=True)
    st.plotly_chart(fig1.update_layout(height=350, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"), width="stretch")
    c1, c2 = st.columns(2)
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    by_day = trend_df.groupby("day_name", as_index=False).agg(Orders=("order_id", "nunique"))
    by_day = by_day.set_index("day_name").reindex(day_order, fill_value=0).reset_index()
    with c1: 
        fig2 = px.bar(by_day, x="day_name", y="Orders", title="Orders by Day", color="Orders")
        st.plotly_chart(fig2.update_layout(height=320, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"), width="stretch")
    with c2:
        heat = trend_df.groupby(["day_num", "hour"], as_index=False).size().rename(columns={"size": "Orders"})
        pivot = heat.pivot(index="day_num", columns="hour", values="Orders").reindex(index=range(7), columns=range(24), fill_value=0)
        fig3 = px.imshow(pivot.values, x=list(range(24)), y=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], title="Activity Heatmap")
        st.plotly_chart(fig3.update_layout(height=320, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"), width="stretch")
