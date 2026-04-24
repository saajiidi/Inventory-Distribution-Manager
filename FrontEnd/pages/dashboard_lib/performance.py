import pandas as pd
import streamlit as st
import plotly.express as px
from .data_helpers import estimate_line_revenue

def render_product_performance(df: pd.DataFrame):
    st.subheader("Product Performance")
    if df.empty: return
    product_df = df.copy()
    product_df["line_revenue"] = estimate_line_revenue(product_df)
    grouped = product_df.groupby("item_name").agg(Revenue=("line_revenue", "sum"), Units=("qty", "sum"), Orders=("order_id", "nunique")).reset_index()
    grouped = grouped[grouped["item_name"].astype(str).str.strip() != ""].sort_values("Revenue", ascending=False)
    if grouped.empty: return
    top_products = grouped.head(10)
    fig = px.bar(top_products, x="Revenue", y="item_name", orientation="h", title="Top 10 Products by Revenue", color="Revenue")
    st.plotly_chart(fig.update_layout(height=450, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"), width="stretch")
    st.dataframe(grouped.head(50), width="stretch", hide_index=True)
