import pandas as pd
import streamlit as st
from FrontEnd.components import ui
from .data_helpers import sum_order_level_revenue, build_order_level_dataset

def render_executive_summary(df_sales: pd.DataFrame, df_customers: pd.DataFrame, summary: dict):
    st.subheader("Executive Summary")
    df = df_sales[df_sales["order_date"].notna()].copy()
    order_df = build_order_level_dataset(df)
    total_revenue = sum_order_level_revenue(df)
    total_orders = order_df["order_id"].replace("", pd.NA).dropna().nunique() if not order_df.empty else 0
    active_customers = df["customer_key"].replace("", pd.NA).dropna().nunique()
    total_items = float(df["qty"].sum())
    pending_count = len(df[df["order_status"].str.lower().isin(["pending", "processing", "on-hold"])])

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1: ui.icon_metric("Revenue", f"TK {total_revenue:,.0f}", icon="💰"); ui.badge("Order-level totals")
    with k2: ui.icon_metric("Orders", f"{total_orders:,}", icon="🛒"); ui.badge("Normalized count")
    with k3: ui.icon_metric("AOV", f"TK { (total_revenue/total_orders) if total_orders else 0:,.0f}", icon="💳"); ui.badge("Direct calc")
    with k4: ui.icon_metric("Customers", f"{active_customers:,}", icon="👥"); ui.badge("Distinct keys")
    with k5: ui.icon_metric("Pending", f"{pending_count:,}", icon="⏳", delta="Action required" if pending_count > 5 else "Healthy", delta_color="inverse" if pending_count > 5 else "normal")

    s1, s2, s3 = st.columns(3)
    with s1: ui.icon_metric("Items Sold", f"{total_items:,.0f}", icon="📦"); st.caption(f"WooCommerce: {summary.get('woocommerce_live',0):,} rows")
    with s2: ui.icon_metric("Repeat Rate", f"{float((df_customers['total_orders'] > 1).mean() * 100) if not df_customers.empty else 0:.1f}%", icon="🔄"); ui.badge("Retention base")
    with s3: ui.icon_metric("Latest Order", df["order_date"].max().strftime("%Y-%m-%d %H:%M") if not df.empty and pd.notna(df["order_date"].max()) else "N/A", icon="🗓️")

    insights = []
    if pending_count > 10: insights.append("Fulfillment pressure is rising. The pending order queue justifies immediate review.")
    mean_qty = df.groupby("order_id")["qty"].sum().mean() if total_orders else 0
    if mean_qty and mean_qty < 1.5: insights.append("Basket depth is light. Consider cross-sell programs.")
    if not df_customers.empty and "segment" in df_customers.columns:
        vip_count = int((df_customers["segment"] == "VIP").sum())
        if vip_count: insights.append(f"{vip_count} VIP customers are active. Prioritize their experience.")
    if not insights: insights.append("Business pulse is stable. Focus on retention programs.")
    ui.commentary("Intelligence Commentary", insights)
