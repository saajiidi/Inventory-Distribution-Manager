"""Optimized Main Dashboard Controller using a modular library structure."""

from __future__ import annotations
from datetime import date, timedelta, datetime
import pandas as pd
import streamlit as st

from BackEnd.services.customer_insights import generate_customer_insights_from_sales
from BackEnd.services.hybrid_data_loader import (
    get_woocommerce_orders_cache_status,
    load_hybrid_data,
    load_cached_woocommerce_stock_data,
    start_orders_background_refresh,
)
from BackEnd.services.ml_insights import build_ml_insight_bundle
from FrontEnd.components import ui
from FrontEnd.utils.error_handler import log_error

# Modular Library Imports
from .dashboard_lib.data_helpers import prune_dataframe, build_order_level_dataset, sum_order_level_revenue
from .dashboard_lib.story import render_dashboard_story
from .dashboard_lib.bi_analytics import (
    render_today_vs_last_day_sales_chart,
    render_last_7_days_sales_chart,
    render_market_overview_timeseries
)
from .dashboard_lib.trends import render_sales_trends
from .dashboard_lib.performance import render_product_performance
from .dashboard_lib.inventory import render_inventory_health
from .dashboard_lib.deep_dive import render_deep_dive_tab
from .dashboard_lib.audit import render_data_audit, render_data_trust_panel
from .dashboard_lib.live_dashboard import render_live_tab

DASHBOARD_SALES_COLUMNS = [
    "order_id", "order_date", "order_total", "customer_key", "customer_name",
    "order_status", "source", "city", "state", "qty", "item_name",
    "item_revenue", "line_total", "item_cost", "price", "sku", "Category", "Coupons"
]

# Internal Page Logic will be appended below

def render_intelligence_hub_page():
    st.markdown('<div class="live-indicator"><span class="live-dot"></span>System Online | Intelligence Hub Active</div>', unsafe_allow_html=True)
    
    global_sync = st.session_state.get("global_sync_request", False)
    if global_sync:
        st.session_state["global_sync_request"] = False # Reset
        
    # 1. Map Time Window to Query Range
    window = st.session_state.get("time_window", "Last 7 Days")
    
    today = date.today()
    if window == "MTD":
        days_back = (today - today.replace(day=1)).days
    elif window == "YTD":
        days_back = (today - today.replace(month=1, day=1)).days
    else:
        window_map = {
            "Yesterday & Today": 1,
            "Last 3 Days": 3,
            "Last 7 Days": 7,
            "Last Month": 30,
            "Last 3 Months": 90,
            "Last Quarter": 120, # 4 Months as requested
            "Last Half Year": 180,
            "Last Year": 365
        }
        days_back = window_map.get(window, 7)
    
    # Range for current view (e.g. Yesterday + Today)
    end_date_str = date.today().strftime("%Y-%m-%d")
    start_date_str = (date.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    
    # Range for comparative view (The block before that)
    # If Yesterday & Today (1 day), we want the day before Yesterday to Today - 1
    prev_start_date_str = (date.today() - timedelta(days=days_back * 2)).strftime("%Y-%m-%d")
    prev_end_date_str = (date.today() - timedelta(days=days_back + 1)).strftime("%Y-%m-%d")

    orders_status = get_woocommerce_orders_cache_status(start_date_str, end_date_str)
    
    # Force a sync for current-day data requests
    should_force = global_sync or (window == "Yesterday & Today")
    start_orders_background_refresh(start_date_str, end_date_str, force=should_force)
    
    sync_mode = "live" if (window == "Yesterday & Today" or global_sync) else "cache_only"
    df_sales_raw = prune_dataframe(load_hybrid_data(start_date=start_date_str, end_date=end_date_str, woocommerce_mode=sync_mode), DASHBOARD_SALES_COLUMNS)
    
    # Apply Business Rules (Logic consistency)
    from BackEnd.core.categories import get_category_for_sales
    df_sales_raw["Category"] = df_sales_raw["item_name"].apply(get_category_for_sales)
    
    # Fetch Previous context (Unfiltered)
    df_prev_raw = load_hybrid_data(start_date=prev_start_date_str, end_date=prev_end_date_str, woocommerce_mode="cache_only")
    df_prev_raw = prune_dataframe(df_prev_raw, DASHBOARD_SALES_COLUMNS)
    df_prev_raw["Category"] = df_prev_raw["item_name"].apply(get_category_for_sales)

    # SECURE ANALYTICS: Create filtered versions for high-level pillars
    # Only "Completed" or "Shipped" are counted for the 6 Executive Pillars
    valid_statuses = ["completed", "shipped"]
    df_sales_exec = df_sales_raw[df_sales_raw["order_status"].str.lower().isin(valid_statuses)].copy()
    df_prev_exec = df_prev_raw[df_prev_raw["order_status"].str.lower().isin(valid_statuses)].copy()

    df_customers = generate_customer_insights_from_sales(df_sales_exec, include_rfm=True)
    ml_bundle = build_ml_insight_bundle(df_sales_exec, df_customers, horizon_days=7)
    stock_df = load_cached_woocommerce_stock_data()
    
    st.session_state.dashboard_data = {
        "sales": df_sales_raw,        # Operational logic needs RAW (processing/pending)
        "sales_exec": df_sales_exec, # High-level stats use FILTERED
        "prev_sales": df_prev_raw,
        "prev_exec": df_prev_exec,
        "customers": df_customers,
        "ml": ml_bundle,
        "stock": stock_df,
        "summary": {"woocommerce_live": len(df_sales_raw), "stock_rows": len(stock_df)},
        "hint": orders_status.get("status_message", ""),
        "window_label": "7 days" if days_back == 7 else ("month" if days_back == 30 else f"{days_back} days")
    }

    data = st.session_state.dashboard_data
    
    # 1. Core Metrics (6 Pillars)
    df_exec = data["sales_exec"]
    exec_orders = build_order_level_dataset(df_exec)
    
    total_rev = sum_order_level_revenue(df_exec)
    order_count = exec_orders["order_id"].nunique() if not exec_orders.empty else 0
    cust_count = df_exec["customer_key"].nunique()
    total_items = df_exec["qty"].sum()
    
    aov = (total_rev / order_count) if order_count else 0
    avg_orders_per_day = order_count / max(1, days_back)
    
    # --- Comparative Logic ---
    df_prev_exec = data["prev_exec"]
    prev_items_val = df_prev_exec["qty"].sum() if not df_prev_exec.empty else 0
    prev_rev_val = sum_order_level_revenue(df_prev_exec)
    
    prev_orders_level = build_order_level_dataset(df_prev_exec)
    prev_orders_val = prev_orders_level["order_id"].nunique() if not prev_orders_level.empty else 0
    prev_aov_val = (prev_rev_val / prev_orders_val) if prev_orders_val else 0
    prev_cust_val = df_prev_exec["customer_key"].nunique() if not df_prev_exec.empty else 0
    prev_avg_orders_val = (prev_orders_val / days_back) if days_back else 0

    def calc_delta(curr, prev):
        if not prev: return "", 0
        diff = curr - prev
        pct = (diff / prev * 100)
        label = f"{pct:+.1f}% vs last {data['window_label']}"
        return label, diff

    d_items_label, d_items_val = calc_delta(total_items, prev_items_val)
    d_rev_label, d_rev_val = calc_delta(total_rev, prev_rev_val)
    d_orders_label, d_orders_val = calc_delta(order_count, prev_orders_val)
    d_avg_label, d_avg_val = calc_delta(avg_orders_per_day, prev_avg_orders_val)
    d_cust_label, d_cust_val = calc_delta(cust_count, prev_cust_val)
    d_aov_label, d_aov_val = calc_delta(aov, prev_aov_val)

    # Single-Row Metric Layout (6 Pillars)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1: ui.icon_metric("Total Item Sold", f"{total_items:,}", icon="📦", delta=d_items_label, delta_val=d_items_val)
    with c2: ui.icon_metric("Revenue", f"৳{total_rev:,.0f}", icon="💰", delta=d_rev_label, delta_val=d_rev_val)
    with c3: ui.icon_metric("Orders", f"{order_count:,}", icon="🛒", delta=d_orders_label, delta_val=d_orders_val)
    with c4: ui.icon_metric("Avg. Orders / Day", f"{avg_orders_per_day:,.0f}", icon="📅", delta=d_avg_label, delta_val=d_avg_val)
    with c5: ui.icon_metric("Customers", f"{cust_count:,}", icon="👥", delta=d_cust_label, delta_val=d_cust_val)
    with c6: ui.icon_metric("Basket Size", f"৳{aov:,.0f}", icon="💎", delta=d_aov_label, delta_val=d_aov_val)

    st.markdown("<br>", unsafe_allow_html=True)

    # Routing based on sidebar selection
    selection = st.session_state.get("active_section", "💎 Market Overview")

    if selection == "💎 Market Overview":
        # Global Narrative & Summary
        render_dashboard_story(data["sales_exec"], data["customers"], data["ml"], window)
        
        st.divider()
        render_market_overview_timeseries(data["sales_exec"])
        
    elif selection == "🚢 Operational Live":
        st.subheader("⚡ Live Operational Terminal")
        render_live_tab()
    
    elif selection == "👥 Customer Behavior":
        st.subheader("Customer Intelligence")
        render_customer_insight_tab()
        
    elif selection == "🔍 Deep-Dive Clusters":
        render_deep_dive_tab(data["sales"], data["stock"])
        
    elif selection == "📦 Inventory Health":
        st.subheader("Operational Forecasting")
        render_inventory_health(data["stock"], data["ml"].get("forecast"))
        
    elif selection == "🛡️ Data Trust":
        st.subheader("System Reliability Audit")
        render_data_trust_panel(data["sales"])
        render_data_audit(data["sales"], data["customers"])


# --- MERGED COMPONENT LOGIC ---

def render_customer_insight_tab():
    """Merged Customer Intelligence Component."""
    if "dashboard_data" not in st.session_state:
        st.info("Please sync data to view customer intelligence.")
        return
        
    df = st.session_state.dashboard_data["customers"]
    if df.empty:
        st.warning("No customer segments identified in this period.")
        return

    # Visual Segments
    t1, t2, t3 = st.tabs(["📊 Value Segments", "🎯 Priority Outreach", "🔍 Identity Ledger"])
    
    with t1:
        # Segment Mix
        mix_df = df["segment"].value_counts().reset_index()
        mix_df.columns = ["Segment", "Count"]
        
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(ui.donut_chart(mix_df, values="Count", names="Segment", title="Segment Distribution"), use_container_width=True)
        with c2:
            rev_df = df.groupby("segment")["total_revenue"].sum().reset_index().sort_values("total_revenue", ascending=False)
            st.plotly_chart(ui.bar_chart(rev_df, x="total_revenue", y="segment", title="Revenue by Segment", color_scale="Tealgrn"), use_container_width=True)

    with t2:
        # CRM Queue
        st.caption("Strategic segments requiring immediate attention based on recency and business value.")
        priority = df.sort_values(["total_revenue", "recency_days"], ascending=[False, True]).head(15)
        st.dataframe(
            priority[["primary_name", "segment", "total_revenue", "total_orders", "recency_days"]].rename(
                columns={"primary_name": "Customer", "total_revenue": "LTV", "recency_days": "Last Purchase (Days)"}
            ),
            use_container_width=True, hide_index=True
        )

    with t3:
        st.dataframe(df, use_container_width=True, hide_index=True)


# End of Dashboard controller logic
