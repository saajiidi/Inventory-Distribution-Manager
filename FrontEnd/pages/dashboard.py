"""Optimized Main Dashboard Controller using a modular library structure."""

from __future__ import annotations
from datetime import date, timedelta, datetime
import pandas as pd
import numpy as np
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
from .dashboard_lib.data_helpers import (
    prune_dataframe, 
    build_order_level_dataset, 
    sum_order_level_revenue, 
    apply_global_filters,
    get_available_filters
)
from BackEnd.core.categories import get_master_category_list, format_category_label, get_subcategory_name
from .dashboard_lib.story import render_dashboard_story
from .dashboard_lib.bi_analytics import (
    render_today_vs_last_day_sales_chart,
    render_last_7_days_sales_chart,
    render_sales_overview_timeseries
)
from .dashboard_lib.trends import render_sales_trends
from .dashboard_lib.performance import render_product_performance
from .dashboard_lib.inventory import render_inventory_health
from .dashboard_lib.deep_dive import render_deep_dive_tab
from .dashboard_lib.audit import render_data_audit, render_data_trust_panel
from .dashboard_lib.acquisition import render_acquisition_analytics
from .dashboard_lib.operations import render_operational_health
from BackEnd.services.customer_insights import generate_cohort_matrix
from .dashboard_lib.customer_insight_page import (
    render_customer_insight_page,
    render_enhanced_customer_insight_tab,
)

DASHBOARD_SALES_COLUMNS = [
    "order_id", "order_date", "order_total", "customer_key", "customer_name",
    "order_status", "source", "city", "state", "qty", "item_name",
    "item_revenue", "line_total", "item_cost", "price", "sku", "Category", "Coupons"
]

# Internal Page Logic will be appended below

def render_intelligence_hub_page():
    import os
    import base64
    banner_path = os.path.join("FrontEnd", "assets", "data_analytics_banner.png")
    
    if os.path.exists(banner_path):
        with open(banner_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode()
        
        st.markdown(
            f'''
            <div style="position: relative; margin-bottom: 25px; border-radius: 12px; overflow: hidden; height: 160px; border: 1px solid rgba(255,255,255,0.1); box-shadow: 0 6px 16px rgba(0,0,0,0.3);">
                <img src="data:image/png;base64,{encoded_string}" style="width: 100%; height: 100%; object-fit: cover; opacity: 0.8;">
                <div style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; background: linear-gradient(to right, rgba(0,0,0,0.85) 0%, rgba(0,0,0,0.3) 50%, rgba(0,0,0,0) 100%); display: flex; flex-direction: column; justify-content: center; padding-left: 30px;">
                    <h1 style="color: white; margin: 0; font-size: 2.2rem; font-weight: 800; letter-spacing: -1px; text-shadow: 2px 2px 4px rgba(0,0,0,0.5);">
                        DEEN <span style="color: #3b82f6;">Business Intelligence</span>
                    </h1>
                    <div style="display: flex; align-items: center; margin-top: 12px; background: rgba(16, 185, 129, 0.1); border: 1px solid rgba(16, 185, 129, 0.2); padding: 4px 12px; border-radius: 20px; width: fit-content;">
                        <div style="width: 8px; height: 8px; background: #10b981; border-radius: 50%; margin-right: 10px; box-shadow: 0 0 10px #10b981; animation: pulse 2s infinite;"></div>
                        <span style="color: #10b981; font-size: 0.75rem; font-weight: 700; text-transform: uppercase; letter-spacing: 1px;">System Online | Intelligence Hub Active</span>
                    </div>
                </div>
            </div>
            <style>
                @keyframes pulse {{
                    0% {{ transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }}
                    70% {{ transform: scale(1); box-shadow: 0 0 0 6px rgba(16, 185, 129, 0); }}
                    100% {{ transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }}
                }}
            </style>
            ''',
            unsafe_allow_html=True
        )
    else:
        st.markdown('<div class="live-indicator"><span class="live-dot"></span>System Online | Intelligence Hub Active</div>', unsafe_allow_html=True)
    global_sync = st.session_state.get("global_sync_request", False)
    if global_sync:
        st.session_state["global_sync_request"] = False # Reset
        
    # 1. Map Time Window to Query Range
    window = st.session_state.get("time_window", "Last Month")
    
    today = date.today()
    start_dt = end_dt = today
    
    if window == "MTD":
        days_back = (today - today.replace(day=1)).days
    elif window == "YTD":
        days_back = (today - today.replace(month=1, day=1)).days
    elif window == "Custom Date Range":
        start_dt = st.session_state.get("wc_sync_start_date", today)
        end_dt = st.session_state.get("wc_sync_end_date", today)
        
        # Calculate duration for delta comparisons
        days_back = (end_dt - start_dt).days
        
        # Construct ISO strings with full-day coverage
        start_date_str = f"{start_dt}T00:00:00"
        end_date_str = f"{end_dt}T23:59:59"
    else:
        window_map = {
            "Last Day": 1,
            "Last 3 Days": 3,
            "Last 7 Days": 7,
            "Last 15 Days": 15,
            "Last Month": 30,
            "Last 3 Months": 90,
            "Last Quarter": 90,
            "Last Half Year": 180,
            "Last 9 Months": 270,
            "Last Year": 365
        }
        days_back = window_map.get(window, 30)
    
    if window == "Custom Date Range":
        end_date_str = end_dt.strftime("%Y-%m-%d")
        start_date_str = start_dt.strftime("%Y-%m-%d")
        duration = max(1, days_back)
        prev_end_date_str = (start_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_start_date_str = (start_dt - timedelta(days=duration)).strftime("%Y-%m-%d")
    else:
        # Calculate proper date range for ALL window types
        end_dt = today
        start_dt = today - timedelta(days=days_back)
        end_date_str = end_dt.strftime("%Y-%m-%d")
        start_date_str = start_dt.strftime("%Y-%m-%d")
        prev_start_date_str = (today - timedelta(days=days_back * 2)).strftime("%Y-%m-%d")
        prev_end_date_str = (today - timedelta(days=days_back + 1)).strftime("%Y-%m-%d")

    orders_status = get_woocommerce_orders_cache_status(start_date_str, end_date_str)

    # === SKELETON LOADING: Render UI instantly while data loads ===
    cache_empty = not orders_status.get("cache_exists", False)
    data_load_key = f"data_loaded_{start_date_str}_{end_date_str}"
    data_ready = st.session_state.get(data_load_key, False)

    if cache_empty and not data_ready:
        # Render skeleton UI immediately for fast perceived performance
        st.markdown("""
            <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 20px; padding: 12px 16px; background: linear-gradient(90deg, rgba(59,130,246,0.1) 0%, rgba(59,130,246,0.05) 100%); border-left: 3px solid #3b82f6; border-radius: 8px;">
                <span style="animation: spin 1s linear infinite;">🔄</span>
                <div>
                    <div style="font-weight: 600; color: #3b82f6; font-size: 0.9rem;">Initializing Data Stream...</div>
                    <div style="font-size: 0.75rem; color: #64748b;">Connecting to WooCommerce API. Dashboard will populate automatically.</div>
                </div>
            </div>
            <style>@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }</style>
        """, unsafe_allow_html=True)

        # Show skeleton metrics row
        ui.skeleton_row(count=6)
        st.markdown("<br>", unsafe_allow_html=True)
        ui.skeleton_row(count=6)
        st.markdown("<br>", unsafe_allow_html=True)

        # Trigger data load and rerun
        st.session_state[data_load_key] = False
        st.session_state[f"{data_load_key}_loading"] = True
        st.rerun()

    needs_history = window == "Custom Date Range" and not orders_status.get("is_covered", True)
    if needs_history:
        st.warning(f"⏳ Connecting to WooCommerce Live to sync deep archival history ({start_date_str} to {end_date_str}). This runs seamlessly in the background and may take a few minutes. Your metrics will automatically populate once caching finishes!")
    from FrontEnd.utils.config import USE_STATIC_SNAPSHOT, SNAPSHOT_DATE, SNAPSHOT_LABEL, MAP_FORCE_SNAPSHOT
    
    # Determine Snapshot mode based on configuration OR manual user override (Slow Connection)
    slow_conn = st.session_state.get("conn_speed_mode") == "Slow Connection"
    active_snapshot_mode = USE_STATIC_SNAPSHOT or slow_conn
    
    # === DATA LOADING: Load actual data (cache hit or fresh fetch) ===
    # Skeleton UI already rendered above if cache was empty
    data_load_key = f"data_loaded_{start_date_str}_{end_date_str}"

    if active_snapshot_mode:
        st.info(f"📶 **{SNAPSHOT_LABEL}**: Performance optimized for slow connections. Real-time syncing is paused.")
        df_sales_raw = load_hybrid_data(woocommerce_mode="cache_only", use_snapshot=True)
        df_sales_raw = prune_dataframe(df_sales_raw, DASHBOARD_SALES_COLUMNS)
    else:
        # Standard Live Logic
        should_force = global_sync or (window == "Last Day") or needs_history or cache_empty

        # Start background refresh for freshness
        start_orders_background_refresh(start_date_str, end_date_str, force=should_force)

        # Load data (cached or fresh based on mode)
        sync_mode = "live" if (window == "Last Day" or global_sync) else "cache_only"
        df_sales_raw = prune_dataframe(
            load_hybrid_data(start_date=start_date_str, end_date=end_date_str, woocommerce_mode=sync_mode),
            DASHBOARD_SALES_COLUMNS
        )

    # Mark data as ready for future renders
    st.session_state[data_load_key] = True
    st.session_state[f"{data_load_key}_loading"] = False
    
    # 1. Map categories and apply global filters immediately for UI context
    from BackEnd.core.categories import get_category_for_sales
    
    if "Category" not in df_sales_raw.columns:
        df_sales_raw["Category"] = df_sales_raw["item_name"].apply(get_category_for_sales)
    
    # Load Previous Context (Archival comparison range)
    df_prev_raw = load_hybrid_data(start_date=prev_start_date_str, end_date=prev_end_date_str, woocommerce_mode="cache_only")
    df_prev_raw = prune_dataframe(df_prev_raw, DASHBOARD_SALES_COLUMNS)
    df_prev_raw["Category"] = df_prev_raw["item_name"].apply(get_category_for_sales)
    
    global_cats = st.session_state.get("global_categories", ["All"])
    global_stats = st.session_state.get("global_statuses", ["All"])
    
    # Store Raw Date-Ranged Data before global category/status filtering
    df_sales_full = df_sales_raw.copy() 
    df_prev_full = df_prev_raw.copy()
    
    # v10.2: Ensure robust item-level revenue estimation
    from .dashboard_lib.data_helpers import estimate_line_revenue
    # Apply estimation to full datasets so it persists
    df_sales_full["item_revenue"] = estimate_line_revenue(df_sales_full)
    df_prev_full["item_revenue"] = estimate_line_revenue(df_prev_full)
    
    # Update current filtered sets with calculated revenue
    df_sales_filtered = apply_global_filters(df_sales_full, global_cats, global_stats)
    df_prev_filtered = apply_global_filters(df_prev_full, global_cats, global_stats)

    # 1.5 Geographic Intelligence (District Code Resolution)
    from BackEnd.core.geo import get_region_display
    df_sales_full["_region_display"] = df_sales_full.apply(lambda x: get_region_display(x.get("city", ""), x.get("state", "")), axis=1)
    df_prev_full["_region_display"] = df_prev_full.apply(lambda x: get_region_display(x.get("city", ""), x.get("state", "")), axis=1)
        
    # --- HYBRID MAP DATA: The map ALWAYS uses a clean snapshot to save memory ---
    if MAP_FORCE_SNAPSHOT:
        df_sales_map = load_hybrid_data(use_snapshot=True)
        # Apply categories to map snapshot if missing
        if "Category" not in df_sales_map.columns:
            from BackEnd.core.categories import get_category_for_sales
            df_sales_map["Category"] = df_sales_map["item_name"].apply(get_category_for_sales)
    else:
        df_sales_map = df_sales_filtered

    # Fetch pre-calculated ML bundle if in snapshot mode, else build it
    if active_snapshot_mode:
        from BackEnd.services.hybrid_data_loader import load_static_ml_bundle
        ml_bundle = load_static_ml_bundle()
    else:
        ml_bundle = None # Will be built below
    
    # 2. Status Filtering Layers (Derived from Sidebar Filtered Sets)
    # - Strict: For financial/secure analytics (Completed/Shipped)
    # - Active: For operational overview (Excluding Cancelled/Failed)
    valid_statuses = ["completed", "shipped"]
    exclude_statuses = ["cancelled", "failed", "trash"]
    
    # Define Datasets (Using _filtered to respect sidebar)
    # a) Secure Set (Strict)
    df_sales_strict = df_sales_filtered[df_sales_filtered["order_status"].str.lower().isin(valid_statuses)].copy()
    df_prev_strict = df_prev_filtered[df_prev_filtered["order_status"].str.lower().isin(valid_statuses)].copy()
    
    # b) Active Set (Loose - Used for overall revenue/volume reporting)
    df_sales_active = df_sales_filtered[~df_sales_filtered["order_status"].str.lower().isin(exclude_statuses)].copy()
    df_prev_active = df_prev_filtered[~df_prev_filtered["order_status"].str.lower().isin(exclude_statuses)].copy()

    if not active_snapshot_mode:
        df_customers = generate_customer_insights_from_sales(df_sales_strict, include_rfm=True)
        ml_bundle = build_ml_insight_bundle(df_sales_strict, df_customers, horizon_days=7)
    else:
        # Snapshot mode provides these
        if ml_bundle and "customers" in ml_bundle: # if bundle includes it
            df_customers = ml_bundle["customers"]
        else:
            df_customers = generate_customer_insights_from_sales(df_sales_strict, include_rfm=True)
    
    stock_df = load_cached_woocommerce_stock_data()
    if stock_df.empty and not active_snapshot_mode:
        with st.spinner("📦 Syncing Inventory levels..."):
             from BackEnd.services.hybrid_data_loader import load_woocommerce_stock_data
             stock_df = load_woocommerce_stock_data()
    
    if not stock_df.empty:
        from BackEnd.core.categories import get_category_for_sales
        stock_df["Category"] = stock_df["Name"].apply(get_category_for_sales)
        
        # Apply Global Strategy Filter to Inventory
        if global_cats and "All" not in global_cats:
            mask = pd.Series(False, index=stock_df.index)
            for cat in global_cats:
                mask |= stock_df["Category"].str.startswith(cat, na=False)
            stock_df = stock_df[mask]
    
    # Fetch Registered Customer Stats
    from BackEnd.services.hybrid_data_loader import load_woocommerce_customer_count
    customer_count = load_woocommerce_customer_count()
    
    st.session_state.dashboard_data = {
        "sales": df_sales_raw,
        "sales_map": df_sales_map,       # Dedicated map dataset
        "sales_active": df_sales_active,  # Operational dataset
        "sales_strict": df_sales_strict,  # Financial dataset
        "prev_sales_active": df_prev_active,
        "prev_sales_strict": df_prev_strict,
        "customers": df_customers,
        "customer_count": customer_count,
        "ml": ml_bundle,
        "stock": stock_df,
        "summary": {"woocommerce_live": len(df_sales_raw), "stock_rows": len(stock_df)},
        "hint": orders_status.get("status_message", ""),
        "window_label": window.lower() if window != "Custom Date Range" else f"{days_back} days"
    }

    data = st.session_state.dashboard_data
    
    # Dashboard analyzes full cleansed dataset (Sidebar filters removed)
    segment_filter = ["All"]
    status_filter = ["All"]

    # Apply user-selected global filters (already handled but ensuring scope)
    df_exec = data["sales_active"]
    
    # Re-calculate core dataset metrics
    exec_orders = build_order_level_dataset(df_exec)
    total_rev = sum_order_level_revenue(df_exec, order_df=exec_orders)
    order_count = exec_orders["order_id"].nunique() if not exec_orders.empty else 0
    cust_count = df_exec["customer_key"].nunique()
    total_items = int(df_exec["qty"].sum())
    aov = (total_rev / order_count) if order_count else 0
    avg_orders_per_day = order_count / max(1, days_back)
    
    # --- Comparative Logic ---
    df_prev_comp = data["prev_sales_active"] # Compare Active vs Active
    prev_orders_level = build_order_level_dataset(df_prev_comp)
    
    prev_items_val = df_prev_comp["qty"].sum() if not df_prev_comp.empty else 0
    prev_rev_val = sum_order_level_revenue(df_prev_comp, order_df=prev_orders_level)
    
    prev_orders_val = prev_orders_level["order_id"].nunique() if not prev_orders_level.empty else 0
    prev_aov_val = (prev_rev_val / prev_orders_val) if prev_orders_val else 0
    prev_cust_val = df_prev_comp["customer_key"].nunique() if not df_prev_comp.empty else 0
    prev_avg_orders_val = (prev_orders_val / days_back) if days_back else 0
    
    def calc_delta(curr, prev):
        if not prev or prev <= 0: return "", 0
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

    # 1. Executive Summary Pillars (Global Across Tabs)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1: ui.icon_metric("Total Items Sold", f"{total_items:,}", icon="📦", delta=d_items_label, delta_val=d_items_val)
    with c2: ui.icon_metric("Revenue", f"৳{total_rev:,.0f}", icon="💰", delta=d_rev_label, delta_val=d_rev_val)
    with c3: ui.icon_metric("Orders", f"{order_count:,}", icon="🛒", delta=d_orders_label, delta_val=d_orders_val)
    with c4: ui.icon_metric("Avg. Orders / Day", f"{avg_orders_per_day:,.0f}", icon="📅", delta=d_avg_label, delta_val=d_avg_val)
    with c5: ui.icon_metric("Customers", f"{cust_count:,}", icon="👥", delta=d_cust_label, delta_val=d_cust_val)
    with c6: ui.icon_metric("Basket Size", f"৳{aov:,.0f}", icon="💎", delta=d_aov_label, delta_val=d_aov_val)

    st.markdown("<br>", unsafe_allow_html=True)
    
    # --- NET SALES FINANCIAL IMPACT (STAGED LOADING FOR FAST RENDER) ---
    from BackEnd.services.returns_tracker import load_returns_data, get_current_sync_window, calculate_net_sales_metrics
    from threading import Thread
    import time

    sync_window = get_current_sync_window()

    def _load_returns_async(window: str, sales_df: pd.DataFrame):
        """Background thread function to load returns data."""
        try:
            st.session_state["returns_loading"] = True
            st.session_state["returns_load_started"] = time.time()
            returns_df = load_returns_data(sync_window=window, sales_df=sales_df)
            st.session_state.returns_data = returns_df
            st.session_state.last_returns_sync = window
            st.session_state["returns_loading"] = False
            st.session_state["returns_load_complete"] = True
        except Exception as e:
            st.session_state["returns_loading"] = False
            st.session_state["returns_load_error"] = str(e)
            log_error(e, context="Returns Background Load")

    # Check if we need to load returns data
    needs_load = (
        "returns_data" not in st.session_state
        or st.session_state.get("last_returns_sync") != sync_window
        or st.session_state.get("returns_data").empty
    )

    # Start background loading if needed and not already loading
    if needs_load and not st.session_state.get("returns_loading", False):
        # Initialize with empty DataFrame for fast render
        if "returns_data" not in st.session_state:
            st.session_state.returns_data = pd.DataFrame()
        # Start background thread for loading
        thread = Thread(target=_load_returns_async, args=(sync_window, df_exec), daemon=True)
        thread.start()
        st.session_state["returns_loading"] = True
        st.session_state["returns_load_started"] = time.time()

    # Use current returns data (cached or from previous load)
    df_returns_all = st.session_state.get("returns_data", pd.DataFrame()).copy()

    # Filter returns data to match the global time window
    if not df_returns_all.empty and "date" in df_returns_all.columns:
        valid_dates = df_returns_all["date"].notna()
        date_mask = (df_returns_all["date"].dt.date >= start_dt) & (df_returns_all["date"].dt.date <= end_dt)
        mask = valid_dates & date_mask
        df_returns_filtered = df_returns_all[mask]
    else:
        df_returns_filtered = df_returns_all

    net_metrics = calculate_net_sales_metrics(df_returns_filtered, sales_df=df_exec)

    # Show loading indicator if background load is active
    is_loading = st.session_state.get("returns_loading", False)
    load_elapsed = time.time() - st.session_state.get("returns_load_started", 0) if is_loading else 0
    
    # Debug info for returns data
    with st.expander("🔍 DEBUG: Returns Data Info", expanded=False):
        st.write(f"Returns data rows: {len(df_returns_all)}")
        st.write(f"Filtered returns rows: {len(df_returns_filtered)}")
        st.write(f"Date range: {start_dt} to {end_dt}")
        if not df_returns_filtered.empty:
            if "issue_type" in df_returns_filtered.columns:
                st.write(f"Issue types: {df_returns_filtered['issue_type'].value_counts().to_dict()}")
            if "returned_items" in df_returns_filtered.columns and len(df_returns_filtered) > 0:
                st.write(f"Sample returned_items: {df_returns_filtered['returned_items'].iloc[0]}")
                # Check for items with non-empty returned_items
                has_items = df_returns_filtered[df_returns_filtered['returned_items'].apply(lambda x: len(x) > 0 if isinstance(x, list) else False)]
                st.write(f"Rows with items: {len(has_items)}")
                if len(has_items) > 0:
                    st.write("Sample row with items:")
                    debug_cols = [c for c in ['order_id_raw', 'issue_type', 'product_details', 'returned_items'] if c in has_items.columns]
                    if debug_cols:
                        st.dataframe(has_items[debug_cols].head(2))
        st.write(f"Net metrics: {net_metrics}")
    
    # Loading indicator row
    if is_loading:
        loading_cols = st.columns([0.7, 0.3])
        with loading_cols[0]:
            st.markdown('<div class="sidebar-group-label" style="font-size:0.85rem; letter-spacing:1px;">💰 TRUE REVENUE & FINANCIAL IMPACT</div>', unsafe_allow_html=True)
        with loading_cols[1]:
            st.markdown(f"""
                <div style="display: flex; align-items: center; justify-content: flex-end; gap: 8px; font-size: 0.75rem; color: #f59e0b;">
                    <span class="animate-spin">🔄</span>
                    <span>Syncing Returns... ({load_elapsed:.0f}s)</span>
                </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown('<div class="sidebar-group-label" style="font-size:0.85rem; letter-spacing:1px;">💰 TRUE REVENUE & FINANCIAL IMPACT</div>', unsafe_allow_html=True)

    gross = net_metrics.get('gross_sales', 0)
    net_sales = net_metrics.get('net_sales', 0)
    net_yield_pct = (net_sales / gross * 100) if gross > 0 else 0.0

    # Total returned items and their value (moved to first position)
    total_ret_qty = net_metrics.get('total_return_qty_all', 0)  # Total qty from ALL returns
    total_ret_value = net_metrics.get('return_value_extracted', 0)
    returned_orders_pct = net_metrics.get('returned_orders_pct', 0.0)

    nc1, nc2, nc3, nc4, nc5, nc6 = st.columns(6)
    with nc1: ui.icon_metric("Total Returned Items", f"{total_ret_qty} Units", icon="📦", delta=f"{returned_orders_pct:.1f}% Orders", delta_val=-total_ret_value)
    with nc2: ui.icon_metric("Total Exchanged Items", f"{net_metrics.get('total_exchanged_items', 0)} Units", icon="🔄", delta="Total", delta_val=net_metrics.get('total_exchanged_items', 0))
    with nc3: ui.icon_metric("Loss (Returns + Partials)", f"৳{(net_metrics.get('return_value_extracted', 0) + net_metrics.get('partial_amounts', 0)):,.0f}", icon="📉", delta="Lost", delta_val=-(net_metrics.get('return_value_extracted', 0) + net_metrics.get('partial_amounts', 0)))
    with nc4: ui.icon_metric("Net Settled Sales", f"৳{net_sales:,.0f}", icon="🌟", delta="Net", delta_val=net_sales)
    with nc5: ui.icon_metric("Net Yield %", f"{net_yield_pct:.1f}%", icon="📊", delta="Efficiency", delta_val=net_yield_pct)
    with nc6: ui.icon_metric("Returned Orders %", f"{returned_orders_pct:.1f}%", icon="📈", delta=f"{net_metrics.get('return_count', 0)} Orders", delta_val=-returned_orders_pct)

    # --- RESTORED FINANCIAL INTEGRITY CHART ---
    # Only render if returns data is available with date column
    returns_ready = "returns_data" in st.session_state and not st.session_state.returns_data.empty and "date" in st.session_state.returns_data.columns

    if returns_ready:
        # Prepare Daily Financial Gap Data
        import plotly.graph_objects as go

        daily_gross = df_exec.groupby(df_exec['order_date'].dt.date)['item_revenue'].sum().reset_index()
        daily_gross.columns = ['date', 'gross']

        # Calculate daily lost value by cross-referencing returns with sales revenue
        ret_df_local = st.session_state.returns_data.copy()
        ret_df_local['date'] = pd.to_datetime(ret_df_local['date']).dt.date

        # Identify full returns and merge with sales to get their revenue value
        full_returns = ret_df_local[ret_df_local["issue_type"].isin(["Paid Return", "Non Paid Return"])].copy()
        # Ensure ID types match for merging
        full_returns['order_id'] = full_returns['order_id'].astype(str)
        sales_for_join = df_exec[['order_id', 'item_revenue']].copy()
        sales_for_join['order_id'] = sales_for_join['order_id'].astype(str)

        full_returns_with_val = pd.merge(full_returns, sales_for_join, on='order_id', how='left')

        # Group by date for mapping
        daily_full_loss = full_returns_with_val.groupby('date')['item_revenue'].sum().reset_index(name='val_lost')
        daily_partial_loss = ret_df_local[ret_df_local["issue_type"] == "Partial"].groupby('date')['partial_amount'].sum().reset_index(name='part_lost')

        # Merge losses
        daily_returns = pd.merge(daily_full_loss, daily_partial_loss, on='date', how='outer').fillna(0)
        daily_returns['total_lost'] = daily_returns['val_lost'] + daily_returns['part_lost']

        # Merge for plotting
        fin_plot = pd.merge(daily_gross, daily_returns[['date', 'total_lost']], on='date', how='left').fillna(0)
        fin_plot['net'] = fin_plot['gross'] - fin_plot['total_lost']
        fin_plot = fin_plot.sort_values('date')

        if not fin_plot.empty:
            fig_gap = go.Figure()
            fig_gap.add_trace(go.Scatter(
                x=fin_plot['date'], y=fin_plot['gross'],
                fill='tonexty', mode='lines', line=dict(color='rgba(59, 130, 246, 0.4)', width=0.5),
                name='Gross Verified', stackgroup='one'
            ))
            fig_gap.add_trace(go.Scatter(
                x=fin_plot['date'], y=fin_plot['net'],
                fill='tozeroy', mode='lines', line=dict(color='#10b981', width=3),
                name='Net Settled', stackgroup='one'
            ))
            fig_gap.update_layout(
                height=280, title="Sales Integrity Gap (Gross vs. Net Settled Sales)",
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=0,r=0,t=40,b=0), hovermode="x unified",
                legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center")
            )
            st.plotly_chart(fig_gap, use_container_width=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Routing based on sidebar selection
    selection = st.session_state.get("active_section", "💎 Sales Overview")

    if selection == "💎 Sales Overview":
        # --- MERGED STRATEGIC INTELLIGENCE HUB ---
        from BackEnd.services.strategic_intelligence import generate_executive_narrative
        from .dashboard_lib.story import render_dashboard_story
        
        # 1. Fetch Narrative Components
        story_points = render_dashboard_story(
            data["sales_active"], data["customers"], data["ml"], 
            window, df_prev_sales=data["prev_sales_active"], return_raw=True
        )
        
        briefing_points = generate_executive_narrative(
            data["sales_active"], 
            st.session_state.get("returns_data", pd.DataFrame()), 
            total_rev, prev_rev_val
        )
        
        # 2. Combine & Render
        all_points = story_points + briefing_points
        
        with st.container():
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, rgba(30, 58, 138, 0.08) 0%, rgba(30, 27, 75, 0.05) 100%); 
                        border-left: 5px solid #3b82f6; border-radius: 12px; padding: 24px; margin: 20px 0;
                        border: 1px solid rgba(59, 130, 246, 0.1); box-shadow: 0 4px 15px rgba(0,0,0,0.15);">
                <div style="display: flex; align-items: center; margin-bottom: 15px;">
                    <div style="font-size: 1.2rem; margin-right: 12px;">💎</div>
                    <div style="font-weight: 800; color: #3b82f6; font-size: 0.85rem; letter-spacing: 1.5px; text-transform: uppercase;">
                        Strategic Executive Intelligence
                    </div>
                </div>
                <div style="font-size: 0.95rem; line-height: 1.7; color: var(--text-color);">
                    {"<div style='margin-bottom:10px;'>" + "</div><div style='margin-bottom:10px;'>".join([f"• {point}" for point in all_points]) + "</div>"}
                </div>
            </div>
            """, unsafe_allow_html=True)
            
        st.divider()
        render_sales_overview_timeseries(data["sales_active"], ml_bundle=data["ml"])

        

    
    elif selection == "📊 Traffic & Acquisition":
        render_acquisition_analytics(data["sales_active"])
        
    elif selection == "👥 Customer Insight":
        st.subheader("Customer Insight")
        # Calculate registered vs guest revenue
        df_exec = data["sales_active"]
        if "customer_key" in df_exec.columns:
            is_registered = df_exec["customer_key"].str.startswith("reg_", na=False)
            reg_val = df_exec[is_registered]["item_revenue"].sum() if is_registered.any() else 0
            guest_val = df_exec[~is_registered]["item_revenue"].sum() if (~is_registered).any() else 0
        else:
            reg_val = 0
            guest_val = 0
        # Pass executive sales for analysis consistency
        render_customer_insight_tab(reg_val, guest_val, data["customer_count"], data["sales_active"])
        
    elif selection == "🔄 Returns Insights":
        from .dashboard_lib.returns_tracker import render_returns_tracker_page
        render_returns_tracker_page()

    elif selection == "📥 Sales Data Ingestion":
        render_deep_dive_tab(data["sales_active"], data["stock"], data["prev_sales_active"], window_label=data["window_label"])
        
    elif selection == "📦 Stock Insight":
        st.subheader("Operational Forecasting")
        render_inventory_health(data["stock"], data["ml"].get("forecast"), data["sales"])
        
    elif selection == "🚀 Data Pilot":
        render_data_pilot_page(data["sales"], data["stock"], data["customers"])


# --- MERGED COMPONENT LOGIC ---

def render_customer_insight_tab(reg_rev: float, guest_rev: float, total_accounts: int, df_sales: pd.DataFrame = None):
    """Enhanced Customer Intelligence Component with deep-dive analysis.
    
    This enhanced version combines the original segment analysis with
    the new Customer Insight module for dynamic filtering and detailed
    customer reports.
    """
    # Use the enhanced version that includes both legacy insights and new module
    render_enhanced_customer_insight_tab(reg_rev, guest_rev, total_accounts, df_sales)


# End of Dashboard controller logic

def render_data_pilot_page(sales_df: pd.DataFrame, stock_df: pd.DataFrame, customers_df: pd.DataFrame = None):
    """The AI-first command interface for natural language operations and Strategic Intelligence."""
    st.markdown('<div class="live-indicator"><span class="live-dot" style="background:#4f46e5; box-shadow: 0 0 10px #4f46e5;"></span>Intelligence Center Active | Data Pilot v11.0</div>', unsafe_allow_html=True)
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "🚀 Ask Data Pilot", 
        "🚨 Strategic War-Room", 
        "📦 Market Basket Intel",
        "🛡️ Data Trust"
    ])
    
    with tab1:
        st.markdown("### 🤖 Operations Data Pilot")
        st.caption("Ask natural language questions about your e-commerce health, stockouts, or revenue trends.")
        from FrontEnd.components.insights import render_ai_pilot_chat
        render_ai_pilot_chat(sales_df)
    
    with tab2:
        from .dashboard_lib.war_room import render_war_room_page
        returns_df = st.session_state.get("returns_data", pd.DataFrame())
        render_war_room_page(sales_df, returns_df)

    with tab3:
        st.markdown("#### 🔍 Automated Market Basket Intelligence")
        from BackEnd.services.affinity_engine import MarketBasketEngine
        engine = MarketBasketEngine(sales_df)
        rules = engine.get_associations(min_lift=1.2)
        
        if not rules.empty:
            st.markdown("##### Detected High-Lift Product Affinities")
            st.dataframe(rules[["Antecedent", "Consequent", "Lift", "Confidence", "Frequency"]].head(10), use_container_width=True, hide_index=True)
            
            # Bundle Fulfillment
            st.markdown("##### 📦 Bundle Fulfillment Analysis")
            from BackEnd.services.inventory_intel import InventoryIntelligence
            if stock_df.empty:
                st.info("Stock data unavailable. Sync inventory to see fulfillment analysis.")
            else:
                inv_intel = InventoryIntelligence(sales_df, stock_df)
                pairs = rules.head(5).apply(lambda x: {'A': x['Antecedent'], 'B': x['Consequent']}, axis=1).tolist()
                bundles = inv_intel.calculate_bundle_fulfillment(pairs)
                st.dataframe(bundles, use_container_width=True, hide_index=True)
        else:
            st.info("Insufficient transaction density to discover complex product associations. Check back after more orders.")

    with tab4:
        st.markdown("### 🛡️ System Reliability Audit")
        render_data_trust_panel(sales_df)
        if customers_df is not None:
            render_data_audit(sales_df, customers_df)
        else:
            st.warning("Customer data unavailable for deep audit.")
