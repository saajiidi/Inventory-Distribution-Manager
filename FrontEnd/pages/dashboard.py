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
from BackEnd.core.categories import sort_categories, format_category_label, get_subcategory_name
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
        end_date_str = today.strftime("%Y-%m-%d")
        start_date_str = (today - timedelta(days=days_back)).strftime("%Y-%m-%d")
        prev_start_date_str = (today - timedelta(days=days_back * 2)).strftime("%Y-%m-%d")
        prev_end_date_str = (today - timedelta(days=days_back + 1)).strftime("%Y-%m-%d")

    orders_status = get_woocommerce_orders_cache_status(start_date_str, end_date_str)
    
    needs_history = window == "Custom Date Range" and not orders_status.get("is_covered", True)
    if needs_history:
        st.warning(f"⏳ Connecting to WooCommerce Live to sync deep archival history ({start_date_str} to {end_date_str}). This runs seamlessly in the background and may take a few minutes. Your metrics will automatically populate once caching finishes!")
    from FrontEnd.utils.config import USE_STATIC_SNAPSHOT, SNAPSHOT_DATE, SNAPSHOT_LABEL, MAP_FORCE_SNAPSHOT
    
    # Determine Snapshot mode based on configuration OR manual user override (Slow Connection)
    slow_conn = st.session_state.get("conn_speed_mode") == "Slow Connection"
    active_snapshot_mode = USE_STATIC_SNAPSHOT or slow_conn
    
    if active_snapshot_mode:
        st.info(f"📶 **{SNAPSHOT_LABEL}**: Performance optimized for slow connections. Real-time syncing is paused.")
        df_sales_raw = load_hybrid_data(woocommerce_mode="cache_only", use_snapshot=True)
        df_sales_raw = prune_dataframe(df_sales_raw, DASHBOARD_SALES_COLUMNS)
    else:
        # Standard Live Logic
        # Force a sync for current-day data requests OR first-time initialization
        cache_empty = not orders_status.get("cache_exists", False)
        should_force = global_sync or (window == "Last Day") or needs_history or cache_empty
        
        if cache_empty:
             with st.spinner("🚀 Establishing first-time connection to WooCommerce..."):
                 # Blocking sync for the very first hit to ensure the app isn't empty
                 df_sales_raw = load_hybrid_data(start_date=start_date_str, end_date=end_date_str, woocommerce_mode="live")
                 df_sales_raw = prune_dataframe(df_sales_raw, DASHBOARD_SALES_COLUMNS)
        else:
             # Regular background refresh for existing users
             start_orders_background_refresh(start_date_str, end_date_str, force=should_force)
             sync_mode = "live" if (window == "Last Day" or global_sync) else "cache_only"
             df_sales_raw = prune_dataframe(load_hybrid_data(start_date=start_date_str, end_date=end_date_str, woocommerce_mode=sync_mode), DASHBOARD_SALES_COLUMNS)
    
    # 1. Map categories and apply global filters immediately
    if "Category" not in df_sales_raw.columns:
        from BackEnd.core.categories import get_category_for_sales
        df_sales_raw["Category"] = df_sales_raw["item_name"].apply(get_category_for_sales)
    
    global_cats = st.session_state.get("global_categories", ["All"])
    global_stats = st.session_state.get("global_statuses", ["All"])
    df_sales_raw = apply_global_filters(df_sales_raw, global_cats, global_stats)

    # v10.2: Ensure robust item-level revenue estimation
    from .dashboard_lib.data_helpers import estimate_line_revenue
    df_sales_raw["item_revenue"] = estimate_line_revenue(df_sales_raw)
    
    # 1.5 Geographic Intelligence (District Code Resolution)
    from BackEnd.core.geo import get_region_display
    df_sales_raw["_region_display"] = df_sales_raw.apply(lambda x: get_region_display(x.get("city", ""), x.get("state", "")), axis=1)
        
    # --- HYBRID MAP DATA: The map ALWAYS uses a clean snapshot to save memory ---
    if MAP_FORCE_SNAPSHOT:
        df_sales_map = load_hybrid_data(use_snapshot=True)
        # Apply categories to map snapshot if missing
        if "Category" not in df_sales_map.columns:
            from BackEnd.core.categories import get_category_for_sales
            df_sales_map["Category"] = df_sales_map["item_name"].apply(get_category_for_sales)
    else:
        df_sales_map = df_sales_raw

    # Fetch pre-calculated ML bundle if in snapshot mode, else build it
    if active_snapshot_mode:
        from BackEnd.services.hybrid_data_loader import load_static_ml_bundle
        ml_bundle = load_static_ml_bundle()
    else:
        ml_bundle = None # Will be built below
    
    # Fetch Previous context (Unfiltered by date, but filtered by Category/Status)
    df_prev_raw = load_hybrid_data(start_date=prev_start_date_str, end_date=prev_end_date_str, woocommerce_mode="cache_only")
    df_prev_raw = prune_dataframe(df_prev_raw, DASHBOARD_SALES_COLUMNS)
    from BackEnd.core.categories import get_category_for_sales
    from BackEnd.core.geo import get_region_display
    df_prev_raw["Category"] = df_prev_raw["item_name"].apply(get_category_for_sales)
    df_prev_raw["_region_display"] = df_prev_raw.apply(lambda x: get_region_display(x.get("city", ""), x.get("state", "")), axis=1)
    df_prev_raw = apply_global_filters(df_prev_raw, global_cats, global_stats)

    # SECURE ANALYTICS: Force Analysis to Shipped and Completed only
    valid_statuses = ["completed", "shipped"]
    df_sales_exec = df_sales_raw[df_sales_raw["order_status"].str.lower().isin(valid_statuses)].copy()
    df_prev_exec = df_prev_raw[df_prev_raw["order_status"].str.lower().isin(valid_statuses)].copy()
    
    if not active_snapshot_mode:
        df_customers = generate_customer_insights_from_sales(df_sales_exec, include_rfm=True)
        ml_bundle = build_ml_insight_bundle(df_sales_exec, df_customers, horizon_days=7)
    else:
        # Snapshot mode provides these
        if ml_bundle and "customers" in ml_bundle: # if bundle includes it
            df_customers = ml_bundle["customers"]
        else:
            df_customers = generate_customer_insights_from_sales(df_sales_exec, include_rfm=True)
    
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
        "sales_exec": df_sales_exec,
        "prev_sales": df_prev_raw,
        "prev_sales_exec": df_prev_exec,
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

    # Strictly remove Cancelled/Failed from raw data pool
    exclude_statuses = ["cancelled", "failed", "trash"]
    df_sales_raw = df_sales_raw[~df_sales_raw["order_status"].str.lower().isin(exclude_statuses)]
    
    # Apply user-selected filters
    df_exec = apply_global_filters(df_sales_raw, segment_filter, status_filter)
    
    # Re-calculate core dataset metrics
    exec_orders = build_order_level_dataset(df_exec)
    total_rev = sum_order_level_revenue(df_exec, order_df=exec_orders)
    order_count = exec_orders["order_id"].nunique() if not exec_orders.empty else 0
    cust_count = df_exec["customer_key"].nunique()
    total_items = df_exec["qty"].sum()
    aov = (total_rev / order_count) if order_count else 0
    avg_orders_per_day = order_count / max(1, days_back)
    
    # --- Comparative Logic ---
    df_prev_exec = data["prev_sales_exec"]
    prev_orders_level = build_order_level_dataset(df_prev_exec)
    
    prev_items_val = df_prev_exec["qty"].sum() if not df_prev_exec.empty else 0
    prev_rev_val = sum_order_level_revenue(df_prev_exec, order_df=prev_orders_level)
    
    prev_orders_val = prev_orders_level["order_id"].nunique() if not prev_orders_level.empty else 0
    prev_aov_val = (prev_rev_val / prev_orders_val) if prev_orders_val else 0
    prev_cust_val = df_prev_exec["customer_key"].nunique() if not df_prev_exec.empty else 0
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
    with c1: ui.icon_metric("Total Item Sold", f"{total_items:,}", icon="📦", delta=d_items_label, delta_val=d_items_val)
    with c2: ui.icon_metric("Revenue", f"৳{total_rev:,.0f}", icon="💰", delta=d_rev_label, delta_val=d_rev_val)
    with c3: ui.icon_metric("Orders", f"{order_count:,}", icon="🛒", delta=d_orders_label, delta_val=d_orders_val)
    with c4: ui.icon_metric("Avg. Orders / Day", f"{avg_orders_per_day:,.0f}", icon="📅", delta=d_avg_label, delta_val=d_avg_val)
    with c5: ui.icon_metric("Customers", f"{cust_count:,}", icon="👥", delta=d_cust_label, delta_val=d_cust_val)
    with c6: ui.icon_metric("Basket Size", f"৳{aov:,.0f}", icon="💎", delta=d_aov_label, delta_val=d_aov_val)

    st.markdown("<br>", unsafe_allow_html=True)
    

    # Routing based on sidebar selection
    selection = st.session_state.get("active_section", "💎 Sales Overview")

    if selection == "💎 Sales Overview":
        # Global Narrative & Summary
        render_dashboard_story(data["sales_exec"], data["customers"], data["ml"], window, df_prev_sales=data["prev_sales_exec"])
        
        # 🧾 Performance Report Download
        st.markdown("---")
        ex1, ex2 = st.columns([3, 1])
        with ex1:
            st.markdown("#### 🧾 Executive Performance Summary")
            st.caption("Download a high-level performance matrix including category-wise revenue and volume distribution.")
        with ex2:
            from datetime import datetime
            # Build Performance Matrix
            from BackEnd.core.categories import get_display_category
            perf_df = data["sales_exec"].groupby("Category").agg(
                Revenue=("item_revenue", "sum"),
                Orders=("order_id", "nunique"),
                Units=("qty", "sum")
            ).reset_index().sort_values("Revenue", ascending=False)
            perf_df["AOV"] = (perf_df["Revenue"] / perf_df["Orders"]).round(2)
            
            # Context-Aware Labels (Sub-category if filtered, else Parent)
            selected_cats = st.session_state.get("global_categories", ["All"])
            perf_df["Category"] = perf_df["Category"].apply(lambda x: get_display_category(x, selected_cats))
            
            perf_bytes = ui.export_to_excel(perf_df, "Performance Matrix")
            st.download_button(
                label="📊 Download Performance Matrix",
                data=perf_bytes,
                file_name=f"deen_performance_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

        st.divider()
        render_sales_overview_timeseries(data["sales_exec"], ml_bundle=data["ml"])

        

    
    elif selection == "📊 Traffic & Acquisition":
        render_acquisition_analytics(data["sales_exec"])
        
    elif selection == "👥 Customer Insight":
        st.subheader("Customer Insight")
        # Pass executive sales for analysis consistency
        render_customer_insight_tab(reg_val, guest_val, data["customer_count"], data["sales_exec"])
        
    elif selection == "📥 Sales Data Ingestion":
        render_deep_dive_tab(data["sales_exec"], data["stock"], data["prev_sales_exec"])
        
    elif selection == "📦 Stock Insight":
        st.subheader("Operational Forecasting")
        render_inventory_health(data["stock"], data["ml"].get("forecast"), data["sales"])
        
    elif selection == "🛡️ Data Trust":
        st.subheader("System Reliability Audit")
        render_data_trust_panel(data["sales"])
        render_data_audit(data["sales"], data["customers"])
        
    elif selection == "🚀 Data Pilot":
        render_data_pilot_page(data["sales"], data["stock"])


# --- MERGED COMPONENT LOGIC ---

def render_customer_insight_tab(reg_rev: float, guest_rev: float, total_accounts: int, df_sales: pd.DataFrame = None):
    """Merged Customer Intelligence Component."""
    if "dashboard_data" not in st.session_state:
        st.info("Please sync data to view customer intelligence.")
        return
        
    df = st.session_state.dashboard_data["customers"]
    if df.empty:
        st.warning("No customer segments identified in this period.")
        return

    # v10.5: Account Registrations Summary (Moved to Top for Visibility)
    st.markdown("### 🔐 Account Registrations")
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Registered Customers", f"{total_accounts:,}")
    
    reg_pct = (reg_rev / (reg_rev + guest_rev) * 100) if (reg_rev + guest_rev) > 0 else 0
    m2.metric("Registered Revenue Share", f"{reg_pct:.1f}%")
    
    is_reg = df["customer_id"].str.startswith("reg_", na=False)
    reg_aov = df[is_reg]["avg_order_value"].mean()
    gst_aov = df[~is_reg]["avg_order_value"].mean()
    gap = ((reg_aov - gst_aov) / gst_aov * 100) if gst_aov > 0 else 0
    m3.metric("Member vs. Guest AOV", f"{gap:+.1f}% Gap", help="Registered members usually spend significantly more per order.")
    
    st.divider()

    # Visual Segments sequentially rendered
    st.markdown("### 📊 Value Segments")
    if True:
        # Segment Mix
        mix_df = df["segment"].value_counts().reset_index()
        mix_df.columns = ["Segment", "Count"]
        
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(ui.donut_chart(mix_df, values="Count", names="Segment", title="Segment Distribution"), use_container_width=True)
        with c2:
            rev_df = df.groupby("segment")["total_revenue"].sum().reset_index().sort_values("total_revenue", ascending=False)
            st.plotly_chart(ui.bar_chart(rev_df, x="total_revenue", y="segment", title="Revenue by Segment", color_scale="Tealgrn"), use_container_width=True)

        c3, c4 = st.columns(2)
        with c3:
            cycle_df = df.groupby("segment")["purchase_cycle_days"].mean().reset_index().dropna()
            if not cycle_df.empty:
                st.plotly_chart(ui.bar_chart(cycle_df, x="purchase_cycle_days", y="segment", title="Avg Days Between Purchases", color_scale="Plasma"), use_container_width=True)
        with c4:
            clv_df = df.groupby("segment")["clv"].mean().reset_index().dropna()
            if not clv_df.empty:
                st.plotly_chart(ui.bar_chart(clv_df, x="clv", y="segment", title="Avg Customer Lifetime Value (CLV)", color_scale="Purpor"), use_container_width=True)

    st.divider()
    st.markdown("### 🎯 Priority Outreach")
    if True:
        # CRM Queue
        st.caption("Strategic segments requiring immediate attention based on recency and business value.")
        priority = df.sort_values(["total_revenue", "recency_days"], ascending=[False, True]).head(15)
        st.dataframe(
            priority[["primary_name", "segment", "total_revenue", "total_orders", "recency_days"]].rename(
                columns={"primary_name": "Customer", "total_revenue": "LTV", "recency_days": "Last Purchase (Days)"}
            ),
            use_container_width=True, hide_index=True
        )


        
        st.divider()
        c5, c6 = st.columns(2)
        with c5:
            # Registration Mix logic
            mix_df = pd.DataFrame({
                "Type": ["Registered Account", "Guest Checkout"],
                "Revenue": [reg_rev, guest_rev]
            })
            st.plotly_chart(ui.donut_chart(mix_df, values="Revenue", names="Type", title="Sales by Enrollment Type"), use_container_width=True)
        with c6:
            # AOV Comparison logic
            aov_df = pd.DataFrame({
                "Account Type": ["Registered", "Guest"],
                "AOV": [reg_aov, gst_aov]
            })
            st.plotly_chart(ui.bar_chart(aov_df, x="AOV", y="Account Type", title="Average Order Value Comparison"), use_container_width=True)

    st.divider()
    st.markdown("### 📈 Cohort Retention")
    if True:
        st.markdown("#### 📈 Customer Retention Cohorts")
        st.caption("Tracking how cohorts of new customers return and purchase in subsequent months.")
        
        cohort_matrix = generate_cohort_matrix(df_sales)
        if not cohort_matrix.empty:
            import plotly.graph_objects as go
            
            # Heatmap representation
            fig = go.Figure(data=go.Heatmap(
                z=cohort_matrix.values,
                x=[f"Month {i}" for i in cohort_matrix.columns],
                y=[str(i) for i in cohort_matrix.index],
                colorscale='YlGnBu',
                text=np.around(cohort_matrix.values, decimals=1),
                texttemplate="%{text}%",
                hoverinfo='z'
            ))
            fig.update_layout(title="Monthly Retention Matrix (%)", xaxis_title="Retention Month", yaxis_title="Cohort Month")
            st.plotly_chart(fig, use_container_width=True)
            
            with st.expander("📝 Understanding Cohorts"):
                st.info("Each row represents a 'Cohort' of customers who made their first purchase in that month. The columns show the percentage of those customers who returned to buy in subsequent months.")
        else:
            st.info("Insufficient longitudinal data to generate a cohort matrix.")

    st.divider()
    st.markdown("### 🔍 Identity Search")
    if True:
        st.markdown("#### 🔍 Customer Identity 360")
        st.caption("Locate any customer profile by Phone, Email, Name, or Order ID.")
        
        c_search1, c_search2 = st.columns([4, 1])
        with c_search1:
            q = st.text_input("Search ID", placeholder="017..., user@email.com, Deen, #10234", label_visibility="collapsed", key="cust_search_input").strip()
        with c_search2:
            search_trigger = st.button("🔍 Search", key="btn_cust_search", use_container_width=True)
        
        if q or search_trigger:
            if not q:
                st.warning("Please enter a search term.")
            else:
                from BackEnd.services.customer_insights import search_customers
            # We use the segmented dataframe 'df' for high-level search
            results = search_customers(q, df)
            
            if q.startswith("#") or (q.isdigit() and len(q) > 4 and len(q) < 10):
                # Potential Order ID search
                oid = q.replace("#", "")
                if df_sales is not None:
                    match_order = df_sales[df_sales["order_id"].astype(str) == oid]
                    if not match_order.empty:
                        st.success(f"Found Order: #{oid}")
                        st.dataframe(match_order[["item_name", "qty", "price", "order_status", "order_date"]], hide_index=True)
                        # Derive customer from order
                        name_from_order = match_order.iloc[0]["customer_name"]
                        results = df[df["primary_name"].str.contains(name_from_order, case=False, na=False)]
            
            if results.empty:
                st.info("No matching profiles found in the current intelligence cache.")
            else:
                if len(results) > 1:
                    st.warning(f"Found {len(results)} potential matches. Select a profile below:")
                    selected_name = st.selectbox("Select Profile", results["primary_name"].tolist())
                    profile = results[results["primary_name"] == selected_name].iloc[0]
                else:
                    profile = results.iloc[0]
                
                # Render Profile
                st.markdown(f"### Profile: {profile['primary_name']}")
                p1, p2, p3, p4 = st.columns(4)
                
                reg_status = "✅ Registered" if str(profile["customer_id"]).startswith("reg_") else "👤 Guest"
                segment = profile.get("segment", "Regular")
                tier_colors = {"VIP": "#FDE047", "Potential Loyalist": "#10B981", "New": "#3B82F6", "At Risk": "#F87171", "Churned": "#6B7280"}
                tier_color = tier_colors.get(segment, "#E2E8F0")
                
                st.markdown(f'<div style="background:{tier_color}33; color:{tier_color}; border:1px solid {tier_color}; border-radius:12px; padding:4px 12px; display:inline-block; font-weight:700; margin-bottom:12px;">🏆 {segment}</div>', unsafe_allow_html=True)
                
                p1.metric("Account Type", reg_status)
                p2.metric("Lifetime Orders", f"{int(profile['total_orders'])}")
                p3.metric("Lifetime Value (LTV)", f"৳{profile['total_revenue']:,.0f}")
                p4.metric("Avg Basket", f"৳{profile['avg_order_value']:,.0f}")
                
                # Detailed Info
                c_inf1, c_inf2 = st.columns([2, 1])
                with c_inf1:
                    with st.expander("📄 Identification & Contact Details", expanded=True):
                        st.write(f"**Primary Email:** {profile.get('all_emails', 'N/A') or 'N/A'}")
                        st.write(f"**Primary Phone:** {profile.get('all_phones', 'N/A') or 'N/A'}")
                        st.write(f"**First Seen:** {pd.to_datetime(profile['first_order']).strftime('%d %b %Y')}")
                        st.write(f"**Last Active:** {pd.to_datetime(profile['last_order']).strftime('%d %b %Y')} ({int(profile['recency_days'])} days ago)")
                
                with c_inf2:
                    # Tweak 2: Product Affinity
                    if "favorite_product" in profile:
                        st.markdown("##### 🛍️ Product Affinity")
                        st.caption("Top item for this customer.")
                        st.info(f"**Most Bought:** {profile['favorite_product']}")

    st.divider()
    st.markdown("### 🔍 Identity Ledger")
    if True:
        st.dataframe(df, use_container_width=True, hide_index=True)


# End of Dashboard controller logic

def render_data_pilot_page(sales_df: pd.DataFrame, stock_df: pd.DataFrame):
    """The AI-first command interface for natural language operations."""
    st.markdown('<div class="live-indicator"><span class="live-dot" style="background:#4f46e5; box-shadow: 0 0 10px #4f46e5;"></span>AI Service Active | Data Pilot v10.0</div>', unsafe_allow_html=True)
    
    st.markdown("""
    ### 🚀 Operations Data Pilot
    Ask natural language questions about your e-commerce health, stockouts, or revenue trends.
    """)
    
    from FrontEnd.components.insights import render_ai_pilot_chat
    render_ai_pilot_chat()
    
    st.divider()
    
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
