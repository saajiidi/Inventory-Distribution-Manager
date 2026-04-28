"""Customer Insight Page - Unified Dashboard.

- Detailed customer reports
- Consolidated customer ledger (Merged from Data Extractor)
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any

import streamlit as st
import pandas as pd
import numpy as np

# Use EXISTING working API components
from FrontEnd.components.customer_insight.customer_filters import (
    render_customer_filters,
    apply_customer_filters,
)
from FrontEnd.components.customer_insight.customer_selector import (
    render_customer_selector,
)
from FrontEnd.components.customer_insight.customer_report import render_customer_report
from FrontEnd.components.ui import export_to_excel

# Use EXISTING working services (inherits from working dashboard data)
from BackEnd.services.customer_insights import (
    generate_customer_insights_from_sales,
    generate_cohort_matrix
)
from BackEnd.services.customer_manager import (
    load_customer_mapping, 
    update_customer_mapping, 
    get_customer_metrics,
    load_raw_customer_data,
    save_mapping,
    build_customer_mapping
)
from BackEnd.commerce_ops.persistence import KeyManager

from FrontEnd.components import ui
from BackEnd.core.logging_config import get_logger
from FrontEnd.utils.state import app_state


logger = get_logger("customer_insight_page")


def _render_metric_cards(metrics: Dict[str, Any]) -> None:
    """Render consistent, modern metric cards using enterprise components."""
    
    cols = st.columns(4)
    
    with cols[0]:
        ui.metric_highlight(
            label="Total Base (Est.)",
            value=f"{metrics.get('est_total_customers', 0):,}",
            help_text="Lifetime Customers",
            icon="👥"
        )
    with cols[1]:
        ui.metric_highlight(
            label="Total Registered ✨",
            value=f"{metrics.get('total_registered', 0):,}",
            help_text="Exact Account Holders",
            icon="🏆"
        )
    with cols[2]:
        ui.metric_highlight(
            label="Guest Shoppers",
            value=f"{metrics.get('est_total_guests', 0):,}",
            help_text="Lifetime Base (Est.)",
            icon="🚶"
        )
    with cols[3]:
        ui.metric_highlight(
            label="Active in Period",
            value=f"{metrics.get('active_customers', 0):,}",
            help_text=f"Reg: {metrics.get('registered_active', 0):,} | Guest: {metrics.get('guest_active', 0):,}",
            icon="🔥"
        )

def render_customer_insight_page() -> None:
    """Render the main Customer Insight page with tabbed interface.
    
    Uses existing working data from session state (dashboard_data).
    """
    # Single clean header
    st.markdown("## 👥 Customer Insight")
    st.caption("Filter customers by purchase history, order count, and spending")
    
    # Check for existing data (works with existing API)
    if "dashboard_data" not in st.session_state:
        st.info("📊 Please sync data from the main dashboard first")
        st.markdown("""
            **Getting Started:**
            1. Go to **Business Intelligence** dashboard
            2. Click **Sync Data** to load WooCommerce data
            3. Return here to filter customers
        """)
        return

    # Tabbed Interface
    tab_analysis, tab_ledger = st.tabs(["🔍 Active Analysis", "📋 Consolidated Ledger"])
    
    with tab_analysis:
        _render_analysis_tab()
        
    with tab_ledger:
        _render_consolidation_tab()


def _render_analysis_tab() -> None:
    """Existing analysis logic."""
    if "dashboard_data" not in st.session_state:
        st.info("📊 Please sync data to use customer filters.")
        return
        
    sales_df = app_state.sales_active
    if sales_df.empty:
        st.info("📭 No sales data available. Please sync data first.")
        return

    # 1. Global Insights (Metrics & Charts) BEFORE filters
    _render_global_insights(sales_df)
    
    st.markdown("### 🔍 Filter & Analyze Customers")
    st.caption("Find specific customers by purchase history, spending, and accounts")

    # 2. HORIZONTAL LAYOUT: Full width filters on top, results below
    col_f, col_s = st.columns([3, 1])
    with col_f:
        filters = render_customer_filters(
            on_filter_change=_on_filter_change,
            key_prefix=KeyManager.get_key("ci", "page"),
        )
    
    with col_s:
        _render_mapping_updater_fragment()

    # 3. Results section below filters
    _render_filtered_results(sales_df, filters)


@st.fragment
def _render_mapping_updater_fragment() -> None:
    """Isolated UI component that updates customer mapping without full page reload."""
    st.markdown("### ⚙️ Optimization")
    if st.button("🔄 Update Customer Mapping", width="stretch", help="Fetch recent orders and update first-order dates"):
        with st.spinner("Updating mapping..."):
            sales_df = app_state.sales_active
            if not sales_df.empty:
                update_customer_mapping(sales_df)
                st.success("Mapping updated!")
            else:
                st.warning("No active sales data to update from.")
    
    mapping_df = load_customer_mapping()
    if mapping_df.empty:
        st.warning("⚠️ No customer mapping found. Please run the build script or update mapping.")
    else:
        st.success(f"✅ **{len(mapping_df):,}** customers indexed")
        st.caption("Data Sources: WooCommerce History + External Sheets")


def _render_consolidation_tab() -> None:
    """Merged logic from Customer Data Extractor."""
    st.markdown("### 📋 Consolidated Customer Ledger")
    st.caption("Detailed view of unique customers across WooCommerce and External Sheets.")
    
    # Sidebar-like controls but inline for the tab
    col_a, col_b = st.columns([3, 1])
    
    with col_b:
        st.markdown("**Sync Controls**")
        if st.button("🔄 Sync & Consolidate Now", help="Re-process all external sheets and WooCommerce records"):
            with st.spinner("Fetching and processing data..."):
                raw_df = load_raw_customer_data()
                if not raw_df.empty:
                    woo_sales = st.session_state.dashboard_data.get("sales_active", pd.DataFrame()) if "dashboard_data" in st.session_state else pd.DataFrame()
                    consolidated = build_customer_mapping(woo_sales, raw_df)
                    save_mapping(consolidated)
                    st.success("Sync Complete!")
                    st.rerun()
                else:
                    st.error("Failed to load source data from external sheets.")
    
    with col_a:
        # Load Cached Data
        df = load_customer_mapping()
        
        if df.empty:
            st.info("👋 No consolidated customer data found yet.")
            return

        # Search and Filter for Ledger
        l_search = st.text_input("🔍 Search Ledger by Name, Phone, or Email", placeholder="Start typing...", key=KeyManager.get_key("ci", "ledger_search"))
        
        # Apply Filters
        filtered_df = df.copy()
        if l_search:
            mask = (
                filtered_df['primary_name'].str.contains(l_search, case=False, na=False) |
                filtered_df['primary_phone'].str.contains(l_search, case=False, na=False) |
                filtered_df['primary_email'].str.contains(l_search, case=False, na=False) |
                filtered_df['secondary_names'].str.contains(l_search, case=False, na=False)
            )
            filtered_df = filtered_df[mask]
        
        # Display Table
        st.dataframe(
            filtered_df,
            width="stretch",
            hide_index=True,
            column_config={
                "first_order_date": st.column_config.DateColumn("First Order"),
                "last_order_date": st.column_config.DateColumn("Last Order"),
                "total_orders": st.column_config.NumberColumn("Orders", format="%d"),
            }
        )

        # Quick Export
        summary_metrics = {
            "Total Customers in Ledger": len(df),
            "Filtered Records": len(filtered_df),
            "Report Generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        excel_bytes = ui.export_to_excel(
            filtered_df, 
            sheet_name="Customer Ledger", 
            summary_metrics=summary_metrics
        )
        st.download_button(
            label="📥 Download Custom Excel Report",
            data=excel_bytes,
            file_name=f"customer_ledger_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            width="stretch",
            key=KeyManager.get_key("ci", "ledger_export_btn")
        )
        


def _on_filter_change(filters: Dict[str, Any]) -> None:
    """Handle filter changes.
    
    Args:
        filters: Updated filter dictionary
    """
    logger.info(f"Filters applied: {filters}")
    st.session_state[KeyManager.get_key("ci", "filters_applied")] = True

def _get_global_date_range() -> tuple[date, date]:
    """Get start and end dates from global time window."""
    today = date.today()
    window = st.session_state.get("time_window", "Last Month")
    if window == "Custom Date Range":
        start_dt = st.session_state.get("wc_sync_start_date", today - timedelta(days=30))
        end_dt = st.session_state.get("wc_sync_end_date", today)
    else:
        window_map = {
            "Last Day": 1, "Last 3 Days": 3, "Last 7 Days": 7,
            "Last 15 Days": 15, "Last Month": 30, "Last 3 Months": 90,
            "Last Quarter": 90, "Last Half Year": 180, "Last 9 Months": 270, "Last Year": 365
        }
        days_back = window_map.get(window, 30)
        start_dt = today - timedelta(days=days_back)
        end_dt = today
        
    # Ensure we return standard datetime.date objects, stripping off Pandas Timestamp metadata if present
    if hasattr(start_dt, "date"): start_dt = start_dt.date()
    if hasattr(end_dt, "date"): end_dt = end_dt.date()
    return start_dt, end_dt

def _render_global_insights(sales_df: pd.DataFrame) -> None:
    """Render the global insights metrics and charts.
    
    Args:
        sales_df: Current active sales dataframe
    """
    # Get contextual date range from global operational bounds
    start_date, end_date = _get_global_date_range()

    # 1. PRE-COMPUTED GLOBAL METRICS (Always show these)
    mapping_metrics = get_customer_metrics(start_date, end_date)
    
    # Calculate Active count from current sales (pre-filter)
    all_active_sales = sales_df.copy()
    active_count = all_active_sales["customer_key"].nunique() if not all_active_sales.empty else 0
    
    # Calculate Registered vs Guest active customers
    if "customer_key" in all_active_sales.columns:
        reg_mask = all_active_sales["customer_key"].astype(str).str.startswith("reg_")
        registered_active = all_active_sales[reg_mask]["customer_key"].nunique()
        guest_active = active_count - registered_active
    else:
        registered_active = 0
        guest_active = 0

    # Advanced Time-Weighted (EMA) Guest Estimation
    dashboard_data = st.session_state.get("dashboard_data", {})
    total_registered = dashboard_data.get("customer_count", 0)
    full_sales_df = dashboard_data.get("sales", pd.DataFrame())
    
    if not full_sales_df.empty and "customer_key" in full_sales_df.columns and "order_date" in full_sales_df.columns:
        # 1. Extract daily unique guest vs registered cohorts
        trend_df = full_sales_df[["order_date", "customer_key"]].copy()
        trend_df["is_reg"] = trend_df["customer_key"].astype(str).str.startswith("reg_")
        trend_df["date"] = pd.to_datetime(trend_df["order_date"]).dt.date
        
        daily_cohorts = trend_df.groupby(["date", "is_reg"])["customer_key"].nunique().unstack(fill_value=0)
        
        if True in daily_cohorts.columns and False in daily_cohorts.columns:
            # 2. Calculate daily ratio with protection against division by zero
            daily_cohorts["ratio"] = daily_cohorts[False] / daily_cohorts[True].replace(0, 1)
            
            # 3. Apply Time-Series Exponential Moving Average (30-day span) 
            # This weighs recent behavioral trends exponentially higher than historical ones
            ema_ratio = daily_cohorts["ratio"].ewm(span=30, adjust=False).mean().iloc[-1]
            base_ratio = float(ema_ratio)
        else:
            base_ratio = guest_active / max(registered_active, 1)
    else:
        base_ratio = guest_active / max(registered_active, 1) if registered_active > 0 else 1.2
        
    # 4. Apply Logarithmic Maturity Decay (adjusts the estimate slightly downwards as the core base scales)
    maturity_factor = np.log10(max(total_registered, 10)) / 5.0 if total_registered > 1000 else 1.0
    adjusted_ratio = base_ratio * max(0.7, (1.1 - (maturity_factor * 0.15)))
    
    est_total_guests = int(total_registered * adjusted_ratio)
    est_total_customers = total_registered + est_total_guests

    # Calculate Returns count
    return_count = 0
    if "returns_data" in st.session_state:
        ret_df = st.session_state.returns_data
        if not ret_df.empty:
            # Filter returns to date range
            mask = (ret_df["date"].dt.date >= start_date) & (ret_df["date"].dt.date <= end_date)
            return_count = ret_df[mask]["order_id"].nunique()

    # Render Modern Cards
    _render_metric_cards({
        "total_customers": mapping_metrics['total_customers'],
        "active_customers": active_count,
        "est_total_customers": est_total_customers,
        "total_registered": total_registered,
        "est_total_guests": est_total_guests,
        "registered_active": registered_active,
        "guest_active": guest_active,
        "return_count": return_count
    })
    
    st.markdown("<br>", unsafe_allow_html=True)

    # Render EMA Trend Chart for Guest vs Registered
    if 'daily_cohorts' in locals() and not daily_cohorts.empty and True in daily_cohorts.columns and False in daily_cohorts.columns:
        import plotly.express as px
        
        st.markdown("#### 📈 Account Registration Trend (EMA Smoothed)")
        st.caption("Tracking the 14-day exponential moving average of Guest vs Registered activity.")
        
        plot_df = daily_cohorts.copy().reset_index()
        plot_df["Registered Users"] = plot_df[True].ewm(span=14, adjust=False).mean()
        plot_df["Guest Shoppers"] = plot_df[False].ewm(span=14, adjust=False).mean()
        
        melted_df = plot_df.melt(
            id_vars=["date"], 
            value_vars=["Registered Users", "Guest Shoppers"],
            var_name="Account Type",
            value_name="Daily Active (Smoothed)"
        )
        
        fig = px.area(
            melted_df, 
            x="date", 
            y="Daily Active (Smoothed)", 
            color="Account Type",
            color_discrete_map={
                "Registered Users": "rgba(16, 185, 129, 0.7)", # Emerald
                "Guest Shoppers": "rgba(99, 102, 241, 0.5)"    # Indigo
            },
            line_shape="spline"
        )
        
        fig.update_layout(
            height=280,
            margin=dict(l=0, r=0, t=10, b=0),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="",
            yaxis_title="Activity Volume",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")


def _render_filtered_results(sales_df: pd.DataFrame, filters: Dict[str, Any]) -> None:
    """Render the filtered customer results and metrics.
    
    Args:
        sales_df: Current active sales dataframe
        filters: Filter settings
    """
    # Apply filters to existing data
    with st.spinner("🔍 Filtering customers..."):
        # Generate customer insights from sales data
        customers_df = _get_filtered_customers_from_sales(sales_df, filters)
    
    # Inject Account Type and Loyalty Scoring
    if not customers_df.empty:
        if "customer_key" in customers_df.columns:
            customers_df["account_type"] = np.where(
                customers_df["customer_key"].astype(str).str.startswith("reg_"), 
                "Registered", 
                "Guest"
            )
            
        # Loyalty Score Calculation based on Recency/Frequency/Monetary Proxies
        if "total_orders" in customers_df.columns and ("total_revenue" in customers_df.columns or "total_value" in customers_df.columns):
            rev_col = "total_revenue" if "total_revenue" in customers_df.columns else "total_value"
            o_score = (customers_df["total_orders"] / 10.0).clip(upper=1.0) * 50
            r_score = (customers_df[rev_col] / 20000.0).clip(upper=1.0) * 50
            customers_df["loyalty_score"] = (o_score + r_score).round(0)
            
            conditions = [
                customers_df["loyalty_score"] >= 80,
                customers_df["loyalty_score"] >= 50,
                customers_df["loyalty_score"] >= 20
            ]
            choices = ["Platinum 🏆", "Gold 🌟", "Silver ⭐"]
            customers_df["loyalty_tier"] = np.select(conditions, choices, default="Bronze 🥉")

    # Show results summary or early return
    if customers_df.empty:
        st.warning("📭 No customers match your specific filters. Try adjusting your search.")
        return
    
    # Matching Customers summary (Filtered results)
    st.markdown(f"### 🔍 Filtered Report: **{len(customers_df):,}** Customers")
    st.caption("Detailed metrics for the customers matching your current search criteria.")
    
    # Additional insight stats (Revenue/AOV)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        total_orders = customers_df["total_orders"].sum() if "total_orders" in customers_df.columns else 0
        ui.icon_metric("Total Orders (Filtered)", f"{int(total_orders):,}", icon="🛒")
    with col2:
        total_rev = customers_df["total_revenue"].sum() if "total_revenue" in customers_df.columns else customers_df.get("total_value", pd.Series([0])).sum()
        ui.icon_metric("Total Revenue (Filtered)", f"৳{total_rev:,.0f}", icon="💰")
    with col3:
        avg_aov = customers_df["avg_order_value"].mean() if "avg_order_value" in customers_df.columns else 0
        ui.icon_metric("Avg AOV (Filtered)", f"৳{avg_aov:,.0f}", icon="💳")
    with col4:
        if "loyalty_tier" in customers_df.columns:
            plat_count = (customers_df["loyalty_tier"] == "Platinum 🏆").sum()
            ui.icon_metric("Platinum Loyalty", f"{plat_count:,}", icon="👑")
    
    # Export button
    export_col1, export_col2 = st.columns([1, 3])
    with export_col1:
        # Prepare export data with key customer info
        export_df = customers_df.copy()
        # Select relevant columns for export
        export_columns = [
            "customer_id", "customer_key", "primary_name", "name", "all_emails", "all_phones",
            "account_type", "loyalty_tier", "loyalty_score",
            "total_orders", "total_revenue", "total_value", "avg_order_value", 
            "first_order", "last_order", "segment"
        ]
        # Only include columns that exist
        available_cols = [c for c in export_columns if c in export_df.columns]
        export_df = export_df[available_cols]
        
        excel_data = export_to_excel(export_df, "filtered_customers")
        st.download_button(
            label="📥 Export Report",
            data=excel_data,
            file_name=f"filtered_customers_{len(customers_df)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with export_col2:
        st.caption(f"Exporting {len(customers_df)} unique customers with their details")
    
    st.markdown("---")
    
    # Customer selection
    id_col = "customer_id" if "customer_id" in customers_df.columns else "customer_key"
    selected_customer = render_customer_selector(
        customers_df=customers_df,
        on_select=_on_customer_select,
        key_prefix=KeyManager.get_key("ci", "page"),
    )
    
    # Store in session state
    if selected_customer:
        st.session_state[KeyManager.get_key("ci", "selected_customer")] = selected_customer
    
    # Show report if customer selected
    if selected_customer:
        st.markdown("---")
        render_customer_report(
            customer_key=selected_customer,
            customers_df=customers_df,
            key_prefix=KeyManager.get_key("ci", "page"),
        )


def _get_filtered_customers_from_sales(
    sales_df: pd.DataFrame,
    filters: Dict[str, Any]
) -> pd.DataFrame:
    """Generate and filter customer data from existing sales DataFrame.
    
    Args:
        sales_df: Sales data from existing working API
        filters: Filter settings from customer_filters
        
    Returns:
        Filtered customers DataFrame
    """
    if sales_df.empty:
        return pd.DataFrame()
    
    # Use the unified apply_customer_filters which handles hierarchical Category/Product/Size
    from FrontEnd.components.customer_insight.customer_filters import apply_customer_filters
    return apply_customer_filters(sales_df, filters)


def _on_customer_select(customer_key: str) -> None:
    """Handle customer selection.
    
    Args:
        customer_key: Selected customer identifier
    """
    logger.info(f"Customer selected: {customer_key}")


def _show_global_stats() -> None:
    """Show global customer statistics using existing working data."""
    st.markdown("---")
    st.markdown("### 📊 Global Statistics")
    
    try:
        # Use existing working data from session state
        if "dashboard_data" in st.session_state:
            data = st.session_state.dashboard_data
            sales_df = data.get("sales_active", pd.DataFrame())
            
            if not sales_df.empty:
                # Calculate unique customers from sales data
                unique_customers = sales_df["customer_key"].nunique() if "customer_key" in sales_df.columns else 0
                total_rev = sales_df["item_revenue"].sum() if "item_revenue" in sales_df.columns else 0
                total_orders = sales_df["order_id"].nunique() if "order_id" in sales_df.columns else 0
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    ui.icon_metric("Total Customers", f"{unique_customers:,}", icon="👥")
                
                with col2:
                    ui.icon_metric("Total Revenue", f"৳{total_rev:,.0f}", icon="💰")
                
                with col3:
                    avg = total_orders / unique_customers if unique_customers > 0 else 0
                    ui.icon_metric("Avg Orders/Customer", f"{avg:.1f}", icon="🛒")
        else:
            st.info("Sync data to view global statistics")
    except Exception as e:
        logger.warning(f"Could not load global stats: {e}")


def render_enhanced_customer_insight_tab(
    reg_rev: float,
    guest_rev: float,
    total_accounts: int,
    df_sales: pd.DataFrame,
) -> None:
    """Customer Insight tab - SIMPLIFIED single section layout.
    
    Args:
        reg_rev: Not displayed (removed Account Registrations)
        guest_rev: Not displayed
        total_accounts: Not displayed
        df_sales: Sales DataFrame
    """
    # 1. Render Global Insights (Metrics & EMA Chart) BEFORE filters
    _render_global_insights(df_sales)

    # Single header, no redundancy
    st.markdown("### 🔍 Filter & Analyze Customers")
    st.caption("Find customers by products purchased, order count, and spending")
    
    # HORIZONTAL LAYOUT: Filters at top (full width), results below
    filters = render_customer_filters(
        key_prefix=KeyManager.get_key("ci", "tab"),
    )
    
    # Results section below filters
    _render_compact_results(filters)
    
    # Visual Segments below
    st.markdown("---")
    _render_legacy_insights(df_sales)


def _render_legacy_insights(df_sales: pd.DataFrame) -> None:
    """Render segment insights using existing working data."""
    if "dashboard_data" not in st.session_state and df_sales.empty:
        st.info("📊 Please sync data to view customer intelligence.")
        return
    
    # Get customers from session state or generate from sales data
    if "dashboard_data" in st.session_state and st.session_state.dashboard_data.get("customers") is not None:
        df = st.session_state.dashboard_data["customers"]
    elif not df_sales.empty:
        df = generate_customer_insights_from_sales(df_sales, include_rfm=True)
    else:
        st.warning("No customer data available.")
        return
    
    if df.empty:
        st.warning("No customer segments identified in this period.")
        return
    
    # Visual Segments and Retention
    t_seg, t_coh = st.tabs(["📊 Value Segments", "📅 Retention Cohorts"])
    
    with t_seg:
        st.markdown("### 📊 Value Segments")
        mix_df = df["segment"].value_counts().reset_index()
        mix_df.columns = ["Segment", "Count"]
        
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(
            ui.donut_chart(mix_df, values="Count", names="Segment", title="Segment Distribution"),
            width="stretch", key=KeyManager.get_key("ci", "segment_dist_donut")
            )
        with c2:
            rev_df = df.groupby("segment")["total_revenue"].sum().reset_index().sort_values("total_revenue", ascending=False)
            st.plotly_chart(
            ui.bar_chart(rev_df, x="total_revenue", y="segment", title="Revenue by Segment", color_scale="Tealgrn"),
            width="stretch", key=KeyManager.get_key("ci", "rev_by_segment_bar")
            )
            
    with t_coh:
        st.markdown("### 📅 Customer Retention Cohorts")
        st.caption("Percentage of customers returning in subsequent months after their first purchase.")
        cohort_df = generate_cohort_matrix(df_sales, period='M')
        if not cohort_df.empty:
            import plotly.express as px
            cohort_df.index = cohort_df.index.astype(str)  # Format period index for Plotly
            fig_coh = px.imshow(
                cohort_df, text_auto=".1f", aspect="auto", color_continuous_scale="Tealgrn",
                labels=dict(x="Months Since First Order", y="Cohort Month", color="Retention %"),
            )
            fig_coh.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_coh, use_container_width=True, key=KeyManager.get_key("ci", "retention_cohorts_matrix"))
        else:
            st.info("Insufficient longitudinal data to generate retention cohorts.")


def _render_compact_results(filters: Dict[str, Any]) -> None:
    """Render compact results for tab view using existing data.
    
    Args:
        filters: Filter settings
    """
    if not filters.get("applied"):
        st.info("👈 Adjust filters and click 'Apply Filters' to see results")
        return
    
    # Get data from existing working session state
    if "dashboard_data" not in st.session_state:
        st.warning("📊 Please sync data to view customer filters")
        return
    
    sales_df = st.session_state.dashboard_data.get("sales_active", pd.DataFrame())
    
    if sales_df.empty:
        st.warning("📭 No sales data available")
        return
    
    # Fetch and display results
    with st.spinner("Filtering customers..."):
        customers_df = _get_filtered_customers_from_sales(sales_df, filters)
    
    # Inject Account Type and Loyalty Scoring for Compact View too
    if not customers_df.empty:
        if "customer_key" in customers_df.columns:
            customers_df["account_type"] = np.where(
                customers_df["customer_key"].astype(str).str.startswith("reg_"), 
                "Registered", 
                "Guest"
            )
            
        if "total_orders" in customers_df.columns and ("total_revenue" in customers_df.columns or "total_value" in customers_df.columns):
            rev_col = "total_revenue" if "total_revenue" in customers_df.columns else "total_value"
            o_score = (customers_df["total_orders"] / 10.0).clip(upper=1.0) * 50
            r_score = (customers_df[rev_col] / 20000.0).clip(upper=1.0) * 50
            customers_df["loyalty_score"] = (o_score + r_score).round(0)
            
            conditions = [
                customers_df["loyalty_score"] >= 80,
                customers_df["loyalty_score"] >= 50,
                customers_df["loyalty_score"] >= 20
            ]
            choices = ["Platinum 🏆", "Gold 🌟", "Silver ⭐"]
            customers_df["loyalty_tier"] = np.select(conditions, choices, default="Bronze 🥉")

    if customers_df.empty:
        st.warning("No customers match these filters")
        return
    
    st.success(f"Found {len(customers_df)} matching customers")
    
    # Show stats
    col1, col2, col3 = st.columns(3)
    with col1:
        ui.icon_metric("Customers", str(len(customers_df)), icon="👥")
    with col2:
        total = customers_df["total_revenue"].sum() if "total_revenue" in customers_df.columns else customers_df.get("total_value", pd.Series([0])).sum()
        ui.icon_metric("Revenue", f"৳{total:,.0f}", icon="💰")
    with col3:
        orders = customers_df["total_orders"].sum() if "total_orders" in customers_df.columns else 0
        ui.icon_metric("Orders", f"{int(orders):,}", icon="🛒")
    
    # Export button for tab view
    export_col1, export_col2 = st.columns([1, 3])
    with export_col1:
        # Prepare export data
        export_df = customers_df.copy()
        export_columns = [
            "customer_id", "customer_key", "primary_name", "name", "all_emails", "all_phones",
            "account_type", "loyalty_tier", "loyalty_score",
            "total_orders", "total_revenue", "total_value", "avg_order_value",
            "first_order", "last_order", "segment"
        ]
        available_cols = [c for c in export_columns if c in export_df.columns]
        export_df = export_df[available_cols]
        
        excel_data = export_to_excel(export_df, "filtered_customers")
        st.download_button(
            label="📥 Export",
            data=excel_data,
            file_name=f"filtered_customers_{len(customers_df)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key=KeyManager.get_key("ci", "tab_export"),
        )
    with export_col2:
        st.caption(f"Export {len(customers_df)} customers")
    
    st.markdown("---")
    
    # Simplified selector
    id_col = "customer_id" if "customer_id" in customers_df.columns else "customer_key"
    selected = st.selectbox(
        "Select a customer to view details",
        options=customers_df[id_col].tolist() if id_col in customers_df.columns else [],
        format_func=lambda x: _format_customer_option(x, customers_df, id_col),
        key=KeyManager.get_key("ci", "tab_selector"),
    )
    
    if selected:
        # Show mini report
        with st.expander("📋 Customer Details", expanded=True):
            render_customer_report(
                customer_key=selected,
                customers_df=customers_df,
                key_prefix=KeyManager.get_key("ci", "tab"),
            )


def _format_customer_option(key: str, df: pd.DataFrame, id_col: str = "customer_id") -> str:
    """Format customer option for selectbox.
    
    Args:
        key: Customer key
        df: Customers DataFrame
        id_col: Column to match against
        
    Returns:
        Formatted option string
    """
    match = df[df[id_col] == key]
    if match.empty:
        return key
    
    row = match.iloc[0]
    name = row.get("primary_name", row.get("name", "Unknown"))
    orders = row.get("total_orders", 0)
    value = row.get("total_revenue", row.get("total_value", 0))
    
    return f"{name} ({orders} orders, ৳{value:,.0f})"
