"""Customer Insight Page - Filter Customers Main Feature.

Uses existing working API components for:
- Dynamic customer filtering
- Customer selection and browsing
- Detailed customer reports
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any

import streamlit as st
import pandas as pd

# Use EXISTING working API components
from src.components.customer_insight.customer_filters import (
    render_customer_filters,
    apply_customer_filters,
)
from src.components.customer_insight.customer_selector import (
    render_customer_selector,
)
from src.components.customer_insight.customer_report import render_customer_report
from FrontEnd.components.ui import export_to_excel

# Use EXISTING working services (inherits from working dashboard data)
from BackEnd.services.customer_insights import generate_customer_insights_from_sales

from FrontEnd.components import ui
from BackEnd.core.logging_config import get_logger


logger = get_logger("customer_insight_page")


def render_customer_insight_page() -> None:
    """Render the main Customer Insight page - SIMPLIFIED UI.
    
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
    
    # HORIZONTAL LAYOUT: Full width filters on top, results below
    filters = render_customer_filters(
        on_filter_change=_on_filter_change,
        key_prefix="ci_page",
    )
    
    # Results section below filters
    _render_main_content(filters)


def _on_filter_change(filters: Dict[str, Any]) -> None:
    """Handle filter changes.
    
    Args:
        filters: Updated filter dictionary
    """
    logger.info(f"Filters applied: {filters}")
    st.session_state["ci_filters_applied"] = True


def _render_main_content(filters: Dict[str, Any]) -> None:
    """Render the main content area using existing working API.
    
    Args:
        filters: Current filter settings
    """
    # Use existing working data from session state
    if "dashboard_data" not in st.session_state:
        st.info("📊 Please sync data to use customer filters.")
        return
    
    sales_df = st.session_state.dashboard_data.get("sales_active", pd.DataFrame())
    
    if sales_df.empty:
        st.info("📭 No sales data available. Please sync data first.")
        return
    
    # Apply filters to existing data
    with st.spinner("🔍 Filtering customers..."):
        # Generate customer insights from sales data
        customers_df = _get_filtered_customers_from_sales(sales_df, filters)
    
    # Show results summary
    if customers_df.empty:
        st.info("📭 No customers match the current filters. Try adjusting your criteria.")
        
        # Show helpful suggestions
        st.markdown("""
            **Suggestions:**
            - Check your date range isn't too restrictive
            - Try removing product filters
            - Lower the minimum order amount
            - Clear all filters and try again
        """)
        
        # Show raw stats for context
        _show_global_stats()
        return
    
    # Display stats
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Matching Customers", f"{len(customers_df):,}")
    
    with col2:
        total_revenue = customers_df["total_value"].sum() if "total_value" in customers_df.columns else 0
        st.metric("Total Revenue", f"৳{total_revenue:,.0f}")
    
    with col3:
        total_orders = customers_df["total_orders"].sum() if "total_orders" in customers_df.columns else 0
        st.metric("Total Orders", f"{int(total_orders):,}")
    
    with col4:
        avg_aov = customers_df["avg_order_value"].mean() if "avg_order_value" in customers_df.columns else 0
        st.metric("Avg AOV", f"৳{avg_aov:,.0f}")
    
    # Export button
    export_col1, export_col2 = st.columns([1, 3])
    with export_col1:
        # Prepare export data with key customer info
        export_df = customers_df.copy()
        # Select relevant columns for export
        export_columns = [
            "customer_key", "name", "primary_name", "all_emails", "all_phones",
            "total_orders", "total_value", "avg_order_value", 
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
    selected_customer = render_customer_selector(
        customers_df=customers_df,
        on_select=_on_customer_select,
        key_prefix="ci_page",
    )
    
    # Store in session state
    if selected_customer:
        st.session_state["ci_selected_customer"] = selected_customer
    
    # Show report if customer selected
    if selected_customer:
        st.markdown("---")
        render_customer_report(
            customer_key=selected_customer,
            customers_df=customers_df,
            key_prefix="ci_page",
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
    from src.components.customer_insight.customer_filters import apply_customer_filters
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
                    st.metric("Total Customers", f"{unique_customers:,}")
                
                with col2:
                    st.metric("Total Revenue", f"৳{total_rev:,.0f}")
                
                with col3:
                    avg = total_orders / unique_customers if unique_customers > 0 else 0
                    st.metric("Avg Orders/Customer", f"{avg:.1f}")
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
    # Single header, no redundancy
    st.markdown("### 🔍 Filter & Analyze Customers")
    st.caption("Find customers by products purchased, order count, and spending")
    
    # HORIZONTAL LAYOUT: Filters at top (full width), results below
    filters = render_customer_filters(
        key_prefix="ci_tab",
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
    
    # Visual Segments (keep this - it works)
    st.markdown("### 📊 Value Segments")
    mix_df = df["segment"].value_counts().reset_index()
    mix_df.columns = ["Segment", "Count"]
    
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(
            ui.donut_chart(mix_df, values="Count", names="Segment", title="Segment Distribution"),
            use_container_width=True
        )
    with c2:
        rev_df = df.groupby("segment")["total_revenue"].sum().reset_index().sort_values("total_revenue", ascending=False)
        st.plotly_chart(
            ui.bar_chart(rev_df, x="total_revenue", y="segment", title="Revenue by Segment", color_scale="Tealgrn"),
            use_container_width=True
        )


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
    
    if customers_df.empty:
        st.warning("No customers match these filters")
        return
    
    st.success(f"Found {len(customers_df)} matching customers")
    
    # Show stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Customers", len(customers_df))
    with col2:
        total = customers_df["total_value"].sum() if "total_value" in customers_df.columns else 0
        st.metric("Revenue", f"৳{total:,.0f}")
    with col3:
        orders = customers_df["total_orders"].sum() if "total_orders" in customers_df.columns else 0
        st.metric("Orders", f"{int(orders):,}")
    
    # Export button for tab view
    export_col1, export_col2 = st.columns([1, 3])
    with export_col1:
        # Prepare export data
        export_df = customers_df.copy()
        export_columns = [
            "customer_key", "name", "primary_name", "all_emails", "all_phones",
            "total_orders", "total_value", "avg_order_value",
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
            key="ci_tab_export",
        )
    with export_col2:
        st.caption(f"Export {len(customers_df)} customers")
    
    st.markdown("---")
    
    # Simplified selector
    selected = st.selectbox(
        "Select a customer to view details",
        options=customers_df["customer_key"].tolist(),
        format_func=lambda x: _format_customer_option(x, customers_df),
        key="ci_tab_selector",
    )
    
    if selected:
        # Show mini report
        with st.expander("📋 Customer Details", expanded=True):
            render_customer_report(
                customer_key=selected,
                customers_df=customers_df,
                key_prefix="ci_tab",
            )


def _format_customer_option(key: str, df: pd.DataFrame) -> str:
    """Format customer option for selectbox.
    
    Args:
        key: Customer key
        df: Customers DataFrame
        
    Returns:
        Formatted option string
    """
    match = df[df["customer_key"] == key]
    if match.empty:
        return key
    
    row = match.iloc[0]
    name = row.get("name", "Unknown")
    orders = row.get("total_orders", 0)
    value = row.get("total_value", 0)
    
    return f"{name} ({orders} orders, ৳{value:,.0f})"
