"""Order history table component for customer report.

Provides a detailed table view of customer orders with:
- Sortable columns
- Status indicators
- Product details expansion
- Export functionality
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any
from datetime import datetime

import streamlit as st
import pandas as pd

from BackEnd.utils.woocommerce_helpers import format_currency, format_wc_date
from BackEnd.core.logging_config import get_logger


logger = get_logger("order_history_table")


def render_order_history(
    orders_df: pd.DataFrame,
    customer_key: Optional[str] = None,
    key_prefix: str = "ci_history",
) -> None:
    """Render order history table for customer.
    
    Args:
        orders_df: DataFrame with order data
        customer_key: Optional customer key for context
        key_prefix: Prefix for Streamlit keys
    """
    if orders_df.empty:
        st.info("📭 No orders found for this customer in the selected date range.")
        return
    
    st.markdown("### 📜 Order History")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Orders", f"{len(orders_df):,}")
    
    with col2:
        total_items = orders_df["items_count"].sum() if "items_count" in orders_df.columns else 0
        st.metric("Total Items", f"{int(total_items):,}")
    
    with col3:
        total_value = orders_df["total"].sum() if "total" in orders_df.columns else 0
        st.metric("Total Value", format_currency(total_value))
    
    with col4:
        avg_value = orders_df["total"].mean() if "total" in orders_df.columns else 0
        st.metric("Average Order", format_currency(avg_value))
    
    st.markdown("---")
    
    # Prepare display data
    display_df = _prepare_orders_display(orders_df)
    
    # Filters
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Status filter
        if "Status" in display_df.columns:
            statuses = ["All"] + sorted(display_df["Status"].dropna().unique().tolist())
            selected_status = st.selectbox(
                "Filter by status",
                options=statuses,
                key=f"{key_prefix}_status_filter",
            )
            
            if selected_status != "All":
                display_df = display_df[display_df["Status"] == selected_status]
    
    with col2:
        # Export button
        if not display_df.empty:
            csv_data = display_df.to_csv(index=False)
            st.download_button(
                "📥 Export CSV",
                data=csv_data,
                file_name=f"order_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key=f"{key_prefix}_export",
                use_container_width=True,
            )
    
    # Display table
    if display_df.empty:
        st.info("No orders match the current filter.")
        return
    
    # Add row numbers
    display_df.insert(0, "#", range(1, len(display_df) + 1))
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "#": st.column_config.NumberColumn("#", width="small"),
            "Order ID": st.column_config.NumberColumn("Order ID", width="small"),
            "Order #": st.column_config.TextColumn("Order #", width="small"),
            "Date": st.column_config.DatetimeColumn("Date", width="medium", format="YYYY-MM-DD HH:mm"),
            "Status": st.column_config.TextColumn("Status", width="small"),
            "Total": st.column_config.TextColumn("Total", width="small"),
            "Items": st.column_config.NumberColumn("Items", width="small"),
            "Payment": st.column_config.TextColumn("Payment", width="medium"),
        },
    )
    
    # Order details expansion
    st.markdown("---")
    st.markdown("#### 📦 Order Details")
    st.caption("Click to expand and view products in each order")
    
    # Limit to most recent 10 for performance
    recent_orders = orders_df.head(10)
    
    for idx, order in recent_orders.iterrows():
        order_id = order.get("order_id", "Unknown")
        order_number = order.get("order_number", order_id)
        order_date = format_wc_date(order.get("date_created"), "%Y-%m-%d %H:%M")
        order_total = format_currency(order.get("total", 0))
        order_status = order.get("status", "unknown")
        
        # Status emoji
        status_emoji = _get_status_emoji(order_status)
        
        with st.expander(
            f"{status_emoji} Order #{order_number} - {order_date} - {order_total}",
        ):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**Order ID:** {order_id}")
                st.markdown(f"**Status:** {order_status.title()}")
                st.markdown(f"**Payment:** {order.get('payment_method', 'N/A')}")
            
            with col2:
                st.markdown(f"**Currency:** {order.get('currency', 'USD')}")
                shipping = order.get("shipping_total", 0)
                if shipping:
                    st.markdown(f"**Shipping:** {format_currency(shipping)}")
                discount = order.get("discount_total", 0)
                if discount:
                    st.markdown(f"**Discount:** -{format_currency(discount)}")
            
            # Line items
            line_items = order.get("line_items", [])
            if line_items:
                st.markdown("**Products:**")
                items_data = []
                for item in line_items:
                    items_data.append({
                        "Product": item.get("name", "Unknown"),
                        "SKU": item.get("sku", "N/A"),
                        "Qty": item.get("quantity", 0),
                        "Price": format_currency(item.get("price", 0)),
                        "Total": format_currency(item.get("total", 0)),
                    })
                
                st.dataframe(
                    pd.DataFrame(items_data),
                    use_container_width=True,
                    hide_index=True,
                )
            
            # Coupon codes
            coupons = order.get("coupon_codes", [])
            if coupons:
                st.markdown(f"**Coupons used:** {', '.join(coupons)}")


def _prepare_orders_display(orders_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare orders DataFrame for display.
    
    Args:
        orders_df: Raw orders data
        
    Returns:
        Formatted DataFrame
    """
    df = orders_df.copy()
    
    # Select and rename columns
    column_map = {
        "order_id": "Order ID",
        "order_number": "Order #",
        "date_created": "Date",
        "status": "Status",
        "total": "Total",
        "items_count": "Items",
        "payment_method": "Payment",
        "currency": "Currency",
    }
    
    # Only include columns that exist
    display_cols = {}
    for old, new in column_map.items():
        if old in df.columns:
            display_cols[old] = new
    
    result_df = df[list(display_cols.keys())].copy()
    result_df = result_df.rename(columns=display_cols)
    
    # Format date
    if "Date" in result_df.columns:
        result_df["Date"] = pd.to_datetime(result_df["Date"], errors="coerce")
    
    # Format currency
    if "Total" in result_df.columns:
        result_df["Total"] = result_df["Total"].apply(format_currency)
    
    # Format status
    if "Status" in result_df.columns:
        result_df["Status"] = result_df["Status"].str.title()
    
    # Format items count
    if "Items" in result_df.columns:
        result_df["Items"] = result_df["Items"].fillna(0).astype(int)
    
    return result_df


def _get_status_emoji(status: str) -> str:
    """Get emoji for order status.
    
    Args:
        status: Order status string
        
    Returns:
        Status emoji
    """
    status_emojis = {
        "completed": "✅",
        "processing": "🔄",
        "on-hold": "⏸️",
        "pending": "⏳",
        "cancelled": "❌",
        "refunded": "↩️",
        "failed": "⚠️",
        "draft": "📝",
        "trash": "🗑️",
    }
    
    return status_emojis.get(status.lower(), "📦")


def render_spending_trend_chart(orders_df: pd.DataFrame, key_prefix: str = "ci_trend") -> None:
    """Render spending trend line chart.
    
    Args:
        orders_df: DataFrame with order data
        key_prefix: Prefix for Streamlit keys
    """
    if orders_df.empty or "date_created" not in orders_df.columns:
        return
    
    import plotly.express as px
    
    df = orders_df.copy()
    df["date_created"] = pd.to_datetime(df["date_created"], errors="coerce")
    
    # Group by date
    daily_spend = df.groupby(df["date_created"].dt.date).agg({
        "total": "sum",
        "order_id": "count",
    }).reset_index()
    
    daily_spend.columns = ["Date", "Revenue", "Orders"]
    
    # Create line chart
    fig = px.line(
        daily_spend,
        x="Date",
        y="Revenue",
        title="Spending Trend Over Time",
        markers=True,
    )
    
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Order Total (৳)",
        showlegend=False,
        height=400,
    )
    
    st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_chart")
