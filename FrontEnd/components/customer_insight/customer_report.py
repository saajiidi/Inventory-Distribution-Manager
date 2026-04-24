"""Customer report component for detailed customer view.

Provides comprehensive customer report with:
- Customer profile (name, email, phone, addresses)
- Order metrics summary
- Order history table
- Optional spending trend chart
"""

from __future__ import annotations

from typing import Optional, Dict, Any, List
from datetime import datetime, date

import streamlit as st
import pandas as pd
import plotly.express as px

from BackEnd.utils.woocommerce_helpers import (
    format_currency,
    format_wc_date,
    calculate_customer_metrics,
    clean_phone,
    clean_email,
)
from BackEnd.services.woocommerce_client.fetch_orders import fetch_customer_orders
from BackEnd.services.woocommerce_client.fetch_customers import fetch_customer_by_id
from BackEnd.core.logging_config import get_logger


logger = get_logger("customer_report")


def render_customer_report(
    customer_key: str,
    customers_df: pd.DataFrame,
    orders_df: Optional[pd.DataFrame] = None,
    key_prefix: str = "ci_report",
) -> None:
    """Render comprehensive customer report.
    
    Args:
        customer_key: Unique customer identifier
        customers_df: DataFrame with aggregated customer data
        orders_df: Optional pre-fetched orders DataFrame
        key_prefix: Prefix for Streamlit keys
    """
    if not customer_key:
        st.info("👈 Select a customer from the list above to view their detailed report.")
        return
    
    # Get customer details
    customer_data = _get_customer_data(customer_key, customers_df)
    
    if not customer_data:
        st.error("⚠️ Customer data not found. Please refresh and try again.")
        return
    
    st.markdown(f"---")
    st.markdown(f"## 📋 Customer Report: **{customer_data.get('name', 'Unknown')}**")
    
    # Fetch orders for this customer if not provided
    if orders_df is None:
        customer_id = customer_data.get("customer_id")
        if customer_id and customer_id > 0:
            orders_df = fetch_customer_orders(customer_id)
        else:
            # For guest customers, filter from main orders by email/phone
            orders_df = _filter_orders_for_guest(customer_data)
    
    # Calculate metrics
    metrics = calculate_customer_metrics(orders_df) if orders_df is not None else {}
    
    # Render sections
    _render_customer_profile(customer_data, metrics)
    _render_order_metrics(metrics, customer_data)
    _render_order_history(orders_df, key_prefix)
    _render_spending_trend(orders_df, key_prefix)


def _get_customer_data(
    customer_key: str,
    customers_df: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    """Extract customer data from DataFrame.
    
    Args:
        customer_key: Customer unique key
        customers_df: Customer DataFrame
        
    Returns:
        Customer data dictionary
    """
    match = customers_df[customers_df["customer_key"] == customer_key]
    
    if match.empty:
        return None
    
    row = match.iloc[0]
    
    # Extract customer ID from key if registered
    customer_id = 0
    if customer_key.startswith("reg_"):
        try:
            customer_id = int(customer_key.replace("reg_", ""))
        except ValueError:
            pass
    
    return {
        "customer_key": customer_key,
        "customer_id": customer_id,
        "name": row.get("name") or row.get("primary_name", "Unknown"),
        "email": row.get("unique_emails") or row.get("email", ""),
        "phone": row.get("unique_phones") or row.get("phone", ""),
        "total_orders": row.get("total_orders", 0),
        "total_value": row.get("total_value") or row.get("total_revenue", 0),
        "first_order": row.get("first_order_date") or row.get("first_order"),
        "last_order": row.get("last_order_date") or row.get("last_order"),
        "return_count": row.get("return_count", 0),
        "return_rate": row.get("return_rate", 0.0),
    }


def _filter_orders_for_guest(customer_data: Dict[str, Any]) -> pd.DataFrame:
    """Filter orders for guest customer by email/phone.
    
    Args:
        customer_data: Customer identification data
        
    Returns:
        Filtered orders DataFrame
    """
    from BackEnd.services.woocommerce_client.fetch_orders import fetch_orders
    
    # Fetch recent orders
    orders_df = fetch_orders()
    
    if orders_df is None or orders_df.empty:
        return pd.DataFrame()
    
    email = clean_email(customer_data.get("email", ""))
    phone = clean_phone(customer_data.get("phone", ""))
    
    # Filter by email or phone
    mask = pd.Series(False, index=orders_df.index)
    
    if email and "billing_email" in orders_df.columns:
        mask |= orders_df["billing_email"].str.lower() == email
    
    if phone and "billing_phone" in orders_df.columns:
        mask |= orders_df["billing_phone"].apply(clean_phone) == phone
    
    return orders_df[mask].copy()


def _render_customer_profile(
    customer_data: Dict[str, Any],
    metrics: Dict[str, Any],
) -> None:
    """Render customer profile section.
    
    Args:
        customer_data: Customer identification data
        metrics: Calculated customer metrics
    """
    st.markdown("### 👤 Customer Profile")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Contact Information**")
        
        name = customer_data.get("name", "Unknown")
        email = customer_data.get("email", "Not provided")
        phone = customer_data.get("phone", "Not provided")
        
        st.markdown(f"**Name:** {name}")
        st.markdown(f"📧 **Email:** {email}")
        st.markdown(f"📞 **Phone:** {phone}")
        
        # Show customer type
        customer_key = customer_data.get("customer_key", "")
        if customer_key.startswith("reg_"):
            st.success("✅ Registered Customer")
            customer_id = customer_data.get("customer_id")
            if customer_id:
                st.caption(f"Customer ID: {customer_id}")
        else:
            st.info("👤 Guest Customer")
            
        # Add Reliability Score if available
        rel_score = customer_data.get("rel_score")
        if rel_score is not None:
            stars = "⭐" * int(rel_score)
            st.markdown(f"**Reliability Score:** {stars} ({rel_score}/5)")
    
    with col2:
        st.markdown("**Account Timeline**")
        
        first_order = customer_data.get("first_order")
        last_order = customer_data.get("last_order")
        
        if first_order:
            first_str = format_wc_date(first_order, "%B %d, %Y")
            st.markdown(f"🗓️ **First Order:** {first_str}")
        
        if last_order:
            last_str = format_wc_date(last_order, "%B %d, %Y")
            st.markdown(f"🗓️ **Last Order:** {last_str}")
        
        # Days since last order
        if last_order and not pd.isna(last_order):
            try:
                last_dt = pd.to_datetime(last_order)
                days_since = (datetime.now() - last_dt).days
                if days_since == 0:
                    st.markdown("🟢 **Active:** Ordered today")
                elif days_since == 1:
                    st.markdown("🟢 **Active:** Ordered yesterday")
                elif days_since <= 7:
                    st.markdown(f"🟡 **Recent:** Last ordered {days_since} days ago")
                elif days_since <= 30:
                    st.markdown(f"🟠 **Warning:** Last ordered {days_since} days ago")
                else:
                    st.markdown(f"🔴 **At Risk:** Last ordered {days_since} days ago")
            except Exception:
                pass
    
    # Fetch full profile from WooCommerce if registered
    customer_id = customer_data.get("customer_id", 0)
    if customer_id > 0:
        with st.expander("📍 Full Profile (from WooCommerce)", expanded=False):
            full_profile = fetch_customer_by_id(customer_id)
            
            if full_profile:
                _render_full_profile(full_profile)
            else:
                st.info("Full profile not available. Customer may be a guest or profile was deleted.")
    
    st.markdown("---")


def _render_full_profile(profile: Dict[str, Any]) -> None:
    """Render full customer profile from WooCommerce.
    
    Args:
        profile: WooCommerce customer data
    """
    billing = profile.get("billing", {})
    shipping = profile.get("shipping", {})
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Billing Address**")
        
        address_parts = [
            billing.get("address_1", ""),
            billing.get("address_2", ""),
            billing.get("city", ""),
            billing.get("state", ""),
            billing.get("postcode", ""),
            billing.get("country", ""),
        ]
        address = ", ".join([p for p in address_parts if p])
        
        if address:
            st.text(address)
        else:
            st.caption("No billing address on file")
        
        st.markdown(f"**Email:** {billing.get('email', 'N/A')}")
        st.markdown(f"**Phone:** {billing.get('phone', 'N/A')}")
    
    with col2:
        st.markdown("**Shipping Address**")
        
        address_parts = [
            shipping.get("address_1", ""),
            shipping.get("address_2", ""),
            shipping.get("city", ""),
            shipping.get("state", ""),
            shipping.get("postcode", ""),
            shipping.get("country", ""),
        ]
        address = ", ".join([p for p in address_parts if p])
        
        if address:
            st.text(address)
        else:
            st.caption("No shipping address on file")
    
    # Registration info
    date_created = profile.get("date_created")
    if date_created:
        reg_date = format_wc_date(date_created, "%B %d, %Y")
        st.caption(f"Registered: {reg_date}")


def _render_order_metrics(metrics: Dict[str, Any]) -> None:
    """Render order metrics summary.
    
    Args:
        metrics: Customer metrics dictionary
    """
    st.markdown("### 📊 Order Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Orders",
            f"{metrics.get('total_orders', 0):,}",
        )
    
    with col2:
        st.metric(
            "Total Items",
            f"{metrics.get('total_items', 0):,}",
        )
    
    with col3:
        total_value = metrics.get("total_value", 0)
        st.metric(
            "Total Value",
            format_currency(total_value),
        )
    
    with col4:
        aov = metrics.get("avg_order_value", 0)
        st.metric(
            "Average Order",
            format_currency(aov),
        )
    
    # Additional metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        first_order = metrics.get("first_order_date")
        if first_order and not pd.isna(first_order):
            first_str = format_wc_date(first_order, "%Y-%m-%d")
            st.metric("First Order Date", first_str)
    
    with col2:
        last_order = metrics.get("last_order_date")
        if last_order and not pd.isna(last_order):
            last_str = format_wc_date(last_order, "%Y-%m-%d")
            st.metric("Last Order Date", last_str)
            
    with col3:
        return_count = customer_data.get("return_count", 0)
        return_rate = customer_data.get("return_rate", 0.0)
        st.metric(
            "Return Rate", 
            f"{return_rate:.1%}", 
            delta=f"{return_count} orders",
            delta_color="inverse" if return_rate > 0.15 else "normal"
        )

    with col4:
        lifespan = metrics.get("customer_lifespan_days", 0)
        if lifespan > 0:
            st.metric("Customer Lifespan", f"{lifespan} days")
    
    st.markdown("---")


def _render_order_history(
    orders_df: Optional[pd.DataFrame],
    key_prefix: str,
) -> None:
    """Render order history table.
    
    Args:
        orders_df: Orders DataFrame
        key_prefix: Prefix for Streamlit keys
    """
    from .order_history_table import render_order_history
    
    if orders_df is not None and not orders_df.empty:
        render_order_history(orders_df, key_prefix=key_prefix)
    else:
        st.info("No order history available for this customer.")


def _render_spending_trend(
    orders_df: Optional[pd.DataFrame],
    key_prefix: str,
) -> None:
    """Render spending trend chart.
    
    Args:
        orders_df: Orders DataFrame
        key_prefix: Prefix for Streamlit keys
    """
    from .order_history_table import render_spending_trend_chart
    
    if orders_df is not None and not orders_df.empty and len(orders_df) > 1:
        st.markdown("---")
        st.markdown("### 📈 Spending Trend")
        render_spending_trend_chart(orders_df, key_prefix=key_prefix)
