"""Order data fetching service with caching.

Provides functions to fetch order data from WooCommerce REST API
with Streamlit caching for performance optimization.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st
from typing import Optional, Callable, List
from datetime import datetime

from .api_client import WooCommerceAPI, get_woocommerce_api
from .base_api_client import APIError
from BackEnd.core.logging_config import get_logger


logger = get_logger("fetch_orders")


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_orders(
    after: Optional[str] = None,
    before: Optional[str] = None,
    status: Optional[str] = None,
    customer: Optional[int] = None,
    product: Optional[int] = None,
    per_page: int = 100,
    _progress_callback: Optional[Callable] = None,
) -> pd.DataFrame:
    """Fetch orders from WooCommerce with caching.
    
    This function fetches orders using pagination and returns
    a normalized DataFrame with order information.
    
    Args:
        after: ISO8601 date to filter orders created after
        before: ISO8601 date to filter orders created before
        status: Filter by order status (e.g., 'completed', 'processing')
        customer: Filter by customer ID
        product: Filter by product ID
        per_page: Items per page for pagination
        _progress_callback: Internal callback for progress updates (not cached)
        
    Returns:
        DataFrame with order columns:
        - order_id: WooCommerce order ID
        - order_number: Order number
        - status: Order status
        - date_created: Order creation date
        - total: Order total amount
        - currency: Currency code
        - customer_id: Customer ID (0 for guests)
        - customer_ip: Customer IP address
        - payment_method: Payment method title
        - items_count: Total quantity of items
        - billing_email: Customer email
        - billing_phone: Customer phone
        - billing_name: Customer full name
        - line_items: List of line item dictionaries
        
    Raises:
        st.error: If API credentials are missing or request fails
    """
    api = get_woocommerce_api()
    
    if not api:
        st.error(
            "🔐 **WooCommerce API Not Connected**\n\n"
            "Please configure your WooCommerce credentials in `.streamlit/secrets.toml`"
        )
        return pd.DataFrame()
    
    try:
        with st.spinner("📡 Fetching orders from WooCommerce..."):
            orders = api.get_all_orders(
                per_page=per_page,
                after=after,
                before=before,
                status=status,
                customer=customer,
                product=product,
                progress_callback=_progress_callback,
            )
        
        if not orders:
            return pd.DataFrame()
        
        df = _normalize_orders(orders)
        logger.info(f"Fetched {len(df)} orders from WooCommerce")
        return df
        
    except APIError as e:
        logger.error(f"API error fetching orders: {e}")
        st.error(
            f"❌ **Failed to fetch orders**\n\n"
            f"{str(e)}\n\n"
            f"**Quick checks:**\n"
            f"1. Verify WordPress permalinks are enabled (not 'Plain')\n"
            f"2. Check API key has 'read' permission for orders\n"
            f"3. Ensure WooCommerce plugin is active"
        )
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Failed to fetch orders: {e}")
        st.error(
            f"❌ **Failed to fetch orders**\n\n"
            f"Error: {str(e)}\n\n"
            f"**Possible fixes:**\n"
            f"- Verify API credentials are correct\n"
            f"- Check store URL accessibility\n"
            f"- Ensure 'orders' read permission is enabled"
        )
        return pd.DataFrame()


def fetch_customer_orders(
    customer_id: int,
    after: Optional[str] = None,
    before: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch all orders for a specific customer.
    
    This is a convenience wrapper around fetch_orders with
    customer filter pre-set.
    
    Args:
        customer_id: WooCommerce customer ID
        after: ISO8601 date to filter orders created after
        before: ISO8601 date to filter orders created before
        
    Returns:
        DataFrame with customer orders
    """
    return fetch_orders(customer=customer_id, after=after, before=before)


def _normalize_orders(orders: List[dict]) -> pd.DataFrame:
    """Normalize raw WooCommerce order JSON to DataFrame.
    
    Args:
        orders: List of order dictionaries from API
        
    Returns:
        Normalized DataFrame
    """
    normalized = []
    
    for order in orders:
        billing = order.get("billing", {})
        
        # Count total items
        line_items = order.get("line_items", [])
        items_count = sum(item.get("quantity", 0) for item in line_items)
        
        # Extract product information
        products = []
        product_ids = []
        for item in line_items:
            products.append({
                "product_id": item.get("product_id"),
                "name": item.get("name"),
                "sku": item.get("sku"),
                "quantity": item.get("quantity"),
                "price": float(item.get("price", 0)),
                "total": float(item.get("total", 0)),
            })
            product_ids.append(item.get("product_id"))
        
        normalized.append({
            "order_id": order.get("id"),
            "order_number": order.get("number"),
            "status": order.get("status"),
            "date_created": order.get("date_created"),
            "date_modified": order.get("date_modified"),
            "date_completed": order.get("date_completed"),
            "total": float(order.get("total", 0)),
            "currency": order.get("currency", "USD"),
            "customer_id": int(order.get("customer_id", 0)) if order.get("customer_id") else 0,
            "customer_ip": order.get("customer_ip_address", ""),
            "payment_method": order.get("payment_method_title", ""),
            "items_count": items_count,
            "billing_email": billing.get("email", ""),
            "billing_phone": billing.get("phone", ""),
            "billing_name": f"{billing.get('first_name', '')} {billing.get('last_name', '')}".strip(),
            "billing_city": billing.get("city", ""),
            "billing_state": billing.get("state", ""),
            "billing_country": billing.get("country", ""),
            "line_items": products,
            "product_ids": product_ids,
            "coupon_codes": [c.get("code", "") for c in order.get("coupon_lines", [])],
            "shipping_total": float(order.get("shipping_total", 0)),
            "discount_total": float(order.get("discount_total", 0)),
        })
    
    df = pd.DataFrame(normalized)
    
    # Parse dates
    date_cols = ["date_created", "date_modified", "date_completed"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    
    # Sort by creation date (newest first)
    if "date_created" in df.columns:
        df = df.sort_values("date_created", ascending=False)
    
    return df


def get_orders_for_products(
    product_ids: List[int],
    after: Optional[str] = None,
    before: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch orders that contain specific products.
    
    Note: WooCommerce API doesn't support filtering orders by multiple
    products directly, so this fetches orders in date range and filters locally.
    
    Args:
        product_ids: List of product IDs to filter by
        after: ISO8601 date to filter orders created after
        before: ISO8601 date to filter orders created before
        
    Returns:
        DataFrame with orders containing the specified products
    """
    df = fetch_orders(after=after, before=before)
    
    if df.empty or not product_ids:
        return df
    
    # Filter orders that contain any of the specified products
    mask = df["product_ids"].apply(
        lambda ids: any(pid in product_ids for pid in ids) if isinstance(ids, list) else False
    )
    
    return df[mask].copy()


def clear_orders_cache() -> None:
    """Clear the orders data cache."""
    fetch_orders.clear()
    logger.info("Cleared orders cache")
    st.success("🗑️ Orders cache cleared. Data will refresh on next load.")
