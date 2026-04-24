"""Customer data fetching service with caching.

Provides functions to fetch customer data from WooCommerce REST API
with Streamlit caching for performance optimization.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st
from typing import Optional, Callable
from datetime import datetime, date

from .api_client import WooCommerceAPI, get_woocommerce_api
from .base_api_client import APIError
from BackEnd.core.logging_config import get_logger, timed


logger = get_logger("fetch_customers")


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_customers(
    after: Optional[str] = None,
    before: Optional[str] = None,
    search: Optional[str] = None,
    per_page: int = 100,
    _progress_callback: Optional[Callable] = None,
) -> pd.DataFrame:
    """Fetch all customers from WooCommerce with caching.
    
    This function fetches customers using pagination and returns
    a normalized DataFrame with customer information.
    
    Args:
        after: ISO8601 date to filter customers registered after
        before: ISO8601 date to filter customers registered before  
        search: Search term for customer name or email
        per_page: Items per page for pagination
        _progress_callback: Internal callback for progress updates (not cached)
        
    Returns:
        DataFrame with customer columns:
        - customer_id: WooCommerce customer ID
        - email: Customer email
        - first_name: First name
        - last_name: Last name
        - name: Full name
        - phone: Phone number
        - date_created: Registration date
        - billing_address: Formatted billing address
        - shipping_address: Formatted shipping address
        
    Raises:
        st.error: If API credentials are missing or request fails
    """
    api = get_woocommerce_api()
    
    if not api:
        st.error(
            "🔐 **WooCommerce API Not Connected**\n\n"
            "Please configure your WooCommerce credentials in `.streamlit/secrets.toml`:\n\n"
            "```toml\n"
            "[woocommerce]\n"
            'store_url = "https://yourstore.com"\n'
            'consumer_key = "ck_..."\n'
            'consumer_secret = "cs_..."\n'
            "```"
        )
        return pd.DataFrame()
    
    try:
        with st.spinner("📡 Fetching customers from WooCommerce..."):
            customers = api.get_all_customers(
                per_page=per_page,
                after=after,
                before=before,
                search=search,
                progress_callback=_progress_callback,
            )
        
        if not customers:
            return pd.DataFrame()
        
        # Normalize customer data
        normalized = []
        for customer in customers:
            billing = customer.get("billing", {})
            shipping = customer.get("shipping", {})
            
            normalized.append({
                "customer_id": customer.get("id"),
                "email": customer.get("email", ""),
                "first_name": customer.get("first_name", ""),
                "last_name": customer.get("last_name", ""),
                "name": f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip(),
                "phone": billing.get("phone", ""),
                "date_created": customer.get("date_created"),
                "billing_address": _format_address(billing),
                "shipping_address": _format_address(shipping),
                "billing_city": billing.get("city", ""),
                "billing_state": billing.get("state", ""),
                "billing_postcode": billing.get("postcode", ""),
                "billing_country": billing.get("country", ""),
            })
        
        df = pd.DataFrame(normalized)
        
        # Parse dates
        if "date_created" in df.columns:
            df["date_created"] = pd.to_datetime(df["date_created"], errors="coerce")
        
        logger.info(f"Fetched {len(df)} customers from WooCommerce")
        return df
        
    except APIError as e:
        logger.error(f"API error fetching customers: {e}")
        st.error(
            f"❌ **Failed to fetch customers**\n\n"
            f"{str(e)}\n\n"
            f"**Quick checks:**\n"
            f"1. Verify WordPress permalinks are enabled (not 'Plain')\n"
            f"2. Check API key has 'read' permission for customers\n"
            f"3. Ensure WooCommerce plugin is active"
        )
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Failed to fetch customers: {e}")
        st.error(
            f"❌ **Failed to fetch customers**\n\n"
            f"Error: {str(e)}\n\n"
            f"**Possible fixes:**\n"
            f"- Verify API credentials are correct\n"
            f"- Check store URL accessibility\n"
            f"- Ensure 'customers' read permission is enabled"
        )
        return pd.DataFrame()


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_customer_by_id(customer_id: int) -> Optional[dict]:
    """Fetch a single customer by ID with caching.
    
    Args:
        customer_id: WooCommerce customer ID
        
    Returns:
        Customer dictionary or None if not found
    """
    if not customer_id:
        return None
    
    api = get_woocommerce_api()
    
    if not api:
        return None
    
    try:
        customer = api.get_customer(customer_id)
        logger.info(f"Fetched customer {customer_id}")
        return customer
    except Exception as e:
        logger.warning(f"Failed to fetch customer {customer_id}: {e}")
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_customer_orders_df(
    customer_id: int,
    after: Optional[str] = None,
    before: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch all orders for a specific customer.
    
    Args:
        customer_id: WooCommerce customer ID
        after: ISO8601 date to filter orders created after
        before: ISO8601 date to filter orders created before
        
    Returns:
        DataFrame with order data
    """
    api = get_woocommerce_api()
    
    if not api:
        return pd.DataFrame()
    
    try:
        with st.spinner(f"📦 Fetching orders for customer {customer_id}..."):
            orders = api.get_all_orders(
                customer=customer_id,
                after=after,
                before=before,
                per_page=100,
            )
        
        if not orders:
            return pd.DataFrame()
        
        # Normalize order data
        normalized = []
        for order in orders:
            # Count items
            items_count = sum(
                item.get("quantity", 0) 
                for item in order.get("line_items", [])
            )
            
            # Get product IDs for filtering
            product_ids = [
                item.get("product_id") 
                for item in order.get("line_items", [])
            ]
            
            normalized.append({
                "order_id": order.get("id"),
                "order_number": order.get("number"),
                "status": order.get("status"),
                "date_created": order.get("date_created"),
                "date_modified": order.get("date_modified"),
                "date_completed": order.get("date_completed"),
                "total": float(order.get("total", 0)),
                "currency": order.get("currency", "USD"),
                "payment_method": order.get("payment_method_title", ""),
                "items_count": items_count,
                "product_ids": product_ids,
                "billing_email": order.get("billing", {}).get("email", ""),
                "billing_phone": order.get("billing", {}).get("phone", ""),
            })
        
        df = pd.DataFrame(normalized)
        
        # Parse dates
        date_cols = ["date_created", "date_modified", "date_completed"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        
        # Sort by date
        if "date_created" in df.columns:
            df = df.sort_values("date_created", ascending=False)
        
        logger.info(f"Fetched {len(df)} orders for customer {customer_id}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to fetch customer orders: {e}")
        return pd.DataFrame()


def _format_address(address_dict: dict) -> str:
    """Format address dictionary into string.
    
    Args:
        address_dict: Address components dictionary
        
    Returns:
        Formatted address string
    """
    if not address_dict:
        return ""
    
    parts = [
        address_dict.get("address_1", ""),
        address_dict.get("address_2", ""),
        address_dict.get("city", ""),
        address_dict.get("state", ""),
        address_dict.get("postcode", ""),
        address_dict.get("country", ""),
    ]
    
    # Filter empty parts
    parts = [p for p in parts if p]
    
    return ", ".join(parts) if parts else ""


def clear_customer_cache() -> None:
    """Clear the customer data cache.
    
    Call this when you need to force a fresh fetch.
    """
    fetch_customers.clear()
    fetch_customer_by_id.clear()
    fetch_customer_orders_df.clear()
    logger.info("Cleared customer data cache")
    st.success("🗑️ Customer cache cleared. Data will refresh on next load.")
