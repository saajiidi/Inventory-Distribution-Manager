"""Utility helpers for WooCommerce data processing.

This module provides helper functions for:
- Date parsing and formatting
- Customer metrics calculation
- Data transformation and aggregation
"""

from __future__ import annotations

import re
import hashlib
from datetime import datetime, date, timedelta
from typing import Optional, Union, List
from urllib.parse import urlparse

import pandas as pd
import numpy as np
from dateutil import parser as date_parser

from BackEnd.core.logging_config import get_logger


logger = get_logger("woocommerce_helpers")


def parse_wc_date(date_string: Optional[str]) -> Optional[datetime]:
    """Parse WooCommerce ISO8601 date string to datetime.
    
    WooCommerce returns dates in ISO8601 format:
    - "2024-01-15T10:30:00"
    - "2024-01-15T10:30:00+00:00"
    - "2024-01-15"
    
    Args:
        date_string: ISO8601 date string from WooCommerce
        
    Returns:
        Parsed datetime or None if invalid
    """
    if not date_string or pd.isna(date_string):
        return None
    
    try:
        # Handle Z suffix (UTC)
        if isinstance(date_string, str):
            date_string = date_string.replace("Z", "+00:00")
        
        # Try dateutil parser first (handles various formats)
        dt = date_parser.parse(date_string)
        return dt.replace(tzinfo=None)  # Remove timezone for consistency
    except (ValueError, TypeError) as e:
        logger.debug(f"Failed to parse date '{date_string}': {e}")
        return None


def format_wc_date(date_value: Optional[Union[datetime, date, str]], 
                   format_str: str = "%Y-%m-%d %H:%M") -> str:
    """Format date for display.
    
    Args:
        date_value: Date to format
        format_str: Output format string
        
    Returns:
        Formatted date string or empty string if invalid
    """
    if not date_value or pd.isna(date_value):
        return ""
    
    try:
        if isinstance(date_value, str):
            dt = parse_wc_date(date_value)
            if dt:
                return dt.strftime(format_str)
            return date_value
        elif isinstance(date_value, (datetime, date)):
            return date_value.strftime(format_str)
        return str(date_value)
    except Exception:
        return str(date_value)


def to_iso8601(date_value: Optional[Union[datetime, date, str]]) -> Optional[str]:
    """Convert date to ISO8601 format for API requests.
    
    Args:
        date_value: Date to convert
        
    Returns:
        ISO8601 formatted string or None
    """
    if not date_value:
        return None
    
    try:
        if isinstance(date_value, str):
            # Try to parse and re-format
            dt = parse_wc_date(date_value)
            if dt:
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            return date_value
        elif isinstance(date_value, datetime):
            return date_value.strftime("%Y-%m-%dT%H:%M:%S")
        elif isinstance(date_value, date):
            return date_value.strftime("%Y-%m-%dT00:00:00")
    except Exception as e:
        logger.warning(f"Failed to convert to ISO8601: {e}")
    
    return None


def clean_phone(phone: Optional[str]) -> str:
    """Clean and normalize phone number.
    
    Args:
        phone: Raw phone number string
        
    Returns:
        Cleaned phone number with digits only
    """
    if not phone or pd.isna(phone):
        return ""
    
    # Extract digits only
    digits = re.sub(r"\D", "", str(phone).strip())
    
    # Handle common formatting variations
    if len(digits) == 10 and digits.startswith("1"):
        # US number without country code
        digits = "0" + digits
    
    return digits


def clean_email(email: Optional[str]) -> str:
    """Clean and normalize email address.
    
    Args:
        email: Raw email string
        
    Returns:
        Lowercase trimmed email or empty string
    """
    if not email or pd.isna(email):
        return ""
    
    return str(email).strip().lower()
    
def normalize_name(name: Optional[str]) -> str:
    """Normalize customer name for consistency.
    
    Args:
        name: Raw name string
        
    Returns:
        Cleaned, titled name string
    """
    if not name or pd.isna(name):
        return ""
    
    # Remove extra spaces and title case
    return " ".join(str(name).split()).title()


def generate_customer_key(
    customer_id: Optional[int],
    email: Optional[str],
    phone: Optional[str],
    order_id: Optional[str] = None
) -> str:
    """Generate unique customer key for deduplication.
    
    Priority order:
    1. Registered customer ID (if available)
    2. Cleaned phone number
    3. Hashed email address
    4. Order ID as fallback
    
    Args:
        customer_id: WooCommerce customer ID
        email: Customer email
        phone: Customer phone
        order_id: Order ID for anonymous fallback
        
    Returns:
        Unique customer key string
    """
    # 1. Registered customer priority
    if customer_id and customer_id > 0:
        return f"reg_{customer_id}"
    
    # 2. Phone number priority
    clean_p = clean_phone(phone)
    if clean_p:
        return f"guest_p_{clean_p}"
    
    # 3. Email fallback
    clean_e = clean_email(email)
    if clean_e:
        # Hash email for consistent length
        email_hash = hashlib.md5(clean_e.encode()).hexdigest()[:12]
        return f"guest_e_{email_hash}"
    
    # 4. Anonymous fallback
    anon_id = order_id or "unknown"
    return f"anon_{anon_id}"


def calculate_customer_metrics(orders_df: pd.DataFrame) -> dict:
    """Calculate comprehensive customer metrics from orders.
    
    Args:
        orders_df: DataFrame of customer orders
        
    Returns:
        Dictionary with calculated metrics:
        - total_orders: Number of orders
        - total_items: Total quantity of items
        - total_value: Sum of order totals
        - avg_order_value: Average order value
        - first_order_date: Date of first order
        - last_order_date: Date of last order
        - days_between_orders: Average days between orders
        - customer_lifespan_days: Days from first to last order
    """
    if orders_df.empty:
        return {
            "total_orders": 0,
            "total_items": 0,
            "total_value": 0.0,
            "avg_order_value": 0.0,
            "first_order_date": None,
            "last_order_date": None,
            "days_between_orders": None,
            "customer_lifespan_days": 0,
        }
    
    # Ensure date columns are parsed
    if "date_created" in orders_df.columns:
        orders_df = orders_df.copy()
        orders_df["date_created"] = pd.to_datetime(orders_df["date_created"], errors="coerce")
    
    # Calculate metrics
    total_orders = len(orders_df)
    total_items = orders_df["items_count"].sum() if "items_count" in orders_df.columns else 0
    total_value = orders_df["total"].sum() if "total" in orders_df.columns else 0.0
    
    avg_order_value = total_value / total_orders if total_orders > 0 else 0.0
    
    # Date calculations
    if "date_created" in orders_df.columns:
        dates = orders_df["date_created"].dropna()
        first_order_date = dates.min() if not dates.empty else None
        last_order_date = dates.max() if not dates.empty else None
        
        if first_order_date and last_order_date:
            lifespan = (last_order_date - first_order_date).days
            days_between = lifespan / (total_orders - 1) if total_orders > 1 else 0
        else:
            lifespan = 0
            days_between = None
    else:
        first_order_date = None
        last_order_date = None
        lifespan = 0
        days_between = None
    
    return {
        "total_orders": total_orders,
        "total_items": int(total_items),
        "total_value": round(float(total_value), 2),
        "avg_order_value": round(float(avg_order_value), 2),
        "first_order_date": first_order_date,
        "last_order_date": last_order_date,
        "days_between_orders": round(days_between, 1) if days_between else None,
        "customer_lifespan_days": lifespan,
    }


def aggregate_customer_orders(orders_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate orders to customer level.
    
    Args:
        orders_df: DataFrame with orders data
        
    Returns:
        DataFrame with one row per customer and aggregated metrics
    """
    if orders_df.empty:
        return pd.DataFrame(columns=[
            "customer_key", "total_orders", "total_items", "total_value",
            "avg_order_value", "first_order_date", "last_order_date",
            "unique_emails", "unique_phones"
        ])
    
    # Generate customer keys
    orders_df = orders_df.copy()
    orders_df["customer_key"] = orders_df.apply(
        lambda row: generate_customer_key(
            row.get("customer_id"),
            row.get("billing_email"),
            row.get("billing_phone"),
            str(row.get("order_id"))
        ),
        axis=1
    )
    
    # Aggregate by customer key
    grouped = orders_df.groupby("customer_key").agg({
        "order_id": "count",
        "items_count": "sum",
        "total": "sum",
        "date_created": ["min", "max"],
        "billing_email": lambda x: ", ".join(x.dropna().unique()),
        "billing_phone": lambda x: ", ".join(x.dropna().unique()),
        "billing_name": "first",
    }).reset_index()
    
    # Flatten column names
    grouped.columns = [
        "customer_key", "total_orders", "total_items", "total_value",
        "first_order_date", "last_order_date", "unique_emails", "unique_phones", "name"
    ]
    
    # Calculate derived metrics
    grouped["avg_order_value"] = (
        grouped["total_value"] / grouped["total_orders"]
    ).round(2)
    
    # Parse dates
    grouped["first_order_date"] = pd.to_datetime(grouped["first_order_date"], errors="coerce")
    grouped["last_order_date"] = pd.to_datetime(grouped["last_order_date"], errors="coerce")
    
    return grouped


def filter_orders_by_date_range(
    orders_df: pd.DataFrame,
    start_date: Optional[Union[datetime, date, str]] = None,
    end_date: Optional[Union[datetime, date, str]] = None,
) -> pd.DataFrame:
    """Filter orders by date range.
    
    Args:
        orders_df: DataFrame with orders
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        
    Returns:
        Filtered DataFrame
    """
    if orders_df.empty or "date_created" not in orders_df.columns:
        return orders_df
    
    df = orders_df.copy()
    df["date_created"] = pd.to_datetime(df["date_created"], errors="coerce")
    
    if start_date:
        start = parse_wc_date(start_date) if isinstance(start_date, str) else start_date
        if start:
            df = df[df["date_created"] >= start]
    
    if end_date:
        end = parse_wc_date(end_date) if isinstance(end_date, str) else end_date
        if end:
            # Set to end of day
            if isinstance(end, date) and not isinstance(end, datetime):
                end = datetime.combine(end, datetime.max.time())
            df = df[df["date_created"] <= end]
    
    return df


def filter_orders_by_products(
    orders_df: pd.DataFrame,
    product_ids: List[int],
) -> pd.DataFrame:
    """Filter orders that contain specific products.
    
    Args:
        orders_df: DataFrame with orders
        product_ids: List of product IDs to filter by
        
    Returns:
        Filtered DataFrame with orders containing specified products
    """
    if orders_df.empty or not product_ids or "product_ids" not in orders_df.columns:
        return orders_df
    
    mask = orders_df["product_ids"].apply(
        lambda ids: any(pid in product_ids for pid in ids) if isinstance(ids, list) else False
    )
    
    return orders_df[mask].copy()


def get_store_domain(store_url: str) -> str:
    """Extract clean domain from store URL.
    
    Args:
        store_url: Full store URL
        
    Returns:
        Clean domain without protocol or www
    """
    try:
        parsed = urlparse(store_url)
        domain = parsed.netloc or store_url
        # Remove www. prefix
        domain = domain.replace("www.", "")
        return domain
    except Exception:
        return store_url


def format_currency(amount: float, currency: str = "৳") -> str:
    """Format amount as currency string.
    
    Args:
        amount: Numeric amount
        currency: Currency symbol (default: ৳ for BDT)
        
    Returns:
        Formatted currency string
    """
    try:
        return f"{currency}{amount:,.2f}"
    except (ValueError, TypeError):
        return f"{currency}0.00"


def calculate_date_range(window: str) -> tuple[Optional[date], date]:
    """Calculate start and end dates from a time window string.
    
    Args:
        window: Time window description (e.g., "Last 7 Days", "Last Month")
        
    Returns:
        Tuple of (start_date, end_date)
    """
    today = date.today()
    
    window_map = {
        "Last Day": 1,
        "Last 3 Days": 3,
        "Last 7 Days": 7,
        "Last 15 Days": 15,
        "Last Month": 30,
        "Last 3 Months": 90,
        "Last 6 Months": 180,
        "Last Year": 365,
    }
    
    days = window_map.get(window, 30)
    start = today - timedelta(days=days)
    
    return start, today
