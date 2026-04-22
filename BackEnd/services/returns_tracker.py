"""Returns Insights - Backend Service.

Syncs delivery-issue data from Google Sheets, classifies orders as
Return / Partial / Exchange / Refund, and calculates Net Sales metrics.

Data valid from August 2025 onwards.
"""

from __future__ import annotations

import gc
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

import pandas as pd
import numpy as np
import streamlit as st

from BackEnd.core.cache_storage import (
    build_cache_target,
    read_json as storage_read_json,
    read_parquet as storage_read_parquet,
    target_exists,
    write_json as storage_write_json,
    write_parquet as storage_write_parquet,
)
from BackEnd.core.logging_config import get_logger
from BackEnd.core.memory_utils import (
    optimize_dtypes,
    safe_groupby_transform,
    safe_merge,
    cleanup_memory,
    safe_operation
)

# Set pandas option to avoid fragmentation warnings
pd.set_option('mode.copy_on_write', True)

logger = get_logger("returns_tracker")

# ── Cache Configuration ──
RETURNS_CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "returns"
CACHE_TTL_HOURS = 24


def _returns_cache_target(name: str) -> str | Path:
    return build_cache_target(filename=name, local_dir=RETURNS_CACHE_DIR)


def _load_cached_returns() -> pd.DataFrame:
    """Load cached returns data if it exists and is fresh."""
    returns_cache_file = _returns_cache_target("returns_data.parquet")
    returns_meta_file = _returns_cache_target("returns_meta.json")

    if not target_exists(returns_cache_file):
        return pd.DataFrame()
    try:
        df = storage_read_parquet(returns_cache_file)
        # Check if cache is fresh
        if target_exists(returns_meta_file):
            meta = storage_read_json(returns_meta_file)
            cached_at = pd.to_datetime(meta.get("cached_at"), errors="coerce")
            if pd.notna(cached_at):
                age = datetime.now() - cached_at.to_pydatetime()
                if age > timedelta(hours=CACHE_TTL_HOURS):
                    logger.info(f"Returns cache is {age.total_seconds()/3600:.1f} hours old, will check for new data")
        return df
    except Exception as e:
        logger.warning(f"Failed to load cached returns: {e}")
        return pd.DataFrame()


def _save_returns_cache(df: pd.DataFrame, last_date: Optional[datetime] = None):
    """Save returns data to cache with metadata."""
    returns_cache_file = _returns_cache_target("returns_data.parquet")
    returns_meta_file = _returns_cache_target("returns_meta.json")
    try:
        storage_write_parquet(df, returns_cache_file, index=False)
        meta = {
            "cached_at": datetime.now().isoformat(),
            "row_count": len(df),
            "last_date": last_date.isoformat() if last_date else None,
        }
        storage_write_json(returns_meta_file, meta)
        logger.info(f"Saved {len(df)} returns rows to cache (last date: {last_date})")
    except Exception as e:
        logger.error(f"Failed to save returns cache: {e}")


def _get_last_cached_date() -> Optional[datetime]:
    """Get the last date from cached returns data."""
    cached_df = _load_cached_returns()
    if cached_df.empty or "date" not in cached_df.columns:
        return None
    last_date = cached_df["date"].max()
    return last_date if pd.notna(last_date) else None


# ── Google Sheets Published CSV ──
DEFAULT_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vQ4j3i94IWVlVYI5gErxzfmmaYNiirGqnrncRKrDCbHvmLYpzH9l4_etjYmfCoDj_Gv-_mps2gnufXE"
    "/pub?gid=0&single=true&output=csv"
)

# ── Classification Keywords ──
EXCHANGE_KEYWORDS = [
    "exchange", "size change", "product change", "replace", "reverse",
    "wrong product sent", "wrong colour sent", "wrong color sent",
]
PARTIAL_KEYWORDS = [
    "partial", "returned", "refunded", "partial return", "partial delivery",
    "missing items", "get return", "get back",
]
RETURN_REASON_CATEGORIES = {
    "Size Issue": ["size issue", "size", "over-size", "over sized", "too tight",
                   "too loose", "too slim", "too short", "too long", "thigh",
                   "sleeve", "chest size", "waist size"],
    "Quality Issue": ["quality", "fabric", "fabrics", "stitch", "faded",
                      "dirty", "damaged", "gsm", "overpriced", "swing"],
    "Color Issue": ["colour", "color", "faded color", "color difference",
                    "color dissatisfaction", "colour dissatisfaction"],
    "Wrong Product": ["wrong product", "wrong colour", "wrong color"],
    "CNR": ["cnr", "call not receive", "unreachable", "not available",
            "unresponsive", "denied to rec", "denied to receive",
            "not in location", "out of location", "not responded"],
    "Changed Mind": ["changed mind", "doesn't need", "doesn't want",
                     "will order later", "reorder", "cancel"],
    "Fraud": ["fraud", "fake customer"],
    "Timing Issue": ["timing", "late", "rider didn't call",
                     "delivery man didn't call", "deliveryman didn't call"],
}


def get_current_sync_window() -> str:
    """Return an identifier for the current sync window (10:30 AM or 4:30 PM)."""
    now = datetime.now()
    t_1030 = now.replace(hour=10, minute=30, second=0, microsecond=0)
    t_1630 = now.replace(hour=16, minute=30, second=0, microsecond=0)
    
    if now < t_1030:
        return f"{(now.date() - pd.Timedelta(days=1)).isoformat()}_16:30"
    elif now < t_1630:
        return f"{now.date().isoformat()}_10:30"
    else:
        return f"{now.date().isoformat()}_16:30"

def _process_returns_chunk(
    df: pd.DataFrame,
    sales_df: Optional[pd.DataFrame] = None,
    stock_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """Process a chunk of returns data (standardize, classify, cross-reference, verify with stock)."""
    if df.empty:
        return df

    # ── Standardize column names ──
    col_map = {
        "Date": "date",
        "Order ID": "order_id_raw",
        "Courier ID": "courier_id",
        "Delivery Issue": "delivery_issue",
        "Courier": "courier",
        "Issue Or Product Details": "product_details",
        "Courier Reason": "courier_reason",
        "Customer Reason": "customer_reason",
        "Follow up Date": "followup_date",
        "FU Status": "fu_status",
        "On Time": "on_time",
        "Inventory Updated": "inventory_updated",
        "Received Date": "received_date",
        "Assigned To": "assigned_to",
        "Remarks": "remarks",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # ── Parse dates ──
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], format="mixed", dayfirst=False, errors="coerce")
    else:
        df["date"] = pd.NaT

    # ── Normalize Order ID ──
    if "order_id_raw" in df.columns:
        df["order_id_raw"] = df["order_id_raw"].astype(str).str.strip()
        df["order_id"] = df["order_id_raw"].apply(_normalize_order_id)
    else:
        df["order_id_raw"] = ""
        df["order_id"] = ""

    # ── Fill NaN text columns ──
    text_cols = ["delivery_issue", "product_details", "courier_reason",
                 "customer_reason", "remarks"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()
        else:
            df[col] = ""

    # ── Classify each row ──
    df["issue_type"] = df.apply(_classify_issue_type, axis=1)
    df["return_reason"] = df.apply(_extract_return_reason, axis=1)
    df["is_exchange"] = df["issue_type"] == "Exchange"
    df["is_return"] = df["issue_type"].isin(["Paid Return", "Non Paid Return", "Refund"])
    df["is_partial"] = df["issue_type"] == "Partial"

    # ── Ensure product_details column exists ──
    if "product_details" not in df.columns:
        df["product_details"] = ""

    # ── Extract partial amount ──
    df["partial_amount"] = df["product_details"].apply(_extract_partial_amount)

    # ── Normalize & Extract Returned Products (with stock lookup) ──
    df["returned_items"] = df["product_details"].apply(
        lambda x: _normalize_product_names(x, stock_df)
    )

    # ── Keep ONLY allowed types ──
    allowed_types = ["Paid Return", "Non Paid Return", "Partial", "Exchange"]
    df = df[df["issue_type"].isin(allowed_types)].copy()

    # ── Drop rows with missing critical values ──
    # Ignore rows with no date, no order_id, no product_details, or empty returned_items
    df = df.dropna(subset=["date"])

    # Filter out rows with empty order_id
    if "order_id" in df.columns:
        df = df[df["order_id"].astype(str).str.strip() != ""]

    # Filter out rows with empty product_details
    if "product_details" in df.columns:
        df = df[df["product_details"].astype(str).str.strip() != ""]
        df = df[df["product_details"].astype(str).str.lower() != "nan"]

    # Filter out rows where returned_items extraction returned empty list
    df = df[df["returned_items"].apply(lambda x: len(x) > 0 if isinstance(x, list) else False)]

    logger.info(f"After filtering missing values: {len(df)} rows remain")

    # Cross-reference with WooCommerce data
    if sales_df is not None and not sales_df.empty:
        df = cross_reference_return_items(df, sales_df)

    return df


@st.cache_data(show_spinner=False, max_entries=2)
def load_returns_data(
    url: Optional[str] = None,
    sync_window: str = "",
    sales_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Load and clean returns/delivery-issue data with incremental caching.

    Only processes new rows from Google Sheets, merging with cached historical data.
    Historical data stays the same - only new entries are fetched and processed.

    Args:
        url: Google Sheets published CSV URL (defaults to DEEN sheet).
        sync_window: Sync window identifier for caching.
        sales_df: Optional WooCommerce sales data for cross-referencing items.

    Returns:
        Cleaned DataFrame with standardized columns and cross-referenced items.
    """
    # ── Load cached historical data ──
    cached_df = _load_cached_returns()
    last_cached_date = _get_last_cached_date()

    logger.info(f"Cached returns: {len(cached_df)} rows, last date: {last_cached_date}")

    # ── Fetch fresh data from Google Sheets ──
    try:
        source = url or DEFAULT_SHEET_URL
        fresh_df = pd.read_csv(source)
        logger.info(f"Fetched {len(fresh_df)} total rows from returns data source")
    except Exception as e:
        logger.error(f"Failed to load returns data: {e}")
        # Return cached data if fetch fails
        if not cached_df.empty:
            logger.info("Returning cached data due to fetch failure")
            return cached_df
        return pd.DataFrame()

    if fresh_df.empty:
        return cached_df if not cached_df.empty else fresh_df

    # ── Parse dates to find new rows ──
    date_col = "Date" if "Date" in fresh_df.columns else None
    if date_col:
        fresh_df["_parsed_date"] = pd.to_datetime(fresh_df[date_col], format="mixed", dayfirst=False, errors="coerce")
    else:
        fresh_df["_parsed_date"] = pd.NaT

    # ── Filter to only NEW rows (after last cached date) ──
    if last_cached_date is not None and "_parsed_date" in fresh_df.columns:
        # Add buffer of 1 day to catch any late entries
        cutoff_date = last_cached_date - timedelta(days=1)
        new_rows = fresh_df[fresh_df["_parsed_date"] > cutoff_date].copy()
        logger.info(f"Found {len(new_rows)} new rows after {cutoff_date}")
    else:
        # No cache or no date info - process all
        new_rows = fresh_df.copy()
        logger.info(f"No cache found, processing all {len(new_rows)} rows")

    # Drop the temporary parsed date column
    fresh_df = fresh_df.drop(columns=["_parsed_date"], errors="ignore")
    if "_parsed_date" in new_rows.columns:
        new_rows = new_rows.drop(columns=["_parsed_date"])

    # ── Load WooCommerce stock data for product verification ──
    stock_df = pd.DataFrame()
    try:
        from BackEnd.services.hybrid_data_loader import load_cached_woocommerce_stock_data
        stock_df = load_cached_woocommerce_stock_data()
        if not stock_df.empty:
            logger.info(f"Loaded {len(stock_df)} stock items for product verification")
    except Exception as e:
        logger.warning(f"Could not load stock data for verification: {e}")

    # ── Process ONLY the new rows ──
    if new_rows.empty:
        logger.info("No new returns data to process")
        return cached_df if not cached_df.empty else pd.DataFrame()

    processed_new = _process_returns_chunk(new_rows, sales_df, stock_df)
    logger.info(
        f"Processed {len(processed_new)} new entries: "
        f"{processed_new['is_return'].sum()} returns, "
        f"{processed_new['is_partial'].sum()} partials, "
        f"{processed_new['is_exchange'].sum()} exchanges"
    )

    # ── Merge with cached historical data ──
    if cached_df.empty:
        merged_df = processed_new
    else:
        # Concatenate and remove duplicates based on order_id and date
        merged_df = pd.concat([cached_df, processed_new], ignore_index=True)
        if "order_id" in merged_df.columns and "date" in merged_df.columns:
            merged_df = merged_df.drop_duplicates(subset=["order_id", "date"], keep="last")
        merged_df = merged_df.sort_values("date", ascending=False).reset_index(drop=True)
        logger.info(f"Merged data: {len(cached_df)} cached + {len(processed_new)} new = {len(merged_df)} total")

    # ── Save to cache ──
    last_date = merged_df["date"].max() if "date" in merged_df.columns and not merged_df.empty else None
    _save_returns_cache(merged_df, last_date)

    return merged_df


def fetch_woocommerce_order_by_id(order_id: str) -> Optional[Dict[str, Any]]:
    """Fetch a specific order from WooCommerce by ID.

    Args:
        order_id: The order ID to fetch

    Returns:
        Order data dict or None if not found
    """
    from BackEnd.services.woocommerce_service import WooCommerceService

    try:
        wc = WooCommerceService(ui_enabled=False)
        if not wc.wcapi:
            return None

        # Try to fetch the order by ID
        response = wc.wcapi.get(f"orders/{order_id}")
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        logger.error(f"Failed to fetch WooCommerce order {order_id}: {e}")
        return None


def fetch_woocommerce_orders_by_ids(order_ids: List[str]) -> pd.DataFrame:
    """Fetch multiple orders from WooCommerce by their IDs.

    Args:
        order_ids: List of order IDs to fetch

    Returns:
        DataFrame with order details including line items
    """
    from BackEnd.services.woocommerce_service import WooCommerceService

    orders_data = []
    try:
        wc = WooCommerceService(ui_enabled=False)
        if not wc.wcapi:
            return pd.DataFrame()

        for order_id in order_ids:
            try:
                response = wc.wcapi.get(f"orders/{order_id}")
                if response.status_code == 200:
                    order = response.json()
                    # Extract line items
                    for item in order.get("line_items", []):
                        orders_data.append({
                            "order_id": str(order_id),
                            "product_id": item.get("product_id"),
                            "item_name": item.get("name", ""),
                            "sku": item.get("sku", "N/A"),
                            "qty": item.get("quantity", 1),
                            "price": float(item.get("price", 0) or 0),
                            "total": float(item.get("total", 0) or 0),
                        })
            except Exception as e:
                logger.warning(f"Failed to fetch order {order_id}: {e}")
                continue

    except Exception as e:
        logger.error(f"WooCommerce API error: {e}")

    return pd.DataFrame(orders_data)


def _order_data_to_dataframe(order_data: Dict[str, Any], order_id: str) -> pd.DataFrame:
    """Convert WooCommerce order API response to DataFrame format.

    Args:
        order_data: Order data dict from WooCommerce API
        order_id: The order ID

    Returns:
        DataFrame with line items or empty DataFrame if no items
    """
    items = []
    for item in order_data.get("line_items", []):
        items.append({
            "order_id": str(order_id),
            "product_id": item.get("product_id"),
            "item_name": item.get("name", ""),
            "sku": item.get("sku", "N/A"),
            "qty": item.get("quantity", 1),
            "price": float(item.get("price", 0) or 0),
            "total": float(item.get("total", 0) or 0),
        })
    return pd.DataFrame(items)


def cross_reference_return_items(
    returns_df: pd.DataFrame,
    sales_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """Cross-reference returns with WooCommerce data for accurate item tracking.

    For each return order:
    - Paid/Non Paid Return: Match items from WooCommerce order to identify what was returned
    - Partial: Compare Google Sheet product details with WooCommerce items
    - Exchange: Track item changes without revenue deduction

    Args:
        returns_df: DataFrame with returns data
        sales_df: Optional cached sales data

    Returns:
        DataFrame with enhanced returned_items including SKU matching
    """
    if returns_df.empty:
        return returns_df

    # Get unique order IDs that need cross-referencing
    order_ids = returns_df["order_id"].unique().tolist()

    # Try to get from cached sales data first
    if sales_df is not None and not sales_df.empty:
        # Filter sales data for our return orders
        order_sales = sales_df[sales_df["order_id"].astype(str).isin(order_ids)].copy()
    else:
        order_sales = pd.DataFrame()

    # If no cached data, fetch from WooCommerce (limited to avoid API abuse)
    if order_sales.empty and len(order_ids) <= 50:  # Limit API calls
        logger.info(f"Fetching {len(order_ids)} orders from WooCommerce for cross-reference")
        order_sales = fetch_woocommerce_orders_by_ids(order_ids)

    # Fallback: Fetch missing individual orders using single-order API
    if not order_sales.empty:
        found_order_ids = set(order_sales["order_id"].unique())
        missing_order_ids = [oid for oid in order_ids if str(oid) not in found_order_ids]

        if missing_order_ids:
            logger.info(f"Fetching {len(missing_order_ids)} missing orders individually")
            for order_id in missing_order_ids:
                order_data = fetch_woocommerce_order_by_id(str(order_id))
                if order_data:
                    order_df = _order_data_to_dataframe(order_data, str(order_id))
                    if not order_df.empty:
                        order_sales = pd.concat([order_sales, order_df], ignore_index=True)
                        logger.debug(f"Fetched missing order {order_id} with {len(order_df)} items")
                else:
                    logger.warning(f"Could not fetch order {order_id} from WooCommerce")

    if order_sales.empty:
        logger.warning("No WooCommerce data available for cross-referencing")
        return returns_df

    # Enhance returned_items with SKU matching
    enhanced_items = []
    for _, row in returns_df.iterrows():
        order_id = str(row["order_id"])
        issue_type = row.get("issue_type", "Unknown")
        returned_items = row.get("returned_items", [])

        if not isinstance(returned_items, list):
            enhanced_items.append(returned_items)
            continue

        # Get WooCommerce items for this order
        wc_items = order_sales[order_sales["order_id"] == order_id]

        enhanced_row_items = []
        for item in returned_items:
            if not isinstance(item, dict):
                enhanced_row_items.append(item)
                continue

            item_name = item.get("name", "").lower().strip()
            item_size = item.get("size", "").lower().strip()

            # Try to match with WooCommerce item
            matched = False
            for _, wc_item in wc_items.iterrows():
                wc_name = wc_item.get("item_name", "").lower().strip()
                wc_sku = wc_item.get("sku", "N/A")

                # Match by name similarity or SKU in name
                if item_name in wc_name or wc_name in item_name or wc_sku.lower() in item_name:
                    item["sku"] = wc_sku
                    item["price"] = wc_item.get("price", 0)
                    item["matched_from_wc"] = True
                    matched = True
                    break

            if not matched:
                item["matched_from_wc"] = False

            # Classify based on issue type
            if issue_type == "Exchange":
                item["transaction_type"] = "exchange"
                item["revenue_impact"] = 0  # No revenue deduction
            elif issue_type == "Partial":
                item["transaction_type"] = "partial_return"
                item["revenue_impact"] = item.get("price", 0) * item.get("qty", 1) * 0.5  # Estimate 50%
            elif issue_type in ["Paid Return", "Non Paid Return"]:
                item["transaction_type"] = "full_return"
                item["revenue_impact"] = item.get("price", 0) * item.get("qty", 1)
            else:
                item["transaction_type"] = "unknown"
                item["revenue_impact"] = 0

            enhanced_row_items.append(item)

        enhanced_items.append(enhanced_row_items)

    returns_df["returned_items"] = enhanced_items
    returns_df["items_cross_referenced"] = returns_df["returned_items"].apply(
        lambda items: any(isinstance(i, dict) and i.get("matched_from_wc") for i in items) if isinstance(items, list) else False
    )

    return returns_df


def _normalize_order_id(raw_id: str) -> str:
    """Extract 6-digit order number by removing all non-numeric characters.

    Handles various formats: '194829', '194829 RI', 'D-194829', '194829w', 'DD194829'

    Examples:
        '194829' → '194829'
        '194829 RI' → '194829'
        'D-194829' → '194829'
        '194829w' → '194829'
        'DD194829' → '194829'
        '194829-' → '194829'
    """
    clean = str(raw_id).strip()
    # Extract only digits (0-9)
    digits_only = re.sub(r'\D', '', clean)  # \D matches any non-digit character
    
    # Ensure it's exactly 6 digits (or return what we have if not)
    if len(digits_only) >= 6:
        # If longer than 6, take the last 6 (order numbers are typically at the end)
        return digits_only[-6:]
    return digits_only


def _classify_issue_type(row: pd.Series) -> str:
    """Classify the row into a canonical issue type.

    Priority: exact match on delivery_issue column → D- prefix → fuzzy.
    """
    di = str(row.get("delivery_issue", "")).strip().lower()
    raw_id = str(row.get("order_id_raw", ""))
    product = str(row.get("product_details", "")).lower()
    courier_reason = str(row.get("courier_reason", "")).lower()

    # 1. Exact match on delivery_issue column - PRIORITY ORDER MATTERS
    # "Paid Return/Reverse" should be Paid Return (not Exchange)
    if di == "paid return/reverse" or di.startswith("paid return/reverse"):
        return "Paid Return"
    
    type_map = {
        "paid return": "Paid Return",
        "non paid return": "Non Paid Return",
        "partial": "Partial",
        "exchange": "Exchange",
        "reverse": "Paid Return",  # Reverse alone = Paid Return (delivery fee only)
        "reverse ": "Paid Return",
        "refund": "Refund",
        "cancel": "Cancel",
        "delivered": "Delivered",
        "items lost": "Items Lost",
        "delivery issue": "Delivery Issue",
    }
    for key, label in type_map.items():
        if di == key or di.startswith(key):
            return label

    # 2. D- prefix on order ID → exchange
    if raw_id.upper().startswith("D-"):
        return "Exchange"

    # 3. Fuzzy match on exchange keywords (but NOT if it says "paid return/reverse")
    combined = f"{di} {product} {courier_reason}"
    if "paid return/reverse" not in di:
        for kw in EXCHANGE_KEYWORDS:
            if kw in combined:
                return "Exchange"

    # 4. Fuzzy match on partial keywords
    for kw in PARTIAL_KEYWORDS:
        if kw in combined:
            return "Partial"

    # 5. Pure Return keywords
    if "return" in di:
        return "Paid Return" if "paid" in di else "Non Paid Return"

    # 6. Default - return "Other" for anything not matching the 4 main types
    if di:
        return "Other"
    return "Unknown"


def _extract_return_reason(row: pd.Series) -> str:
    """Extract and categorize the return reason from text fields."""
    texts = [
        str(row.get("customer_reason", "")),
        str(row.get("courier_reason", "")),
        str(row.get("product_details", "")),
        str(row.get("delivery_issue", "")),
    ]
    combined = " ".join(texts).lower()

    # Skip noise
    if combined.strip() in ("", "nan", "#ref!"):
        return "Unknown"

    for category, keywords in RETURN_REASON_CATEGORIES.items():
        for kw in keywords:
            if kw in combined:
                return category

    return "Other"


def _extract_partial_amount(details: str) -> float:
    """Try to extract a numeric amount from partial order details.

    Examples:
        '1442tk - Get Return: Regular Fit...' → 1442.0
        '3258tk= Embroidered...' → 3258.0
        '589=Premium Jade...' → 589.0
    """
    if not details:
        return 0.0

    # Match patterns like '1442tk', '3258tk=', '589='
    match = re.match(r'^\s*(\d+)\s*(?:tk|=)', details, re.IGNORECASE)
    if match:
        return float(match.group(1))

    # Match patterns like '(1957TK)' or '(2296TK)'
    match = re.search(r'\((\d+)\s*TK?\)', details, re.IGNORECASE)
    if match:
        return float(match.group(1))

    # Match '=> 810TK' or '= 120TK'
    match = re.search(r'[=>]+\s*(\d+)\s*TK', details, re.IGNORECASE)
    if match:
        return float(match.group(1))

    return 0.0


def _normalize_product_names(details: str, stock_df: Optional[pd.DataFrame] = None) -> list[dict[str, Any]]:
    """Granular extraction of product details (Name, Size, SKU, Qty, Category).

    NEW APPROACH:
    1. Detect FULL SKU first (pattern: XXX-XXXX-XXX before ;)
    2. Fetch product name from WooCommerce using SKU
    3. Extract size from the string (between name and SKU)

    Format: Product name – size – SKU-XXX-XXX ;
    Multiple items separated by semicolon (;)
    """
    if not details or details.lower() == "nan":
        return []

    # 1. Clean prefix (e.g., "50 tk - ", "Return: ", etc.)
    clean = re.sub(r'^\s*(\d+)\s*(?:tk|=)[^:]*:?', '', details, flags=re.IGNORECASE).strip()
    clean = re.sub(r'^(?:Get Return|Return|Exchange|Partial|Issue)\s*:?\s*', '', clean, flags=re.IGNORECASE).strip()

    if not clean:
        return []

    # 2. Split items by semicolon (primary separator for multiple items)
    raw_items = re.split(r'[;]', clean)
    processed = []

    # Build SKU lookup from stock data if available
    sku_to_name = {}
    if stock_df is not None and not stock_df.empty:
        for _, row in stock_df.iterrows():
            sku = str(row.get("SKU", "")).strip()
            name = str(row.get("Name", "")).strip()
            if sku and name:
                sku_to_name[sku.lower()] = name

    for item in raw_items:
        item = item.strip()
        if not item:
            continue

        qty = 1
        name = "N/A"
        size = "N/A"
        sku = "N/A"

        # --- STEP 1: Try to detect FULL SKU with 3-dash pattern ---
        # Pattern: something-XXX-XXXX-XXX (at end before optional qty)
        # SKU has format: prefix-size_code-variant (3 parts separated by -)
        full_sku_match = re.search(
            r'([A-Z0-9]+-[A-Z0-9]+-[A-Z0-9]+)\s*(?:[x×]\s*\d+)?$',
            item,
            re.IGNORECASE
        )

        if full_sku_match:
            # Found full SKU with 3 parts
            sku = full_sku_match.group(1).strip()

            # Remove SKU and any qty from item to get name-size part
            name_size_part = item[:full_sku_match.start()].strip()

            # --- STEP 2: Extract qty if present ---
            qty_match = re.search(r'\s*[x×]\s*(\d+)$', name_size_part, re.IGNORECASE)
            if qty_match:
                qty = int(qty_match.group(1))
                name_size_part = name_size_part[:qty_match.start()].strip()

            # --- STEP 3: Split name and size from remaining part ---
            # Last dash usually separates name from size
            last_dash_idx = name_size_part.rfind('-')
            if last_dash_idx > 0:
                size = name_size_part[last_dash_idx + 1:].strip()
                extracted_name = name_size_part[:last_dash_idx].strip()
            else:
                # No dash - size might be at end without dash
                size_match = re.search(r'\s+(XS|S|M|L|XL|XXL|3XL|4XL|[0-9]{2})\s*$', name_size_part, re.IGNORECASE)
                if size_match:
                    size = size_match.group(1).strip()
                    extracted_name = name_size_part[:size_match.start()].strip()
                else:
                    extracted_name = name_size_part

            # --- STEP 4: Fetch name from WooCommerce using SKU ---
            if sku != "N/A":
                sku_lower = sku.lower()
                if sku_lower in sku_to_name:
                    name = sku_to_name[sku_lower]
                else:
                    # Fallback: use extracted name if WC lookup fails
                    name = extracted_name if extracted_name else "N/A"
            else:
                name = extracted_name if extracted_name else "N/A"

        else:
            # --- FALLBACK: Old logic for items without 3-part SKU ---
            dash_parts = [p.strip() for p in item.split('-') if p.strip()]

            # Extract qty from any part
            for i, part in enumerate(dash_parts):
                qty_match = re.search(r'\s*[x×]\s*(\d+)$', part, re.IGNORECASE)
                if qty_match:
                    qty = int(qty_match.group(1))
                    dash_parts[i] = re.sub(r'\s*[x×]\s*\d+$', '', part).strip()
                    break

            if len(dash_parts) >= 3:
                # Assume last part is SKU, second-to-last is size
                potential_sku = dash_parts[-1]
                # Check if it looks like a partial SKU we can look up
                potential_sku_lower = potential_sku.lower()

                # Try to find full SKU from stock
                full_sku = None
                for stock_sku in sku_to_name.keys():
                    if stock_sku.startswith(potential_sku_lower + '-') or stock_sku == potential_sku_lower:
                        full_sku = stock_sku
                        break

                if full_sku:
                    sku = full_sku
                    name = sku_to_name[full_sku]
                else:
                    sku = potential_sku
                    name = ' - '.join(dash_parts[:-2])

                size = dash_parts[-2]

            elif len(dash_parts) == 2:
                last_part = dash_parts[-1]
                if re.match(r'^[A-Z0-9]{3,}$', last_part):
                    sku = last_part
                    name = dash_parts[0]
                else:
                    size = last_part
                    name = dash_parts[0]
            else:
                name = item.strip()
                # Try to extract size from brackets or end
                size_match = re.search(r'[\(\[\{](.*?)[\)\]\}]', item)
                if size_match:
                    size = size_match.group(1).strip()
                    name = item[:size_match.start()].strip()
                else:
                    size_match_alt = re.search(r'\s+(XS|S|M|L|XL|XXL|3XL|4XL|[0-9]{2})\s*$', item, re.IGNORECASE)
                    if size_match_alt:
                        size = size_match_alt.group(1).strip()
                        name = item[:size_match_alt.start()].strip()

        # --- Clean up ---
        name = name.strip(' -_')

        # --- Infer Category ---
        category = "General"
        cat_keywords = {
            "Jeans": ["jeans", "denim", "pant"],
            "Shirt": ["shirt", "flannel", "polo", "tshirt", "t-shirt"],
            "Jacket": ["jacket", "hoodie", "sweater", "blazer"],
            "Accessories": ["belt", "wallet", "socks", "cap"],
        }
        for cat, keywords in cat_keywords.items():
            if any(k in name.lower() for k in keywords):
                category = cat
                break

        if len(name) > 2:
            processed.append({
                "name": name,
                "size": size,
                "sku": sku,
                "qty": qty,
                "category": category
            })

    return processed


def _verify_and_correct_product(
    extracted_item: dict[str, Any],
    stock_df: Optional[pd.DataFrame] = None
) -> dict[str, Any]:
    """Verify and correct extracted product details against WooCommerce stock data.

    Example issue: "Navy Polka Dot Full Sleeve Shirt - XL - 102" 
    gets size=XL, sku=102, but actual SKU is 102-0302-006 (with variant codes)

    This function looks up WooCommerce stock data and handles partial SKU matching
    to get the full correct SKU.
    """
    if stock_df is None or stock_df.empty:
        return extracted_item

    item = extracted_item.copy()
    name = item.get("name", "")
    extracted_size = item.get("size", "N/A")
    extracted_sku = item.get("sku", "N/A")

    if not name:
        return item

    # Build a lookup index from stock data
    # Index by: cleaned name, SKU, and partial SKU prefixes
    stock_lookup = {}
    sku_lookup = {}
    partial_sku_index = {}  # Maps partial SKUs to full SKUs

    for _, row in stock_df.iterrows():
        stock_name = str(row.get("Name", "")).strip()
        stock_sku = str(row.get("SKU", "")).strip()

        if stock_name:
            # Index by cleaned name (remove common size patterns)
            clean_name = re.sub(r'\s*-\s*(XS|S|M|L|XL|XXL|3XL|[0-9]+)\s*$', '', stock_name, flags=re.IGNORECASE)
            clean_name = re.sub(r'\s*\(\s*(XS|S|M|L|XL|XXL|3XL|[0-9]+)\s*\)', '', clean_name, flags=re.IGNORECASE)
            clean_name = clean_name.strip().lower()

            stock_lookup[clean_name] = {
                "name": stock_name,
                "sku": stock_sku,
                "size": _extract_size_from_name(stock_name),
                "category": row.get("Category", "General")
            }

        if stock_sku and stock_sku != "nan":
            sku_lookup[stock_sku.lower()] = stock_name

            # Build partial SKU index (e.g., "102-0302-006" → "102", "102-0302")
            sku_parts = stock_sku.split('-')
            for i in range(1, len(sku_parts) + 1):
                partial = '-'.join(sku_parts[:i]).lower()
                if partial not in partial_sku_index:
                    partial_sku_index[partial] = []
                partial_sku_index[partial].append({
                    "full_sku": stock_sku,
                    "name": stock_name,
                    "size": _extract_size_from_name(stock_name),
                    "category": row.get("Category", "General")
                })

    # Try to match extracted name to stock
    # Clean the extracted name same way
    clean_extracted = re.sub(r'\s*-\s*(XS|S|M|L|XL|XXL|3XL|[0-9]+)\s*$', '', name, flags=re.IGNORECASE)
    clean_extracted = re.sub(r'\s*\(\s*(XS|S|M|L|XL|XXL|3XL|[0-9]+)\s*\)', '', clean_extracted, flags=re.IGNORECASE)
    clean_extracted = clean_extracted.strip().lower()

    matched = False

    # 1. Direct match by cleaned name
    if clean_extracted in stock_lookup:
        stock_info = stock_lookup[clean_extracted]
        matched = True

        # Only update if extracted values look wrong
        # Size: if extracted is numeric (like "0302") but stock has proper size
        if extracted_size.isdigit() or extracted_size in ["N/A", ""]:
            stock_size = stock_info.get("size")
            if stock_size and stock_size != "N/A":
                item["size"] = stock_size
                logger.debug(f"Corrected size from '{extracted_size}' to '{stock_size}' for {name}")

        # SKU: if extracted looks wrong (numeric only) or is a partial match
        extracted_sku_lower = extracted_sku.lower()
        stock_sku = stock_info.get("sku", "")

        # Check if extracted is a partial SKU prefix of the full SKU
        if (extracted_sku.isdigit() or extracted_sku in ["N/A", ""] or
            (stock_sku and stock_sku.lower().startswith(extracted_sku_lower + '-')) or
            (extracted_sku_lower in partial_sku_index and len(partial_sku_index[extracted_sku_lower]) == 1)):

            if stock_sku and stock_sku != extracted_sku:
                item["sku"] = stock_sku
                logger.debug(f"Corrected SKU from '{extracted_sku}' to '{stock_sku}' for {name}")

        # Category from stock if we have it
        stock_cat = stock_info.get("category")
        if stock_cat and stock_cat != "":
            item["category"] = stock_cat

    # 2. Fuzzy match if no direct match
    if not matched:
        best_match = None
        best_score = 0

        for clean_stock_name, stock_info in stock_lookup.items():
            # Simple word overlap score
            extracted_words = set(clean_extracted.split())
            stock_words = set(clean_stock_name.split())
            if extracted_words and stock_words:
                overlap = len(extracted_words & stock_words)
                score = overlap / max(len(extracted_words), len(stock_words))

                if score > best_score and score > 0.6:  # 60% word overlap threshold
                    best_score = score
                    best_match = stock_info

        if best_match:
            stock_size = best_match.get("size")
            stock_sku = best_match.get("sku")

            # Update size if extracted looks wrong
            if extracted_size.isdigit() or extracted_size in ["N/A", ""]:
                if stock_size and stock_size != "N/A":
                    item["size"] = stock_size

            # Update SKU if extracted looks wrong or is partial
            extracted_sku_lower = extracted_sku.lower()
            if (extracted_sku.isdigit() or extracted_sku in ["N/A", ""] or
                (stock_sku and stock_sku.lower().startswith(extracted_sku_lower + '-'))):
                if stock_sku and stock_sku != extracted_sku:
                    item["sku"] = stock_sku

    # 3. Direct partial SKU match (e.g., "102" → "102-0302-006")
    if not matched and extracted_sku not in ["N/A", ""]:
        extracted_sku_lower = extracted_sku.lower()
        if extracted_sku_lower in partial_sku_index:
            candidates = partial_sku_index[extracted_sku_lower]
            # If only one match, use it
            if len(candidates) == 1:
                stock_info = candidates[0]
                item["sku"] = stock_info["full_sku"]
                logger.debug(f"Matched partial SKU '{extracted_sku}' to full SKU '{stock_info['full_sku']}'")

                # Also update size if available
                stock_size = stock_info.get("size")
                if stock_size and stock_size != "N/A" and (extracted_size.isdigit() or extracted_size in ["N/A", ""]):
                    item["size"] = stock_size

    return item


def _extract_size_from_name(name: str) -> str:
    """Extract size from product name using common patterns."""
    if not name:
        return "N/A"

    # Pattern: "Product Name - Size" or "Product Name (Size)"
    size_patterns = [
        r'\s*-\s*(XS|S|M|L|XL|XXL|3XL|4XL)\s*$',  # Dash before size at end
        r'\s*\(\s*(XS|S|M|L|XL|XXL|3XL|4XL)\s*\)',  # Size in parentheses
        r'\s+(XS|S|M|L|XL|XXL|3XL|4XL)\s*$',  # Size at end without dash
        r'\s*-\s*([0-9]{2,3})\s*$',  # Numeric size like 30, 32, 38
    ]

    for pattern in size_patterns:
        match = re.search(pattern, name, re.IGNORECASE)
        if match:
            return match.group(1).strip().upper()

    return "N/A"


def _verify_products_with_stock(
    items: list[dict[str, Any]],
    stock_df: Optional[pd.DataFrame] = None
) -> list[dict[str, Any]]:
    """Verify all extracted items against WooCommerce stock data."""
    if not items or stock_df is None or stock_df.empty:
        return items

    verified = []
    for item in items:
        verified_item = _verify_and_correct_product(item, stock_df)
        verified.append(verified_item)

    return verified


def map_items_to_skus(order_id: str, items: list[dict[str, Any]], sales_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Map granular item details to SKUs."""
    if not items or sales_df is None or sales_df.empty:
        for item in items: item["sku"] = "N/A"
        return items

    results = []
    for item in items:
        # Safety for old string format
        if isinstance(item, dict):
            name = item.get("name", "Unknown")
            item_copy = item.copy()
        else:
            name = str(item)
            item_copy = {"name": name, "sku": "N/A", "size": "N/A", "qty": 1, "category": "N/A"}
        
        # Filter sales for this specific order
        order_sales = sales_df[sales_df["order_id"].astype(str) == str(order_id)]
        
        # Match - using regex=False
        match = pd.DataFrame()
        if not order_sales.empty:
            match = order_sales[order_sales["item_name"].str.contains(name, case=False, na=False, regex=False)]
        
        if match.empty:
            # Global fallback
            match = sales_df[sales_df["item_name"].str.contains(name, case=False, na=False, regex=False)]
            
        if not match.empty:
            item_copy["sku"] = match.iloc[0].get("sku", "N/A")
        else:
            item_copy["sku"] = "N/A"
            
        results.append(item_copy)
                
    return results


def get_order_items_breakdown(order_id: str, returned_items: list[dict[str, Any]], sales_df: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    """Categorize items with granular details."""
    # Safety for old string format in input
    returned_names = []
    for i in returned_items:
        if isinstance(i, dict):
            returned_names.append(i.get("name", "Unknown"))
        else:
            returned_names.append(str(i))
    
    # Use existing helper to find SKUs for returned items
    returned_with_skus = map_items_to_skus(order_id, returned_items, sales_df)
    
    if sales_df is None or sales_df.empty:
        return {"returned": returned_with_skus, "delivered": []}

    order_sales = sales_df[sales_df["order_id"].astype(str) == str(order_id)].copy()
    if order_sales.empty:
        return {"returned": returned_with_skus, "delivered": []}

    delivered_records = []
    remaining_sales = order_sales.copy()
    
    # Remove matched returned items from remaining
    for item in returned_with_skus:
        name = item["name"]
        match_idx = remaining_sales[remaining_sales["item_name"].str.contains(name, case=False, na=False, regex=False)].index
        if not match_idx.empty:
            remaining_sales = remaining_sales.drop(match_idx[0])
            
    for _, row in remaining_sales.iterrows():
        delivered_records.append({
            "name": row["item_name"],
            "sku": row.get("sku", "N/A"),
            "size": "N/A", # Size info might not be parsed in sales_df similarly
            "qty": row.get("qty", 1),
            "category": "N/A"
        })
        
    return {
        "returned": returned_with_skus,
        "delivered": delivered_records
    }


def _estimate_sales_line_revenue(sales_df: pd.DataFrame) -> pd.Series:
    """Estimate line revenue using the best available columns.
    
    Memory-safe implementation with chunked processing for large datasets.
    """
    if sales_df is None or sales_df.empty:
        return pd.Series(dtype="float64")
    
    # Use optimized dtypes to reduce memory
    sales_df = optimize_dtypes(sales_df)

    try:
        qty = pd.to_numeric(sales_df.get("qty", 0), errors="coerce").fillna(0)

        for col in ["item_revenue", "line_total", "total"]:
            if col in sales_df.columns:
                values = pd.to_numeric(sales_df[col], errors="coerce")
                if values.notna().any() and values.sum() > 0:
                    return values.fillna(0.0)

        for col in ["item_cost", "price"]:
            if col in sales_df.columns:
                unit_price = pd.to_numeric(sales_df[col], errors="coerce").fillna(0.0)
                if unit_price.sum() > 0:
                    return unit_price * qty

        order_total = pd.to_numeric(sales_df.get("order_total", 0), errors="coerce").fillna(0.0)
        
        # Memory-safe groupby using fallback for large DataFrames
        if "order_id" in sales_df.columns:
            group_key = sales_df["order_id"]
            # Use simple arithmetic fallback for large datasets
            if len(sales_df) > 100000:
                # Approximate allocation by dividing order total by line count per order
                order_line_counts = sales_df.groupby("order_id").size()
                line_counts = sales_df["order_id"].map(order_line_counts).replace(0, 1)
                qty_totals = qty.groupby(sales_df["order_id"]).transform("sum").replace(0, 1)
                gc.collect()  # Force cleanup after groupby
                return (order_total * (qty / qty_totals)).fillna(order_total / line_counts).fillna(order_total)
            else:
                qty_totals = qty.groupby(group_key).transform("sum").replace(0, 1)
                line_counts = sales_df.groupby(group_key).cumcount() * 0 + 1
                line_counts = line_counts.groupby(group_key).transform("sum").replace(0, 1)
                return (order_total * (qty / qty_totals)).fillna(order_total / line_counts).fillna(order_total)
        else:
            # No order_id, distribute evenly
            line_count = len(sales_df)
            return order_total / line_count if line_count > 0 else order_total
            
    except MemoryError as e:
        logger.warning(f"Memory error in revenue estimation, using fallback: {e}")
        gc.collect()
        # Fallback: return order_total divided evenly
        order_total = pd.to_numeric(sales_df.get("order_total", 0), errors="coerce").fillna(0.0)
        line_count = len(sales_df)
        return order_total / line_count if line_count > 0 else order_total
    except Exception as e:
        logger.error(f"Error estimating line revenue: {e}")
        return pd.Series(0.0, index=sales_df.index)


def _normalize_match_text(value: Any) -> str:
    """Normalize free text for loose item matching."""
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _prepare_sales_context(sales_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Normalize sales rows for revenue attribution with memory safety."""
    if sales_df is None or sales_df.empty or "order_id" not in sales_df.columns:
        return pd.DataFrame()

    try:
        # Optimize dtypes first to reduce memory
        sales = optimize_dtypes(sales_df).copy()
        
        sales["order_id"] = sales["order_id"].astype(str).str.strip()
        sales["qty"] = pd.to_numeric(sales.get("qty", 1), errors="coerce").fillna(1).clip(lower=1)
        
        # Memory-safe revenue estimation
        sales["_line_revenue"] = _estimate_sales_line_revenue(sales).fillna(0.0)
        
        # Use vectorized operations instead of apply for memory efficiency
        item_names = sales["item_name"] if "item_name" in sales.columns else pd.Series("", index=sales.index)
        skus = sales["sku"] if "sku" in sales.columns else pd.Series("", index=sales.index)
        
        # For large DataFrames, avoid apply() which can cause memory spikes
        if len(sales) > 50000:
            sales["_item_name_norm"] = item_names.str.lower().str.replace(r'[^a-z0-9]+', ' ', regex=True)
            sales["_sku_norm"] = skus.str.lower().str.replace(r'[^a-z0-9]+', ' ', regex=True)
        else:
            sales["_item_name_norm"] = item_names.apply(_normalize_match_text)
            sales["_sku_norm"] = skus.apply(_normalize_match_text)
        
        sales["_line_key"] = sales.index.astype(str)
        
        # Cleanup intermediate objects
        gc.collect()
        
        return sales
        
    except MemoryError as e:
        logger.error(f"Memory error preparing sales context: {e}")
        gc.collect()
        # Return minimal DataFrame
        return pd.DataFrame({
            "order_id": sales_df["order_id"].astype(str),
            "_line_revenue": 0.0
        })
    except Exception as e:
        logger.error(f"Error preparing sales context: {e}")
        return pd.DataFrame()


def _match_returned_items_to_sales(row: pd.Series, order_sales: pd.DataFrame) -> Tuple[float, int, int]:
    """Match returned items to order lines and estimate impact."""
    items = row.get("returned_items", [])
    if order_sales.empty or not isinstance(items, list):
        return 0.0, 0, 0

    available = order_sales.copy()
    matched_loss = 0.0
    matched_items = 0
    estimated_items = 0

    for item in items:
        if not isinstance(item, dict):
            continue

        qty = int(pd.to_numeric(item.get("qty", 1), errors="coerce") or 1)
        qty = max(qty, 1)
        item_name = _normalize_match_text(item.get("name", ""))
        sku = _normalize_match_text(item.get("sku", ""))

        candidates = pd.DataFrame()
        if sku and sku != "n a":
            candidates = available[available["_sku_norm"] == sku]

        if candidates.empty and item_name:
            candidates = available[
                available["_item_name_norm"].str.contains(item_name, na=False, regex=False)
                | available["_item_name_norm"].apply(lambda text: item_name in text if text else False)
            ]

        if candidates.empty:
            estimated_items += qty
            continue

        match_row = candidates.sort_values("_line_revenue", ascending=False).iloc[0]
        matched_loss += float(match_row.get("_line_revenue", 0.0))
        matched_items += qty
        available = available[available["_line_key"] != match_row["_line_key"]]

    return matched_loss, matched_items, estimated_items


def _build_daily_financials(returns_df: pd.DataFrame, sales_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Build a day-level gross/loss/net series for charts with memory safety."""
    columns = ["date", "gross_sales", "return_loss", "partial_loss", "total_loss", "net_sales"]
    
    try:
        sales_daily = pd.DataFrame(columns=["date", "gross_sales"])

        if sales_df is not None and not sales_df.empty and "order_date" in sales_df.columns:
            # Optimize dtypes to reduce memory
            sales_local = optimize_dtypes(sales_df).copy()
            sales_local["order_date"] = pd.to_datetime(sales_local["order_date"], errors="coerce")
            sales_local["_line_revenue"] = _estimate_sales_line_revenue(sales_local).fillna(0.0)
            sales_local = sales_local.dropna(subset=["order_date"])
            if not sales_local.empty:
                # Use safe groupby for large datasets
                if len(sales_local) > 100000:
                    # Chunked processing for very large datasets
                    sales_daily = (
                        sales_local.groupby(sales_local["order_date"].dt.date)["_line_revenue"]
                        .sum()
                        .reset_index(name="gross_sales")
                        .rename(columns={"order_date": "date"})
                    )
                    gc.collect()
                else:
                    sales_daily = (
                        sales_local.groupby(sales_local["order_date"].dt.date)["_line_revenue"]
                        .sum()
                        .reset_index(name="gross_sales")
                        .rename(columns={"order_date": "date"})
                    )

        if returns_df.empty or "date" not in returns_df.columns:
            if sales_daily.empty:
                return pd.DataFrame(columns=columns)
            sales_daily["return_loss"] = 0.0
            sales_daily["partial_loss"] = 0.0
            sales_daily["total_loss"] = 0.0
            sales_daily["net_sales"] = sales_daily["gross_sales"]
            return sales_daily[columns].sort_values("date")

        # Optimize returns DataFrame
        returns_local = optimize_dtypes(returns_df).copy()
        returns_local["date"] = pd.to_datetime(returns_local["date"], errors="coerce").dt.date
        
        # Memory-efficient date key generation
        sales_dates = sales_daily["date"].tolist() if not sales_daily.empty else []
        returns_dates = returns_local["date"].dropna().unique().tolist()
        date_keys = sorted(set(sales_dates) | set(returns_dates))
        
        if not date_keys:
            return pd.DataFrame(columns=columns)

        # Use safe_merge for memory efficiency
        timeline = pd.DataFrame({"date": date_keys})
        timeline = safe_merge(timeline, sales_daily, on="date", how="left")
        timeline["gross_sales"] = timeline["gross_sales"].fillna(0.0)

        full_rows = returns_local[returns_local["issue_type"].isin(["Paid Return", "Non Paid Return"])]
        partial_rows = returns_local[returns_local["issue_type"] == "Partial"]

        if full_rows.empty:
            full_daily = pd.DataFrame(columns=["date", "return_loss"])
        else:
            full_daily = full_rows.groupby("date")["_resolved_revenue_impact"].sum().reset_index(name="return_loss")

        if partial_rows.empty:
            partial_daily = pd.DataFrame(columns=["date", "partial_loss"])
        else:
            partial_daily = partial_rows.groupby("date")["_resolved_revenue_impact"].sum().reset_index(name="partial_loss")

        # Use safe_merge instead of chained merge
        timeline = safe_merge(timeline, full_daily, on="date", how="left")
        timeline = safe_merge(timeline, partial_daily, on="date", how="left")

        # Ensure columns exist (safe_merge may not add them if right side is empty)
        if "return_loss" not in timeline.columns:
            timeline["return_loss"] = 0.0
        if "partial_loss" not in timeline.columns:
            timeline["partial_loss"] = 0.0

        timeline["return_loss"] = pd.to_numeric(timeline["return_loss"], errors="coerce").fillna(0.0)
        timeline["partial_loss"] = pd.to_numeric(timeline["partial_loss"], errors="coerce").fillna(0.0)
        timeline["total_loss"] = timeline["return_loss"] + timeline["partial_loss"]
        timeline["net_sales"] = (timeline["gross_sales"] - timeline["total_loss"]).clip(lower=0.0)
        
        # Cleanup intermediate DataFrames
        del returns_local, full_rows, partial_rows, full_daily, partial_daily
        gc.collect()
        
        return timeline[columns].sort_values("date")
        
    except MemoryError as e:
        logger.error(f"Memory error in _build_daily_financials: {e}")
        gc.collect()
        # Return minimal DataFrame with available sales data only
        if not sales_daily.empty:
            sales_daily["return_loss"] = 0.0
            sales_daily["partial_loss"] = 0.0
            sales_daily["total_loss"] = 0.0
            sales_daily["net_sales"] = sales_daily["gross_sales"]
            return sales_daily[columns].sort_values("date")
        return pd.DataFrame(columns=columns)
    except Exception as e:
        logger.error(f"Error in _build_daily_financials: {e}")
        gc.collect()
        return pd.DataFrame(columns=columns)


def calculate_net_sales_metrics(
    returns_df: pd.DataFrame,
    sales_df: Optional[pd.DataFrame] = None,
    total_items_sold: int = 0,
) -> Dict[str, Any]:
    """Calculate comprehensive net sales metrics.

    Args:
        returns_df: Classified returns DataFrame.
        sales_df: WooCommerce active sales DataFrame to calculate precise values.
        total_items_sold: Total items sold in the same period (for calculating return item %).

    Returns:
        Dictionary of computed KPIs.
    """
    sales_context = _prepare_sales_context(sales_df)
    gross_sales = float(pd.to_numeric(sales_context.get("_line_revenue", 0.0), errors="coerce").fillna(0.0).sum()) if not sales_context.empty else 0.0
    total_orders = int(sales_context["order_id"].nunique()) if not sales_context.empty else 0

    if returns_df.empty:
        return {
            "total_issues": 0,
            "return_count": 0, "total_returned_items": 0,
            "total_returned_items_pct": 0.0,
            "partial_count": 0, "partial_amounts": 0,
            "exchange_count": 0,
            "gross_sales": gross_sales,
            "total_orders": total_orders,
            "return_value_extracted": 0.0,
            "full_return_loss": 0.0,
            "partial_loss": 0.0,
            "total_loss": 0.0,
            "net_sales": gross_sales,
            "net_yield_pct": 100.0 if gross_sales > 0 else 0.0,
            "attribution_confidence_pct": 0.0,
            "attributed_issue_orders": 0,
            "unattributed_issue_orders": 0,
            "matched_returned_items": 0,
            "estimated_returned_items": 0,
            "daily_financials": _build_daily_financials(pd.DataFrame(), sales_context),
            "return_rate": 0.0,
            "returned_orders_pct": 0.0,
            "total_items_sold": total_items_sold,
        }

    # Deduplicate by normalized order_id for counting unique orders
    unique_orders = returns_df.drop_duplicates(subset=["order_id"]).copy()
    unique_orders["_resolved_revenue_impact"] = 0.0
    unique_orders["_impact_source"] = "unattributed"
    unique_orders["_matched_item_qty"] = 0
    unique_orders["_estimated_item_qty"] = 0

    order_totals = (
        sales_context.groupby("order_id")["_line_revenue"].sum().to_dict()
        if not sales_context.empty else {}
    )

    for idx, row in unique_orders.iterrows():
        order_id = str(row.get("order_id", "")).strip()
        issue_type = row.get("issue_type", "Unknown")
        order_sales = sales_context[sales_context["order_id"] == order_id] if not sales_context.empty else pd.DataFrame()
        matched_loss, matched_items, estimated_items = _match_returned_items_to_sales(row, order_sales)
        order_total = float(order_totals.get(order_id, 0.0))
        partial_amount = float(pd.to_numeric(row.get("partial_amount", 0.0), errors="coerce") or 0.0)

        resolved_impact = 0.0
        impact_source = "unattributed"

        if issue_type in ["Paid Return", "Non Paid Return"]:
            if matched_loss > 0:
                resolved_impact = matched_loss
                impact_source = "item_match"
            elif order_total > 0:
                resolved_impact = order_total
                impact_source = "order_fallback"
        elif issue_type == "Partial":
            if partial_amount > 0:
                resolved_impact = partial_amount
                impact_source = "partial_amount"
            elif matched_loss > 0:
                resolved_impact = matched_loss
                impact_source = "item_match"
        elif issue_type == "Exchange":
            impact_source = "exchange"

        unique_orders.at[idx, "_resolved_revenue_impact"] = float(resolved_impact)
        unique_orders.at[idx, "_impact_source"] = impact_source
        unique_orders.at[idx, "_matched_item_qty"] = int(matched_items)
        unique_orders.at[idx, "_estimated_item_qty"] = int(estimated_items)

    # ── Counts ──
    return_mask = unique_orders["issue_type"].isin(["Paid Return", "Non Paid Return"])
    partial_mask = unique_orders["issue_type"] == "Partial"
    exchange_mask = unique_orders["issue_type"] == "Exchange"

    return_count = return_mask.sum()
    partial_count = partial_mask.sum()
    exchange_count = exchange_mask.sum()

    # ── Partial amounts (extracted from text) ──
    partial_amounts = returns_df.loc[
        returns_df["issue_type"] == "Partial", "partial_amount"
    ].sum()

    # ── Sub-type breakdown ──
    paid_return_count = (unique_orders["issue_type"] == "Paid Return").sum()
    non_paid_return_count = (unique_orders["issue_type"] == "Non Paid Return").sum()

    # ── Return reasons ──
    reason_counts = (
        unique_orders[return_mask | partial_mask | exchange_mask]
        ["return_reason"]
        .value_counts()
        .to_dict()
    )

    # ── Monthly breakdown ──
    monthly = (
        unique_orders
        .set_index("date")
        .resample("ME")["order_id"]
        .count()
        .reset_index()
    )
    monthly.columns = ["month", "issue_count"]

    # ── Issue type by month ──
    monthly_by_type = (
        unique_orders
        .groupby([pd.Grouper(key="date", freq="ME"), "issue_type"])
        .size()
        .reset_index(name="count")
    )

    # ── Total Items in Returns ──
    # sum of qty for PAID RETURN items only (excluding Non Paid Return and Partial)
    total_returned_items = 0
    for items in returns_df[returns_df["issue_type"] == "Paid Return"]["returned_items"]:
        if isinstance(items, list):
            for i in items:
                if isinstance(i, dict):
                    total_returned_items += i.get("qty", 1)
                else:
                    total_returned_items += 1
        else:
            total_returned_items += 1

    # ── Total Items in Exchanges ──
    # sum of qty for EXCHANGE items
    total_exchanged_items = 0
    for items in returns_df[returns_df["issue_type"] == "Exchange"]["returned_items"]:
        if isinstance(items, list):
            for i in items:
                if isinstance(i, dict):
                    total_exchanged_items += i.get("qty", 1)
                else:
                    total_exchanged_items += 1
        else:
            total_exchanged_items += 1

    # ── Total Return Qty (ALL types: Paid, Non Paid, Partial) ──
    total_return_qty_all = 0
    for items in returns_df[returns_df["issue_type"].isin(["Paid Return", "Non Paid Return", "Partial"])]["returned_items"]:
        if isinstance(items, list):
            for i in items:
                if isinstance(i, dict):
                    total_return_qty_all += i.get("qty", 1)
                else:
                    total_return_qty_all += 1
        else:
            total_return_qty_all += 1

    # ── Returned Orders Percentage ──
    # Ensure total_orders is a scalar to avoid ambiguous truth value error
    total_orders_scalar = int(total_orders) if hasattr(total_orders, '__int__') else total_orders
    returned_orders_pct = (return_count / total_orders_scalar * 100) if total_orders_scalar > 0 else 0.0

    # ── Calculate Revenue Impact from Cross-Referenced Items ──
    # Sum up revenue impact from enhanced item data
    full_return_loss = float(unique_orders.loc[return_mask, "_resolved_revenue_impact"].sum())
    partial_loss = float(unique_orders.loc[partial_mask, "_resolved_revenue_impact"].sum())
    exchange_revenue_impact = float(unique_orders.loc[exchange_mask, "_resolved_revenue_impact"].sum())
    total_loss = full_return_loss + partial_loss
    financial_issue_orders = int((return_mask | partial_mask).sum())
    attributed_orders = int(unique_orders.loc[return_mask | partial_mask, "_impact_source"].ne("unattributed").sum())
    attribution_confidence_pct = (attributed_orders / financial_issue_orders * 100) if financial_issue_orders > 0 else 0.0
    matched_returned_items = int(unique_orders["_matched_item_qty"].sum())
    estimated_returned_items = int(unique_orders["_estimated_item_qty"].sum())
    daily_financials = _build_daily_financials(unique_orders, sales_context)

    # ── Fix percentage calculations with correct denominators ──
    # returned_orders_pct: % of unique orders that had returns (use already-defined scalar)
    returned_orders_pct_fixed = (return_count / total_orders_scalar * 100) if total_orders_scalar > 0 else 0.0
    
    # total_returned_items_pct: % of total items sold that were returned (vs total_items_sold, not orders)
    total_returned_items_pct = (total_return_qty_all / total_items_sold * 100) if total_items_sold > 0 else 0.0

    metrics = {
        "total_issues": len(unique_orders),
        "return_count": int(return_count),
        "total_returned_items": int(total_returned_items),
        "total_return_qty_all": int(total_return_qty_all),
        "total_returned_items_pct": round(total_returned_items_pct, 1),
        "returned_orders_pct": round(returned_orders_pct_fixed, 1),
        "total_exchanged_items": int(total_exchanged_items),
        "paid_return_count": int(paid_return_count),
        "non_paid_return_count": int(non_paid_return_count),
        "partial_count": int(partial_count),
        "partial_amounts": float(partial_amounts),
        "exchange_count": int(exchange_count),
        "reason_counts": reason_counts,
        "monthly_trend": monthly,
        "monthly_by_type": monthly_by_type,
        "gross_sales": gross_sales,
        "total_orders": total_orders,
        "total_items_sold": total_items_sold,
        "return_value_extracted": round(full_return_loss, 2),
        "full_return_loss": round(full_return_loss, 2),
        "return_revenue_impact": round(full_return_loss, 2),
        "exchange_revenue_impact": round(exchange_revenue_impact, 2),
        "partial_revenue_impact": round(partial_loss, 2),
        "partial_loss": round(partial_loss, 2),
        "total_loss": round(total_loss, 2),
        "net_sales": max(0.0, gross_sales - total_loss),
        "net_yield_pct": round(((gross_sales - total_loss) / gross_sales * 100), 2) if gross_sales > 0 else 0.0,
        "attribution_confidence_pct": round(attribution_confidence_pct, 1),
        "attributed_issue_orders": attributed_orders,
        "unattributed_issue_orders": max(0, financial_issue_orders - attributed_orders),
        "matched_returned_items": matched_returned_items,
        "estimated_returned_items": estimated_returned_items,
        "daily_financials": daily_financials,
    }

    # ── Return rate ──
    if total_orders_scalar and total_orders_scalar > 0:
        metrics["return_rate"] = round(
            (return_count + partial_count + exchange_count) / total_orders_scalar * 100, 2
        )
    else:
        metrics["return_rate"] = 0.0

    return metrics


def get_issue_type_color(issue_type: str) -> str:
    """Return a consistent color for each issue type."""
    colors = {
        "Paid Return": "#ef4444",
        "Non Paid Return": "#f97316",
        "Partial": "#eab308",
        "Exchange": "#8b5cf6",
        "Refund": "#ec4899",
        "Cancel": "#6b7280",
        "Items Lost": "#dc2626",
        "Delivery Issue": "#f59e0b",
        "Delivered": "#10b981",
        "Unknown": "#9ca3af",
    }
    return colors.get(issue_type, "#6b7280")


def track_reordering_customers(returns_df: pd.DataFrame, sales_df: pd.DataFrame) -> pd.DataFrame:
    """Identify customers who ordered again after a return/partial issue.
    
    Returns a dataframe of reordering events.
    """
    if returns_df.empty or sales_df.empty:
        return pd.DataFrame()

    # 1. Map returns to customers using sales_df
    # Ensure ID types match
    returns_local = returns_df.copy()
    returns_local["order_id_str"] = returns_local["order_id"].astype(str)
    
    sales_local = sales_df.copy()
    sales_local["order_id_str"] = sales_local["order_id"].astype(str)
    sales_local["order_date"] = pd.to_datetime(sales_local["order_date"])
    
    # Get unique order-customer mapping
    order_cust = sales_local.drop_duplicates(subset=["order_id_str"])[["order_id_str", "customer_key", "customer_name", "order_date"]]
    
    # Merge returns with customer info
    returned_customers = pd.merge(returns_local, order_cust, on="order_id_str", how="inner")
    
    if returned_customers.empty:
        return pd.DataFrame()

    reorder_events = []
    
    for _, ret in returned_customers.iterrows():
        cust_key = ret["customer_key"]
        return_date = ret["order_date"]
        
        # Find subsequent orders for this customer
        future_orders = order_cust[
            (order_cust["customer_key"] == cust_key) & 
            (order_cust["order_date"] > return_date)
        ].sort_values("order_date")
        
        if not future_orders.empty:
            next_order = future_orders.iloc[0]
            reorder_events.append({
                "Customer": ret["customer_name"],
                "Issue Date": return_date.strftime("%Y-%m-%d"),
                "Issue Order": ret["order_id_raw"],
                "Issue Type": ret["issue_type"],
                "Next Order": next_order["order_id_str"],
                "Next Order Date": next_order["order_date"].strftime("%Y-%m-%d"),
                "Days to Reorder": (next_order["order_date"] - return_date).days
            })
            
    return pd.DataFrame(reorder_events)
