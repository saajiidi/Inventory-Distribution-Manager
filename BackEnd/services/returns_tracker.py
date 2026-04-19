"""Returns & Net Sales Tracker - Backend Service.

Syncs delivery-issue data from Google Sheets, classifies orders as
Return / Partial / Exchange / Refund, and calculates Net Sales metrics.

Data valid from August 2025 onwards.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

import pandas as pd
import streamlit as st

from BackEnd.core.logging_config import get_logger

logger = get_logger("returns_tracker")

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

@st.cache_data(show_spinner=False, max_entries=2)
def load_returns_data(
    url: Optional[str] = None,
    sync_window: str = "",
) -> pd.DataFrame:
    """Load and clean returns/delivery-issue data.

    Args:
        url: Google Sheets published CSV URL (defaults to DEEN sheet).
        uploaded_file: Optional Streamlit UploadedFile for manual upload.

    Returns:
        Cleaned DataFrame with standardized columns.
    """
    try:
        source = url or DEFAULT_SHEET_URL
        df = pd.read_csv(source)

        logger.info(f"Loaded {len(df)} rows from returns data source")
    except Exception as e:
        logger.error(f"Failed to load returns data: {e}")
        return pd.DataFrame()

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

    # ── Extract partial amount (if embedded in product_details) ──
    df["partial_amount"] = df["product_details"].apply(_extract_partial_amount)

    # ── Normalize & Extract Returned Products ──
    # We'll do this later when we have sales_df for SKU mapping,
    # or just extract names for now.
    df["returned_items"] = df["product_details"].apply(_normalize_product_names)

    # ── Keep ONLY Paid Return, Non Paid Return, Partial, Exchange ──
    allowed_types = ["Paid Return", "Non Paid Return", "Partial", "Exchange"]
    df = df[df["issue_type"].isin(allowed_types)].copy()

    # ── Drop rows with no valid date ──
    df = df.dropna(subset=["date"])

    logger.info(
        f"Processed {len(df)} entries: "
        f"{df['is_return'].sum()} returns, "
        f"{df['is_partial'].sum()} partials, "
        f"{df['is_exchange'].sum()} exchanges"
    )

    return df


def _normalize_order_id(raw_id: str) -> str:
    """Remove w/c/s suffixes and D- prefix markers for grouping.

    Examples:
        '12345w' → '12345'
        '12345c' → '12345'
        'D-12345' → '12345'  (but flagged as exchange)
    """
    clean = str(raw_id).strip()
    # Remove trailing w, c, s (case-insensitive)
    clean = re.sub(r'[wcsWCS]$', '', clean)
    # Remove D- prefix (exchange marker) for grouping
    clean = re.sub(r'^D-', '', clean, flags=re.IGNORECASE)
    return clean


def _classify_issue_type(row: pd.Series) -> str:
    """Classify the row into a canonical issue type.

    Priority: exact match on delivery_issue column → D- prefix → fuzzy.
    """
    di = str(row.get("delivery_issue", "")).strip().lower()
    raw_id = str(row.get("order_id_raw", ""))
    product = str(row.get("product_details", "")).lower()
    courier_reason = str(row.get("courier_reason", "")).lower()

    # 1. Exact match on delivery_issue column
    type_map = {
        "paid return": "Paid Return",
        "non paid return": "Non Paid Return",
        "partial": "Partial",
        "exchange": "Exchange",
        "refund": "Refund",
        "cancel": "Cancel",
        "delivered": "Delivered",
        "items lost": "Items Lost",
        "delivery issue": "Delivery Issue",
        "reverse": "Exchange",  # Reverse = exchange variant
        "reverse ": "Exchange",
    }
    for key, label in type_map.items():
        if di == key or di.startswith(key):
            return label

    # 2. D- prefix on order ID → exchange
    if raw_id.upper().startswith("D-"):
        return "Exchange"

    # 3. Fuzzy match on exchange keywords
    combined = f"{di} {product} {courier_reason}"
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

    # 6. Default
    if di:
        return di.title()
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


def _normalize_product_names(details: str) -> list[dict[str, Any]]:
    """Granular extraction of product details (Name, Size, Qty, Category)."""
    if not details or details.lower() == "nan":
        return []

    # 1. Clean prefix
    clean = re.sub(r'^\s*(\d+)\s*(?:tk|=)[^:]*:?', '', details, flags=re.IGNORECASE).strip()
    clean = re.sub(r'^(?:Get Return|Return|Exchange|Partial|Issue)\s*:?\s*', '', clean, flags=re.IGNORECASE).strip()

    if not clean:
        return []

    # 2. Split items
    raw_items = re.split(r'[,+;]', clean)
    processed = []

    for item in raw_items:
        item = item.strip()
        if not item: continue

        # --- Extract Qty (e.g., x2, *2, 2pcs) ---
        qty = 1
        qty_match = re.search(r'\s*[x*]\s*(\d+)\s*$', item, re.IGNORECASE)
        if qty_match:
            qty = int(qty_match.group(1))
            item = item[:qty_match.start()].strip()
        
        # --- Extract Size (e.g., (32), (L), [XL], size 34) ---
        size = "N/A"
        size_match = re.search(r'[\(\[\{](.*?)[\)\]\}]', item)
        if size_match:
            size = size_match.group(1).strip()
            item = item[:size_match.start()].strip()
        else:
            # Try finding size without brackets at the end
            size_match_alt = re.search(r'\s+([SLM]|[XL]{1,2}|3\d|4\d|2\d)\s*$', item, re.IGNORECASE)
            if size_match_alt:
                size = size_match_alt.group(1).strip()
                item = item[:size_match_alt.start()].strip()

        # --- Clean Name ---
        name = item.strip()
        
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
                "qty": qty,
                "category": category
            })
            
    return processed


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


def calculate_net_sales_metrics(
    returns_df: pd.DataFrame,
    sales_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """Calculate comprehensive net sales metrics.

    Args:
        returns_df: Classified returns DataFrame.
        sales_df: WooCommerce active sales DataFrame to calculate precise values.

    Returns:
        Dictionary of computed KPIs.
    """
    gross_sales = 0.0
    total_orders = 0
    return_value = 0.0

    if sales_df is not None and not sales_df.empty:
        if "item_revenue" in sales_df.columns:
            gross_sales = pd.to_numeric(sales_df["item_revenue"], errors="coerce").sum()
        if "order_id" in sales_df.columns:
            total_orders = sales_df["order_id"].nunique()

    if returns_df.empty:
        return {
            "total_issues": 0,
            "return_count": 0, "total_returned_items": 0,
            "partial_count": 0, "partial_amounts": 0,
            "exchange_count": 0,
            "gross_sales": gross_sales,
            "total_orders": total_orders,
            "return_value_extracted": 0.0,
            "net_sales": gross_sales, # If no returns, net = gross
            "return_rate": 0.0,
        }

    if sales_df is not None and not sales_df.empty:
        if "order_id" in sales_df.columns:
            # Map returns to WooCommerce to extract full return value
            return_orders = returns_df[returns_df["issue_type"].isin(["Paid Return", "Non Paid Return"])]["order_id"].unique()
            sales_unique = sales_df.drop_duplicates(subset=["order_id"]).copy()
            sales_unique["str_id"] = sales_unique["order_id"].astype(str)
            
            matched = sales_unique[sales_unique["str_id"].isin(return_orders)]
            if "order_total" in matched.columns:
                return_value = pd.to_numeric(matched["order_total"], errors="coerce").sum()
            elif "item_revenue" in sales_df.columns:
                matched_items = sales_df[sales_df["order_id"].astype(str).isin(return_orders)]
                return_value = pd.to_numeric(matched_items["item_revenue"], errors="coerce").sum()

    # Deduplicate by normalized order_id for counting unique orders
    unique_orders = returns_df.drop_duplicates(subset=["order_id"])

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
    # sum of qty for all returned items
    total_returned_items = 0
    for items in returns_df[returns_df["issue_type"].isin(["Paid Return", "Non Paid Return"])]["returned_items"]:
        if isinstance(items, list):
            for i in items:
                if isinstance(i, dict):
                    total_returned_items += i.get("qty", 1)
                else:
                    total_returned_items += 1
        else:
            total_returned_items += 1

    metrics = {
        "total_issues": len(unique_orders),
        "return_count": int(return_count),
        "total_returned_items": int(total_returned_items),
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
        "return_value_extracted": return_value,
        "net_sales": max(0.0, gross_sales - return_value - partial_amounts),
    }

    # ── Return rate ──
    if total_orders and total_orders > 0:
        metrics["return_rate"] = round(
            (return_count + partial_count + exchange_count) / total_orders * 100, 2
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
