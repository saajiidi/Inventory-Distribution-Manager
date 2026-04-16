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


def load_returns_data(
    url: Optional[str] = None,
    uploaded_file=None,
) -> pd.DataFrame:
    """Load and clean returns/delivery-issue data.

    Args:
        url: Google Sheets published CSV URL (defaults to DEEN sheet).
        uploaded_file: Optional Streamlit UploadedFile for manual upload.

    Returns:
        Cleaned DataFrame with standardized columns.
    """
    try:
        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file)
        else:
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

    # 5. Default
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


def calculate_net_sales_metrics(
    returns_df: pd.DataFrame,
    gross_sales: Optional[float] = None,
    total_orders: Optional[int] = None,
) -> Dict[str, Any]:
    """Calculate comprehensive net sales metrics.

    Args:
        returns_df: Classified returns DataFrame.
        gross_sales: Total gross sales from WooCommerce (if available).
        total_orders: Total order count from WooCommerce (if available).

    Returns:
        Dictionary of computed KPIs.
    """
    if returns_df.empty:
        return {
            "total_issues": 0,
            "return_count": 0, "return_partial_amounts": 0,
            "partial_count": 0, "partial_amounts": 0,
            "exchange_count": 0,
            "refund_count": 0,
            "cancel_count": 0,
            "items_lost_count": 0,
            "delivery_issue_count": 0,
            "gross_sales": gross_sales or 0,
            "total_orders": total_orders or 0,
        }

    # Deduplicate by normalized order_id for counting unique orders
    unique_orders = returns_df.drop_duplicates(subset=["order_id"])

    # ── Counts ──
    return_mask = unique_orders["issue_type"].isin(["Paid Return", "Non Paid Return"])
    partial_mask = unique_orders["issue_type"] == "Partial"
    exchange_mask = unique_orders["issue_type"] == "Exchange"
    refund_mask = unique_orders["issue_type"] == "Refund"
    cancel_mask = unique_orders["issue_type"] == "Cancel"
    lost_mask = unique_orders["issue_type"] == "Items Lost"
    delivery_mask = unique_orders["issue_type"] == "Delivery Issue"

    return_count = return_mask.sum()
    partial_count = partial_mask.sum()
    exchange_count = exchange_mask.sum()
    refund_count = refund_mask.sum()

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

    metrics = {
        "total_issues": len(unique_orders),
        "return_count": int(return_count),
        "paid_return_count": int(paid_return_count),
        "non_paid_return_count": int(non_paid_return_count),
        "partial_count": int(partial_count),
        "partial_amounts": float(partial_amounts),
        "exchange_count": int(exchange_count),
        "refund_count": int(refund_count),
        "cancel_count": int(cancel_mask.sum()),
        "items_lost_count": int(lost_mask.sum()),
        "delivery_issue_count": int(delivery_mask.sum()),
        "reason_counts": reason_counts,
        "monthly_trend": monthly,
        "monthly_by_type": monthly_by_type,
        "gross_sales": gross_sales or 0,
        "total_orders": total_orders or 0,
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
