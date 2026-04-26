from __future__ import annotations

from typing import Iterable
import re
import gc
from BackEnd.core.logging_config import get_logger

import pandas as pd

CANONICAL_ALIASES: dict[str, list[str]] = {
    "order_id": ["order_id", "Order ID", "Order Number", "order number", "order id", "id"],
    "order_date": ["order_date", "Order Date", "Date", "Created At", "date_created", "Created"],
    "customer_name": ["customer_name", "Customer Name", "Full Name (Billing)", "Full Name", "Name", "customer"],
    "phone": ["phone", "Phone", "Phone (Billing)", "billing phone", "Mobile", "Contact"],
    "email": ["email", "Email", "Customer Email", "billing email"],
    "state": ["state", "State", "State Name (Billing)", "City, State, Zip (Billing)", "City", "Customer State"],
    "city": ["city", "City", "City (Billing)", "City, State, Zip (Billing)"],
    "item_name": ["item_name", "Item Name", "Product Name (main)", "Product Name", "Product", "Item"],
    "qty": ["qty", "Qty", "Quantity", "quantity", "Units"],
    "order_total": ["order_total", "Order Total Amount", "Order Total", "total", "Total Amount"],
    "order_status": ["order_status", "Order Status", "Status", "status"],
    "tracking": ["tracking", "Tracking"],
    "shipped_date": ["shipped_date", "Shipped Date"],
    "payment_method": ["payment_method", "Payment Method Title", "Payment Method"],
    "sku": ["sku", "SKU"],
    "source": ["_source", "source"],
    "year": ["year", "Year"],
}

logger = get_logger("sales_schema")



def _first_present(columns: Iterable[str], candidates: list[str]) -> str | None:
    normalized = {str(col).strip().lower(): col for col in columns}
    for candidate in candidates:
        match = normalized.get(candidate.strip().lower())
        if match is not None:
            return match
    return None



def resolve_column(df: pd.DataFrame, canonical_name: str) -> str | None:
    return _first_present(df.columns, CANONICAL_ALIASES.get(canonical_name, [canonical_name]))



def ensure_sales_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Add canonical e-commerce analytics columns without dropping original source columns."""
    if df is None or df.empty:
        cols = list(CANONICAL_ALIASES.keys()) + ["customer_key", "order_item_key", "Category"]
        out = pd.DataFrame(columns=cols)
        # Ensure critical types for empty DF to avoid .dt errors
        out["order_date"] = pd.to_datetime(out["order_date"])
        out["qty"] = pd.to_numeric(out["qty"])
        out["order_total"] = pd.to_numeric(out["order_total"])
        return out

    out = df.copy()

    for canonical_name, aliases in CANONICAL_ALIASES.items():
        if canonical_name in out.columns:
            continue
        source_col = _first_present(out.columns, aliases)
        if source_col is not None:
            out[canonical_name] = out[source_col]
        else:
            out[canonical_name] = pd.NA

    out["order_date"] = pd.to_datetime(out["order_date"], errors="coerce")
    out["shipped_date"] = pd.to_datetime(out["shipped_date"], errors="coerce")
    out["qty"] = pd.to_numeric(out["qty"], errors="coerce").fillna(0)
    out["order_total"] = pd.to_numeric(out["order_total"], errors="coerce").fillna(0)

    # Memory-efficient string cleaning for text columns
    # NOTE: shipped_date is a datetime column, not text - handled separately
    for text_col in [
        "order_id", "customer_name", "phone", "email", "state", "city",
        "item_name", "order_status", "tracking",
        "payment_method", "sku", "source",
    ]:
        if text_col in out.columns:
            # Convert categorical to object first to avoid "Cannot setitem on a Categorical" errors
            if pd.api.types.is_categorical_dtype(out[text_col]):
                out[text_col] = out[text_col].astype(object)
            # fillna("") and astype(str) can be heavy; we use a more direct approach
            out[text_col] = out[text_col].fillna("").astype(str).str.strip()

    if out["year"].isna().all() and out["order_date"].notna().any():
        out["year"] = out["order_date"].dt.year.astype("Int64")

    out["customer_key"] = out["email"].where(out["email"] != "", out["phone"])
    out["customer_key"] = out["customer_key"].fillna("").astype(str).str.strip().str.lower()

    out["order_item_key"] = (
        out["order_id"].astype(str).str.strip().str.lower()
        + "|"
        + out["item_name"].astype(str).str.strip().str.lower()
        + "|"
        + out["qty"].astype(str)
        + "|"
        + out["order_total"].round(2).astype(str)
    )

    from BackEnd.core.categories import apply_category_expert_rules
    out = apply_category_expert_rules(out, name_col="item_name")

    return out



def pick_first_existing(df: pd.DataFrame, *canonical_names: str) -> str:
    for canonical_name in canonical_names:
        col = resolve_column(df, canonical_name)
        if col:
            return col
    return ""


def dedupe_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure order-item level integrity by removing duplicate records."""
    if df.empty or "order_item_key" not in df.columns:
        return df
    return df.drop_duplicates(subset=["order_item_key"], keep="first")


def estimate_line_revenue(df: pd.DataFrame) -> pd.Series:
    """Estimate line revenue using the best available columns.
    
    Memory-safe implementation with chunked processing for large datasets.
    """
    sales_df = ensure_sales_schema(df)
    if sales_df is None or sales_df.empty:
        return pd.Series(dtype="float64")

    try:
        qty = pd.to_numeric(sales_df.get("qty", 0), errors="coerce").fillna(0)

        for col in ["item_revenue", "Item Revenue", "line_total", "Line Total", "total"]:
            if col in sales_df.columns:
                values = pd.to_numeric(sales_df[col], errors="coerce")
                if values.notna().any() and values.sum() > 0:
                    return values.fillna(0.0)

        for col in ["item_cost", "Item Cost", "price", "Price"]:
            if col in sales_df.columns:
                unit_price = pd.to_numeric(sales_df[col], errors="coerce").fillna(0.0)
                if unit_price.sum() > 0:
                    return unit_price * qty

        order_total = pd.to_numeric(sales_df.get("order_total", 0), errors="coerce").fillna(0.0)
        
        if "order_id" in sales_df.columns:
            group_key = sales_df["order_id"]
            if len(sales_df) > 100000:
                order_line_counts = sales_df.groupby("order_id").size()
                line_counts = sales_df["order_id"].map(order_line_counts).replace(0, 1)
                qty_totals = qty.groupby(sales_df["order_id"]).transform("sum").replace(0, 1)
                gc.collect()
                return (order_total * (qty / qty_totals)).fillna(order_total / line_counts).fillna(order_total)
            else:
                qty_totals = qty.groupby(group_key).transform("sum").replace(0, 1)
                line_counts = sales_df.groupby(group_key).cumcount() * 0 + 1
                line_counts = line_counts.groupby(group_key).transform("sum").replace(0, 1)
                return (order_total * (qty / qty_totals)).fillna(order_total / line_counts).fillna(order_total)
        else:
            line_count = len(sales_df)
            return order_total / line_count if line_count > 0 else order_total
            
    except Exception as e:
        logger.error(f"Error estimating line revenue: {e}")
        order_total = pd.to_numeric(sales_df.get("order_total", 0), errors="coerce").fillna(0.0)
        line_count = len(sales_df)
        return order_total / line_count if line_count > 0 else order_total
