from __future__ import annotations

from typing import Iterable

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
        return pd.DataFrame(columns=list(CANONICAL_ALIASES.keys()))

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
    out["qty"] = pd.to_numeric(out["qty"], errors="coerce").fillna(0)
    out["order_total"] = pd.to_numeric(out["order_total"], errors="coerce").fillna(0)

    for text_col in [
        "order_id",
        "customer_name",
        "phone",
        "email",
        "state",
        "city",
        "item_name",
        "order_status",
        "tracking",
        "shipped_date",
        "payment_method",
        "sku",
        "source",
    ]:
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

    return out



def pick_first_existing(df: pd.DataFrame, *canonical_names: str) -> str:
    for canonical_name in canonical_names:
        col = resolve_column(df, canonical_name)
        if col:
            return col
    return ""
