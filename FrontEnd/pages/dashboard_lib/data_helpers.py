import pandas as pd
import streamlit as st
from BackEnd.utils.sales_schema import ensure_sales_schema

def apply_global_filters(df: pd.DataFrame, categories: list[str] = None, statuses: list[str] = None) -> pd.DataFrame:
    """Applies global filters with hierarchical matching for categories and strict matching for statuses."""
    if df.empty:
        return df
    
    filtered_df = df.copy()
    
    # 1. Category Filter (Hierarchical)
    if categories and "All" not in categories:
        mask = pd.Series(False, index=filtered_df.index)
        for cat in categories:
            # Hierarchical match (e.g., 'Jeans' matches 'Jeans - Slim Fit')
            mask |= filtered_df["Category"].str.startswith(cat, na=False)
        filtered_df = filtered_df[mask]
        
    # 2. Status Filter
    if statuses and "All" not in statuses:
        filtered_df = filtered_df[filtered_df["order_status"].str.lower().isin([s.lower() for s in statuses])]
        
    return filtered_df

def get_available_filters(df: pd.DataFrame):
    """Returns master category list and statuses for global filter controls.
    
    Uses the centralized master category list to ensure consistent
    hierarchy display regardless of data availability.
    """
    from BackEnd.core.categories import get_master_category_list
    
    # Always return the complete master category list (preserves custom order)
    unique_cats = get_master_category_list()
    
    # Statuses - keep sorted as order doesn't matter for statuses
    unique_statuses = sorted([str(s).title() for s in df["order_status"].dropna().unique()]) if not df.empty else []
    
    return unique_cats, unique_statuses

def prune_dataframe(df: pd.DataFrame, preferred_columns: list[str]) -> pd.DataFrame:
    sales = ensure_sales_schema(df)
    # Ensure all preferred columns exist, fill missing with pd.NA
    for col in preferred_columns:
        if col not in sales.columns:
            sales[col] = pd.NA
    return sales[preferred_columns].copy()

def build_order_level_dataset(df: pd.DataFrame) -> pd.DataFrame:
    sales = ensure_sales_schema(df)
    if sales.empty:
        return pd.DataFrame()

    optional_columns = [col for col in ["order_day", "day_name", "day_num", "hour", "region", "_import_time"] if col in sales.columns]
    
    # 1. Clean order IDs and separate rows
    sales["order_id"] = sales["order_id"].astype(str).str.strip().replace(["", "nan", "None", "NaN"], pd.NA)
    order_rows = sales[sales["order_id"].notna()].copy()
    no_order_id_rows = sales[sales["order_id"].isna()].copy()

    grouped_orders = pd.DataFrame()
    if not order_rows.empty:
        # Pre-sort to ensure 'first' picks the earliest date/status
        order_rows = order_rows.sort_values("order_date", ascending=True)
        
        # Identify non-numeric columns to use 'first' (picking non-null)
        # Note: order_date is already in aggregations with 'min', so not needed here
        meta_cols = ["shipped_date", "customer_key", "customer_name", "order_status", "source", "city", "state"]
        
        # Core aggregations (Vectorized)
        aggregations = {
            "order_date": "min",
            "order_total": "max",
            "qty": "sum",
        }
        for col in optional_columns:
            aggregations[col] = "first"

        # Group numeric totals (Dictionary-based aggregation for stability)
        grouped_orders = order_rows.groupby("order_id", as_index=False).agg(aggregations)
        
        # Group metadata (Picking the first non-null/non-empty value per order)
        # We can optimize this by replacing empty strings with pd.NA first
        meta_df = order_rows[["order_id"] + meta_cols].copy()
        # Only clean string columns (not datetime columns)
        datetime_cols = {"shipped_date"}
        for col in meta_cols:
            if col not in datetime_cols:
                meta_df[col] = meta_df[col].astype(str).str.strip().replace(["", "nan", "None", "NaN"], pd.NA)
        
        meta_grouped = meta_df.groupby("order_id", as_index=False).first().fillna("")
        
        # Merge back
        grouped_orders = grouped_orders.merge(meta_grouped, on="order_id", how="left")

    passthrough_rows = pd.DataFrame()
    if not no_order_id_rows.empty:
        available_cols = ["order_id", "order_date", "order_total", "customer_key", "customer_name", "order_status", "source", "city", "state", "qty"] + optional_columns
        passthrough_rows = no_order_id_rows[[c for c in available_cols if c in no_order_id_rows.columns]].copy()

    frames = [frame for frame in [grouped_orders, passthrough_rows] if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=["order_id", "order_date", "order_total", "customer_key", "customer_name", "order_status", "source", "city", "state", "qty"] + optional_columns)
    
    final_df = pd.concat(frames, ignore_index=True, sort=False)
    # Ensure consistency in common types
    if "order_total" in final_df.columns:
        final_df["order_total"] = pd.to_numeric(final_df["order_total"], errors="coerce").fillna(0.0)
    if "qty" in final_df.columns:
        final_df["qty"] = pd.to_numeric(final_df["qty"], errors="coerce").fillna(0).astype(int)
        
    return final_df

def sum_order_level_revenue(df: pd.DataFrame, order_df: pd.DataFrame = None) -> float:
    orders = order_df if order_df is not None else build_order_level_dataset(df)
    if orders.empty:
        return 0.0
    return float(pd.to_numeric(orders["order_total"], errors="coerce").fillna(0).sum())

def estimate_line_revenue(df: pd.DataFrame) -> pd.Series:
    sales = ensure_sales_schema(df)
    if sales.empty:
        return pd.Series(dtype="float64")
    qty = pd.to_numeric(sales.get("qty", 0), errors="coerce").fillna(0)
    direct_candidates = []
    for col in ["item_revenue", "Item Revenue", "line_total", "Line Total"]:
        if col in sales.columns:
            vals = pd.to_numeric(sales[col], errors="coerce")
            if vals.notna().any() and vals.sum() > 0:
                return vals.fillna(0)
    
    # Fallback to Unit Price * Qty
    for col in ["item_cost", "Item Cost", "price", "Price"]:
        if col in sales.columns:
            unit_price = pd.to_numeric(sales[col], errors="coerce").fillna(0)
            if unit_price.sum() > 0:
                return unit_price * qty
                
    # Pro-rata Distribution of Order Total
    line_counts = sales.groupby("order_id")["order_id"].transform("size").replace(0, 1)
    qty_totals = sales.groupby("order_id")["qty"].transform("sum").replace(0, 1)
    order_total = pd.to_numeric(sales.get("order_total", 0), errors="coerce").fillna(0)
    
    return (order_total * (qty / qty_totals)).fillna(order_total / line_counts).fillna(order_total)
