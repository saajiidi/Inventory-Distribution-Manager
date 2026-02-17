import math
import io
from dataclasses import dataclass
from typing import Dict, Tuple, Optional

import pandas as pd


def normalize_key(val) -> str:
    """Normalize values from Excel/CSV so keys match reliably (e.g., 123.0 -> '123')."""
    if pd.isna(val):
        return ""
    if isinstance(val, (int,)):
        return str(int(val))
    if isinstance(val, (float,)):
        if math.isfinite(val) and float(val).is_integer():
            return str(int(val))
        return str(val).strip()
    s = str(val).strip()
    if s.endswith(".0") and s[:-2].replace(".", "", 1).isdigit():
        s = s[:-2]
    return s


def normalize_size(val) -> str:
    if pd.isna(val) or val == "":
        return "NO_SIZE"
    s = str(val).strip()
    if not s:
        return "NO_SIZE"
    if s.endswith(".0"):
        s = s[:-2]
    # Normalize common "no size" variants (case-insensitive)
    s_cf = s.casefold()
    if s_cf in {"no_size", "no size", "nosize", "no-size"}:
        return "NO_SIZE"
    return s


def item_name_to_title_size(item_name: str) -> Tuple[str, str]:
    """
    Convert product list 'Item Name' into (title, size).
    Expected common format: "Title - Size" (split on last ' - ').
    If size can't be parsed, returns ("<item_name>", "NO_SIZE").
    """
    if item_name is None or (isinstance(item_name, float) and pd.isna(item_name)):
        return "", "NO_SIZE"
    s = normalize_key(item_name)
    if not s:
        return "", "NO_SIZE"

    if " - " in s:
        left, right = s.rsplit(" - ", 1)
        title = left.strip()
        size = normalize_size(right.strip())
        if title and size and size != "NO_SIZE":
            return title, size

    return s.strip(), "NO_SIZE"


def build_title_size_key(title: str, size: str) -> str:
    title_norm = normalize_key(title).strip()
    size_norm = normalize_size(size)
    if not title_norm:
        return ""
    if size_norm and size_norm != "NO_SIZE":
        return f"{title_norm} - {size_norm}".casefold()
    return title_norm.casefold()


def identify_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Auto-identify relevant columns based on headers."""
    cols = [str(c) for c in df.columns]
    cols_map = {c.lower().strip(): c for c in cols}

    size_col = None
    qty_col = None
    title_col = None

    for c_lower, c_orig in cols_map.items():
        if "size" in c_lower and size_col is None:
            size_col = c_orig
        if (("quantity" in c_lower) or ("qty" in c_lower) or ("stock" in c_lower)) and qty_col is None:
            qty_col = c_orig
        # Prefer explicit "item name" over generic "title"
        if ("item name" in c_lower or "product name" in c_lower) and title_col is None:
            title_col = c_orig
        elif "title" in c_lower and title_col is None:
            title_col = c_orig

    if not qty_col and "Quantity" in df.columns:
        qty_col = "Quantity"

    return size_col, qty_col, title_col


def add_title_size_column(df: pd.DataFrame, title_col: str, size_col: Optional[str]) -> pd.DataFrame:
    """Add a 'Title - Size' column to an inventory dataframe."""

    def _joined(r):
        title = normalize_key(r.get(title_col, ""))
        size = "NO_SIZE"
        if size_col and size_col in df.columns:
            size = normalize_size(r.get(size_col, ""))
        if title and size and size != "NO_SIZE":
            return f"{title} - {size}"
        return title

    df = df.copy()
    df["Title - Size"] = df.apply(_joined, axis=1)
    return df


def _read_uploaded(file_obj) -> pd.DataFrame:
    file_obj.seek(0)
    if getattr(file_obj, "name", "").endswith(".csv"):
        return pd.read_csv(file_obj)
    return pd.read_excel(file_obj)


def load_inventory_from_uploads(uploaded_files: Dict[str, object]):
    """
    Build inventory mapping from uploaded inventory files.
    Matching is based only on 'Title - Size' (computed from Title + Size).
    """
    inventory: Dict[str, Dict[str, int]] = {}
    all_locations = list(uploaded_files.keys())
    warnings = []
    enriched_dfs: Dict[str, pd.DataFrame] = {}

    for loc_name, file_obj in uploaded_files.items():
        if file_obj is None:
            continue
        try:
            df = _read_uploaded(file_obj)
            size_col, qty_col, title_col = identify_columns(df)

            if not title_col:
                warnings.append(f"⚠️ {loc_name}: Missing 'Title/Item Name' column. Skipped.")
                continue

            if not qty_col:
                warnings.append(f"⚠️ {loc_name}: Missing 'Quantity' column. Assuming 0 stock.")

            df = add_title_size_column(df, title_col=title_col, size_col=size_col)
            enriched_dfs[loc_name] = df

            for _, row in df.iterrows():
                qty = 0
                if qty_col and qty_col in df.columns:
                    try:
                        val = row[qty_col]
                        if pd.notna(val):
                            if isinstance(val, str):
                                val = val.replace(",", "").strip()
                                if val == "":
                                    val = 0
                            qty = int(float(val))
                    except Exception:
                        qty = 0

                joined = normalize_key(row.get("Title - Size", ""))
                key = joined.casefold() if joined else ""
                if not key:
                    continue
                if key not in inventory:
                    inventory[key] = {loc: 0 for loc in all_locations}
                inventory[key][loc_name] += qty

        except Exception as e:
            warnings.append(f"❌ Error in {loc_name}: {e}")

    return inventory, warnings, enriched_dfs


def add_stock_columns_from_inventory(
    product_df: pd.DataFrame,
    item_name_col: str,
    inventory: Dict[str, Dict[str, int]],
    locations: list[str],
) -> Tuple[pd.DataFrame, int]:
    """
    Add one column per location to product_df by matching Item Name -> Title - Size.
    Returns (output_df, matched_row_count).
    """
    df = product_df.copy()
    matched = set()

    def row_key(r) -> Tuple[str, str, str, str]:
        title, size = item_name_to_title_size(r.get(item_name_col, ""))
        key = build_title_size_key(title, size)
        title_only = build_title_size_key(title, "NO_SIZE") if title else ""
        return title, size, key, title_only

    keys = [row_key(r) for _, r in df.iterrows()]

    for loc in locations:
        vals = []
        for i, (_, r) in enumerate(df.iterrows()):
            _, _, key, title_only = keys[i]
            found = False
            v = 0
            if key and key in inventory:
                v = inventory[key].get(loc, 0)
                found = True
            elif title_only and title_only in inventory:
                v = inventory[title_only].get(loc, 0)
                found = True
            if found:
                matched.add(i)
            vals.append(v)
        df[loc] = vals

    return df, len(matched)

