from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from src.core.paths import CACHE_DIR, DATA_DIR
from src.core.sync import load_shared_gsheet
from src.data.normalized_sales import normalize_sales_dataframe

CORE_WORKBOOK_PATH = DATA_DIR / "TotalOrder_TillLastTime.xlsx"
CORE_PARQUET_SNAPSHOT_PATH = DATA_DIR / "TotalOrder_TillLastTime.parquet"
MASTER_CACHE_FILE = CACHE_DIR / "historical_master.parquet"


def load_master_sales_dataset(force_refresh: bool = False) -> tuple[pd.DataFrame | None, str]:
    base_source_mtime = _get_base_source_mtime()
    if MASTER_CACHE_FILE.exists() and not force_refresh:
        cache_mtime = os.path.getmtime(MASTER_CACHE_FILE)
        if base_source_mtime is None or cache_mtime >= base_source_mtime:
            try:
                cached = pd.read_parquet(MASTER_CACHE_FILE)
                if not cached.empty:
                    return cached, f"Historical cache ready ({len(cached):,} rows)"
            except Exception:
                pass

    try:
        base_df, info = _load_local_base_dataset(force_refresh=force_refresh)
    except Exception as exc:
        if MASTER_CACHE_FILE.exists():
            try:
                cached = pd.read_parquet(MASTER_CACHE_FILE)
                return cached, f"Fallback cache in use ({exc})"
            except Exception:
                pass
        return None, f"Failed to load local historical source: {exc}"

    delta_df = _load_2026_delta(base_df, force_refresh=force_refresh)
    final_df = pd.concat([base_df, delta_df], ignore_index=True, copy=False) if not delta_df.empty else base_df

    try:
        final_df.to_parquet(MASTER_CACHE_FILE, index=False)
    except Exception:
        pass

    msg = (
        f"{info['source_label']} loaded: {info['base_rows']:,} rows"
        f"{_format_sheet_count(info.get('sheet_count'))}"
        f" + {len(delta_df):,} new 2026 rows"
    )
    return final_df, msg


def _get_base_source_mtime() -> float | None:
    workbook_mtime = _safe_getmtime(CORE_WORKBOOK_PATH)
    snapshot_mtime = _safe_getmtime(CORE_PARQUET_SNAPSHOT_PATH)

    if workbook_mtime is None and snapshot_mtime is None:
        return None
    if workbook_mtime is None:
        return snapshot_mtime
    if snapshot_mtime is None:
        return workbook_mtime
    return max(workbook_mtime, snapshot_mtime)


def _safe_getmtime(path: Path) -> float | None:
    if path.exists():
        return os.path.getmtime(path)
    return None


def _load_local_base_dataset(force_refresh: bool = False) -> tuple[pd.DataFrame, dict]:
    snapshot_ready = _snapshot_is_fresh()

    if CORE_PARQUET_SNAPSHOT_PATH.exists() and snapshot_ready and not force_refresh:
        snapshot_df = pd.read_parquet(CORE_PARQUET_SNAPSHOT_PATH)
        return snapshot_df, {
            "base_rows": len(snapshot_df),
            "sheet_count": None,
            "source_label": "Local Parquet snapshot",
        }

    if CORE_WORKBOOK_PATH.exists():
        workbook_df, info = _load_core_workbook()
        _write_snapshot(workbook_df)
        return workbook_df, {
            "base_rows": info["base_rows"],
            "sheet_count": info["sheet_count"],
            "source_label": "Workbook core",
        }

    if CORE_PARQUET_SNAPSHOT_PATH.exists():
        snapshot_df = pd.read_parquet(CORE_PARQUET_SNAPSHOT_PATH)
        return snapshot_df, {
            "base_rows": len(snapshot_df),
            "sheet_count": None,
            "source_label": "Local Parquet snapshot",
        }

    raise FileNotFoundError(
        f"Neither {CORE_WORKBOOK_PATH.name} nor {CORE_PARQUET_SNAPSHOT_PATH.name} is available"
    )


def _snapshot_is_fresh() -> bool:
    if not CORE_PARQUET_SNAPSHOT_PATH.exists():
        return False
    if not CORE_WORKBOOK_PATH.exists():
        return True
    return os.path.getmtime(CORE_PARQUET_SNAPSHOT_PATH) >= os.path.getmtime(CORE_WORKBOOK_PATH)


def _write_snapshot(df: pd.DataFrame) -> None:
    try:
        df.to_parquet(CORE_PARQUET_SNAPSHOT_PATH, index=False)
    except Exception:
        pass


def _format_sheet_count(sheet_count: int | None) -> str:
    if sheet_count is None:
        return ""
    return f" across {sheet_count} tabs"


def _load_core_workbook() -> tuple[pd.DataFrame, dict]:
    xl = pd.ExcelFile(CORE_WORKBOOK_PATH)
    frames = []

    for sheet_name in xl.sheet_names:
        raw_df = xl.parse(sheet_name)
        if raw_df is None or raw_df.empty:
            continue
        master_df = _to_master_schema(raw_df, sheet_name)
        if not master_df.empty:
            frames.append(master_df)

    if not frames:
        return pd.DataFrame(), {"base_rows": 0, "sheet_count": 0}

    master_df = pd.concat(frames, ignore_index=True)
    return master_df, {"base_rows": len(master_df), "sheet_count": len(frames)}


def _load_2026_delta(base_df: pd.DataFrame, force_refresh: bool = False) -> pd.DataFrame:
    try:
        raw_2026, _, _ = load_shared_gsheet("2026", force_refresh=force_refresh)
    except Exception:
        return pd.DataFrame(columns=base_df.columns)

    if raw_2026 is None or raw_2026.empty:
        return pd.DataFrame(columns=base_df.columns)

    new_2026 = _to_master_schema(raw_2026, "2026")
    if new_2026.empty:
        return pd.DataFrame(columns=base_df.columns)

    base_2026 = base_df[base_df["_src_tab"].isin(["2026-tillLastTime", "2026"])].copy()
    existing = set(base_2026["_row_fingerprint"].dropna().astype(str))
    if not existing:
        return new_2026

    delta = new_2026[~new_2026["_row_fingerprint"].astype(str).isin(existing)].copy()
    return delta


def _to_master_schema(raw_df: pd.DataFrame, source_tab: str) -> pd.DataFrame:
    normalized_df, _ = normalize_sales_dataframe(raw_df, source_tab=source_tab)
    if normalized_df.empty:
        return pd.DataFrame()

    master = raw_df.copy()
    master["_src_tab"] = source_tab
    master["_p_name"] = normalized_df["item_name"]
    master["_p_cust_name"] = normalized_df["customer_name"]
    master["_p_cost"] = normalized_df["unit_price"]
    master["_p_qty"] = normalized_df["qty"]
    master["_p_date"] = normalized_df["order_date"]
    master["_p_order"] = normalized_df["order_id"]
    master["_p_phone"] = normalized_df["phone"]
    master["_p_email"] = normalized_df["email"]
    master["_p_state"] = normalized_df["state"]
    master["_p_sku"] = normalized_df["sku"]
    master["_p_order_total"] = normalized_df["order_total"]
    master["_p_status"] = normalized_df["order_status"]
    master["_p_archive_status"] = normalized_df["archive_status"]
    master["_row_fingerprint"] = normalized_df.apply(_fingerprint_row, axis=1)
    return master


def _fingerprint_row(row: pd.Series) -> str:
    parts = [
        row.get("order_id", ""),
        row.get("order_date", ""),
        row.get("customer_name", ""),
        row.get("sku", ""),
        row.get("item_name", ""),
        row.get("qty", 0),
        row.get("unit_price", 0),
        row.get("order_total", 0),
    ]
    cleaned = []
    for part in parts:
        if isinstance(part, pd.Timestamp):
            cleaned.append(part.isoformat())
        else:
            cleaned.append(str(part).strip())
    return "|".join(cleaned)
