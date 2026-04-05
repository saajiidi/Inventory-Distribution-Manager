"""Hybrid data loader - combines historical parquet with live Google Sheet and WooCommerce data."""

from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import streamlit as st

from BackEnd.utils.sales_schema import ensure_sales_schema
from FrontEnd.utils.error_handler import log_error

DATA_FILE = Path(__file__).parent.parent.parent / "data" / "data.parquet"
LIVE_SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTOiRkybNzMNvEaLxSFsX0nGIiM07BbNVsBbsX1dG8AmGOmSu8baPrVYL0cOqoYN4tRWUj1UjUbH1Ij/pub?gid=2118542421&single=true&output=csv"


@st.cache_data(ttl=900)
def load_woocommerce_live_data(days: int = 30) -> pd.DataFrame:
    if "woocommerce" not in st.secrets:
        return pd.DataFrame()

    try:
        from BackEnd.services.woocommerce_service import WooCommerceService

        wc_service = WooCommerceService()
        after = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
        df = wc_service.fetch_all_historical_orders(after=after, status="any")
        if df.empty:
            return df
        df["_source"] = df.get("_source", "woocommerce_live")
        return ensure_sales_schema(df)
    except Exception as exc:
        log_error(exc, context="Hybrid Loader - WooCommerce Live", details={"days": days})
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_live_2026_data() -> pd.DataFrame:
    try:
        response = requests.get(LIVE_SHEET_URL, timeout=60)
        response.raise_for_status()
        df = pd.read_csv(BytesIO(response.content))
        if df.empty:
            return df
        df["year"] = df.get("year", "2026")
        df["_source"] = df.get("_source", "live_gsheet")
        df["_imported_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return ensure_sales_schema(df)
    except Exception as exc:
        log_error(exc, context="Hybrid Loader - Google Sheet Live", details={"url": LIVE_SHEET_URL})
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_historical_data() -> pd.DataFrame:
    data_dir = DATA_FILE.parent
    parquet_files = list(data_dir.glob("*.parquet"))
    partitioned_files = list(data_dir.glob("year=*/*.parquet"))
    if not parquet_files and not partitioned_files:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for path in parquet_files + partitioned_files:
        try:
            frame = pd.read_parquet(path)
            if frame.empty:
                continue
            if "year" not in frame.columns and "year=" in str(path.parent.name):
                try:
                    frame["year"] = int(path.parent.name.replace("year=", ""))
                except ValueError:
                    pass
            frames.append(frame)
        except Exception as exc:
            log_error(exc, context="Hybrid Loader - Historical Parquet", details={"path": str(path)})

    if not frames:
        return pd.DataFrame()

    return ensure_sales_schema(pd.concat(frames, ignore_index=True, sort=False))



def _dedupe_orders(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "order_item_key" not in df.columns:
        return df
    return df.drop_duplicates(subset=["order_item_key", "source"], keep="last")



def load_hybrid_data(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_gsheet: bool = True,
    include_woocommerce: bool = True,
) -> pd.DataFrame:
    df_hist = load_historical_data()
    df_gsheet = load_live_2026_data() if include_gsheet else pd.DataFrame()
    df_woo = load_woocommerce_live_data() if include_woocommerce else pd.DataFrame()

    frames = [df for df in [df_hist, df_gsheet, df_woo] if df is not None and not df.empty]
    if not frames:
        return pd.DataFrame()

    merged = ensure_sales_schema(pd.concat(frames, ignore_index=True, sort=False))
    merged = _dedupe_orders(merged)

    if start_date:
        merged = merged[merged["order_date"] >= pd.to_datetime(start_date, errors="coerce")]
    if end_date:
        merged = merged[merged["order_date"] <= pd.to_datetime(end_date, errors="coerce") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)]

    merged = merged.sort_values("order_date", ascending=False, na_position="last")
    merged = merged.reset_index(drop=True)
    return merged



def get_data_summary() -> dict:
    df_hist = load_historical_data()
    df_live = load_live_2026_data()
    df_woo = load_woocommerce_live_data()
    return {
        "historical": len(df_hist),
        "live_2026": len(df_live),
        "woocommerce_live": len(df_woo),
        "total": len(df_hist) + len(df_live) + len(df_woo),
    }
