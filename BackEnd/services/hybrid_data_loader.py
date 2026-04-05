"""Hybrid data loader - combines historical parquet with live Google Sheet and WooCommerce data."""

from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import streamlit as st

from BackEnd.services.woocommerce_service import get_woocommerce_credentials
from BackEnd.utils.sales_schema import ensure_sales_schema
from FrontEnd.utils.error_handler import log_error

DATA_FILE = Path(__file__).parent.parent.parent / "data" / "data.parquet"
LIVE_SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTOiRkybNzMNvEaLxSFsX0nGIiM07BbNVsBbsX1dG8AmGOmSu8baPrVYL0cOqoYN4tRWUj1UjUbH1Ij/pub?gid=2118542421&single=true&output=csv"
LIVE_STREAM_URL = "https://docs.google.com/spreadsheets/d/1QQX4gDIEurTDkiyXcK1SO2-oYNqarhEg1fqRCVHspQw/export?format=csv&gid=2118542421"
COMPARISON_SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTOiRkybNzMNvEaLxSFsX0nGIiM07BbNVsBbsX1dG8AmGOmSu8baPrVYL0cOqoYN4tRWUj1UjUbH1Ij/pub?gid=2136999354&single=true&output=csv"


@st.cache_data(ttl=900)
def load_woocommerce_live_data(
    days: int = 30,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    if not get_woocommerce_credentials():
        return pd.DataFrame()

    try:
        from BackEnd.services.woocommerce_service import WooCommerceService

        wc_service = WooCommerceService()
        after = (
            pd.to_datetime(start_date, errors="coerce").strftime("%Y-%m-%dT00:00:00Z")
            if start_date and pd.notna(pd.to_datetime(start_date, errors="coerce"))
            else (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
        )
        before = (
            pd.to_datetime(end_date, errors="coerce").strftime("%Y-%m-%dT23:59:59Z")
            if end_date and pd.notna(pd.to_datetime(end_date, errors="coerce"))
            else None
        )
        df = wc_service.fetch_all_historical_orders(after=after, before=before, status="any")
        if df.empty:
            return df
        df["_source"] = df.get("_source", "woocommerce_live")
        return ensure_sales_schema(df)
    except Exception as exc:
        log_error(
            exc,
            context="Hybrid Loader - WooCommerce Live",
            details={"days": days, "start_date": start_date, "end_date": end_date},
        )
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


@st.cache_data(ttl=900)
def load_live_stream_data() -> pd.DataFrame:
    """Load exclusive Live Stream data."""
    try:
        response = requests.get(LIVE_STREAM_URL, timeout=60)
        response.raise_for_status()
        df = pd.read_csv(BytesIO(response.content))
        if df.empty:
            return df
        df["_source"] = "live_stream_gsheet"
        df["_imported_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return ensure_sales_schema(df)
    except Exception as exc:
        log_error(exc, context="Hybrid Loader - Live Stream", details={"url": LIVE_STREAM_URL})
        return pd.DataFrame()


@st.cache_data(ttl=900)
def load_comparison_data() -> pd.DataFrame:
    """Load Comparison data for Today vs Last Day analysis."""
    try:
        response = requests.get(COMPARISON_SHEET_URL, timeout=60)
        response.raise_for_status()
        df = pd.read_csv(BytesIO(response.content))
        if df.empty:
            return df
        df["_source"] = "comparison_gsheet"
        return ensure_sales_schema(df)
    except Exception as exc:
        log_error(exc, context="Hybrid Loader - Comparison Sheet", details={"url": COMPARISON_SHEET_URL})
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
    df_woo = (
        load_woocommerce_live_data(start_date=start_date, end_date=end_date)
        if include_woocommerce
        else pd.DataFrame()
    )

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
    df_stream = load_live_stream_data()
    return {
        "historical": len(df_hist),
        "live_2026": len(df_live),
        "woocommerce_live": len(df_woo),
        "live_stream": len(df_stream),
        "total": len(df_hist) + len(df_live) + len(df_woo) + len(df_stream),
    }
