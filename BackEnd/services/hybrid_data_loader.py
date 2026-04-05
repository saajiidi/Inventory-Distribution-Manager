"""Hybrid data loader - combines historical parquet with live Google Sheet data.

This module implements a hybrid system:
1. Historical data (2022-2025): Loaded from local merged data.parquet (fast)
2. Live data (2026): Fetched from Google Sheet CSV (dynamic)
3. Merged on-the-fly using DuckDB for analysis
"""

from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import requests
import streamlit as st

# Configuration
DATA_FILE = Path(__file__).parent.parent.parent / "data" / "data.parquet"
LIVE_SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTOiRkybNzMNvEaLxSFsX0nGIiM07BbNVsBbsX1dG8AmGOmSu8baPrVYL0cOqoYN4tRWUj1UjUbH1Ij/pub?gid=2118542421&single=true&output=csv"


@st.cache_data(ttl=900)  # Shorter cache for live commerce data
def load_woocommerce_live_data(days: int = 30) -> pd.DataFrame:
    """Fetch live data directly from WooCommerce API.
    
    Returns:
        DataFrame with recent WooCommerce orders
    """
    if "woocommerce" not in st.secrets:
        return pd.DataFrame()
        
    try:
        from BackEnd.services.woocommerce_service import WooCommerceService
        wc_service = WooCommerceService()
        
        # Calculate 'after' date
        after = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
        
        with st.spinner(f"Fetching latest {days} days of WooCommerce data..."):
            df = wc_service.fetch_all_historical_orders(after=after, status="any")
            if not df.empty:
                df["_source"] = "woocommerce_live"
                return df
    except Exception as e:
        st.warning(f"Could not load live WooCommerce data: {e}")
    
    return pd.DataFrame()
@st.cache_data(ttl=3600)  # Cache for 1 hour - refresh live data periodically
def load_live_2026_data() -> pd.DataFrame:
    """Load live 2026 data from Google Sheet CSV export.

    Returns:
        DataFrame with 2026 orders from Google Sheet
    """
    try:
        with st.spinner("Syncing live 2026 data from Google Sheet..."):
            response = requests.get(LIVE_SHEET_URL, timeout=60)
            response.raise_for_status()

            # Parse CSV
            df = pd.read_csv(BytesIO(response.content))

            # Add year column
            df["year"] = "2026"
            df["_source"] = "live_gsheet"
            df["_imported_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Validate for missing dates
            validate_missing_dates(df)

            return df

    except Exception as e:
        st.warning(f"Could not load live 2026 data: {e}")
        return pd.DataFrame()


def validate_missing_dates(df: pd.DataFrame):
    """Check for missing dates in the data and show warnings."""
    # Find date column
    date_cols = ["Order Date", "order_date", "Date", "date", "Created At", "Timestamp"]
    date_col = None
    for col in date_cols:
        if col in df.columns:
            date_col = col
            break

    if not date_col:
        return

    # Convert to datetime
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df_valid = df.dropna(subset=[date_col])

    if df_valid.empty:
        return

    # Get date range
    min_date = df_valid[date_col].min()
    max_date = df_valid[date_col].max()

    # Generate expected dates
    expected_dates = pd.date_range(start=min_date, end=max_date, freq="D")
    actual_dates = set(df_valid[date_col].dt.date)

    # Find missing
    missing = [d for d in expected_dates if d.date() not in actual_dates]

    if missing:
        st.warning(f"⚠️ Found {len(missing)} missing dates in Google Sheet data!")

@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_historical_data() -> pd.DataFrame:
    """Load historical data from local parquet files using hive partitioning."""
    data_dir = DATA_FILE.parent # h:/Analysis/Automation-Pivot/data
    
    # Check if we have the monolithic data.parquet OR partitioned folders
    if not any(data_dir.glob("*.parquet")) and not any(data_dir.glob("year=*")):
        return pd.DataFrame()

    try:
        con = duckdb.connect(database=":memory:")
        
        # Enhanced query to read all parquets including partitioned ones
        # Use hive_partitioning=True to automatically extract 'year' from folder names
        query = f"""
            SELECT * 
            FROM read_parquet([
                '{data_dir}/*.parquet',
                '{data_dir}/year=*/*.parquet'
            ], hive_partitioning=True)
            WHERE year < '{datetime.now().year}'
        """
        df = con.execute(query).fetchdf()
        con.close()
        return df
    except Exception as e:
        st.error(f"Error loading historical data: {e}")
        return pd.DataFrame()


def load_hybrid_data(
    start_date: Optional[str] = None, end_date: Optional[str] = None, 
    include_gsheet: bool = True, include_woocommerce: bool = True
) -> pd.DataFrame:
    """Load combined historical + live data (GSheet + WooCommerce)."""
    
    # 1. Sources
    df_hist = load_historical_data()
    df_gsheet = load_live_2026_data() if include_gsheet else pd.DataFrame()
    df_woo = load_woocommerce_live_data() if include_woocommerce else pd.DataFrame()

    # 2. Merge logic
    all_dfs = []
    if not df_hist.empty: all_dfs.append(df_hist)
    if not df_gsheet.empty: all_dfs.append(df_gsheet)
    if not df_woo.empty: all_dfs.append(df_woo)

    if not all_dfs:
        return pd.DataFrame()

    # Use DuckDB for a high-performance UNION
    con = duckdb.connect(database=":memory:")
    
    # Register all available dataframes
    if not df_hist.empty: con.register("df_hist", df_hist)
    else: con.execute("CREATE TABLE df_hist AS SELECT * FROM (SELECT 1 as x) WHERE 1=0")
    
    if not df_gsheet.empty: con.register("df_gsheet", df_gsheet)
    else: con.execute("CREATE TABLE df_gsheet AS SELECT * FROM (SELECT 1 as x) WHERE 1=0")
    
    if not df_woo.empty: con.register("df_woo", df_woo)
    else: con.execute("CREATE TABLE df_woo AS SELECT * FROM (SELECT 1 as x) WHERE 1=0")

    # Find common columns for union
    cols = []
    if not df_hist.empty: cols.append(set(df_hist.columns))
    if not df_gsheet.empty: cols.append(set(df_gsheet.columns))
    if not df_woo.empty: cols.append(set(df_woo.columns))
    
    common_cols_set = set.intersection(*cols) if cols else set()
    common_cols = list(common_cols_set)
    
    if not common_cols:
        # Fallback if union is impossible directly
        return pd.concat(all_dfs, ignore_index=True)

    col_str = ", ".join([f'"{c}"' for c in common_cols])
    
    # Apply date filters in query if possible
    date_filter = ""
    # We look for standard date columns
    date_col = next((c for c in ["Order Date", "order_date", "Date"] if c in common_cols), None)
    
    if start_date and end_date and date_col:
        date_filter = f"WHERE \"{date_col}\" >= '{start_date}' AND \"{date_col}\" <= '{end_date}'"

    query = f"""
        (SELECT {col_str} FROM df_hist)
        UNION ALL
        (SELECT {col_str} FROM df_gsheet)
        UNION ALL
        (SELECT {col_str} FROM df_woo)
        {date_filter}
    """

    try:
        df_merged = con.execute(query).fetchdf()
    except Exception as e:
        st.warning(f"DuckDB Union failed, falling back to concat: {e}")
        df_merged = pd.concat(all_dfs, ignore_index=True)
        if start_date and end_date and date_col:
            df_merged[date_col] = pd.to_datetime(df_merged[date_col], errors="coerce")
            df_merged = df_merged[(df_merged[date_col] >= start_date) & (df_merged[date_col] <= end_date)]

    con.close()
    
    # Final cleanup
    if not df_merged.empty and date_col:
        df_merged[date_col] = pd.to_datetime(df_merged[date_col], errors="coerce")
        df_merged = df_merged.sort_values(date_col, ascending=False)

    return df_merged


def get_data_summary():
    """Get summary of available data sources."""
    summary = {"historical": 0, "live_2026": 0, "total": 0}

    if DATA_FILE.exists():
        df_hist = pd.read_parquet(DATA_FILE)
        summary["historical"] = len(df_hist)

    try:
        response = requests.get(LIVE_SHEET_URL, timeout=30)
        if response.status_code == 200:
            df_live = pd.read_csv(BytesIO(response.content))
            summary["live_2026"] = len(df_live)
    except:
        pass

    summary["total"] = summary["historical"] + summary["live_2026"]

    return summary
