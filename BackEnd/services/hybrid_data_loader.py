"""WooCommerce-focused data loader with local cache and background refresh support."""

from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO
import os
from pathlib import Path
import sys
from typing import Optional
from threading import Thread
# Robust Streamlit Context Import
try:
    from streamlit.runtime.scriptrunner import add_script_run_context as add_ctx
except ImportError:
    try:
        from streamlit.runtime.scriptrunner.script_run_context import add_script_run_context as add_ctx
    except ImportError:
        try:
            from streamlit.runtime.scriptrunner_utils.script_run_context import add_script_run_ctx as add_ctx
        except ImportError:
            # Fallback to no-op if all fail
            def add_ctx(thread): return thread

def add_script_run_context(thread):
    """Unified wrapper for Streamlit's internal thread context attachment."""
    return add_ctx(thread)

import pandas as pd
import requests
import streamlit as st

# --- Polars Engine Auto-Detection ---
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

from BackEnd.core.cache_storage import (
    build_cache_target,
    read_json as storage_read_json,
    read_parquet as storage_read_parquet,
    remove_target,
    target_exists,
    write_json as storage_write_json,
    write_parquet as storage_write_parquet,
)
from BackEnd.utils.sales_schema import ensure_sales_schema, dedupe_sales_data
from FrontEnd.utils.config import DATA_SYNC_MODE
from FrontEnd.utils.error_handler import log_error

# Configuration
LOCAL_CACHE_DIR = Path("BackEnd/cache")
STATIC_SNAPSHOT_DIR = Path("FrontEnd/data")
WOO_CACHE_TTL_MINUTES = 360  # 6 hours
STOCK_CACHE_TTL_MINUTES = 20  # 20 minutes
FULL_HISTORY_SYNC_DAYS = 3650  # ~10 years
MAX_HISTORY_DAYS = 3650


def _local_user_slug() -> str:
    """Identify current user for multi-tenant cache separation."""
    try:
        import getpass
        raw = getpass.getuser()
    except Exception:
        raw = "anonymous"
    slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in raw).strip("_")
    return slug or "default_user"


def _user_cache_dir() -> Path:
    path = LOCAL_CACHE_DIR / _local_user_slug()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _cache_file(name: str) -> str | Path:
    return build_cache_target(
        filename=name,
        local_dir=LOCAL_CACHE_DIR,
        local_subdir=_local_user_slug(),
    )


def _read_json(path: str | Path) -> dict:
    return storage_read_json(path)


def _write_json(path: str | Path, payload: dict):
    storage_write_json(path, payload)


def _read_parquet(path: str | Path) -> pd.DataFrame:
    return storage_read_parquet(path)


def _write_parquet(df: pd.DataFrame, path: str | Path, *, index: bool = False):
    storage_write_parquet(df, path, index=index)


def _remove_file(path: str | Path):
    remove_target(path)


def _is_fresh(timestamp: str | None, ttl_minutes: int) -> bool:
    if not timestamp:
        return False
    parsed = pd.to_datetime(timestamp, errors="coerce")
    if pd.isna(parsed):
        return False
    age = datetime.now() - parsed.to_pydatetime()
    return age <= timedelta(minutes=ttl_minutes)


def _normalize_bounds(start_date: Optional[str], end_date: Optional[str], days: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    start_ts = (
        pd.to_datetime(start_date, errors="coerce")
        if start_date
        else pd.Timestamp.now().normalize() - pd.Timedelta(days=days)
    )
    end_ts = pd.to_datetime(end_date, errors="coerce") if end_date else pd.Timestamp.now()
    if pd.isna(start_ts):
        start_ts = pd.Timestamp.now().normalize() - pd.Timedelta(days=days)
    if pd.isna(end_ts):
        end_ts = pd.Timestamp.now()
    
    if end_date and len(str(end_date)) <= 10:  # Date-only format like "2026-04-05"
        end_ts = end_ts.normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    
    return start_ts, end_ts


def _filter_by_date_range(df: pd.DataFrame, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    if df.empty or "order_date" not in df.columns:
        return df
    filtered = df.copy()
    filtered["order_date"] = pd.to_datetime(filtered["order_date"], errors="coerce")
    filtered = filtered[filtered["order_date"].between(start_ts, end_ts, inclusive="both")]
    return filtered.reset_index(drop=True)


def _refresh_lock_path(kind: str) -> Path:
    return _cache_file(f"{kind}_refresh.lock")


def _refresh_status_path(kind: str) -> Path:
    return _cache_file(f"{kind}_refresh_status.json")


def _set_refresh_lock(kind: str, payload: dict):
    _write_json(_refresh_lock_path(kind), payload)


def _clear_refresh_lock(kind: str):
    _remove_file(_refresh_lock_path(kind))


def _mark_refresh_status(kind: str, state: str, **extra):
    payload = {
        "kind": kind,
        "state": state,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    payload.update(extra)
    _write_json(_refresh_status_path(kind), payload)


def _refresh_is_running(kind: str) -> bool:
    lock_path = _refresh_lock_path(kind)
    if not lock_path.exists():
        return False
    lock = _read_json(lock_path)
    started_at = pd.to_datetime(lock.get("started_at"), errors="coerce")
    if pd.isna(started_at):
        return False
    # Auto-expire locks older than 2 hours
    if (datetime.now() - started_at.to_pydatetime()) > timedelta(hours=2):
        _clear_refresh_lock(kind)
        return False
    return True


def get_woocommerce_credentials() -> dict:
    from BackEnd.services.woocommerce_service import get_woocommerce_credentials as get_creds
    return get_creds()


def _cache_range_is_covered(meta: dict, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> bool:
    cached_start = pd.to_datetime(meta.get("range_start"), errors="coerce")
    cached_end = pd.to_datetime(meta.get("range_end"), errors="coerce")
    return (
        not pd.isna(cached_start)
        and not pd.isna(cached_end)
        and cached_start <= start_ts
        and cached_end >= end_ts
    )


def _build_orders_cache_status(start_date: Optional[str], end_date: Optional[str], days: int = 30) -> dict:
    start_ts, end_ts = _normalize_bounds(start_date, end_date, days)
    cache_path = _cache_file("woo_orders.parquet")
    meta = _read_json(_cache_file("woo_orders_meta.json"))
    status_meta = _read_json(_refresh_status_path("orders"))
    cache_exists = target_exists(cache_path)
    is_covered = _cache_range_is_covered(meta, start_ts, end_ts)
    is_fresh = _is_fresh(meta.get("fetched_at"), WOO_CACHE_TTL_MINUTES)
    is_running = _refresh_is_running("orders")

    if cache_exists and is_covered and is_fresh:
        message = "Loaded from local cache. WooCommerce orders are already fresh for this date range."
    elif cache_exists and is_covered and is_running:
        message = "Loaded from local cache. WooCommerce orders are refreshing in the background."
    elif cache_exists and is_covered:
        message = "Loaded from local cache. A background refresh can update this date range without blocking the screen."
    elif cache_exists and is_running:
        message = "Partial local cache is available. WooCommerce is filling the missing date range in the background."
    elif cache_exists:
        message = "Local cache exists for another date span. WooCommerce is preparing this range in the background."
    elif is_running:
        message = "First WooCommerce sync is running in the background. The dashboard can open without waiting."
    else:
        message = "No WooCommerce cache yet. The dashboard can open now and build the cache in the background."

    return {
        "kind": "orders",
        "cache_exists": cache_exists,
        "is_covered": is_covered,
        "is_fresh": is_fresh,
        "is_running": is_running,
        "last_refresh": meta.get("fetched_at") or status_meta.get("updated_at"),
        "status_message": message,
        "needs_refresh": not (cache_exists and is_covered and is_fresh),
    }


def _build_full_history_sync_status(end_date: Optional[str] = None) -> dict:
    meta = _read_json(_cache_file("woo_orders_meta.json"))
    status_meta = _read_json(_refresh_status_path("full_history"))
    cache_exists = target_exists(_cache_file("woo_orders.parquet"))
    is_running = _refresh_is_running("full_history")
    is_complete = bool(meta.get("full_history_complete"))
    last_full_sync = meta.get("last_full_sync_at") or status_meta.get("updated_at")

    if is_complete:
        message = "Lifetime WooCommerce history is available in local cache."
    elif is_running:
        message = "A one-time full WooCommerce history sync is running in the background."
    elif cache_exists:
        message = "Partial WooCommerce history is available. A one-time full sync can complete the lifetime cache."
    else:
        message = "No lifetime WooCommerce history cache exists yet. A one-time full sync will build it."

    return {
        "kind": "full_history",
        "cache_exists": cache_exists,
        "is_running": is_running,
        "is_complete": is_complete,
        "last_full_sync": last_full_sync,
        "status_message": message,
        "needs_sync": not is_complete,
        "end_date": end_date,
    }


def _build_stock_cache_status() -> dict:
    cache_path = _cache_file("woo_stock.parquet")
    meta = _read_json(_cache_file("woo_stock_meta.json"))
    status_meta = _read_json(_refresh_status_path("stock"))
    cache_exists = target_exists(cache_path)
    is_fresh = _is_fresh(meta.get("fetched_at"), STOCK_CACHE_TTL_MINUTES)
    is_running = _refresh_is_running("stock")

    if cache_exists and is_fresh:
        message = "Inventory snapshot loaded from local cache."
    elif cache_exists and is_running:
        message = "Inventory snapshot loaded from local cache while a background refresh updates stock."
    elif cache_exists:
        message = "Inventory snapshot loaded from local cache. A fresher stock sync can run in the background."
    elif is_running:
        message = "Inventory sync is running in the background."
    else:
        message = "No local inventory snapshot yet. A background stock sync can build it."

    return {
        "kind": "stock",
        "cache_exists": cache_exists,
        "is_fresh": is_fresh,
        "is_running": is_running,
        "last_refresh": meta.get("fetched_at") or status_meta.get("updated_at"),
        "status_message": message,
        "needs_refresh": not (cache_exists and is_fresh),
    }


def get_woocommerce_orders_cache_status(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days: int = 30,
) -> dict:
    return _build_orders_cache_status(start_date, end_date, days=days)


def get_woocommerce_stock_cache_status() -> dict:
    return _build_stock_cache_status()


def get_woocommerce_full_history_status(end_date: Optional[str] = None) -> dict:
    return _build_full_history_sync_status(end_date=end_date)


def estimate_woocommerce_load_time(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> str:
    status = get_woocommerce_orders_cache_status(start_date, end_date)
    if status["cache_exists"]:
        return "Estimated load time: under 2 seconds from local cache."
    return "Estimated load time: under 60 seconds for first-time sync."


def load_cached_woocommerce_live_data(
    days: int = 30,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    start_ts, end_ts = _normalize_bounds(start_date, end_date, days)
    cache_path = _cache_file("woo_orders.parquet")
    
    if not target_exists(cache_path):
        return pd.DataFrame()
        
    if POLARS_AVAILABLE:
        try:
            # 🚀 Polars Advantage: Lazy execution scans the disk 10x faster 
            # than Pandas and uses a fraction of the peak memory.
            lf = pl.scan_parquet(str(cache_path))
            df = lf.collect().to_pandas()
            cached_df = ensure_sales_schema(df)
            return _filter_by_date_range(cached_df, start_ts, end_ts)
        except Exception as exc:
            log_error(exc, context="Polars Engine Live Load Fallback")
            
    # Fallback to Pandas
    cached_df = ensure_sales_schema(_read_parquet(cache_path))
    if cached_df.empty:
        return pd.DataFrame()
    return _filter_by_date_range(cached_df, start_ts, end_ts)


def load_cached_woocommerce_stock_data() -> pd.DataFrame:
    cache_path = _cache_file("woo_stock.parquet")
    
    if not target_exists(cache_path):
        return pd.DataFrame()
        
    if POLARS_AVAILABLE and target_exists(cache_path):
        try:
            return pl.read_parquet(str(cache_path)).to_pandas()
        except Exception as exc:
            log_error(exc, context="Polars Engine Stock Load Fallback")
            
    return _read_parquet(_cache_file("woo_stock.parquet"))


def load_cached_woocommerce_customer_count() -> int:
    meta = _read_json(_cache_file("woo_orders_meta.json"))
    return meta.get("total_customer_count", 0)


def load_cached_woocommerce_history() -> pd.DataFrame:
    cache_path = _cache_file("woo_orders.parquet")
    
    if not target_exists(cache_path):
        return pd.DataFrame()
        
    if POLARS_AVAILABLE:
        try:
            df = pl.read_parquet(str(cache_path)).to_pandas()
            return ensure_sales_schema(df)
        except Exception as exc:
            log_error(exc, context="Polars Engine History Load Fallback")
            
    return ensure_sales_schema(_read_parquet(cache_path))


def load_full_woocommerce_history(end_date: Optional[str] = None) -> pd.DataFrame:
    if not get_woocommerce_credentials():
        return ensure_sales_schema(_generate_demo_sales(start_date="2023-01-01", end_date=end_date))

    cached = load_cached_woocommerce_history()
    if cached is None or cached.empty:
        return pd.DataFrame()

    merged = dedupe_sales_data(ensure_sales_schema(cached))
    if end_date:
        end_ts = pd.to_datetime(end_date, errors="coerce")
        if pd.notna(end_ts):
            merged = merged[merged["order_date"] <= end_ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)]
    return merged.sort_values("order_date", ascending=False, na_position="last").reset_index(drop=True)


def _run_refresh_worker_thread(kind: str, start_date: Optional[str] = None, end_date: Optional[str] = None, days: int = 30):
    """Internal thread worker function."""
    try:
        if kind == "orders":
            refresh_woocommerce_orders_cache(
                days=days,
                start_date=start_date,
                end_date=end_date,
            )
        elif kind == "full_history":
            refresh_woocommerce_orders_cache(
                days=days,
                start_date=None,
                end_date=end_date,
                full_sync=True,
            )
        elif kind == "stock":
            refresh_woocommerce_stock_cache()
    except Exception as exc:
        log_error(exc, context=f"Background {kind} Refresh Thread")


def _spawn_refresh_worker(kind: str, start_date: Optional[str] = None, end_date: Optional[str] = None, days: int = 30) -> bool:
    """Spawns a background thread to refresh the cache. Ensuring it has Streamlit context for secrets."""
    try:
        thread = Thread(
            target=_run_refresh_worker_thread,
            args=(kind, start_date, end_date, days),
            daemon=True
        )
        add_script_run_context(thread)
        thread.start()
        return True
    except Exception as exc:
        log_error(exc, context="Background Refresh Thread Spawn")
        return False


def start_orders_background_refresh(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days: int = 30,
    force: bool = False,
) -> bool:
    if not get_woocommerce_credentials():
        return False
    status = get_woocommerce_orders_cache_status(start_date, end_date, days=days)
    if status["is_running"]:
        return False
    if not force and not status["needs_refresh"]:
        return False
    _set_refresh_lock(
        "orders",
        {
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "start_date": start_date,
            "end_date": end_date,
            "days": days,
        },
    )
    _mark_refresh_status("orders", "queued", start_date=start_date, end_date=end_date, days=days)
    try:
        started = _spawn_refresh_worker("orders", start_date=start_date, end_date=end_date, days=days)
        if not started:
            _clear_refresh_lock("orders")
            _mark_refresh_status("orders", "failed", error="Unable to start orders refresh worker.")
        return started
    except Exception as exc:
        _clear_refresh_lock("orders")
        _mark_refresh_status("orders", "failed", error=str(exc))
        log_error(exc, context="Background Orders Refresh Spawn")
        return False


def start_stock_background_refresh(force: bool = False) -> bool:
    if not get_woocommerce_credentials():
        return False
    status = get_woocommerce_stock_cache_status()
    if status["is_running"]:
        return False
    if not force and not status["needs_refresh"]:
        return False
    _set_refresh_lock("stock", {"started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    _mark_refresh_status("stock", "queued")
    try:
        started = _spawn_refresh_worker("stock")
        if not started:
            _clear_refresh_lock("stock")
            _mark_refresh_status("stock", "failed", error="Unable to start stock refresh worker.")
        return started
    except Exception as exc:
        _clear_refresh_lock("stock")
        _mark_refresh_status("stock", "failed", error=str(exc))
        log_error(exc, context="Background Stock Refresh Spawn")
        return False


def start_full_history_background_refresh(
    end_date: Optional[str] = None,
    force: bool = False,
) -> bool:
    if not get_woocommerce_credentials():
        return False
    status = get_woocommerce_full_history_status(end_date=end_date)
    if status["is_running"]:
        return False
    if not force and not status["needs_sync"]:
        return False
    _set_refresh_lock(
        "full_history",
        {
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": end_date,
        },
    )
    _mark_refresh_status("full_history", "queued", end_date=end_date)
    try:
        started = _spawn_refresh_worker(
            "full_history",
            start_date=None,
            end_date=end_date,
            days=FULL_HISTORY_SYNC_DAYS,
        )
        if not started:
            _clear_refresh_lock("full_history")
            _mark_refresh_status("full_history", "failed", error="Unable to start full history refresh worker.")
        return started
    except Exception as exc:
        _clear_refresh_lock("full_history")
        _mark_refresh_status("full_history", "failed", error=str(exc))
        log_error(exc, context="Background Full History Refresh Spawn")
        return False


def refresh_woocommerce_orders_cache(
    days: int = 30,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    full_sync: bool = False,
) -> pd.DataFrame:
    if not get_woocommerce_credentials():
        return pd.DataFrame()

    start_ts, end_ts = _normalize_bounds(start_date, end_date, days)
    cache_path = _cache_file("woo_orders.parquet")
    meta_path = _cache_file("woo_orders_meta.json")
    refresh_kind = "full_history" if full_sync else "orders"
    _set_refresh_lock(
        refresh_kind,
        {
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "start_date": start_date,
            "end_date": end_date,
            "days": days,
        },
    )
    _mark_refresh_status(refresh_kind, "syncing", start_date=start_date, end_date=end_date, days=days)

    try:
        from BackEnd.services.woocommerce_service import WooCommerceService
        wc = WooCommerceService(ui_enabled=False)
        df_new = wc.fetch_orders_range(start_ts, end_ts)
        
        # Load existing cache and merge
        existing = pd.DataFrame()
        if cache_path.exists():
            existing = _read_parquet(cache_path)
            
        merged = dedupe_sales_data(pd.concat([ensure_sales_schema(df_new), ensure_sales_schema(existing)], ignore_index=True))
        _write_parquet(merged, cache_path)
        
        # Update meta
        meta = _read_json(meta_path)
        meta["fetched_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if full_sync:
            meta["full_history_complete"] = True
            meta["last_full_sync_at"] = meta["fetched_at"]
        
        # Update covered range
        current_start = pd.to_datetime(meta.get("range_start"), errors="coerce")
        current_end = pd.to_datetime(meta.get("range_end"), errors="coerce")
        
        meta["range_start"] = min(start_ts, current_start).strftime("%Y-%m-%d %H:%M:%S") if pd.notna(current_start) else start_ts.strftime("%Y-%m-%d %H:%M:%S")
        meta["range_end"] = max(end_ts, current_end).strftime("%Y-%m-%d %H:%M:%S") if pd.notna(current_end) else end_ts.strftime("%Y-%m-%d %H:%M:%S")
        meta["total_customer_count"] = wc.get_registered_customer_count()
        _write_json(meta_path, meta)
        
        _clear_refresh_lock(refresh_kind)
        _mark_refresh_status(refresh_kind, "complete", rows=len(df_new))
        return _filter_by_date_range(merged, start_ts, end_ts)
    except Exception as exc:
        _clear_refresh_lock(refresh_kind)
        _mark_refresh_status(refresh_kind, "failed", error=str(exc))
        log_error(exc, context=f"WooCommerce {refresh_kind} Refresh")
        return pd.DataFrame()


def refresh_woocommerce_stock_cache() -> pd.DataFrame:
    if not get_woocommerce_credentials():
        return pd.DataFrame()

    _set_refresh_lock("stock", {"started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    _mark_refresh_status("stock", "syncing")
    try:
        from BackEnd.services.woocommerce_service import WooCommerceService
        wc = WooCommerceService(ui_enabled=False)
        df_stock = wc.fetch_stock_inventory()
        _write_parquet(df_stock, _cache_file("woo_stock.parquet"))
        
        meta = {"fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        _write_json(_cache_file("woo_stock_meta.json"), meta)
        
        _clear_refresh_lock("stock")
        _mark_refresh_status("stock", "complete", rows=len(df_stock))
        return df_stock
    except Exception as exc:
        _clear_refresh_lock("stock")
        _mark_refresh_status("stock", "failed", error=str(exc))
        log_error(exc, context="WooCommerce Stock Refresh")
        return pd.DataFrame()


def load_woocommerce_live_data(
    days: int = 30,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force: bool = False,
) -> pd.DataFrame:
    """Intelligent live loader: serves from cache if fresh, otherwise triggers sync."""
    status = get_woocommerce_orders_cache_status(start_date, end_date, days=days)
    if not force and not status["needs_refresh"]:
        return load_cached_woocommerce_live_data(start_date=start_date, end_date=end_date, days=days)
    
    # Check if we should use direct (synchronous) or hybrid (background) sync
    if DATA_SYNC_MODE == "direct":
        # Direct fetch: Synchronous blocking call (the "early type" requested by user)
        return refresh_woocommerce_orders_cache(days=days, start_date=start_date, end_date=end_date)
    
    # Hybrid fetch: Trigger background refresh if not running
    start_orders_background_refresh(start_date=start_date, end_date=end_date, days=days, force=force)
    
    # Return what we have (even if stale) to keep UI responsive
    return load_cached_woocommerce_live_data(start_date=start_date, end_date=end_date, days=days)


def load_woocommerce_stock_data() -> pd.DataFrame:
    if not get_woocommerce_credentials():
        return _generate_demo_stock()

    status = get_woocommerce_stock_cache_status()
    if not status["needs_refresh"]:
        return load_cached_woocommerce_stock_data()
    
    start_stock_background_refresh()
    return load_cached_woocommerce_stock_data()


@st.cache_data(ttl=43200)
def load_woocommerce_customer_count() -> int:
    """Fetch total count of registered store customers (12h cache)."""
    if not get_woocommerce_credentials():
        return 5240  # Demo count
        
    try:
        cached_count = load_cached_woocommerce_customer_count()
        if cached_count:
            return int(cached_count)

        from BackEnd.services.woocommerce_service import WooCommerceService
        wc = WooCommerceService(ui_enabled=False)
        return wc.get_registered_customer_count()
    except Exception as exc:
        log_error(exc, context="Hybrid Loader - Customer Count")
        return int(load_cached_woocommerce_customer_count() or 0)


def _generate_demo_sales(start_date=None, end_date=None) -> pd.DataFrame:
    """Generates realistic mock sales data for demo purposes."""
    import numpy as np
    from datetime import datetime, timedelta
    
    end = pd.to_datetime(end_date) if end_date else datetime.now()
    start = pd.to_datetime(start_date) if start_date else end - timedelta(days=30)
    
    days = max(1, (end - start).days)
    num_orders = days * np.random.randint(15, 35)
    
    dates = [start + timedelta(minutes=int(np.random.randint(0, days * 24 * 60))) for _ in range(num_orders)]
    dates.sort()
    
    items = ["Premium T-Shirt", "Slim Fit Jeans", "Leather Wallet", "Cotton Panjabi", "Casual Shirt", "Active Joggers", "Winter Hoodie"]
    skus = ["TS-01", "JN-02", "WL-03", "PJ-04", "SH-05", "JG-06", "HD-07"]
    cats = ["T-Shirt", "Jeans", "Wallet", "Panjabi", "FS Shirt", "Trousers", "Sweatshirt"]
    prices = [500, 1200, 800, 2500, 1500, 950, 1800]
    
    item_indices = np.random.randint(0, len(items), size=num_orders)
    
    df = pd.DataFrame({
        "order_id": np.random.randint(100000, 999999, size=num_orders).astype(str),
        "order_date": dates,
        "item_name": [items[i] for i in item_indices],
        "sku": [skus[i] for i in item_indices],
        "qty": np.random.choice([1, 1, 1, 2, 3], size=num_orders),
        "order_status": np.random.choice(
            ["completed", "processing", "on-hold", "refunded", "cancelled"], 
            num_orders, p=[0.75, 0.10, 0.05, 0.05, 0.05]
        ),
        "customer_key": ["reg_" + str(i) for i in np.random.randint(1, 100, size=num_orders)],
        "city": np.random.choice(["Dhaka", "Chittagong", "Sylhet", "Rajshahi", "Khulna"], num_orders),
        "state": np.random.choice(["Dhaka", "Chittagong", "Sylhet", "Rajshahi", "Khulna"], num_orders),
        "Category": [cats[i] for i in item_indices],
        "customer_name": ["Demo Customer " + str(i) for i in np.random.randint(1, 100, size=num_orders)],
        "phone": ["01711" + str(np.random.randint(100000, 999999)) for _ in range(num_orders)],
        "email": ["demo" + str(i) + "@example.com" for i in np.random.randint(1, 100, size=num_orders)],
    })
    
    df["price"] = [prices[i] for i in item_indices]
    df["item_revenue"] = df["qty"] * df["price"]
    df["line_total"] = df["item_revenue"]
    df["order_total"] = df["item_revenue"] + 60
    
    return df


def _generate_demo_stock() -> pd.DataFrame:
    """Generates realistic mock stock data for demo purposes."""
    return pd.DataFrame({
        "ID": range(1, 8),
        "Name": ["Premium T-Shirt", "Slim Fit Jeans", "Leather Wallet", "Cotton Panjabi", "Casual Shirt", "Active Joggers", "Winter Hoodie"],
        "SKU": ["TS-01", "JN-02", "WL-03", "PJ-04", "SH-05", "JG-06", "HD-07"],
        "Stock Status": ["instock", "instock", "outofstock", "instock", "instock", "instock", "instock"],
        "Stock Quantity": [45, 12, 0, 8, 105, 34, 3],
        "Price": [500.0, 1200.0, 800.0, 2500.0, 1500.0, 950.0, 1800.0],
        "Regular Price": [600.0, 1500.0, 1000.0, 3000.0, 1800.0, 1200.0, 2000.0],
        "Category": ["T-Shirt", "Jeans", "Wallet", "Panjabi", "FS Shirt", "Trousers", "Sweatshirt"],
    })


def load_hybrid_data(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_woocommerce: bool = True,
    woocommerce_mode: str = "live",
    force: bool = False,
) -> pd.DataFrame:
    """Primary data entry point. Orchestrates cache vs live loading."""
    if not include_woocommerce:
        return pd.DataFrame()

    if not get_woocommerce_credentials():
        if "demo_toast_shown" not in st.session_state:
            st.toast("🧪 Running in Demo Mode (No credentials found)", icon="✨")
            st.session_state.demo_toast_shown = True
        return ensure_sales_schema(_generate_demo_sales(start_date, end_date))

    df_woo = (
        load_cached_woocommerce_live_data(start_date=start_date, end_date=end_date)
        if woocommerce_mode == "cache_only"
        else load_woocommerce_live_data(start_date=start_date, end_date=end_date, force=force)
    )
    if df_woo.empty:
        return pd.DataFrame()

    merged = ensure_sales_schema(df_woo)
    merged = dedupe_sales_data(merged)

    if start_date:
        start_ts = pd.to_datetime(start_date, errors="coerce")
        if pd.notna(start_ts):
            merged = merged[merged["order_date"] >= start_ts]
            
    if end_date:
        end_ts = pd.to_datetime(end_date, errors="coerce")
        if pd.notna(end_ts):
            merged = merged[merged["order_date"] <= end_ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)]

    merged = merged.sort_values("order_date", ascending=False, na_position="last")
    merged = merged.reset_index(drop=True)
    return merged


def get_data_summary(woocommerce_mode: str = "live") -> dict:
    df_woo = (
        load_cached_woocommerce_live_data()
        if woocommerce_mode == "cache_only"
        else load_woocommerce_live_data()
    )
    df_stock = load_cached_woocommerce_stock_data()
    return {
        "woocommerce_live": len(df_woo),
        "stock_rows": len(df_stock),
        "total": len(df_woo),
    }
