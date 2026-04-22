"""WooCommerce-focused data loader with local cache and background refresh support."""

from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO
import os
from pathlib import Path
import subprocess
import sys
from typing import Optional

import pandas as pd
import requests
import streamlit as st

from BackEnd.core.cache_storage import (
    build_cache_target,
    read_json as storage_read_json,
    read_parquet as storage_read_parquet,
    remove_target,
    target_exists,
    write_json as storage_write_json,
    write_parquet as storage_write_parquet,
)
from BackEnd.services.woocommerce_service import get_woocommerce_credentials
from BackEnd.utils.sales_schema import ensure_sales_schema
from FrontEnd.utils.error_handler import log_error

DATA_FILE = Path(__file__).parent.parent.parent / "data" / "data.parquet"
LOCAL_CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "local_users"
WOO_CACHE_TTL_MINUTES = 360
STOCK_CACHE_TTL_MINUTES = 20
REFRESH_LOCK_TTL_MINUTES = 90
FULL_HISTORY_SYNC_DAYS = 36500
STATIC_SNAPSHOT_DIR = Path(__file__).parent.parent.parent / "data" / "static_snapshot"
LIVE_STREAM_URL = os.getenv("LIVE_STREAM_URL", "https://example.com/live-stream.csv")
COMPARISON_SHEET_URL = os.getenv("COMPARISON_SHEET_URL", "https://example.com/comparison.csv")


def _local_user_slug() -> str:
    raw = (
        os.getenv("USERNAME")
        or os.getenv("USER")
        or os.getenv("COMPUTERNAME")
        or "default_user"
    )
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
    
    # If end_date was provided as date-only (no time component), set to end of day (23:59:59)
    if end_date and len(str(end_date)) <= 10:  # Date-only format like "2026-04-05"
        end_ts = end_ts.normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    
    # Preserve time if it exists in the original strings, otherwise start_ts is normalized and end_ts is now
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
    lock_payload = _read_json(_refresh_lock_path(kind))
    started_at = lock_payload.get("started_at")
    if _is_fresh(started_at, REFRESH_LOCK_TTL_MINUTES):
        return True
    _clear_refresh_lock(kind)
    return False


def _cache_range_is_covered(meta: dict, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> bool:
    cached_start = pd.to_datetime(meta.get("cached_start"), errors="coerce")
    cached_end = pd.to_datetime(meta.get("cached_end"), errors="coerce")
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
        return "Estimated load time: under 2 seconds from local cache. Fresh WooCommerce sync can continue in the background."
    return "Estimated load time: the screen opens immediately, and the first WooCommerce sync usually finishes in 15-60 seconds."


def _load_csv_stream(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return pd.read_csv(BytesIO(response.content))


@st.cache_data(ttl=900)
def load_live_stream_data() -> pd.DataFrame:
    if not LIVE_STREAM_URL:
        return pd.DataFrame()
    try:
        return ensure_sales_schema(_load_csv_stream(LIVE_STREAM_URL))
    except Exception as exc:
        log_error(exc, context="Hybrid Loader - Live Stream")
        return pd.DataFrame()


@st.cache_data(ttl=900)
def load_comparison_data() -> pd.DataFrame:
    if not COMPARISON_SHEET_URL:
        return pd.DataFrame()
    try:
        return ensure_sales_schema(_load_csv_stream(COMPARISON_SHEET_URL))
    except Exception as exc:
        log_error(exc, context="Hybrid Loader - Comparison Stream")
        return pd.DataFrame()


def load_cached_woocommerce_live_data(
    days: int = 30,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    start_ts, end_ts = _normalize_bounds(start_date, end_date, days)
    cached_df = ensure_sales_schema(_read_parquet(_cache_file("woo_orders.parquet")))
    if cached_df.empty:
        return pd.DataFrame()
    return _filter_by_date_range(cached_df, start_ts, end_ts)


def load_cached_woocommerce_stock_data() -> pd.DataFrame:
    from FrontEnd.utils.config import USE_STATIC_SNAPSHOT
    if USE_STATIC_SNAPSHOT:
        snapshot_file = STATIC_SNAPSHOT_DIR / "stock.parquet"
        if snapshot_file.exists():
            return _read_parquet(snapshot_file)
    return _read_parquet(_cache_file("woo_stock.parquet"))


def load_cached_woocommerce_customer_count() -> int:
    from FrontEnd.utils.config import USE_STATIC_SNAPSHOT
    if USE_STATIC_SNAPSHOT:
        meta = _read_json(STATIC_SNAPSHOT_DIR / "metadata.json")
        return meta.get("customer_count", 0)
    
    meta = _read_json(_cache_file("woo_orders_meta.json"))
    return meta.get("total_customer_count", 0)


def load_static_ml_bundle() -> dict:
    """Loads pre-calculated ML insights from the snapshot."""
    import pickle
    bundle_file = STATIC_SNAPSHOT_DIR / "ml_bundle.pkl"
    if bundle_file.exists():
        try:
            with open(bundle_file, "rb") as f:
                return pickle.load(f)
        except Exception:
            pass
    return {}


def load_cached_woocommerce_history() -> pd.DataFrame:
    return ensure_sales_schema(_read_parquet(_cache_file("woo_orders.parquet")))


def load_full_woocommerce_history(end_date: Optional[str] = None) -> pd.DataFrame:
    cached = load_cached_woocommerce_history()
    if cached is None or cached.empty:
        return pd.DataFrame()

    merged = _dedupe_orders(ensure_sales_schema(cached))
    if end_date:
        end_ts = pd.to_datetime(end_date, errors="coerce")
        if pd.notna(end_ts):
            merged = merged[merged["order_date"] <= end_ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)]
    return merged.sort_values("order_date", ascending=False, na_position="last").reset_index(drop=True)


def _spawn_refresh_worker(kind: str, start_date: Optional[str] = None, end_date: Optional[str] = None, days: int = 30) -> bool:
    worker_path = Path(__file__).with_name("cache_refresh_worker.py")
    if not worker_path.exists():
        return False

    command = [sys.executable, str(worker_path), kind, "--days", str(days)]
    if start_date:
        command.extend(["--start-date", start_date])
    if end_date:
        command.extend(["--end-date", end_date])

    creationflags = (
        getattr(subprocess, "DETACHED_PROCESS", 0)
        | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        | getattr(subprocess, "CREATE_NO_WINDOW", 0)
    )
    subprocess.Popen(
        command,
        cwd=str(Path(__file__).parent.parent.parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creationflags,
    )
    return True


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
            "full_sync": full_sync,
        },
    )
    _mark_refresh_status(refresh_kind, "running", start_date=start_date, end_date=end_date, days=days, full_sync=full_sync)

    try:
        from BackEnd.services.woocommerce_service import WooCommerceService

        cached_df = ensure_sales_schema(_read_parquet(cache_path))
        meta = _read_json(meta_path)
        cached_start = pd.to_datetime(meta.get("cached_start"), errors="coerce")
        cached_end = pd.to_datetime(meta.get("cached_end"), errors="coerce")

        wc_service = WooCommerceService(ui_enabled=False)
        # Use full ISO format to preserve time components
        after = None if full_sync else start_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        before = end_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        fetched_df = wc_service.fetch_all_historical_orders(
            after=after,
            before=before,
            status="any",
            show_progress=False,
            show_errors=False,
        )
        if fetched_df.empty:
            _mark_refresh_status(refresh_kind, "completed", rows=0, fetched_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), full_sync=full_sync)
            return _filter_by_date_range(cached_df, start_ts, end_ts) if not cached_df.empty else fetched_df

        fetched_df["_source"] = fetched_df.get("_source", "woocommerce_live")
        fetched_df = ensure_sales_schema(fetched_df)
        merged_cache = fetched_df if cached_df.empty else ensure_sales_schema(
            pd.concat([cached_df, fetched_df], ignore_index=True, sort=False)
        )
        merged_cache = _dedupe_orders(merged_cache)
        _write_parquet(merged_cache, cache_path, index=False)

        fetched_start = pd.to_datetime(fetched_df["order_date"], errors="coerce").min()
        fetched_start = fetched_start if pd.notna(fetched_start) else start_ts
        new_cached_start = min(fetched_start, cached_start) if not pd.isna(cached_start) else fetched_start
        new_cached_end = max(end_ts, cached_end) if not pd.isna(cached_end) else end_ts
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        meta_payload = {
            "cached_start": new_cached_start.strftime("%Y-%m-%d %H:%M:%S"),
            "cached_end": new_cached_end.strftime("%Y-%m-%d %H:%M:%S"),
            "fetched_at": fetched_at,
            "user": _local_user_slug(),
        }
        if full_sync:
            meta_payload["full_history_complete"] = True
            meta_payload["last_full_sync_at"] = fetched_at
        elif "full_history_complete" in meta:
            meta_payload["full_history_complete"] = meta.get("full_history_complete")
            meta_payload["last_full_sync_at"] = meta.get("last_full_sync_at")
        _write_json(
            meta_path,
            meta_payload,
        )
        _mark_refresh_status(refresh_kind, "completed", rows=int(len(merged_cache)), fetched_at=fetched_at, full_sync=full_sync)
        return _filter_by_date_range(merged_cache, start_ts, end_ts)
    except Exception as exc:
        _mark_refresh_status(refresh_kind, "failed", error=str(exc), full_sync=full_sync)
        log_error(
            exc,
            context="Hybrid Loader - Orders Refresh",
            details={"days": days, "start_date": start_date, "end_date": end_date, "full_sync": full_sync},
        )
        return pd.DataFrame()
    finally:
        _clear_refresh_lock(refresh_kind)


def refresh_woocommerce_stock_cache() -> pd.DataFrame:
    if not get_woocommerce_credentials():
        return pd.DataFrame()

    cache_path = _cache_file("woo_stock.parquet")
    meta_path = _cache_file("woo_stock_meta.json")
    _set_refresh_lock("stock", {"started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    _mark_refresh_status("stock", "running")
    try:
        from BackEnd.services.woocommerce_service import WooCommerceService

        cached_df = _read_parquet(cache_path)
        wc_service = WooCommerceService(ui_enabled=False)
        df = wc_service.get_stock_report(show_errors=False)
        if df.empty:
            _mark_refresh_status("stock", "completed", rows=0, fetched_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            return cached_df if not cached_df.empty else df

        stock_df = df.copy()
        if "Stock Quantity" in stock_df.columns:
            stock_df["Stock Quantity"] = pd.to_numeric(stock_df["Stock Quantity"], errors="coerce").fillna(0)
        if "Price" in stock_df.columns:
            stock_df["Price"] = pd.to_numeric(stock_df["Price"], errors="coerce").fillna(0.0)
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        stock_df["_source"] = "woocommerce_stock_api"
        stock_df["_imported_at"] = fetched_at
        _write_parquet(stock_df, cache_path, index=False)
        _write_json(
            meta_path,
            {
                "fetched_at": fetched_at,
                "user": _local_user_slug(),
                "rows": int(len(stock_df)),
            },
        )
        _mark_refresh_status("stock", "completed", rows=int(len(stock_df)), fetched_at=fetched_at)
        return stock_df
    except Exception as exc:
        _mark_refresh_status("stock", "failed", error=str(exc))
        log_error(exc, context="Hybrid Loader - Stock Refresh")
        return pd.DataFrame()
    finally:
        _clear_refresh_lock("stock")


@st.cache_data(ttl=900)
def load_woocommerce_live_data(
    days: int = 30,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    try:
        status = get_woocommerce_orders_cache_status(start_date, end_date, days=days)
        if status["cache_exists"] and status["is_covered"] and status["is_fresh"]:
            return load_cached_woocommerce_live_data(days=days, start_date=start_date, end_date=end_date)
        refreshed = refresh_woocommerce_orders_cache(days=days, start_date=start_date, end_date=end_date)
        if not refreshed.empty:
            return refreshed
        return load_cached_woocommerce_live_data(days=days, start_date=start_date, end_date=end_date)
    except Exception as exc:
        log_error(
            exc,
            context="Hybrid Loader - WooCommerce Live",
            details={"days": days, "start_date": start_date, "end_date": end_date},
        )
        return pd.DataFrame()


@st.cache_data(ttl=900)
def load_woocommerce_stock_data() -> pd.DataFrame:
    """Fetch live stock directly from the WooCommerce REST API."""
    try:
        status = get_woocommerce_stock_cache_status()
        if status["cache_exists"] and status["is_fresh"]:
            return load_cached_woocommerce_stock_data()
        refreshed = refresh_woocommerce_stock_cache()
        if not refreshed.empty:
            return refreshed
        return load_cached_woocommerce_stock_data()
    except Exception as exc:
        log_error(exc, context="Hybrid Loader - WooCommerce Stock")
        return pd.DataFrame()


@st.cache_data(ttl=43200)
def load_woocommerce_customer_count() -> int:
    """Fetch total count of registered store customers (12h cache)."""
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
    include_woocommerce: bool = True,
    woocommerce_mode: str = "live",
    use_snapshot: bool = False,
) -> pd.DataFrame:
    from FrontEnd.utils.config import USE_STATIC_SNAPSHOT
    force_off = os.getenv("USE_STATIC_SNAPSHOT_FORCE_OFF", "False") == "True"
    
    if (USE_STATIC_SNAPSHOT or use_snapshot) and not force_off:
        snapshot_file = STATIC_SNAPSHOT_DIR / "sales.parquet"
        if snapshot_file.exists():
            merged = ensure_sales_schema(_read_parquet(snapshot_file))
        else:
            return pd.DataFrame()
    else:
        if not include_woocommerce:
            return pd.DataFrame()

        df_woo = (
            load_cached_woocommerce_live_data(start_date=start_date, end_date=end_date)
            if woocommerce_mode == "cache_only"
            else load_woocommerce_live_data(start_date=start_date, end_date=end_date)
        )
        if df_woo.empty:
            return pd.DataFrame()

        merged = ensure_sales_schema(df_woo)
        merged = _dedupe_orders(merged)

    if start_date:
        start_ts = pd.to_datetime(start_date, errors="coerce")
        if pd.notna(start_ts):
            merged = merged[merged["order_date"] >= start_ts]
            
    if end_date:
        end_ts = pd.to_datetime(end_date, errors="coerce")
        if pd.notna(end_ts):
            # End of day buffer
            merged = merged[merged["order_date"] <= end_ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)]

    merged = merged.sort_values("order_date", ascending=False, na_position="last")
    merged = merged.reset_index(drop=True)
    return merged


def get_data_summary(woocommerce_mode: str = "live") -> dict:
    from FrontEnd.utils.config import USE_STATIC_SNAPSHOT
    if USE_STATIC_SNAPSHOT:
        df_woo = load_hybrid_data()
        df_stock = load_cached_woocommerce_stock_data()
        return {
            "woocommerce_live": len(df_woo),
            "stock_rows": len(df_stock),
            "total": len(df_woo),
        }

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
