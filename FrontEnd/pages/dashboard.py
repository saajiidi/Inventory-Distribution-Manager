"""Main retail dashboard powered by the normalized WooCommerce sales schema."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from BackEnd.services.customer_insights import generate_customer_insights_from_sales
from BackEnd.services.hybrid_data_loader import (
    estimate_woocommerce_load_time,
    get_woocommerce_full_history_status,
    get_woocommerce_orders_cache_status,
    get_woocommerce_stock_cache_status,
    load_full_woocommerce_history,
    load_hybrid_data,
    load_cached_woocommerce_stock_data,
    start_full_history_background_refresh,
    start_orders_background_refresh,
    start_stock_background_refresh,
)
from BackEnd.services.ml_insights import build_ml_insight_bundle, detect_sales_anomalies, generate_demand_forecast
from BackEnd.utils.sales_schema import ensure_sales_schema
from FrontEnd.components import ui
from FrontEnd.utils.error_handler import log_error


DASHBOARD_SALES_COLUMNS = [
    "order_id",
    "order_date",
    "order_total",
    "customer_key",
    "customer_name",
    "order_status",
    "source",
    "city",
    "state",
    "qty",
    "item_name",
    "email",
    "phone",
    "item_revenue",
    "line_total",
    "item_cost",
    "price",
]

FULL_HISTORY_COLUMNS = [
    "order_id",
    "order_date",
    "customer_name",
    "phone",
    "email",
    "item_name",
    "order_total",
]


def _prune_dataframe(df: pd.DataFrame, preferred_columns: list[str]) -> pd.DataFrame:
    sales = ensure_sales_schema(df)
    if sales.empty:
        return sales

    available = [col for col in preferred_columns if col in sales.columns]
    if not available:
        return sales
    return sales.loc[:, available].copy()


def _build_dashboard_ml_bundle(df_woo_only: pd.DataFrame, df_customers: pd.DataFrame) -> dict[str, pd.DataFrame]:
    try:
        return build_ml_insight_bundle(df_woo_only, df_customers, horizon_days=7)
    except MemoryError as exc:
        log_error(exc, context="Dashboard ML Bundle", details={"mode": "fallback"})
        return {
            "forecast": generate_demand_forecast(df_woo_only, horizon_days=7),
            "customer_risk": pd.DataFrame(),
            "anomalies": detect_sales_anomalies(df_woo_only),
        }


def _build_dashboard_customer_insights(
    df_woo_only: pd.DataFrame,
    full_woo_history: pd.DataFrame,
) -> pd.DataFrame:
    try:
        return generate_customer_insights_from_sales(
            df_woo_only,
            full_history_df=full_woo_history,
            include_rfm=True,
            include_favorites=True,
        )
    except MemoryError as exc:
        log_error(exc, context="Dashboard Customer Insights", details={"mode": "fallback_lightweight"})
        return generate_customer_insights_from_sales(
            df_woo_only,
            full_history_df=full_woo_history,
            include_rfm=False,
            include_favorites=False,
        )


def _render_dashboard_story(df_sales: pd.DataFrame, df_customers: pd.DataFrame, ml_bundle: dict):
    """Translates raw data into a human-readable narrative story."""
    if df_sales.empty:
        return
    
    # Simple metrics
    total_revenue = _sum_order_level_revenue(df_sales)
    order_df = _build_order_level_dataset(df_sales)
    total_orders = order_df["order_id"].nunique()
    aov = total_revenue / total_orders if total_orders else 0
    
    # 7-day trend
    sales_7d = df_sales[df_sales["order_date"] >= (pd.Timestamp.now() - pd.Timedelta(days=7))]
    rev_7d = _sum_order_level_revenue(sales_7d)
    
    # Narrative creation
    narrative = []
    
    # 1. Broad Trend
    if rev_7d > 0:
        avg_daily = rev_7d / 7
        narrative.append(f"In the last 7 days, your store has generated <b>TK {rev_7d:,.0f}</b> in revenue, averaging <b>TK {avg_daily:,.0f}</b> per day.")
    
    # 2. Customer Activity
    if not df_customers.empty and "segment" in df_customers.columns:
        vips = len(df_customers[df_customers["segment"] == "VIP"])
        if vips > 0:
            narrative.append(f"Your <b>{vips} VIP customers</b> continue to represent the most stable growth lever in this window.")
            
    # 3. Forecast insights
    forecast = ml_bundle.get("forecast", pd.DataFrame())
    if not forecast.empty and "forecast_7d_revenue" in forecast.columns:
        next_week_rev = forecast["forecast_7d_revenue"].sum()
        narrative.append(f"The ML engine predicts a rolling 7-day revenue outlook of <b>TK {next_week_rev:,.0f}</b> based on current trajectories.")

    # 4. Anomalies
    anomalies = ml_bundle.get("anomalies", pd.DataFrame())
    if not anomalies.empty:
        spike_count = len(anomalies)
        if spike_count > 0:
            narrative.append(f"Detected <b>{spike_count} unexpected traffic/sales spikes</b> which should be cross-referenced with your marketing schedule.")

    st.markdown(
        f"""
        <div class="bi-commentary">
            <div class="bi-commentary-label">Operational Storytelling</div>
            <div class="bi-audit-body">
                {'<br><br>'.join(narrative)}
            </div>
            <div class="bi-kpi-note" style="margin-top:1.2rem; background:rgba(79, 70, 229, 0.05); border:1px dashed rgba(79, 70, 229, 0.2);">
                💡 Tip: Revenue is counted using order-level totals to ensure 100% accuracy in multi-item checkouts.
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

def _build_order_level_dataset(df: pd.DataFrame) -> pd.DataFrame:
    sales = ensure_sales_schema(df)
    if sales.empty:
        return pd.DataFrame()

    optional_columns = [col for col in ["order_day", "day_name", "day_num", "hour", "region", "_import_time"] if col in sales.columns]
    order_rows = sales[sales["order_id"].replace("", pd.NA).notna()].copy()
    no_order_id_rows = sales[sales["order_id"].replace("", pd.NA).isna()].copy()

    grouped_orders = pd.DataFrame()
    if not order_rows.empty:
        aggregations = {
            "order_date": ("order_date", "min"),
            "order_total": ("order_total", "max"),
            "customer_key": ("customer_key", lambda s: next((v for v in s if str(v).strip()), "")),
            "customer_name": ("customer_name", lambda s: next((v for v in s if str(v).strip()), "")),
            "order_status": ("order_status", lambda s: next((v for v in s if str(v).strip()), "")),
            "source": ("source", lambda s: ", ".join(sorted({str(v) for v in s if str(v).strip()}))),
            "city": ("city", lambda s: next((v for v in s if str(v).strip()), "")),
            "state": ("state", lambda s: next((v for v in s if str(v).strip()), "")),
            "qty": ("qty", "sum"),
        }
        for col in optional_columns:
            aggregations[col] = (col, "first")
        grouped_orders = order_rows.sort_values("order_date").groupby("order_id", as_index=False).agg(**aggregations)

    passthrough_rows = pd.DataFrame()
    if not no_order_id_rows.empty:
        passthrough_rows = no_order_id_rows[
            ["order_id", "order_date", "order_total", "customer_key", "customer_name", "order_status", "source", "city", "state", "qty"] + optional_columns
        ].copy()

    frames = [frame for frame in [grouped_orders, passthrough_rows] if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=["order_id", "order_date", "order_total", "customer_key", "customer_name", "order_status", "source", "city", "state", "qty"] + optional_columns)
    return pd.concat(frames, ignore_index=True, sort=False)


def _sum_order_level_revenue(df: pd.DataFrame) -> float:
    orders = _build_order_level_dataset(df)
    if orders.empty:
        return 0.0
    return float(pd.to_numeric(orders["order_total"], errors="coerce").fillna(0).sum())


def _estimate_line_revenue(df: pd.DataFrame) -> pd.Series:
    sales = ensure_sales_schema(df)
    if sales.empty:
        return pd.Series(dtype="float64")

    qty = pd.to_numeric(sales.get("qty", 0), errors="coerce").fillna(0)
    direct_candidates = []
    for col in ["item_revenue", "Item Revenue", "line_total", "Line Total"]:
        if col in sales.columns:
            direct_candidates.append(pd.to_numeric(sales[col], errors="coerce"))
    if direct_candidates:
        direct = direct_candidates[0].fillna(0)
        return direct

    for col in ["item_cost", "Item Cost", "price", "Price"]:
        if col in sales.columns:
            unit_price = pd.to_numeric(sales[col], errors="coerce").fillna(0)
            if unit_price.gt(0).any():
                return unit_price * qty

    line_counts = sales.groupby("order_id")["order_id"].transform("size").replace(0, pd.NA)
    qty_totals = sales.groupby("order_id")["qty"].transform("sum").replace(0, pd.NA)
    order_total = pd.to_numeric(sales.get("order_total", 0), errors="coerce").fillna(0)
    allocated_by_qty = order_total * (qty / qty_totals)
    allocated_by_lines = order_total / line_counts
    return allocated_by_qty.fillna(allocated_by_lines).fillna(order_total)


def _render_section_date_context(df: pd.DataFrame, label: str):
    sales = ensure_sales_schema(df)
    valid_dates = pd.to_datetime(sales.get("order_date"), errors="coerce")
    if valid_dates is None or valid_dates.empty or not valid_dates.notna().any():
        st.caption(f"{label}: dates are not available in the current result.")
        return
    st.caption(
        f"{label}: {valid_dates.min().strftime('%Y-%m-%d %H:%M')} to {valid_dates.max().strftime('%Y-%m-%d %H:%M')}"
    )


def _is_newer_timestamp(current_value, snapshot_value) -> bool:
    current_ts = pd.to_datetime(current_value, errors="coerce")
    snapshot_ts = pd.to_datetime(snapshot_value, errors="coerce")
    if pd.isna(current_ts) or pd.isna(snapshot_ts):
        return False
    return current_ts > snapshot_ts


def render_dashboard_tab():
    # Inject Responsive CSS
    st.markdown("""
    <style>
    /* Global layout adjustments for mobile */
    @media (max-width: 900px) {
        .main .block-container {
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }
        /* Mobile-friendly metrics */
        div[data-testid="stMetricValue"] {
            font-size: 1.5rem !important;
        }
        div[data-testid="stMetric"] {
            margin-bottom: 0px !important;
        }
        /* Lock charts to a mobile-friendly height */
        .stPlotlyChart {
            height: 320px !important;
        }
        /* Collapse sidebar by default on super small screens is mostly handled by streamlit, 
           but we can hide some captions to reduce noise */
        .stCaption {
            font-size: 0.75rem !important;
        }
    }
    </style>
    """, unsafe_allow_html=True)

    ui.hero(
        "DEEN Commerce BI",
        "A focused BI operating view for revenue, demand, customer health, and geographic performance. The dashboard now opens on the latest 30 days of WooCommerce data so the default experience stays fast and practical.",
        chips=[
            "Last 30 days default",
            "WooCommerce-only",
            "Historical on demand",
            "Inventory visibility",
        ],
    )

    with st.sidebar:
        st.subheader("Data Connectors")
        st.info("Business Intelligence is now powered only by WooCommerce orders, customers, and inventory cache.")

    include_gsheet = False
    include_woo = True

    historical_requested = st.session_state.get("dashboard_historical_requested", False)

    # Default to the latest 30 days so the dashboard stays lightweight.
    end_date = date.today()
    start_date = end_date - timedelta(days=30)

    st.markdown("<div style='margin-bottom: 1rem;'></div>", unsafe_allow_html=True)
    action_col_1, action_col_2 = st.columns(2)
    with action_col_1:
        load_clicked = st.button(
            "Sync Last 30 Days",
            use_container_width=True,
            type="primary",
            help="Refresh the default 30-day WooCommerce sales window and inventory cache.",
        )
    with action_col_2:
        load_history_clicked = st.button(
            "Load Historical Data",
            use_container_width=True,
            help="Fetch lifetime WooCommerce order history only when you need long-term retention context.",
        )
    if load_history_clicked:
        historical_requested = True
        st.session_state.dashboard_historical_requested = True

    st.caption("Default dashboard metrics and charts are based on the latest 30 days of WooCommerce activity.")

    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    st.caption(estimate_woocommerce_load_time(start_date_str, end_date_str))

    orders_status = get_woocommerce_orders_cache_status(start_date_str, end_date_str)
    stock_status = get_woocommerce_stock_cache_status()
    full_history_status = get_woocommerce_full_history_status(end_date=end_date_str) if historical_requested else {}
    if include_woo:
        orders_started = start_orders_background_refresh(start_date_str, end_date_str, force=load_clicked)
        stock_started = start_stock_background_refresh(force=load_clicked)
        full_history_started = False
        if historical_requested:
            full_history_started = start_full_history_background_refresh(
                end_date=end_date_str,
                force=load_history_clicked,
            )
            if full_history_started or full_history_status:
                full_history_status = get_woocommerce_full_history_status(end_date=end_date_str)
        if orders_started:
            orders_status = get_woocommerce_orders_cache_status(start_date_str, end_date_str)
        if stock_started:
            stock_status = get_woocommerce_stock_cache_status()

    request_signature = {
        "start_date": start_date_str,
        "end_date": end_date_str,
        "include_woo": include_woo,
        "historical_requested": historical_requested,
    }
    cache_signature = "|".join(
        [
            str(orders_status.get("last_refresh", "")),
            str(orders_status.get("is_running", False)),
            str(stock_status.get("last_refresh", "")),
            str(stock_status.get("is_running", False)),
            str(full_history_status.get("last_full_sync", "") if historical_requested else ""),
            str(full_history_status.get("is_running", False) if historical_requested else False),
        ]
    )
    should_load = (
        load_clicked
        or "dashboard_data" not in st.session_state
        or st.session_state.get("dashboard_request_signature") != request_signature
        or st.session_state.get("dashboard_cache_signature") != cache_signature
    )
    if should_load:
        try:
            df_sales = _prune_dataframe(
                load_hybrid_data(
                    start_date=start_date_str,
                    end_date=end_date_str,
                    include_gsheet=False,
                    include_woocommerce=True,
                    woocommerce_mode="cache_only",
                ),
                DASHBOARD_SALES_COLUMNS,
            )
            if df_sales.empty:
                st.warning("No WooCommerce sales data was found for the selected date range.")
                return

            full_woo_history = pd.DataFrame()
            if historical_requested and full_history_status.get("is_complete"):
                try:
                    full_woo_history = _prune_dataframe(
                        load_full_woocommerce_history(end_date=end_date_str),
                        FULL_HISTORY_COLUMNS,
                    )
                except MemoryError as exc:
                    log_error(exc, context="Dashboard Load", details={"mode": "full_history_disabled"})
                    full_woo_history = pd.DataFrame()

            try:
                df_customers = _build_dashboard_customer_insights(df_sales, full_woo_history)
            except MemoryError as exc:
                log_error(exc, context="Dashboard Load", details={"mode": "customer_history_disabled"})
                df_customers = pd.DataFrame()

            try:
                ml_bundle = _build_dashboard_ml_bundle(df_sales, df_customers)
            except MemoryError as exc:
                log_error(exc, context="Dashboard Load", details={"mode": "ml_disabled"})
                ml_bundle = {"forecast": pd.DataFrame(), "customer_risk": pd.DataFrame(), "anomalies": pd.DataFrame()}

            try:
                stock_df = load_cached_woocommerce_stock_data()
            except MemoryError as exc:
                log_error(exc, context="Dashboard Load", details={"mode": "stock_disabled"})
                stock_df = pd.DataFrame()

            summary = {
                "woocommerce_live": len(df_sales),
                "stock_rows": len(stock_df),
                "total": len(df_sales),
            }

            if not historical_requested:
                full_history_hint = "Historical customer data is off by default. Click 'Load Historical Data' when you need lifetime retention context."
            elif full_history_status.get("is_complete"):
                full_history_hint = full_history_status.get("status_message", "Lifetime WooCommerce history is loaded.")
            elif full_history_status.get("is_running"):
                full_history_hint = "Historical WooCommerce sync is running in the background. Customer lifetime metrics will become richer after it finishes."
            else:
                full_history_hint = full_history_status.get(
                    "status_message",
                    "Historical sync has been requested but is not complete yet.",
                )

            st.session_state.dashboard_data = {
                "sales": df_sales,
                "customers": df_customers,
                "summary": summary,
                "ml": ml_bundle,
                "stock": stock_df,
                "orders_cache_last_refresh": orders_status.get("last_refresh"),
                "stock_cache_last_refresh": stock_status.get("last_refresh"),
                "full_history_last_refresh": full_history_status.get("last_full_sync"),
                "loaded_from_cache_hint": orders_status.get("status_message", ""),
                "stock_cache_hint": stock_status.get("status_message", ""),
                "full_history_hint": full_history_hint,
                "historical_requested": historical_requested,
                "historical_ready": bool(full_history_status.get("is_complete")),
            }
            st.session_state.dashboard_request_signature = request_signature
            st.session_state.dashboard_cache_signature = cache_signature
        except Exception as exc:
            if isinstance(exc, MemoryError) or "Unable to allocate" in str(exc):
                log_error(exc, context="Dashboard Load", details={"mode": "sales_only_safe_mode"})
                try:
                    df_sales = _prune_dataframe(
                        load_hybrid_data(
                            start_date=start_date_str,
                            end_date=end_date_str,
                            include_gsheet=False,
                            include_woocommerce=True,
                            woocommerce_mode="cache_only",
                        ),
                        DASHBOARD_SALES_COLUMNS,
                    )
                except Exception:
                    df_sales = pd.DataFrame()

                if df_sales.empty:
                    st.error("Error loading dashboard data: not enough memory to open even the lightweight WooCommerce view.")
                    return

                st.session_state.dashboard_data = {
                    "sales": df_sales,
                    "customers": pd.DataFrame(),
                    "summary": {
                        "woocommerce_live": len(df_sales),
                        "stock_rows": 0,
                        "total": len(df_sales),
                    },
                    "ml": {"forecast": pd.DataFrame(), "customer_risk": pd.DataFrame(), "anomalies": pd.DataFrame()},
                    "stock": pd.DataFrame(),
                    "orders_cache_last_refresh": orders_status.get("last_refresh"),
                    "stock_cache_last_refresh": stock_status.get("last_refresh"),
                    "full_history_last_refresh": full_history_status.get("last_full_sync"),
                    "loaded_from_cache_hint": orders_status.get("status_message", ""),
                    "stock_cache_hint": "Inventory was skipped in low-memory safe mode.",
                    "full_history_hint": (
                        "Customer lifetime history was skipped in low-memory safe mode."
                        if historical_requested
                        else "Historical customer data is off by default in the lightweight dashboard."
                    ),
                    "historical_requested": historical_requested,
                    "historical_ready": False,
                }
                st.session_state.dashboard_request_signature = request_signature
                st.session_state.dashboard_cache_signature = cache_signature
            else:
                log_error(exc, context="Dashboard Load")
                st.error(f"Error loading dashboard data: {exc}")
                return

    if "dashboard_data" not in st.session_state:
        st.info("Click 'Sync Last 30 Days' to fetch the latest WooCommerce insights.")
        return

    data = st.session_state.dashboard_data
    df_sales = data["sales"]
    df_woo_only = df_sales
    df_customers = data["customers"]
    summary = data.get("summary", {})
    ml_bundle = data.get("ml", {})
    stock_df = data.get("stock", pd.DataFrame())
    historical_ready = data.get("historical_ready", False)
    historical_requested = data.get("historical_requested", historical_requested)
    st.caption(data.get("loaded_from_cache_hint", ""))
    st.caption(data.get("stock_cache_hint", ""))
    st.caption(data.get("full_history_hint", ""))
    loaded_dates = pd.to_datetime(df_sales.get("order_date"), errors="coerce")
    ui.date_context(
        requested_start=start_date,
        requested_end=end_date,
        loaded_start=loaded_dates.min() if loaded_dates is not None and not loaded_dates.empty and loaded_dates.notna().any() else None,
        loaded_end=loaded_dates.max() if loaded_dates is not None and not loaded_dates.empty and loaded_dates.notna().any() else None,
        label="Loaded sales activity",
    )
    if orders_status.get("is_running") or stock_status.get("is_running") or full_history_status.get("is_running"):
        st.info("Background sync is running. The dashboard is using local cached data now and will pick up fresher WooCommerce data on the next rerun.")
    elif include_woo and df_woo_only.empty and not orders_status.get("cache_exists"):
        st.info("WooCommerce cache is being prepared. Core BI views will fill in as soon as the background sync finishes.")

    stale_reasons = []
    if _is_newer_timestamp(orders_status.get("last_refresh"), data.get("orders_cache_last_refresh")):
        stale_reasons.append("WooCommerce orders cache")
    if _is_newer_timestamp(stock_status.get("last_refresh"), data.get("stock_cache_last_refresh")):
        stale_reasons.append("inventory snapshot")
    if historical_requested and _is_newer_timestamp(
        full_history_status.get("last_full_sync"),
        data.get("full_history_last_refresh"),
    ):
        stale_reasons.append("historical customer cache")

    if stale_reasons:
        stale_labels = ", ".join(stale_reasons)
        st.warning(
            f"The dashboard is showing an older in-memory snapshot. Newer local data is available for: {stale_labels}. "
            "Rerun the page or click the sync button to refresh what you see."
        )

    st.caption("Core business pulse with the most important WooCommerce metrics only.")
    
    # New Storytelling Layer
    _render_dashboard_story(df_sales, df_customers, ml_bundle)
    
    render_executive_summary(df_sales, df_customers, summary)
    st.divider()
    render_business_intelligence(df_sales, df_customers)
    with st.expander("Data Confidence", expanded=False):
        render_data_audit(df_sales, df_customers, start_date, end_date)


def render_business_intelligence(df_sales: pd.DataFrame, df_customers: pd.DataFrame):
    st.subheader("Business Intelligence")
    st.caption("Compact comparison views for the current WooCommerce performance window.")

    with st.expander("📅 Period Filter", expanded=False):
        c1, c2 = st.columns(2)
        with c1:
            bi_start = st.date_input("BI Start Date", value=date.today() - timedelta(days=90))
        with c2:
            bi_end = st.date_input("BI End Date", value=date.today())
    
    st.caption(f"**Active BI Data Window:** {bi_start.strftime('%B %d, %Y')} to {bi_end.strftime('%B %d, %Y')}")
    
    bi_df = df_sales.copy()
    if not bi_df.empty and "order_date" in bi_df.columns:
        bi_df["order_date"] = pd.to_datetime(bi_df["order_date"], errors="coerce")
        mask = (bi_df["order_date"].dt.date >= bi_start) & (bi_df["order_date"].dt.date <= bi_end)
        bi_df = bi_df[mask].copy()

    render_today_vs_last_day_sales_chart(bi_df, df_customers)
    st.divider()
    render_last_7_days_sales_chart(bi_df, df_customers)
    st.divider()
    render_week_over_week_summary(bi_df, df_customers)
    st.divider()
    render_month_over_month_summary(bi_df, df_customers)
    st.divider()

    st.divider()


def render_today_vs_last_day_sales_chart(df_sales: pd.DataFrame, df_customers: pd.DataFrame):
    st.markdown("#### Exact Order Status Breakdown")
    order_df = _build_order_level_dataset(df_sales)
    if not order_df.empty and "order_status" in order_df.columns:
        # Standardize labels for display (e.g. on-hold -> Waiting, completed -> Shipped)
        status_map = {
            "completed": "Shipped",
            "on-hold": "Waiting",
            "processing": "Processing",
            "cancelled": "Cancelled",
            "refunded": "Refunded",
            "pending": "Pending",
            "failed": "Failed"
        }
        
        status_counts = order_df["order_status"].str.lower().value_counts().reset_index()
        status_counts.columns = ["Status", "Orders"]
        
        # Display as cards in a grid
        rows = (len(status_counts) + 3) // 4
        for r in range(rows):
            cols = st.columns(4)
            for c in range(4):
                idx = r * 4 + c
                if idx < len(status_counts):
                    row = status_counts.iloc[idx]
                    raw_status = row["Status"]
                    display_status = status_map.get(raw_status, raw_status.title())
                    with cols[c]:
                        st.metric(display_status, f"{row['Orders']:,}")
    
    st.divider()
    st.markdown("#### Today vs Previous Day Sales Comparison")
    sales = ensure_sales_schema(df_sales)
    sales = sales[sales["order_date"].notna()].copy()
    if sales.empty:
        st.info("No WooCommerce daily comparison data is available right now.")
        return

    sales["order_day"] = sales["order_date"].dt.normalize()
    order_daily = (
        _build_order_level_dataset(sales)
        .groupby("order_day", as_index=False)
        .agg(
            revenue=("order_total", "sum"),
            orders=("order_id", lambda s: s.replace("", pd.NA).dropna().nunique()),
            unique_customers=("customer_key", lambda s: s.replace("", pd.NA).dropna().nunique()),
            units=("qty", "sum"),
        )
        .sort_values("order_day")
        .tail(2)
        .reset_index(drop=True)
    )
    if order_daily.empty:
        st.info("No WooCommerce daily comparison data is available right now.")
        return

    if isinstance(df_customers, pd.DataFrame) and not df_customers.empty and "first_order" in df_customers.columns:
        customer_df = df_customers.copy()
        customer_df["first_order"] = pd.to_datetime(customer_df["first_order"], errors="coerce").dt.normalize()
        new_customer_daily = (
            customer_df[customer_df["first_order"].notna()]
            .groupby("first_order")
            .size()
            .reset_index(name="new_customers")
            .rename(columns={"first_order": "order_day"})
        )
        order_daily = order_daily.merge(new_customer_daily, on="order_day", how="left")
    order_daily["new_customers"] = pd.to_numeric(order_daily.get("new_customers", 0), errors="coerce").fillna(0).astype(int)

    latest_day = order_daily["order_day"].max()
    label_map = {0: "Today", 1: "Previous"}
    order_daily["days_ago"] = (latest_day - order_daily["order_day"]).dt.days
    order_daily["day_label"] = order_daily.apply(
        lambda row: f"{label_map.get(int(row['days_ago']), 'Earlier')} - {row['order_day'].strftime('%A, %d %b')}"
        if pd.notna(row["order_day"]) else "Unknown",
        axis=1,
    )

    c1, c2 = st.columns(2)
    with c1:
        fig_revenue = px.bar(
            order_daily,
            x="day_label",
            y="revenue",
            color="day_label",
            title="Today vs Previous Day Revenue",
            text_auto=".2s",
        )
        fig_revenue.update_layout(height=320, showlegend=False, xaxis_title="Day", yaxis_title="Revenue")
        st.plotly_chart(fig_revenue, use_container_width=True)
    with c2:
        comparison_metrics = order_daily.melt(
            id_vars=["day_label"],
            value_vars=["orders", "unique_customers", "new_customers", "units"],
            var_name="metric",
            value_name="value",
        )
        fig_counts = px.bar(
            comparison_metrics,
            x="metric",
            y="value",
            color="day_label",
            barmode="group",
            title="Today vs Previous Day Volume",
            labels={"metric": "Metric", "value": "Value", "day_label": "Day"},
        )
        fig_counts.update_layout(height=320, xaxis_title="Metric", yaxis_title="Value")
        st.plotly_chart(fig_counts, use_container_width=True)

    st.dataframe(
        order_daily[["day_label", "revenue", "orders", "unique_customers", "new_customers", "units"]].rename(
            columns={
                "day_label": "Day",
                "revenue": "Sales Revenue",
                "orders": "Order Count",
                "unique_customers": "Unique Customers",
                "new_customers": "New Customers",
                "units": "Units Sold",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


def render_last_7_days_sales_chart(df_sales: pd.DataFrame, df_customers: pd.DataFrame):
    st.markdown("#### Daily Comparison: Today vs Last Day vs Previous 7 Days")
    sales = ensure_sales_schema(df_sales)
    sales = sales[sales["order_date"].notna()].copy()
    if sales.empty:
        st.info("No daily comparison data is available for the current filter.")
        return

    sales["order_date"] = pd.to_datetime(sales["order_date"], errors="coerce")
    daily = (
        _build_order_level_dataset(sales.assign(order_day=sales["order_date"].dt.normalize()))
        .groupby("order_day", as_index=False)
        .agg(
            revenue=("order_total", "sum"),
            orders=("order_id", lambda s: s.replace("", pd.NA).dropna().nunique()),
            unique_customers=("customer_key", lambda s: s.replace("", pd.NA).dropna().nunique()),
            units=("qty", "sum"),
        )
        .sort_values("order_day")
        .tail(7)
        .reset_index(drop=True)
    )
    if daily.empty:
        st.info("No daily comparison data is available for the current filter.")
        return

    latest_day = daily["order_day"].max()
    label_map = {
        0: "Today",
        1: "Previous",
        2: "Earlier",
    }
    daily["days_ago"] = (latest_day - daily["order_day"]).dt.days
    daily["day_label"] = daily.apply(
        lambda row: f"{label_map[row['days_ago']]} - {row['order_day'].strftime('%A, %d %b')}"
        if row["days_ago"] in label_map
        else row["order_day"].strftime("%A, %d %b"),
        axis=1,
    )
    daily = daily.sort_values("order_day").reset_index(drop=True)

    if isinstance(df_customers, pd.DataFrame) and not df_customers.empty and "first_order" in df_customers.columns:
        customer_df = df_customers.copy()
        customer_df["first_order"] = pd.to_datetime(customer_df["first_order"], errors="coerce").dt.normalize()
        new_customer_daily = (
            customer_df[customer_df["first_order"].notna()]
            .groupby("first_order")
            .size()
            .reset_index(name="new_customers")
            .rename(columns={"first_order": "order_day"})
        )
        daily = daily.merge(new_customer_daily, on="order_day", how="left")
    daily["new_customers"] = pd.to_numeric(daily.get("new_customers", 0), errors="coerce").fillna(0).astype(int)

    c1, c2 = st.columns(2)
    with c1:
        fig_revenue = px.bar(
            daily,
            x="day_label",
            y="revenue",
            color="revenue",
            title="Last 7 Days Revenue",
            text_auto=".2s",
            color_continuous_scale="Tealgrn",
            labels={"day_label": "Day", "revenue": "Revenue"},
        )
        fig_revenue.update_layout(height=340, xaxis_title="Day", yaxis_title="Revenue")
        st.plotly_chart(fig_revenue, use_container_width=True)
    with c2:
        daily_people = daily.melt(
            id_vars=["day_label"],
            value_vars=["orders", "unique_customers", "new_customers"],
            var_name="metric",
            value_name="value",
        )
        fig_people = px.line(
            daily_people,
            x="day_label",
            y="value",
            color="metric",
            markers=True,
            title="Last 7 Days Orders and Customers",
            labels={"day_label": "Day", "value": "Count", "metric": "Metric"},
        )
        fig_people.update_layout(height=340, xaxis_title="Day", yaxis_title="Count")
        st.plotly_chart(fig_people, use_container_width=True)

    st.dataframe(
        daily[["day_label", "revenue", "orders", "unique_customers", "new_customers", "units"]].rename(
            columns={
                "day_label": "Day",
                "revenue": "Sales Revenue",
                "orders": "Order Count",
                "unique_customers": "Unique Customers",
                "new_customers": "New Customers",
                "units": "Units Sold",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


def build_period_business_metrics(
    df_sales: pd.DataFrame,
    df_customers: pd.DataFrame,
    view_mode: str,
) -> pd.DataFrame:
    sales = ensure_sales_schema(df_sales)
    sales = sales[sales["order_date"].notna()].copy()
    if sales.empty:
        return pd.DataFrame()

    freq_map = {
        "Quarter": "Q",
        "Month": "M",
        "Week": "W",
        "Year": "Y",
    }
    sales["order_date"] = pd.to_datetime(sales["order_date"], errors="coerce")
    period_series = sales["order_date"].dt.to_period(freq_map.get(view_mode, "Q"))
    sales["period"] = period_series
    sales["period_label"] = period_series.astype(str)
    order_metrics = _build_order_level_dataset(sales)
    if order_metrics.empty:
        return pd.DataFrame()
    order_metrics["order_date"] = pd.to_datetime(order_metrics["order_date"], errors="coerce")
    order_metrics["period"] = order_metrics["order_date"].dt.to_period(freq_map.get(view_mode, "Q"))
    order_metrics["period_label"] = order_metrics["period"].astype(str)
    metrics = (
        order_metrics.groupby(["period", "period_label"], as_index=False)
        .agg(
            revenue=("order_total", "sum"),
            orders=("order_id", lambda s: s.replace("", pd.NA).dropna().nunique()),
            unique_customers=("customer_key", lambda s: s.replace("", pd.NA).dropna().nunique()),
        )
        .sort_values("period")
        .reset_index(drop=True)
    )

    if isinstance(df_customers, pd.DataFrame) and not df_customers.empty and "first_order" in df_customers.columns:
        customer_df = df_customers.copy()
        customer_df["first_order"] = pd.to_datetime(customer_df["first_order"], errors="coerce")
        customer_df = customer_df[customer_df["first_order"].notna()].copy()
        if not customer_df.empty:
            customer_df["period"] = customer_df["first_order"].dt.to_period(freq_map.get(view_mode, "Q"))
            new_customer_counts = customer_df.groupby("period").size().reset_index(name="new_customers")
            metrics = metrics.merge(new_customer_counts, on="period", how="left")

    metrics["new_customers"] = pd.to_numeric(metrics.get("new_customers", 0), errors="coerce").fillna(0).astype(int)
    limit_map = {
        "Quarter": 4,
        "Month": 3,
        "Week": 4,
        "Year": 3,
    }
    limit = limit_map.get(view_mode, 4)
    metrics = metrics.tail(limit).reset_index(drop=True)
    return metrics[["period", "period_label", "revenue", "orders", "unique_customers", "new_customers"]]


def render_week_over_week_summary(df_sales: pd.DataFrame, df_customers: pd.DataFrame):
    st.markdown("#### Last 4 Weeks Overview")
    weekly_metrics = build_period_business_metrics(df_sales, df_customers, "Week")
    if weekly_metrics.empty:
        st.info("Not enough weekly data is available yet for a week-over-week comparison.")
        return

    weekly_metrics["period_label"] = weekly_metrics["period_label"].astype(str)

    c1, c2 = st.columns(2)
    with c1:
        week_revenue = weekly_metrics[["period_label", "revenue"]].rename(
            columns={"period_label": "Week", "revenue": "Sales Revenue"}
        )
        fig_revenue = px.bar(
            week_revenue,
            x="Week",
            y="Sales Revenue",
            color="Week",
            title="Last 4 Weeks Revenue",
            text_auto=".2s",
        )
        fig_revenue.update_layout(height=320, showlegend=False)
        st.plotly_chart(fig_revenue, use_container_width=True)
    with c2:
        week_counts = weekly_metrics.melt(
            id_vars=["period_label"],
            value_vars=["orders", "unique_customers", "new_customers"],
            var_name="Metric",
            value_name="Count",
        )
        fig_counts = px.bar(
            week_counts,
            x="Metric",
            y="Count",
            color="period_label",
            barmode="group",
            title="Last 4 Weeks Orders and Customers",
            labels={"period_label": "Week"},
        )
        fig_counts.update_layout(height=320)
        st.plotly_chart(fig_counts, use_container_width=True)


def render_month_over_month_summary(df_sales: pd.DataFrame, df_customers: pd.DataFrame):
    st.markdown("#### Last 3 Months Overview")
    monthly_metrics = build_period_business_metrics(df_sales, df_customers, "Month")
    if monthly_metrics.empty:
        st.info("Not enough monthly data is available yet for a month-over-month comparison.")
        return

    monthly_metrics["period_label"] = monthly_metrics["period_label"].astype(str)
    c1, c2 = st.columns(2)
    with c1:
        month_revenue = monthly_metrics[["period_label", "revenue"]].rename(
            columns={"period_label": "Month", "revenue": "Sales Revenue"}
        )
        fig_revenue = px.bar(
            month_revenue,
            x="Month",
            y="Sales Revenue",
            color="Month",
            title="Last 3 Months Revenue",
            text_auto=".2s",
        )
        fig_revenue.update_layout(height=320, showlegend=False)
        st.plotly_chart(fig_revenue, use_container_width=True)
    with c2:
        month_counts = monthly_metrics.melt(
            id_vars=["period_label"],
            value_vars=["orders", "unique_customers", "new_customers"],
            var_name="Metric",
            value_name="Count",
        )
        fig_counts = px.bar(
            month_counts,
            x="Metric",
            y="Count",
            color="period_label",
            barmode="group",
            title="Last 3 Months Orders and Customers",
            labels={"period_label": "Month"},
        )
        fig_counts.update_layout(height=320)
        st.plotly_chart(fig_counts, use_container_width=True)


def render_live_stream_comparison():
    st.info("This legacy stream comparison is no longer active. The app now runs on WooCommerce-only data.")


def render_inventory_health(stock_df: pd.DataFrame, forecast_df: pd.DataFrame):
    st.subheader("Inventory Health")
    st.caption("Live stock is fetched directly from the WooCommerce REST API.")
    imported_at = pd.to_datetime(stock_df.get("_imported_at"), errors="coerce") if isinstance(stock_df, pd.DataFrame) else pd.Series(dtype="datetime64[ns]")
    if not imported_at.empty and imported_at.notna().any():
        st.caption(
            f"Loaded inventory snapshot: {imported_at.min().strftime('%Y-%m-%d %H:%M')} to {imported_at.max().strftime('%Y-%m-%d %H:%M')}"
        )
    if stock_df is None or stock_df.empty:
        st.info("No live stock snapshot is available yet from WooCommerce.")
        return

    inventory = stock_df.copy()
    inventory["Stock Quantity"] = pd.to_numeric(inventory.get("Stock Quantity", 0), errors="coerce").fillna(0)
    inventory["Price"] = pd.to_numeric(inventory.get("Price", 0), errors="coerce").fillna(0)
    inventory["Inventory Value"] = inventory["Stock Quantity"] * inventory["Price"]

    low_stock_threshold = 5
    low_stock_df = inventory[inventory["Stock Quantity"] <= low_stock_threshold].copy()
    out_of_stock_df = inventory[inventory["Stock Status"].astype(str).str.lower() == "outofstock"].copy()

    notes = [
        f"The inventory snapshot currently covers {len(inventory):,} WooCommerce products.",
        f"{len(low_stock_df):,} products are at or below {low_stock_threshold} units.",
        f"{len(out_of_stock_df):,} products are out of stock right now.",
    ]
    if "_imported_at" in inventory.columns and inventory["_imported_at"].notna().any():
        notes.append(f"Latest stock sync in this session: {inventory['_imported_at'].dropna().max()}.")
    ui.commentary("Inventory Commentary", notes)

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Products", f"{len(inventory):,}")
        ui.badge("Counting mode: WooCommerce product rows")
    with m2:
        st.metric("Low Stock", f"{len(low_stock_df):,}")
        ui.badge("Counting mode: stock quantity <= 5")
    with m3:
        st.metric("Out of Stock", f"{len(out_of_stock_df):,}")
        ui.badge("Counting mode: stock_status = outofstock")
    with m4:
        st.metric("Inventory Value", f"TK {inventory['Inventory Value'].sum():,.0f}")
        ui.badge("Counting mode: stock quantity x price")

    c1, c2 = st.columns([1.2, 1])
    with c1:
        st.markdown("#### Low Stock Watchlist")
        st.dataframe(
            inventory.sort_values(["Stock Quantity", "Name"], ascending=[True, True])
            .head(20)[["Name", "SKU", "Stock Status", "Stock Quantity", "Price", "Inventory Value"]]
            .rename(
                columns={
                    "Name": "Product",
                    "Stock Status": "Status",
                    "Stock Quantity": "Stock Qty",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    with c2:
        status_counts = inventory["Stock Status"].fillna("unknown").astype(str).value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]
        fig_status = px.bar(
            status_counts,
            x="Status",
            y="Count",
            color="Count",
            title="Stock Status Mix",
            color_continuous_scale="Tealgrn",
        )
        fig_status.update_layout(height=350)
        st.plotly_chart(fig_status, use_container_width=True)

    if forecast_df is not None and not forecast_df.empty and "item_name" in forecast_df.columns:
        demand_view = inventory.merge(
            forecast_df,
            left_on="Name",
            right_on="item_name",
            how="left",
        )
        demand_view["forecast_7d_units"] = pd.to_numeric(demand_view.get("forecast_7d_units", 0), errors="coerce").fillna(0)
        demand_view["suggested_buffer_units"] = pd.to_numeric(demand_view.get("suggested_buffer_units", 0), errors="coerce").fillna(0)
        demand_view["coverage_gap"] = demand_view["Stock Quantity"] - demand_view["suggested_buffer_units"]

        st.markdown("#### Demand vs Stock Coverage")
        st.caption("This compares WooCommerce stock counts against the demand forecast buffer for overlapping product names.")
        st.dataframe(
            demand_view.sort_values(["coverage_gap", "Stock Quantity"], ascending=[True, True])
            .head(20)[
                [
                    "Name",
                    "SKU",
                    "Stock Quantity",
                    "forecast_7d_units",
                    "suggested_buffer_units",
                    "coverage_gap",
                    "risk_level",
                    "reorder_comment",
                ]
            ]
            .rename(
                columns={
                    "Name": "Product",
                    "Stock Quantity": "Stock Qty",
                    "forecast_7d_units": "Forecast 7d",
                    "suggested_buffer_units": "Suggested Buffer",
                    "coverage_gap": "Coverage Gap",
                    "risk_level": "Demand State",
                    "reorder_comment": "Suggestion",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )


def render_executive_summary(df_sales: pd.DataFrame, df_customers: pd.DataFrame, summary: dict):
    st.subheader("Executive Summary")
    _render_section_date_context(df_sales, "Loaded executive sales activity")
    df = df_sales[df_sales["order_date"].notna()].copy()
    df["order_day"] = df["order_date"].dt.normalize()
    order_df = _build_order_level_dataset(df)
    total_revenue = _sum_order_level_revenue(df)
    total_orders = order_df["order_id"].replace("", pd.NA).dropna().nunique() if not order_df.empty else 0
    active_customers = df["customer_key"].replace("", pd.NA).dropna().nunique()
    total_items = float(df["qty"].sum())
    pending_count = len(df[df["order_status"].str.lower().isin(["pending", "processing", "on-hold"])])

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("Revenue", f"TK {total_revenue:,.0f}")
        ui.badge("Counting mode: one order_total per distinct order_id")
    with k2:
        st.metric("Orders", f"{total_orders:,}")
        ui.badge("Counting mode: distinct normalized order_id")
    with k3:
        overall_aov = total_revenue / total_orders if total_orders else 0
        st.metric("AOV", f"TK {overall_aov:,.0f}")
        ui.badge("Counting mode: order-level revenue / distinct orders")
    with k4:
        st.metric("Customers", f"{active_customers:,}")
        ui.badge("Counting mode: distinct customer_key")
    with k5:
        st.metric("Pending", f"{pending_count:,}", "Needs action" if pending_count > 5 else "Healthy")
        ui.badge("Counting mode: pending, processing, on-hold")

    s1, s2, s3 = st.columns(3)
    with s1:
        st.metric("Items Sold", f"{total_items:,.0f}")
        ui.badge("Counting mode: summed qty from visible rows")
        st.caption(f"WooCommerce rows in cache: {summary.get('woocommerce_live', 0):,} | Inventory rows: {summary.get('stock_rows', 0):,}")
    with s2:
        repeat_rate = 0.0
        if isinstance(df_customers, pd.DataFrame) and not df_customers.empty and "total_orders" in df_customers.columns:
            repeat_rate = float((df_customers["total_orders"] > 1).mean() * 100)
        st.metric("Repeat Rate", f"{repeat_rate:.1f}%")
        ui.badge("Counting mode: customers with total_orders > 1")
    with s3:
        latest_date = df["order_date"].max()
        st.metric("Latest Order", latest_date.strftime("%Y-%m-%d %H:%M") if pd.notna(latest_date) else "N/A")
        ui.badge("Counting mode: max order_date in filter")

    insights = []
    if pending_count > 10:
        insights.append("Fulfillment pressure is rising. The pending order queue is large enough to justify immediate courier and packing review.")
    mean_basket_qty = df.groupby("order_id")["qty"].sum().mean() if total_orders else 0
    if mean_basket_qty and mean_basket_qty < 1.5:
        insights.append("Basket depth is light. Bundles, cross-sells, and checkout add-ons are the cleanest lever for AOV growth.")
    if isinstance(df_customers, pd.DataFrame) and not df_customers.empty and "segment" in df_customers.columns:
        churned = int((df_customers["segment"] == "Churned").sum())
        if churned:
            insights.append(f"{churned} customers are currently classified as churned. A focused win-back sequence is likely worth testing.")
        vip_count = int((df_customers["segment"] == "VIP").sum())
        if vip_count:
            insights.append(f"{vip_count} customers are in the VIP segment. Protect them with priority support, early launches, and higher-touch campaigns.")
    if not insights:
        insights.append("The core metrics look stable. The biggest next upside will likely come from retention programs and inventory planning.")
    ui.commentary("Intelligence Commentary", insights)
    render_data_trust_panel(df_sales)


def render_data_audit(
    df_sales: pd.DataFrame,
    df_customers: pd.DataFrame,
    start_date: date,
    end_date: date,
):
    st.subheader("Data Audit")
    df = ensure_sales_schema(df_sales)
    if df.empty:
        st.info("No rows are available for audit in the current filter.")
        return

    audit_df = df.copy()
    audit_df["order_date"] = pd.to_datetime(audit_df["order_date"], errors="coerce")
    audit_df["order_day"] = audit_df["order_date"].dt.date
    order_level_audit = _build_order_level_dataset(audit_df)
    order_counts = (
        audit_df[audit_df["order_id"].replace("", pd.NA).notna()]
        .groupby("order_id")
        .agg(
            first_seen=("order_date", "min"),
            line_items=("order_id", "size"),
            units=("qty", "sum"),
            revenue=("order_total", "max"),
            sources=("source", lambda s: ", ".join(sorted({str(v) for v in s if str(v).strip()}))),
        )
        .reset_index()
        .sort_values("first_seen", ascending=False)
    )
    per_source = (
        order_level_audit.groupby("source", dropna=False)
        .agg(
            line_items=("order_id", "size"),
            unique_orders=("order_id", lambda s: s.replace("", pd.NA).dropna().nunique()),
            revenue=("order_total", "sum"),
        )
        .reset_index()
        .rename(columns={"source": "Source"})
        .sort_values(["revenue", "line_items"], ascending=False)
    )
    per_day = (
        order_level_audit.groupby("order_day", dropna=False)
        .agg(
            line_items=("order_id", "size"),
            unique_orders=("order_id", lambda s: s.replace("", pd.NA).dropna().nunique()),
            customers=("customer_key", lambda s: s.replace("", pd.NA).dropna().nunique()),
            revenue=("order_total", "sum"),
        )
        .reset_index()
        .sort_values("order_day", ascending=False)
    )
    duplicate_orders = (
        order_counts[order_counts["line_items"] > 1][["order_id", "first_seen", "line_items", "units", "revenue", "sources"]]
        .head(25)
    )
    unique_orders = audit_df["order_id"].replace("", pd.NA).dropna().nunique()
    unique_customers = audit_df["customer_key"].replace("", pd.NA).dropna().nunique()
    line_items = len(audit_df)
    min_ts = audit_df["order_date"].min()
    max_ts = audit_df["order_date"].max()

    intro = [
        f"The filter currently requests data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}.",
        f"The loaded dataset contains {line_items:,} visible rows, {unique_orders:,} distinct orders, and {unique_customers:,} distinct customers after normalization.",
    ]
    if pd.notna(min_ts) and pd.notna(max_ts):
        intro.append(
            f"The actual timestamps present in the loaded data run from {min_ts.strftime('%Y-%m-%d %H:%M')} to {max_ts.strftime('%Y-%m-%d %H:%M')}."
        )
    ui.commentary("Audit Guidance", intro)

    a1, a2 = st.columns(2)
    with a1:
        ui.info_box(
            "How Order Counting Works",
            "Orders are counted using distinct normalized order_id values. If one checkout contains multiple products, it appears as multiple line items but one order.",
        )
    with a2:
        ui.info_box(
            "How Revenue Counting Works",
            "Revenue is counted once per distinct order_id using order-level totals, so multi-item orders do not multiply revenue in KPI cards and BI comparisons.",
        )

    metrics = st.columns(5)
    with metrics[0]:
        st.metric("Line Items", f"{line_items:,}")
        ui.badge("Every visible row after normalization")
    with metrics[1]:
        st.metric("Unique Orders", f"{unique_orders:,}")
        ui.badge("Distinct order_id values")
    with metrics[2]:
        st.metric("Unique Customers", f"{unique_customers:,}")
        ui.badge("Distinct customer_key values")
    with metrics[3]:
        st.metric("Date Coverage", f"{per_day['order_day'].nunique():,}")
        ui.badge("Distinct days with at least one row")
    with metrics[4]:
        st.metric("Sources", f"{audit_df['source'].replace('', pd.NA).dropna().nunique():,}")
        ui.badge("Distinct active source labels")

    st.markdown("#### Validation Tables")
    c1, c2 = st.columns([1, 1.25])
    with c1:
        st.markdown("##### Source Mix")
        st.dataframe(
            per_source,
            use_container_width=True,
            hide_index=True,
        )
    with c2:
        st.markdown("##### Daily Coverage")
        st.dataframe(
            per_day.rename(
                columns={
                    "order_day": "Date",
                    "line_items": "Line Items",
                    "unique_orders": "Orders",
                    "customers": "Customers",
                    "revenue": "Revenue",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("#### Order-Level Validation Sample")
    st.caption("Use this table to confirm that one order can contain multiple line items while still counting as a single order in KPI cards.")
    st.dataframe(
        order_counts.rename(
            columns={
                "order_id": "Order ID",
                "first_seen": "First Seen",
                "line_items": "Line Items",
                "units": "Units",
                "revenue": "Revenue",
                "sources": "Sources",
            }
        ).head(50),
        use_container_width=True,
        hide_index=True,
    )

    if not duplicate_orders.empty:
        st.markdown("#### Multi-Line Orders")
        st.caption("These are healthy duplicates caused by orders containing more than one item, not double-counted KPI orders.")
        st.dataframe(
            duplicate_orders.rename(
                columns={
                    "order_id": "Order ID",
                    "first_seen": "First Seen",
                    "line_items": "Line Items",
                    "units": "Units",
                    "revenue": "Revenue",
                    "sources": "Sources",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )


def render_sales_trends(df: pd.DataFrame):
    st.subheader("Sales Trends")
    _render_section_date_context(df, "Loaded trend activity")
    df = df[df["order_date"].notna()].copy()
    if df.empty:
        st.info("No date data available for trend analysis.")
        return

    trend_df = df.copy()
    trend_df["order_date"] = pd.to_datetime(trend_df["order_date"], errors="coerce")
    trend_df = trend_df[trend_df["order_date"].notna()].copy()

    trend_df["order_day"] = trend_df["order_date"].dt.date
    trend_df["day_name"] = trend_df["order_date"].dt.day_name()
    trend_df["day_num"] = trend_df["order_date"].dt.dayofweek
    trend_df["hour"] = trend_df["order_date"].dt.hour

    order_trend_df = _build_order_level_dataset(trend_df)
    daily = order_trend_df.groupby("order_day", as_index=False).agg(Revenue=("order_total", "sum"), Orders=("order_id", "nunique"))
    fig_line = px.line(daily, x="order_day", y="Revenue", title="Daily Revenue", markers=True)
    fig_line.update_layout(height=350, xaxis_title="Date")
    st.plotly_chart(fig_line, use_container_width=True)

    ui.commentary = []
    if not daily.empty:
        peak_day = daily.loc[daily["Revenue"].idxmax()]
        ui.commentary.append(
            f"Peak daily revenue in this selection was TK {peak_day['Revenue']:,.0f} on {peak_day['order_day']}."
        )
        if len(daily) > 1:
            recent_avg = daily["Revenue"].tail(min(7, len(daily))).mean()
            ui.commentary.append(
                f"Recent run-rate is about TK {recent_avg:,.0f} per day based on the latest visible period."
            )
    ui.commentary("Trend Commentary", ui.commentary)

    c1, c2 = st.columns(2)
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    by_day = trend_df.groupby("day_name", as_index=False).agg(Orders=("order_id", "nunique"))
    by_day = by_day.set_index("day_name").reindex(day_order, fill_value=0).reset_index()
    by_day.columns = ["Day", "Orders"]
    with c1:
        fig_bar = px.bar(by_day, x="Day", y="Orders", title="Orders by Day of Week", color="Orders", color_continuous_scale="Blues")
        fig_bar.update_layout(height=320)
        st.plotly_chart(fig_bar, use_container_width=True)
    with c2:
        heat = trend_df.groupby(["day_num", "hour"], as_index=False).size()
        heat = heat.rename(columns={"size": "Orders"})
        pivot = heat.pivot(index="day_num", columns="hour", values="Orders").reindex(index=range(7), columns=range(24), fill_value=0)
        fig_heatmap = px.imshow(
            pivot.values,
            x=list(range(24)),
            y=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            labels={"x": "Hour", "y": "Day", "color": "Orders"},
            color_continuous_scale="YlOrRd",
            title="Peak Activity Heatmap",
        )
        fig_heatmap.update_layout(height=320)
        st.plotly_chart(fig_heatmap, use_container_width=True)


def render_product_performance(df: pd.DataFrame):
    st.subheader("Product Performance")
    st.caption("This view uses WooCommerce order items only for cleaner item-level accuracy.")
    _render_section_date_context(df, "Loaded product activity")
    if df.empty:
        st.info("No product data available.")
        return

    product_df = df.copy()
    product_df["line_revenue"] = _estimate_line_revenue(product_df)
    grouped = product_df.groupby("item_name").agg(Revenue=("line_revenue", "sum"), Units=("qty", "sum"), Orders=("order_id", "nunique")).reset_index()
    grouped = grouped[grouped["item_name"].astype(str).str.strip() != ""].sort_values("Revenue", ascending=False)
    if grouped.empty:
        st.info("No product-level metrics are available.")
        return

    top_products = grouped.head(10)
    product_notes = []
    if not top_products.empty:
        leader = top_products.iloc[0]
        product_notes.append(
            f"Top product is {leader['item_name']} with TK {leader['Revenue']:,.0f} revenue and {leader['Units']:,.0f} units sold."
        )
        concentration = top_products["Revenue"].sum() / grouped["Revenue"].sum() if grouped["Revenue"].sum() else 0
        product_notes.append(
            f"The top 10 products contribute {concentration * 100:.1f}% of visible estimated item revenue, which helps show catalog concentration risk."
        )
    ui.commentary("Merchandising Commentary", product_notes)

    c1, c2 = st.columns(2)
    with c1:
        fig_top = ui.bar_chart(
            top_products.sort_values("Revenue"),
            x="Revenue",
            y="item_name",
            title="Top Products by Revenue",
            color="Revenue",
            color_scale="Tealgrn",
            text_auto=".2s",
        )
        st.plotly_chart(fig_top, use_container_width=True)
    with c2:
        fig_units = ui.bar_chart(
            top_products.sort_values("Units"),
            x="Units",
            y="item_name",
            title="Top Products by Units Sold",
            color="Units",
            color_scale="Blues",
            text_auto=".0f",
        )
        st.plotly_chart(fig_units, use_container_width=True)

    st.dataframe(top_products.rename(columns={"item_name": "Product"}), use_container_width=True, hide_index=True)


def render_customer_behavior(
    df_sales: pd.DataFrame,
    df_customers: pd.DataFrame,
    historical_ready: bool = False,
):
    st.subheader("Customer Behavior")
    if historical_ready:
        st.caption("This view includes lifetime WooCommerce history for stronger retention and repeat-customer accuracy.")
    else:
        st.caption("This view is using the latest 30 days by default. Load historical WooCommerce data on demand for lifetime retention accuracy.")
    _render_section_date_context(df_sales, "Loaded customer behavior activity")
    if df_customers is None or df_customers.empty:
        st.info("Customer insights are not available yet for the selected period.")
        return

    new_customers = int((df_customers["segment"] == "New").sum()) if "segment" in df_customers.columns else 0
    returning_customers = int((df_customers["total_orders"] > 1).sum()) if "total_orders" in df_customers.columns else 0
    customer_notes = []
    if len(df_customers):
        customer_notes.append(
            f"The customer base in the current filter includes {len(df_customers):,} distinct customers."
        )
    if returning_customers:
        share = (returning_customers / max(len(df_customers), 1)) * 100
        customer_notes.append(
            f"Returning customers account for {share:.1f}% of the visible customer base."
        )
    churned = int((df_customers["segment"] == "Churned").sum()) if "segment" in df_customers.columns else 0
    if churned:
        customer_notes.append(
            f"{churned} customers are flagged as churned, which makes retention automation a high-value next feature."
        )
    ui.commentary("Retention Commentary", customer_notes)

    c1, c2 = st.columns(2)
    with c1:
        donut_df = pd.DataFrame(
            {
                "Customer Type": ["New", "Returning"],
                "Customers": [new_customers, returning_customers],
            }
        )
        fig_donut = ui.donut_chart(
            donut_df,
            values="Customers",
            names="Customer Type",
            title="New vs Returning Customers",
            color_scale="Tealgrn",
        )
        st.plotly_chart(fig_donut, use_container_width=True)
    with c2:
        segment_counts = df_customers["segment"].value_counts().reset_index()
        segment_counts.columns = ["Segment", "Count"]
        fig_segments = ui.bar_chart(
            segment_counts.sort_values("Count"),
            x="Count",
            y="Segment",
            title="Customer Segments",
            color="Count",
            color_scale="Plasma",
            text_auto=".0f",
        )
        st.plotly_chart(fig_segments, use_container_width=True)

    if all(col in df_customers.columns for col in ["total_orders", "total_revenue"]):
        fig_scatter = px.scatter(
            df_customers,
            x="total_orders",
            y="total_revenue",
            color="segment" if "segment" in df_customers.columns else None,
            size="avg_order_value" if "avg_order_value" in df_customers.columns else None,
            hover_name="primary_name" if "primary_name" in df_customers.columns else None,
            title="Customer Value Matrix",
            labels={"total_orders": "Orders", "total_revenue": "Revenue"},
        )
        fig_scatter = ui.apply_plotly_theme(fig_scatter, height=420)
        st.plotly_chart(fig_scatter, use_container_width=True)


def render_geographic_insights(df: pd.DataFrame):
    st.subheader("Geographic Insights")
    _render_section_date_context(df, "Loaded geographic activity")
    geo = df.copy()
    geo["region"] = geo["state"].where(geo["state"] != "", geo["city"])
    geo = geo[geo["region"].astype(str).str.strip() != ""]
    if geo.empty:
        st.info("No geographic data found in the selected dataset.")
        return

    geo_orders = _build_order_level_dataset(geo)
    geo_sales = geo_orders.groupby("region").agg(Revenue=("order_total", "sum"), Orders=("order_id", "nunique")).reset_index().sort_values("Revenue", ascending=False).head(15)
    geo_notes = []
    if not geo_sales.empty:
        leader = geo_sales.iloc[0]
        geo_notes.append(
            f"Top visible region is {leader['region']} with TK {leader['Revenue']:,.0f} revenue from {leader['Orders']:,} orders."
        )
        geo_notes.append(
            "Use this view to prioritize delivery reliability, ad targeting, and stock placement by region."
        )
    ui.commentary("Regional Commentary", geo_notes)
    c1, c2 = st.columns(2)
    with c1:
        fig_geo = ui.bar_chart(
            geo_sales.sort_values("Revenue"),
            x="Revenue",
            y="region",
            title="Revenue by Region",
            color="Revenue",
            color_scale="Tealgrn",
            text_auto=".2s",
        )
        st.plotly_chart(fig_geo, use_container_width=True)
    with c2:
        fig_orders = ui.bar_chart(
            geo_sales.sort_values("Orders"),
            x="Orders",
            y="region",
            title="Orders by Region",
            color="Orders",
            color_scale="Oranges",
            text_auto=".0f",
        )
        st.plotly_chart(fig_orders, use_container_width=True)


def render_forecast_and_alerts(ml_bundle: dict[str, pd.DataFrame]):
    st.subheader("Forecast & Alerts")
    forecast_df = ml_bundle.get("forecast", pd.DataFrame())
    risk_df = ml_bundle.get("customer_risk", pd.DataFrame())
    anomaly_df = ml_bundle.get("anomalies", pd.DataFrame())

    overview_notes = []
    if not forecast_df.empty:
        lead = forecast_df.iloc[0]
        overview_notes.append(
            f"Highest forecasted item is {lead['item_name']} with about {lead['forecast_7d_units']:.1f} expected units over the next 7 days."
        )
    if not risk_df.empty:
        high_risk = int((risk_df["risk_band"] == "High").sum())
        overview_notes.append(
            f"{high_risk} customers are currently in the high-risk band for churn or inactivity."
        )
    if not anomaly_df.empty:
        overview_notes.append(
            f"{len(anomaly_df):,} recent metric anomalies were detected across revenue, orders, or AOV."
        )
    if not overview_notes:
        overview_notes.append("No predictive signals are available yet. More clean sales history will improve forecast and risk quality.")
    ui.commentary("Predictive Commentary", overview_notes)

    top_row = st.columns(3)
    with top_row[0]:
        total_forecast_units = forecast_df["forecast_7d_units"].sum() if not forecast_df.empty else 0
        st.metric("Forecast Units (7d)", f"{total_forecast_units:,.0f}")
    with top_row[1]:
        high_risk = int((risk_df["risk_band"] == "High").sum()) if not risk_df.empty else 0
        st.metric("High-Risk Customers", f"{high_risk:,}")
    with top_row[2]:
        anomaly_count = len(anomaly_df) if not anomaly_df.empty else 0
        st.metric("Anomalies", f"{anomaly_count:,}")

    if not forecast_df.empty:
        c1, c2 = st.columns([1.3, 1])
        with c1:
            fig_forecast = px.bar(
                forecast_df.sort_values("forecast_7d_units"),
                x="forecast_7d_units",
                y="item_name",
                orientation="h",
                color="trend_ratio",
                color_continuous_scale="Tealgrn",
                title="Next 7-Day Demand Forecast",
                labels={"forecast_7d_units": "Forecast Units", "item_name": "Product"},
            )
            fig_forecast.update_layout(height=420)
            st.plotly_chart(fig_forecast, use_container_width=True)
        with c2:
            st.dataframe(
                forecast_df[["item_name", "forecast_7d_units", "suggested_buffer_units", "risk_level", "reorder_comment"]].rename(
                    columns={
                        "item_name": "Product",
                        "forecast_7d_units": "Forecast 7d",
                        "suggested_buffer_units": "Buffer Units",
                        "risk_level": "Demand State",
                        "reorder_comment": "Suggestion",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

    if not risk_df.empty:
        st.markdown("#### Customer Risk Recommendations")
        st.dataframe(
            risk_df[["primary_name", "segment", "risk_score", "risk_band", "next_purchase_window_days", "recommended_action"]]
            .head(20)
            .rename(
                columns={
                    "primary_name": "Customer",
                    "segment": "Segment",
                    "risk_score": "Risk Score",
                    "risk_band": "Risk Band",
                    "next_purchase_window_days": "Next Window (days)",
                    "recommended_action": "Recommendation",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

    if not anomaly_df.empty:
        st.markdown("#### Metric Anomalies")
        fig_anomaly = px.scatter(
            anomaly_df,
            x="order_day",
            y="z_score",
            color="metric",
            symbol="direction",
            hover_data=["ui.commentary"],
            title="Recent Revenue / Orders / AOV Anomalies",
        )
        fig_anomaly.update_layout(height=360, xaxis_title="Date", yaxis_title="Z-Score")
        st.plotly_chart(fig_anomaly, use_container_width=True)
        st.dataframe(
            anomaly_df[["order_day", "metric", "direction", "z_score", "ui.commentary"]],
            use_container_width=True,
            hide_index=True,
        )


def render_data_trust_panel(df_sales: pd.DataFrame):
    df = ensure_sales_schema(df_sales)
    if df.empty:
        return

    order_df = _build_order_level_dataset(df)
    valid_dates = df["order_date"].dropna()
    min_date = valid_dates.min() if not valid_dates.empty else None
    max_date = valid_dates.max() if not valid_dates.empty else None
    unique_orders = df["order_id"].replace("", pd.NA).dropna().nunique()
    unique_customers = df["customer_key"].replace("", pd.NA).dropna().nunique()
    line_items = len(df)
    active_sources = sorted({src for src in df["source"].dropna().astype(str) if src})
    total_revenue = float(pd.to_numeric(order_df.get("order_total", 0), errors="coerce").fillna(0).sum()) if not order_df.empty else 0.0

    trust_notes = [
        "Unique orders are counted using distinct `order_id` values after source normalization and deduplication.",
        f"Revenue is counted once per order using order-level totals: TK {total_revenue:,.0f} in the current filter.",
        f"Visible line items in the current filter: {line_items:,}.",
        f"Visible unique orders in the current filter: {unique_orders:,}.",
    ]
    if min_date is not None and max_date is not None:
        trust_notes.append(
            f"Current data window spans from {min_date.strftime('%Y-%m-%d %H:%M')} to {max_date.strftime('%Y-%m-%d %H:%M')}."
        )
    if active_sources:
        trust_notes.append(f"Active sources in this view: {', '.join(active_sources)}.")

    with st.expander("Data Trust Panel", expanded=False):
        ui.commentary("How This Dashboard Counts Data", trust_notes)
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Line Items", f"{line_items:,}")
            ui.badge("Visible normalized rows")
        with c2:
            st.metric("Unique Orders", f"{unique_orders:,}")
            ui.badge("Distinct order_id")
        with c3:
            st.metric("Unique Customers", f"{unique_customers:,}")
            ui.badge("Distinct customer_key")
        with c4:
            st.metric("Sources", f"{len(active_sources):,}")
            ui.badge("Active source labels")

        preview_cols = [col for col in ["order_date", "order_id", "item_name", "qty", "order_total", "source"] if col in df.columns]
        st.caption("Sample rows from the exact filtered dataset used in the KPIs above.")
        st.dataframe(
            df[preview_cols].sort_values("order_date", ascending=False).head(25),
            use_container_width=True,
            hide_index=True,
        )


def _pct_delta(current: float, previous: float, suffix: str = "") -> str | None:
    if previous in (0, None):
        return None
    delta = ((current - previous) / previous) * 100
    return f"{delta:+.1f}% {suffix}".strip()

def render_dashboard_customers_section():
    if "dashboard_data" not in st.session_state:
        st.info("⚠️ Please load data from the Business Intelligence tab first to view this section.")
        return
    data = st.session_state.dashboard_data
    st.subheader("Dashboard Customer Behavior")
    st.caption("Customer growth, repeat behavior, and retention signals loaded from BI cache.")
    render_customer_behavior(data["sales"], data["customers"], historical_ready=data.get("historical_ready", False))

def render_dashboard_products_section():
    if "dashboard_data" not in st.session_state:
        st.info("⚠️ Please load data from the Business Intelligence tab first to view this section.")
        return
    data = st.session_state.dashboard_data
    st.subheader("Top Products & Inventory")
    st.caption("Top products, stock position, and demand coverage.")
    render_product_performance(data["sales"])
    st.divider()
    render_inventory_health(data.get("stock", pd.DataFrame()), data.get("ml", {}).get("forecast", pd.DataFrame()))

def render_dashboard_operations_section():
    if "dashboard_data" not in st.session_state:
        st.info("⚠️ Please load data from the Business Intelligence tab first to view this section.")
        return
    data = st.session_state.dashboard_data
    st.subheader("Operational Trends")
    st.caption("Operational views for trends, geography, and machine-generated signals.")
    operations_tabs = st.tabs(["Sales Trends", "Geographic", "Forecast & Alerts"])
    with operations_tabs[0]:
        render_sales_trends(data["sales"])
    with operations_tabs[1]:
        render_geographic_insights(data["sales"])
    with operations_tabs[2]:
        render_forecast_and_alerts(data.get("ml", {}))
