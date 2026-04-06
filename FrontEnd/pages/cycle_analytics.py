"""Business cycle analytics page backed by normalized WooCommerce data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from BackEnd.services.hybrid_data_loader import (
    estimate_woocommerce_load_time,
    get_woocommerce_orders_cache_status,
    load_hybrid_data,
    start_orders_background_refresh,
)
from BackEnd.services.woocommerce_service import get_woocommerce_credentials, get_woocommerce_store_label
from BackEnd.utils.sales_schema import ensure_sales_schema
from FrontEnd.components.ui_components import (
    render_audit_card,
    render_bi_hero,
    render_commentary_panel,
    render_highlight_stat,
    render_kpi_note,
    render_loaded_date_context,
)

BUSINESS_CUTOFF_HOUR = 17
CLOSED_WEEKDAYS = {4}  # Friday
NEW_ORDER_STATUSES = {"processing", "on-hold", "pending"}
SHIPPED_ORDER_STATUSES = {"completed", "shipped"}


@dataclass(frozen=True)
class CycleWindow:
    label: str
    short_label: str
    start: datetime
    end: datetime

    @property
    def span_hours(self) -> int:
        return int((self.end - self.start).total_seconds() // 3600)


def _inject_cycle_styles():
    st.markdown(
        """
        <style>
        .cycle-window-card {
            background: linear-gradient(180deg, rgba(255,255,255,0.97) 0%, rgba(240,247,250,0.96) 100%);
            border: 1px solid rgba(15, 76, 129, 0.12);
            border-radius: 20px;
            padding: 1rem 1.1rem;
            box-shadow: 0 16px 34px rgba(15, 35, 58, 0.08);
            min-height: 160px;
            margin-bottom: 1rem;
        }
        .cycle-window-label {
            font-size: 0.74rem;
            font-weight: 700;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: #0f4c81;
            margin-bottom: 0.55rem;
        }
        .cycle-window-range {
            font-size: 1.02rem;
            font-weight: 700;
            color: #102132;
            line-height: 1.45;
        }
        .cycle-window-meta {
            margin-top: 0.6rem;
            color: #5f7183;
            font-size: 0.86rem;
            line-height: 1.45;
        }
        .cycle-sync-banner {
            background: rgba(20, 184, 166, 0.10);
            border: 1px solid rgba(20, 184, 166, 0.18);
            border-radius: 18px;
            padding: 0.85rem 1rem;
            color: #0f4c81;
            margin-bottom: 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _cutoff(timestamp: datetime) -> datetime:
    return timestamp.replace(hour=BUSINESS_CUTOFF_HOUR, minute=0, second=0, microsecond=0)


def _previous_business_cutoff(boundary: datetime) -> datetime:
    candidate = _cutoff(boundary - timedelta(days=1))
    while candidate.weekday() in CLOSED_WEEKDAYS:
        candidate -= timedelta(days=1)
    return candidate


def _latest_completed_cutoff(reference_time: datetime) -> datetime:
    candidate = _cutoff(reference_time)
    if reference_time < candidate:
        candidate -= timedelta(days=1)
    while candidate.weekday() in CLOSED_WEEKDAYS:
        candidate -= timedelta(days=1)
    return candidate


def get_business_cycles(reference_time: datetime | None = None) -> tuple[datetime, datetime, datetime, datetime]:
    reference = reference_time or datetime.now()
    current_end = _latest_completed_cutoff(reference)
    current_start = _previous_business_cutoff(current_end)
    previous_end = current_start
    previous_start = _previous_business_cutoff(previous_end)
    return current_start, current_end, previous_start, previous_end


def build_recent_cycle_windows(count: int = 8, reference_time: datetime | None = None) -> list[CycleWindow]:
    reference = reference_time or datetime.now()
    windows: list[CycleWindow] = []
    end = _latest_completed_cutoff(reference)
    for _ in range(max(count, 2)):
        start = _previous_business_cutoff(end)
        windows.append(
            CycleWindow(
                label=f"{start.strftime('%d %b %I:%M %p')} to {end.strftime('%d %b %I:%M %p')}",
                short_label=end.strftime("%d %b"),
                start=start,
                end=end,
            )
        )
        end = start
    return windows


def _first_non_blank(series: pd.Series) -> str:
    for value in series:
        text = str(value).strip()
        if text and text.lower() != "nat":
            return text
    return ""


def _classify_status(status: str) -> str:
    normalized = str(status).strip().lower()
    if normalized in NEW_ORDER_STATUSES:
        return "new"
    if normalized in SHIPPED_ORDER_STATUSES:
        return "shipped"
    return "other"


def _within_cycle_window(series: pd.Series, start: datetime, end: datetime) -> pd.Series:
    timestamps = pd.to_datetime(series, errors="coerce")
    return timestamps.gt(start) & timestamps.le(end)


def prepare_cycle_orders(df: pd.DataFrame) -> pd.DataFrame:
    sales = ensure_sales_schema(df)
    if sales.empty:
        return pd.DataFrame(
            columns=[
                "order_key",
                "order_id",
                "order_date",
                "shipped_date",
                "shipped_anchor",
                "customer_name",
                "order_status",
                "status_bucket",
                "order_total",
                "qty",
                "city",
                "source",
                "line_count",
            ]
        )

    prepared = sales.copy()
    prepared["order_status"] = prepared["order_status"].astype(str).str.strip().str.lower()
    prepared["order_key"] = prepared["order_id"].where(
        prepared["order_id"].astype(str).str.strip() != "",
        prepared["order_item_key"],
    )
    prepared["shipped_dt"] = pd.to_datetime(prepared["shipped_date"], errors="coerce")

    grouped = (
        prepared.sort_values(["order_date", "shipped_dt"], na_position="last")
        .groupby("order_key", as_index=False)
        .agg(
            order_id=("order_id", _first_non_blank),
            order_date=("order_date", "min"),
            shipped_date=("shipped_dt", "max"),
            customer_name=("customer_name", _first_non_blank),
            order_status=("order_status", _first_non_blank),
            order_total=("order_total", "max"),
            qty=("qty", "sum"),
            city=("city", _first_non_blank),
            source=("source", _first_non_blank),
            line_count=("item_name", "count"),
        )
    )
    grouped["order_id"] = grouped["order_id"].replace("", pd.NA).fillna(grouped["order_key"])
    grouped["shipped_anchor"] = grouped["shipped_date"].where(grouped["shipped_date"].notna(), grouped["order_date"])
    grouped["status_bucket"] = grouped["order_status"].apply(_classify_status)
    return grouped


def filter_cycle_orders(orders: pd.DataFrame, start: datetime, end: datetime, bucket: str) -> pd.DataFrame:
    if orders is None or orders.empty:
        return pd.DataFrame(columns=list(orders.columns) if isinstance(orders, pd.DataFrame) else [])

    if bucket == "shipped":
        mask = orders["status_bucket"].eq("shipped") & _within_cycle_window(orders["shipped_anchor"], start, end)
    elif bucket == "new":
        mask = orders["status_bucket"].eq("new") & _within_cycle_window(orders["order_date"], start, end)
    elif bucket == "other":
        mask = orders["status_bucket"].eq("other") & _within_cycle_window(orders["order_date"], start, end)
    else:
        mask = _within_cycle_window(orders["order_date"], start, end)

    return orders.loc[mask].copy().sort_values("order_date", ascending=False, na_position="last")


def calculate_cycle_metrics(orders: pd.DataFrame, start: datetime, end: datetime, bucket: str) -> dict[str, float]:
    filtered = filter_cycle_orders(orders, start, end, bucket)
    if filtered.empty:
        return {"items_sold": 0, "num_orders": 0, "revenue": 0.0, "basket_value": 0.0}

    num_orders = int(len(filtered))
    revenue = float(pd.to_numeric(filtered["order_total"], errors="coerce").fillna(0).sum())
    items_sold = int(pd.to_numeric(filtered["qty"], errors="coerce").fillna(0).sum())
    basket_value = revenue / num_orders if num_orders else 0.0
    return {
        "items_sold": items_sold,
        "num_orders": num_orders,
        "revenue": revenue,
        "basket_value": basket_value,
    }


def _load_cycle_sales(start_date: str, end_date: str, use_live_refresh: bool) -> pd.DataFrame:
    return load_hybrid_data(
        start_date=start_date,
        end_date=end_date,
        include_gsheet=False,
        include_woocommerce=True,
        woocommerce_mode="live" if use_live_refresh else "cache_only",
    )


def _format_currency(value: float) -> str:
    return f"Tk {value:,.0f}"


def _format_delta(current: float, previous: float, *, currency: bool = False) -> str:
    delta = current - previous
    if currency:
        return f"Tk {delta:+,.0f}"
    return f"{delta:+,.0f}"


def _safe_pct_change(current: float, previous: float) -> float | None:
    if previous <= 0:
        return None
    return ((current - previous) / previous) * 100


def _headline_text(current_new: dict[str, float], current_shipped: dict[str, float]) -> tuple[str, str]:
    shipped_orders = int(current_shipped["num_orders"])
    new_orders = int(current_new["num_orders"])
    balance = shipped_orders - new_orders
    if balance > 0:
        help_text = f"Fulfillment outpaced intake by {balance} orders in the latest closed cycle."
    elif balance < 0:
        help_text = f"Intake outpaced fulfillment by {abs(balance)} orders, which suggests backlog pressure."
    else:
        help_text = "Intake and fulfillment closed in balance across the latest cycle."
    return f"{shipped_orders} shipped vs {new_orders} new", help_text


def _build_top_new_items(sales_df: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
    sales = ensure_sales_schema(sales_df)
    if sales.empty:
        return pd.DataFrame(columns=["item_name", "units"])

    status_mask = sales["order_status"].astype(str).str.strip().str.lower().isin(NEW_ORDER_STATUSES)
    date_mask = _within_cycle_window(sales["order_date"], start, end)
    filtered = sales.loc[status_mask & date_mask & sales["item_name"].astype(str).str.strip().ne("")]
    if filtered.empty:
        return pd.DataFrame(columns=["item_name", "units"])

    return (
        filtered.groupby("item_name", as_index=False)
        .agg(units=("qty", "sum"))
        .sort_values(["units", "item_name"], ascending=[False, True])
        .head(8)
    )


def _build_top_shipped_cities(orders: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
    shipped = filter_cycle_orders(orders, start, end, "shipped")
    if shipped.empty:
        return pd.DataFrame(columns=["city", "revenue", "orders"])

    shipped = shipped[shipped["city"].astype(str).str.strip() != ""]
    if shipped.empty:
        return pd.DataFrame(columns=["city", "revenue", "orders"])

    return (
        shipped.groupby("city", as_index=False)
        .agg(revenue=("order_total", "sum"), orders=("order_id", "count"))
        .sort_values(["revenue", "orders", "city"], ascending=[False, False, True])
        .head(8)
    )


def _build_touched_orders(orders: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame(columns=list(orders.columns) + ["cycle_bucket", "cycle_anchor"])

    new_orders = filter_cycle_orders(orders, start, end, "new").assign(
        cycle_bucket="New intake",
        cycle_anchor=lambda frame: frame["order_date"],
    )
    shipped_orders = filter_cycle_orders(orders, start, end, "shipped").assign(
        cycle_bucket="Shipped",
        cycle_anchor=lambda frame: frame["shipped_anchor"],
    )
    other_orders = filter_cycle_orders(orders, start, end, "other").assign(
        cycle_bucket="Other status",
        cycle_anchor=lambda frame: frame["order_date"],
    )

    combined = pd.concat([new_orders, shipped_orders, other_orders], ignore_index=True, sort=False)
    if combined.empty:
        return combined

    combined = combined.sort_values("cycle_anchor", ascending=False, na_position="last")
    return combined.drop_duplicates(subset=["order_key", "cycle_bucket"], keep="first")


def _build_cycle_trend_frame(orders: pd.DataFrame, windows: list[CycleWindow]) -> pd.DataFrame:
    rows = []
    for window in reversed(windows):
        new_metrics = calculate_cycle_metrics(orders, window.start, window.end, "new")
        shipped_metrics = calculate_cycle_metrics(orders, window.start, window.end, "shipped")
        rows.append(
            {
                "Cycle": window.short_label,
                "Window": window.label,
                "Span Hours": window.span_hours,
                "New Orders": new_metrics["num_orders"],
                "New Revenue": new_metrics["revenue"],
                "Shipped Orders": shipped_metrics["num_orders"],
                "Shipped Revenue": shipped_metrics["revenue"],
            }
        )
    return pd.DataFrame(rows)


def _build_story_bullets(
    current_new: dict[str, float],
    current_shipped: dict[str, float],
    previous_new: dict[str, float],
    previous_shipped: dict[str, float],
    top_new_items: pd.DataFrame,
    top_shipped_cities: pd.DataFrame,
) -> list[str]:
    bullets: list[str] = []

    shipped_pct = _safe_pct_change(current_shipped["revenue"], previous_shipped["revenue"])
    if current_shipped["num_orders"] > 0:
        if shipped_pct is None:
            bullets.append(
                f"Fulfillment closed {_format_currency(current_shipped['revenue'])} across "
                f"{int(current_shipped['num_orders'])} shipped orders."
            )
        else:
            direction = "up" if shipped_pct >= 0 else "down"
            bullets.append(
                f"Fulfillment closed {_format_currency(current_shipped['revenue'])}, "
                f"{direction} {abs(shipped_pct):.0f}% versus the previous cycle."
            )
    else:
        bullets.append("No shipped orders landed inside the latest closed cycle.")

    balance = int(current_shipped["num_orders"] - current_new["num_orders"])
    if balance > 0:
        bullets.append(f"Shipped throughput stayed ahead of intake by {balance} orders, which helped reduce backlog.")
    elif balance < 0:
        bullets.append(f"New intake ran {abs(balance)} orders ahead of fulfillment, so queue pressure increased.")
    else:
        bullets.append("Intake and fulfillment were balanced in the latest cycle.")

    new_pct = _safe_pct_change(current_new["revenue"], previous_new["revenue"])
    if current_new["num_orders"] > 0 and new_pct is not None:
        direction = "up" if new_pct >= 0 else "down"
        bullets.append(
            f"New-order intake reached {_format_currency(current_new['revenue'])}, "
            f"{direction} {abs(new_pct):.0f}% from the prior cycle."
        )

    if not top_new_items.empty:
        lead_item = top_new_items.iloc[0]
        bullets.append(f"{lead_item['item_name']} led intake with {int(lead_item['units'])} units booked.")

    if not top_shipped_cities.empty:
        lead_city = top_shipped_cities.iloc[0]
        bullets.append(f"{lead_city['city']} generated the most shipped revenue at {_format_currency(float(lead_city['revenue']))}.")

    return bullets[:4]


def _render_cycle_window_card(title: str, window: CycleWindow, note: str):
    st.markdown(
        f"""
        <div class="cycle-window-card">
          <div class="cycle-window-label">{title}</div>
          <div class="cycle-window-range">{window.label}</div>
          <div class="cycle-window-meta">{window.span_hours}-hour closed cycle. {note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_metric_block(title: str, current_metrics: dict[str, float], previous_metrics: dict[str, float], note: str):
    st.subheader(title)
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Items", f"{int(current_metrics['items_sold']):,}", _format_delta(current_metrics["items_sold"], previous_metrics["items_sold"]))
    with m2:
        st.metric("Orders", f"{int(current_metrics['num_orders']):,}", _format_delta(current_metrics["num_orders"], previous_metrics["num_orders"]))
    with m3:
        st.metric("Revenue", _format_currency(current_metrics["revenue"]), _format_delta(current_metrics["revenue"], previous_metrics["revenue"], currency=True))
    with m4:
        st.metric("AOV", _format_currency(current_metrics["basket_value"]), _format_delta(current_metrics["basket_value"], previous_metrics["basket_value"], currency=True))
    render_kpi_note(note)


def render_cycle_analytics_tab():
    _inject_cycle_styles()

    credentials = get_woocommerce_credentials()
    has_credentials = bool(credentials)
    store_label = get_woocommerce_store_label()

    render_bi_hero(
        "Business Cycles",
        (
            f"Track the latest closed operating cycles for {store_label}. "
            "New-order intake is anchored on order creation time, shipped performance is anchored on shipped timestamps when available, "
            "and Friday is skipped in the cutoff calendar."
        ),
        chips=[
            "5 PM operational cutoff",
            "Friday skipped",
            "Cache-backed sync",
            "Intake vs fulfillment story",
        ],
    )

    control_col_1, control_col_2, control_col_3 = st.columns([1.1, 1.4, 1.2])
    with control_col_1:
        cycle_count = st.selectbox("Recent cycles", [6, 8, 10, 12], index=1)
    with control_col_2:
        use_live_refresh = st.checkbox(
            "Use live refresh if cache is stale",
            value=has_credentials,
            disabled=not has_credentials,
            help="When enabled, the page can fetch WooCommerce directly if the local cache does not cover the requested cycles.",
        )
    with control_col_3:
        sync_clicked = st.button(
            "Sync Cycle Data",
            use_container_width=True,
            type="primary",
            disabled=not has_credentials,
            help="Queue a fresh WooCommerce sync for the cycle analytics window.",
        )

    windows = build_recent_cycle_windows(count=cycle_count)
    current_window = windows[0]
    previous_window = windows[1]
    history_start = windows[-1].start.strftime("%Y-%m-%d")
    history_end = datetime.now().strftime("%Y-%m-%d")

    status = get_woocommerce_orders_cache_status(history_start, history_end)
    if has_credentials:
        started = start_orders_background_refresh(history_start, history_end, force=sync_clicked)
        if started or sync_clicked:
            st.cache_data.clear()
            status = get_woocommerce_orders_cache_status(history_start, history_end)

    last_refresh = status.get("last_refresh") or "Not synced yet"
    st.markdown(
        (
            "<div class='cycle-sync-banner'>"
            f"<strong>Sync status:</strong> {status['status_message']} "
            f"<br><strong>Last refresh:</strong> {last_refresh} "
            f"<br><strong>Load estimate:</strong> {estimate_woocommerce_load_time(history_start, history_end)}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )

    if not has_credentials and not status.get("cache_exists"):
        st.warning("WooCommerce credentials are missing and no local cycle cache is available yet.")
        return

    load_live = use_live_refresh and has_credentials and not status.get("is_running", False)
    sales_df = _load_cycle_sales(history_start, history_end, use_live_refresh=load_live)
    if sales_df.empty and has_credentials and not load_live:
        sales_df = _load_cycle_sales(history_start, history_end, use_live_refresh=True)

    if sales_df.empty:
        st.warning("No WooCommerce orders are available for the requested business-cycle window yet.")
        return

    orders = prepare_cycle_orders(sales_df)
    if orders.empty:
        st.warning("Cycle analytics could not build an order-level view from the loaded sales data.")
        return

    render_loaded_date_context(
        requested_start=history_start,
        requested_end=history_end,
        loaded_start=sales_df["order_date"],
        loaded_end=sales_df["order_date"],
        label="Loaded cycle activity",
    )

    current_new = calculate_cycle_metrics(orders, current_window.start, current_window.end, "new")
    current_shipped = calculate_cycle_metrics(orders, current_window.start, current_window.end, "shipped")
    previous_new = calculate_cycle_metrics(orders, previous_window.start, previous_window.end, "new")
    previous_shipped = calculate_cycle_metrics(orders, previous_window.start, previous_window.end, "shipped")

    top_new_items = _build_top_new_items(sales_df, current_window.start, current_window.end)
    top_shipped_cities = _build_top_shipped_cities(orders, current_window.start, current_window.end)
    touched_orders = _build_touched_orders(orders, current_window.start, current_window.end)
    trend_df = _build_cycle_trend_frame(orders, windows)

    headline_value, headline_help = _headline_text(current_new, current_shipped)
    render_highlight_stat("Operational balance", headline_value, headline_help)

    story_bullets = _build_story_bullets(
        current_new,
        current_shipped,
        previous_new,
        previous_shipped,
        top_new_items,
        top_shipped_cities,
    )
    render_commentary_panel("Cycle story", story_bullets)

    shipped_orders_mask = orders["status_bucket"].eq("shipped")
    shipped_missing_anchor = int(shipped_orders_mask.sum() - orders.loc[shipped_orders_mask, "shipped_date"].notna().sum())
    render_audit_card(
        "Metric logic",
        (
            "New-order metrics use order creation time. Shipped metrics use shipped timestamps and fall back to order creation time "
            f"for {max(shipped_missing_anchor, 0)} shipped orders that do not carry a shipped timestamp."
        ),
    )

    cycle_col_1, cycle_col_2 = st.columns(2)
    with cycle_col_1:
        _render_cycle_window_card("Current closed cycle", current_window, "This is the latest completed operating window.")
    with cycle_col_2:
        _render_cycle_window_card("Previous cycle", previous_window, "Use this as the direct baseline for delta metrics.")

    tab_story, tab_trends, tab_ledger = st.tabs(["Cycle Scorecard", "Recent Trend", "Order Ledger"])

    with tab_story:
        story_col_1, story_col_2 = st.columns(2)
        with story_col_1:
            _render_metric_block(
                "New-order intake",
                current_new,
                previous_new,
                "Measures orders created inside the cycle with pending, processing, or on-hold status.",
            )
        with story_col_2:
            _render_metric_block(
                "Fulfillment throughput",
                current_shipped,
                previous_shipped,
                "Measures shipped or completed orders using shipped timestamps when they exist.",
            )

        viz_col_1, viz_col_2 = st.columns(2)
        with viz_col_1:
            scorecard_df = pd.DataFrame(
                {
                    "Metric": ["Orders", "Items", "Revenue", "AOV"],
                    "Current New": [
                        current_new["num_orders"],
                        current_new["items_sold"],
                        current_new["revenue"],
                        current_new["basket_value"],
                    ],
                    "Current Shipped": [
                        current_shipped["num_orders"],
                        current_shipped["items_sold"],
                        current_shipped["revenue"],
                        current_shipped["basket_value"],
                    ],
                }
            )
            fig_compare = go.Figure(
                data=[
                    go.Bar(name="Current New", x=scorecard_df["Metric"], y=scorecard_df["Current New"]),
                    go.Bar(name="Current Shipped", x=scorecard_df["Metric"], y=scorecard_df["Current Shipped"]),
                ]
            )
            fig_compare.update_layout(
                title="Current Cycle Intake vs Fulfillment",
                barmode="group",
                height=380,
                margin=dict(l=12, r=12, t=52, b=12),
            )
            st.plotly_chart(fig_compare, use_container_width=True)

        with viz_col_2:
            status_mix = touched_orders["order_status"].value_counts().reset_index()
            status_mix.columns = ["Status", "Orders"]
            if not status_mix.empty:
                fig_status = px.pie(
                    status_mix,
                    names="Status",
                    values="Orders",
                    hole=0.55,
                    title="Current Cycle Status Mix",
                )
                fig_status.update_layout(height=380, margin=dict(l=12, r=12, t=52, b=12))
                st.plotly_chart(fig_status, use_container_width=True)
            else:
                st.info("No order-status mix is available for the current cycle.")

        insight_col_1, insight_col_2 = st.columns(2)
        with insight_col_1:
            if not top_new_items.empty:
                fig_items = px.bar(
                    top_new_items.sort_values("units"),
                    x="units",
                    y="item_name",
                    orientation="h",
                    title="Top Intake Products by Units",
                    color="units",
                    color_continuous_scale="Tealgrn",
                )
                fig_items.update_layout(height=380, margin=dict(l=12, r=12, t=52, b=12))
                st.plotly_chart(fig_items, use_container_width=True)
            else:
                st.info("No new-order product mix was found in the latest cycle.")

        with insight_col_2:
            if not top_shipped_cities.empty:
                fig_cities = px.bar(
                    top_shipped_cities.sort_values("revenue"),
                    x="revenue",
                    y="city",
                    orientation="h",
                    title="Top Shipped Cities by Revenue",
                    color="orders",
                    color_continuous_scale="Blues",
                )
                fig_cities.update_layout(height=380, margin=dict(l=12, r=12, t=52, b=12))
                st.plotly_chart(fig_cities, use_container_width=True)
            else:
                st.info("City-level shipped revenue is not available for the latest cycle.")

    with tab_trends:
        revenue_trend = trend_df.melt(
            id_vars=["Cycle", "Window"],
            value_vars=["New Revenue", "Shipped Revenue"],
            var_name="Stream",
            value_name="Revenue",
        )
        fig_revenue = px.bar(
            revenue_trend,
            x="Cycle",
            y="Revenue",
            color="Stream",
            barmode="group",
            hover_data=["Window"],
            title="Revenue by Closed Cycle",
            color_discrete_map={"New Revenue": "#14b8a6", "Shipped Revenue": "#0f4c81"},
        )
        fig_revenue.update_layout(height=420, margin=dict(l=12, r=12, t=56, b=12))
        st.plotly_chart(fig_revenue, use_container_width=True)

        order_trend = trend_df.melt(
            id_vars=["Cycle", "Window"],
            value_vars=["New Orders", "Shipped Orders"],
            var_name="Stream",
            value_name="Orders",
        )
        fig_orders = px.line(
            order_trend,
            x="Cycle",
            y="Orders",
            color="Stream",
            markers=True,
            hover_data=["Window"],
            title="Order Flow Across Recent Cycles",
            color_discrete_map={"New Orders": "#14b8a6", "Shipped Orders": "#0f4c81"},
        )
        fig_orders.update_layout(height=420, margin=dict(l=12, r=12, t=56, b=12))
        st.plotly_chart(fig_orders, use_container_width=True)

        st.dataframe(
            trend_df,
            column_config={
                "Cycle": "Cycle",
                "Window": "Window",
                "Span Hours": st.column_config.NumberColumn("Hours", format="%d"),
                "New Orders": st.column_config.NumberColumn("New Orders", format="%d"),
                "New Revenue": st.column_config.NumberColumn("New Revenue", format="Tk %.0f"),
                "Shipped Orders": st.column_config.NumberColumn("Shipped Orders", format="%d"),
                "Shipped Revenue": st.column_config.NumberColumn("Shipped Revenue", format="Tk %.0f"),
            },
            use_container_width=True,
            height=320,
        )

    with tab_ledger:
        ledger_df = touched_orders.copy()
        if ledger_df.empty:
            st.info("No orders landed in the latest closed cycle.")
        else:
            ledger_df = ledger_df[
                [
                    "cycle_bucket",
                    "cycle_anchor",
                    "order_id",
                    "customer_name",
                    "city",
                    "order_status",
                    "order_date",
                    "shipped_date",
                    "qty",
                    "order_total",
                    "source",
                ]
            ].rename(
                columns={
                    "cycle_bucket": "Cycle Bucket",
                    "cycle_anchor": "Cycle Anchor",
                    "order_id": "Order ID",
                    "customer_name": "Customer",
                    "city": "City",
                    "order_status": "Status",
                    "order_date": "Order Date",
                    "shipped_date": "Shipped Date",
                    "qty": "Items",
                    "order_total": "Revenue",
                    "source": "Source",
                }
            )
            st.dataframe(
                ledger_df,
                column_config={
                    "Cycle Anchor": st.column_config.DatetimeColumn("Cycle Anchor", format="YYYY-MM-DD HH:mm"),
                    "Order Date": st.column_config.DatetimeColumn("Order Date", format="YYYY-MM-DD HH:mm"),
                    "Shipped Date": st.column_config.DatetimeColumn("Shipped Date", format="YYYY-MM-DD HH:mm"),
                    "Items": st.column_config.NumberColumn("Items", format="%d"),
                    "Revenue": st.column_config.NumberColumn("Revenue", format="Tk %.0f"),
                },
                use_container_width=True,
                height=420,
            )
            st.download_button(
                "Download Current Cycle Ledger",
                data=ledger_df.to_csv(index=False),
                file_name=f"business_cycle_ledger_{current_window.end.strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
                use_container_width=True,
            )
