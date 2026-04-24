"""Optimized Main Dashboard Controller using a modular library structure."""

from __future__ import annotations

import base64
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from BackEnd.services.hybrid_data_loader import (
    get_woocommerce_orders_cache_status,
    load_cached_woocommerce_stock_data,
    load_hybrid_data,
    start_orders_background_refresh,
)
from FrontEnd.utils.config import DATA_SYNC_MODE
from FrontEnd.components import ui
from .dashboard_lib.data_helpers import (
    apply_global_filters,
    build_order_level_dataset,
    estimate_line_revenue,
    prune_dataframe,
    sum_order_level_revenue,
)

DASHBOARD_SALES_COLUMNS = [
    "order_id", "order_date", "order_total", "customer_key", "customer_name",
    "order_status", "source", "city", "state", "qty", "item_name",
    "item_revenue", "line_total", "item_cost", "price", "sku", "Category", "Coupons"
]

# Backwards-compatible aliases for tests and older imports.
_build_order_level_dataset = build_order_level_dataset
_estimate_line_revenue = estimate_line_revenue
_sum_order_level_revenue = sum_order_level_revenue

SECTIONS_REQUIRING_CUSTOMERS = {"💎 Sales Overview", "👥 Customer Insight", "🚀 Data Pilot"}
SECTIONS_REQUIRING_ML = {"💎 Sales Overview", "📦 Stock Insight"}
SECTIONS_REQUIRING_STOCK = {"📥 Sales Data Ingestion", "📦 Stock Insight", "🚀 Data Pilot"}


@st.cache_data(show_spinner=False)
def _load_banner_base64() -> str | None:
    banner_path = Path(__file__).resolve().parents[1] / "assets" / "data_analytics_banner.png"
    if not banner_path.exists():
        return None
    return base64.b64encode(banner_path.read_bytes()).decode("ascii")


def _render_banner():
    encoded_string = _load_banner_base64()
    if not encoded_string:
        st.markdown(
            '<div class="live-indicator"><span class="live-dot"></span>System Online | Intelligence Hub Active</div>',
            unsafe_allow_html=True,
        )
        return

    st.markdown(
        f"""
        <div style="position: relative; margin-bottom: 25px; border-radius: 12px; overflow: hidden; height: 160px; border: 1px solid rgba(255,255,255,0.1); box-shadow: 0 6px 16px rgba(0,0,0,0.3);">
            <img src="data:image/png;base64,{encoded_string}" style="width: 100%; height: 100%; object-fit: cover; opacity: 0.8;">
            <div style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; background: linear-gradient(to right, rgba(0,0,0,0.85) 0%, rgba(0,0,0,0.3) 50%, rgba(0,0,0,0) 100%); display: flex; flex-direction: column; justify-content: center; padding-left: 30px;">
                <h1 style="color: white; margin: 0; font-size: 2.2rem; font-weight: 800; letter-spacing: -1px; text-shadow: 2px 2px 4px rgba(0,0,0,0.5);">
                    DEEN <span style="color: #3b82f6;">Business Intelligence</span>
                </h1>
                <div style="display: flex; align-items: center; margin-top: 12px; background: rgba(16, 185, 129, 0.1); border: 1px solid rgba(16, 185, 129, 0.2); padding: 4px 12px; border-radius: 20px; width: fit-content;">
                    <div style="width: 8px; height: 8px; background: #10b981; border-radius: 50%; margin-right: 10px; box-shadow: 0 0 10px #10b981; animation: pulse 2s infinite;"></div>
                    <span style="color: #10b981; font-size: 0.75rem; font-weight: 700; text-transform: uppercase; letter-spacing: 1px;">Live Operational Intelligence Hub</span>
                </div>
            </div>
        </div>
        <style>
            @keyframes pulse {{
                0% {{ transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }}
                70% {{ transform: scale(1); box-shadow: 0 0 0 6px rgba(16, 185, 129, 0); }}
                100% {{ transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }}
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _serialize_context_value(value) -> str:
    if isinstance(value, (list, tuple, set)):
        return ",".join(str(item) for item in value)
    return str(value)


def _get_window_config(window: str) -> dict[str, str | int | date]:
    today = date.today()
    start_dt = today
    end_dt = today

    if window == "MTD":
        days_back = (today - today.replace(day=1)).days
    elif window == "YTD":
        days_back = (today - today.replace(month=1, day=1)).days
    elif window == "Custom Date Range":
        start_dt = st.session_state.get("wc_sync_start_date", today)
        end_dt = st.session_state.get("wc_sync_end_date", today)
        days_back = (end_dt - start_dt).days
    else:
        window_map = {
            "Last Day": 1,
            "Last 3 Days": 3,
            "Last 7 Days": 7,
            "Last 15 Days": 15,
            "Last Month": 30,
            "Last 3 Months": 90,
            "Last Quarter": 90,
            "Last Half Year": 180,
            "Last 9 Months": 270,
            "Last Year": 365,
        }
        days_back = window_map.get(window, 30)
        start_dt = today - timedelta(days=days_back)
        end_dt = today

    if window == "Custom Date Range":
        duration = max(1, days_back)
        prev_end_date_str = (start_dt - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_start_date_str = (start_dt - timedelta(days=duration)).strftime("%Y-%m-%d")
    else:
        prev_start_date_str = (today - timedelta(days=days_back * 2)).strftime("%Y-%m-%d")
        prev_end_date_str = (today - timedelta(days=days_back + 1)).strftime("%Y-%m-%d")

    return {
        "days_back": max(1, days_back),
        "start_dt": start_dt,
        "end_dt": end_dt,
        "start_date_str": start_dt.strftime("%Y-%m-%d"),
        "end_date_str": end_dt.strftime("%Y-%m-%d"),
        "prev_start_date_str": prev_start_date_str,
        "prev_end_date_str": prev_end_date_str,
    }


def _needs_category_enrichment(categories: list[str]) -> bool:
    return bool(categories and "All" not in categories)


def _ensure_categories(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "Category" in df.columns and df["Category"].notna().any():
        return df

    from BackEnd.core.categories import get_category_for_sales

    enriched = df.copy()
    enriched["Category"] = enriched["item_name"].apply(get_category_for_sales)
    return enriched


def _filter_stock_by_categories(stock_df: pd.DataFrame, categories: list[str]) -> pd.DataFrame:
    if stock_df.empty or not _needs_category_enrichment(categories):
        return stock_df

    mask = pd.Series(False, index=stock_df.index)
    for category in categories:
        mask |= stock_df["Category"].str.startswith(category, na=False)
    return stock_df[mask]


def _render_initial_sync_placeholder(start_date_str: str, end_date_str: str, status_message: str):
    st.markdown(
        """
        <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 20px; padding: 12px 16px; background: linear-gradient(90deg, rgba(59,130,246,0.1) 0%, rgba(59,130,246,0.05) 100%); border-left: 3px solid #3b82f6; border-radius: 8px;">
            <span style="animation: spin 1s linear infinite;">🔄</span>
            <div>
                <div style="font-weight: 600; color: #3b82f6; font-size: 0.9rem;">Initializing Data Stream...</div>
                <div style="font-size: 0.75rem; color: #64748b;">First-time WooCommerce sync is building the local cache in the background.</div>
            </div>
        </div>
        <style>@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }</style>
        """,
        unsafe_allow_html=True,
    )
    ui.skeleton_row(count=6)
    st.markdown("<br>", unsafe_allow_html=True)
    ui.skeleton_row(count=6)
    st.markdown("<br>", unsafe_allow_html=True)
    st.caption(status_message)
    st_autorefresh(interval=3000, limit=40, key=f"orders_bootstrap_{start_date_str}_{end_date_str}")


def _build_core_dashboard_data(
    *,
    window: str,
    window_config: dict[str, str | int | date],
    global_sync: bool,
    needs_history: bool,
    orders_status: dict,
    global_cats: list[str],
    global_stats: list[str],
) -> dict | None:
    cache_empty = not orders_status.get("cache_exists", False)

    if needs_history:
        st.warning(
            f"⏳ Connecting to WooCommerce Live to sync deep archival history "
            f"({window_config['start_date_str']} to {window_config['end_date_str']}). "
            "This runs in the background and may take a few minutes."
        )

    # Hybrid loading is now the default path.
    should_force = global_sync or (window == "Last Day") or needs_history or cache_empty
    refresh_started = start_orders_background_refresh(
        window_config["start_date_str"],
        window_config["end_date_str"],
        force=should_force,
    )

    if cache_empty and DATA_SYNC_MODE != "direct":
        if refresh_started or orders_status.get("is_running"):
            _render_initial_sync_placeholder(
                window_config["start_date_str"],
                window_config["end_date_str"],
                orders_status.get("status_message", "First WooCommerce sync is running in the background."),
            )
            return None

        with st.spinner("Running first WooCommerce sync..."):
            df_sales_raw = prune_dataframe(
                load_hybrid_data(
                    start_date=window_config["start_date_str"],
                    end_date=window_config["end_date_str"],
                    woocommerce_mode="live",
                    force=should_force,
                ),
                DASHBOARD_SALES_COLUMNS,
            )
    else:
        # Use live mode for Last Day, Custom Date Range, or when global sync requested
        sync_mode = "live" if (window in ("Last Day", "Custom Date Range") or global_sync) else "cache_only"
        df_sales_raw = prune_dataframe(
            load_hybrid_data(
                start_date=window_config["start_date_str"],
                end_date=window_config["end_date_str"],
                woocommerce_mode=sync_mode,
                force=should_force,
            ),
            DASHBOARD_SALES_COLUMNS,
        )

    df_prev_raw = prune_dataframe(
        load_hybrid_data(
            start_date=window_config["prev_start_date_str"],
            end_date=window_config["prev_end_date_str"],
            woocommerce_mode="cache_only",
        ),
        DASHBOARD_SALES_COLUMNS,
    )

    if _needs_category_enrichment(global_cats):
        df_sales_raw = _ensure_categories(df_sales_raw)
        df_prev_raw = _ensure_categories(df_prev_raw)

    df_sales_full = df_sales_raw.copy()
    df_prev_full = df_prev_raw.copy()
    df_sales_full["item_revenue"] = estimate_line_revenue(df_sales_full)
    df_prev_full["item_revenue"] = estimate_line_revenue(df_prev_full)

    df_sales_filtered = apply_global_filters(df_sales_full, global_cats, global_stats)
    df_prev_filtered = apply_global_filters(df_prev_full, global_cats, global_stats)

    valid_statuses = ["completed", "shipped"]
    exclude_statuses = ["cancelled", "failed", "trash"]

    df_sales_strict = df_sales_filtered[df_sales_filtered["order_status"].str.lower().isin(valid_statuses)].copy()
    df_prev_strict = df_prev_filtered[df_prev_filtered["order_status"].str.lower().isin(valid_statuses)].copy()
    df_sales_active = df_sales_filtered[~df_sales_filtered["order_status"].str.lower().isin(exclude_statuses)].copy()
    df_prev_active = df_prev_filtered[~df_prev_filtered["order_status"].str.lower().isin(exclude_statuses)].copy()

    stock_df = load_cached_woocommerce_stock_data()

    return {
        "sales": df_sales_raw,
        "sales_map": pd.DataFrame(),
        "sales_active": df_sales_active,
        "sales_strict": df_sales_strict,
        "prev_sales_active": df_prev_active,
        "prev_sales_strict": df_prev_strict,
        "customers": None,
        "customer_count": None,
        "ml": None,
        "stock": stock_df if isinstance(stock_df, pd.DataFrame) else pd.DataFrame(),
        "summary": {
            "woocommerce_live": len(df_sales_raw),
            "stock_rows": len(stock_df) if isinstance(stock_df, pd.DataFrame) else 0,
        },
        "hint": orders_status.get("status_message", ""),
        "window_label": window.lower() if window != "Custom Date Range" else f"{window_config['days_back']} days",
    }


def _enrich_dashboard_data_for_selection(
    data: dict,
    selection: str,
    global_cats: list[str],
) -> dict:
    if selection in SECTIONS_REQUIRING_STOCK:
        stock_df = data.get("stock", pd.DataFrame())
        if stock_df.empty:
            from BackEnd.services.hybrid_data_loader import load_woocommerce_stock_data

            with st.spinner("📦 Syncing inventory levels..."):
                stock_df = load_woocommerce_stock_data()

        if not stock_df.empty:
            stock_df = stock_df.copy()
            if "Category" not in stock_df.columns or stock_df["Category"].isna().all():
                from BackEnd.core.categories import get_category_for_sales

                stock_df["Category"] = stock_df["Name"].apply(get_category_for_sales)
            stock_df = _filter_stock_by_categories(stock_df, global_cats)

        data["stock"] = stock_df

    if selection in SECTIONS_REQUIRING_CUSTOMERS and data.get("customers") is None:
        from BackEnd.services.customer_insights import generate_customer_insights_from_sales

        with st.spinner("Building customer intelligence..."):
            data["customers"] = generate_customer_insights_from_sales(data["sales_strict"], include_rfm=True)

    if selection in SECTIONS_REQUIRING_ML and data.get("ml") is None:
        from BackEnd.services.ml_insights import build_ml_insight_bundle

        with st.spinner("Generating ML insights..."):
            data["ml"] = build_ml_insight_bundle(data["sales_strict"], data["customers"], horizon_days=7)

    if selection == "👥 Customer Insight" and data.get("customer_count") is None:
        from BackEnd.services.hybrid_data_loader import load_woocommerce_customer_count

        data["customer_count"] = load_woocommerce_customer_count()

    return data


def render_intelligence_hub_page():
    _render_banner()

    global_sync = st.session_state.pop("global_sync_request", False)
    if global_sync:
        st.session_state.pop("dashboard_context_key", None)
        st.session_state.pop("dashboard_data", None)

    window = st.session_state.get("time_window", "Last Month")
    selection = st.session_state.get("active_section", "💎 Sales Overview")
    window_config = _get_window_config(window)
    orders_status = get_woocommerce_orders_cache_status(
        window_config["start_date_str"],
        window_config["end_date_str"],
    )
    needs_history = window == "Custom Date Range" and not orders_status.get("is_covered", True)

    global_cats = st.session_state.get("global_categories", ["All"])
    global_stats = st.session_state.get("global_statuses", ["All"])

    context_key = "|".join(
        [
            window,
            window_config["start_date_str"],
            window_config["end_date_str"],
            window_config["prev_start_date_str"],
            window_config["prev_end_date_str"],
            _serialize_context_value(global_cats),
            _serialize_context_value(global_stats),
            str(orders_status.get("last_refresh") or ""),
        ]
    )

    data = st.session_state.get("dashboard_data")
    if st.session_state.get("dashboard_context_key") != context_key or not isinstance(data, dict):
        st.session_state["dashboard_context_key"] = context_key
        data = _build_core_dashboard_data(
            window=window,
            window_config=window_config,
            global_sync=global_sync,
            needs_history=needs_history,
            orders_status=orders_status,
            global_cats=global_cats,
            global_stats=global_stats,
        )
        if data is None:
            return
        st.session_state["dashboard_data"] = data

    data = _enrich_dashboard_data_for_selection(data, selection, global_cats)
    st.session_state["dashboard_data"] = data

    df_exec = data["sales_active"]
    exec_orders = build_order_level_dataset(df_exec)
    total_rev = sum_order_level_revenue(df_exec, order_df=exec_orders)
    order_count = exec_orders["order_id"].nunique() if not exec_orders.empty else 0
    cust_count = df_exec["customer_key"].nunique() if "customer_key" in df_exec.columns else 0
    total_items = int(df_exec["qty"].sum()) if "qty" in df_exec.columns else 0
    aov = (total_rev / order_count) if order_count else 0
    avg_orders_per_day = order_count / max(1, int(window_config["days_back"]))

    df_prev_comp = data["prev_sales_active"]
    prev_orders_level = build_order_level_dataset(df_prev_comp)
    prev_items_val = df_prev_comp["qty"].sum() if not df_prev_comp.empty else 0
    prev_rev_val = sum_order_level_revenue(df_prev_comp, order_df=prev_orders_level)
    prev_orders_val = prev_orders_level["order_id"].nunique() if not prev_orders_level.empty else 0
    prev_aov_val = (prev_rev_val / prev_orders_val) if prev_orders_val else 0
    prev_cust_val = df_prev_comp["customer_key"].nunique() if not df_prev_comp.empty else 0
    prev_avg_orders_val = prev_orders_val / max(1, int(window_config["days_back"]))

    def calc_delta(curr, prev):
        if not prev or prev <= 0:
            return "", 0
        diff = curr - prev
        pct = diff / prev * 100
        return f"{pct:+.1f}% vs {data['window_label']}", diff

    d_items_label, d_items_val = calc_delta(total_items, prev_items_val)
    d_rev_label, d_rev_val = calc_delta(total_rev, prev_rev_val)
    d_orders_label, d_orders_val = calc_delta(order_count, prev_orders_val)
    d_avg_label, d_avg_val = calc_delta(avg_orders_per_day, prev_avg_orders_val)
    d_cust_label, d_cust_val = calc_delta(cust_count, prev_cust_val)
    d_aov_label, d_aov_val = calc_delta(aov, prev_aov_val)

    def format_compact(num):
        if pd.isna(num): return "0"
        if num >= 1_000_000: return f"{num/1_000_000:.1f}M".replace(".0M", "M")
        if num >= 1_000: return f"{num/1_000:.1f}K".replace(".0K", "K")
        return f"{num:,.0f}" if isinstance(num, (int, float)) else str(num)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        ui.icon_metric("Total Items Sold", format_compact(total_items), icon="📦", delta=d_items_label, delta_val=d_items_val)
    with c2:
        ui.icon_metric("Revenue", f"৳{format_compact(total_rev)}", icon="💰", delta=d_rev_label, delta_val=d_rev_val)
    with c3:
        ui.icon_metric("Orders", format_compact(order_count), icon="🛒", delta=d_orders_label, delta_val=d_orders_val)
    with c4:
        ui.icon_metric("Avg. Orders / Day", format_compact(avg_orders_per_day), icon="📅", delta=d_avg_label, delta_val=d_avg_val)
    with c5:
        ui.icon_metric("Customers", format_compact(cust_count), icon="👥", delta=d_cust_label, delta_val=d_cust_val)
    with c6:
        ui.icon_metric("Basket Size", f"৳{format_compact(aov)}", icon="💎", delta=d_aov_label, delta_val=d_aov_val)

    st.markdown("<br>", unsafe_allow_html=True)

    if selection == "💎 Sales Overview":
        # --- Sales Integrity Gap Chart ---
        from BackEnd.services.returns_tracker import calculate_net_sales_metrics
        import plotly.graph_objects as go
        import numpy as np
        
        returns_df = st.session_state.get("returns_data", pd.DataFrame())
        
        # Apply Global Date Range Filter to Returns
        start_date = pd.to_datetime(window_config["start_date_str"]).date()
        end_date = pd.to_datetime(window_config["end_date_str"]).date()
        
        if not returns_df.empty and "date" in returns_df.columns:
            returns_df = returns_df.copy()
            returns_df["_dt"] = pd.to_datetime(returns_df["date"], errors="coerce").dt.date
            returns_df = returns_df[(returns_df["_dt"] >= start_date) & (returns_df["_dt"] <= end_date)]
            returns_df = returns_df.drop(columns=["_dt"])
            
        ret_metrics = calculate_net_sales_metrics(returns_df, sales_df=data["sales_active"], total_items_sold=total_items)
        fin_plot = ret_metrics.get("daily_financials", pd.DataFrame()).copy()
        
        if not fin_plot.empty:
            fin_plot["date"] = pd.to_datetime(fin_plot["date"], errors="coerce")
            fin_plot = fin_plot.dropna(subset=["date"])
            
            # Strict boundary filter for the plot timeline
            fin_plot = fin_plot[(fin_plot["date"].dt.date >= start_date) & (fin_plot["date"].dt.date <= end_date)].sort_values("date")
            
            if not fin_plot.empty:
                st.markdown("#### ⚖️ Sales Integrity Gap (Gross vs. Net Settled)")
                st.caption(f"Visualizing revenue efficiency for the selected period (**{start_date.strftime('%B %d, %Y')}** to **{end_date.strftime('%B %d, %Y')}**). The shaded red area represents revenue lost to returns.")
                fig_gap = go.Figure()
                
                custom_data = np.stack((fin_plot['gross_sales'], fin_plot['total_loss']), axis=-1)

                # Layer 1: Deep Red flame base
                fig_gap.add_trace(go.Scatter(
                    x=fin_plot['date'], y=fin_plot['net_sales'] * 0.3,
                    fill='tozeroy', mode='lines',
                    line=dict(color='rgba(220, 20, 60, 0.3)', width=0),
                    fillcolor='rgba(220, 20, 60, 0.4)',
                    name='Net Settled Base', stackgroup='one', hoverinfo='skip', showlegend=False
                ))
                
                # Layer 2: Orange-red
                fig_gap.add_trace(go.Scatter(
                    x=fin_plot['date'], y=fin_plot['net_sales'] * 0.6,
                    fill='tonexty', mode='lines',
                    line=dict(color='rgba(255, 69, 0, 0.4)', width=0),
                    fillcolor='rgba(255, 69, 0, 0.5)',
                    name='Net Settled Core', stackgroup='one', hoverinfo='skip', showlegend=False
                ))

                # Layer 3: Flame Orange (Net Settled)
                fig_gap.add_trace(go.Scatter(
                    x=fin_plot['date'], y=fin_plot['net_sales'],
                    fill='tonexty', mode='lines',
                    line=dict(color='rgba(255, 140, 0, 1.0)', width=3),
                    fillcolor='rgba(255, 140, 0, 0.6)',
                    name='Net Settled', stackgroup='one',
                    customdata=custom_data,
                    hovertemplate='<b>Net Settled:</b> ৳%{y:,.0f}<br><b>Loss:</b> ৳%{customdata[1]:,.0f}<extra></extra>'
                ))
                
                # Layer 4: Golden Yellow (Gross Verified)
                fig_gap.add_trace(go.Scatter(
                    x=fin_plot['date'], y=fin_plot['gross_sales'],
                    fill='tonexty', mode='lines',
                    line=dict(color='rgba(255, 215, 0, 1.0)', width=2),
                    fillcolor='rgba(255, 215, 0, 0.4)',
                    name='Gross Verified', stackgroup='one',
                    hovertemplate='<b>Gross Sales:</b> ৳%{y:,.0f}<extra></extra>'
                ))

                fig_gap.update_layout(
                    height=350, 
                    paper_bgcolor='rgba(0,0,0,0)', 
                    plot_bgcolor='rgba(0,0,0,0)', 
                    margin=dict(l=0, r=0, t=15, b=0), 
                    hovermode="x unified", 
                    legend=dict(orientation="h", y=1.15, x=0.5, xanchor="center"),
                    xaxis=dict(showgrid=False, title="", tickformat="%b %d", fixedrange=True),
                    yaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.1)", title="Revenue (৳)", fixedrange=True)
                )
                st.plotly_chart(fig_gap, width="stretch", config={'displayModeBar': False})
                st.divider()

        from BackEnd.services.strategic_intelligence import generate_executive_narrative
        from .dashboard_lib.story import render_dashboard_story

        story_points = render_dashboard_story(
            data["sales_active"],
            data.get("customers", pd.DataFrame()),
            data.get("ml") or {},
            window,
            df_prev_sales=data["prev_sales_active"],
            return_raw=True,
        )
        briefing_points = generate_executive_narrative(
            data["sales_active"],
            st.session_state.get("returns_data", pd.DataFrame()),
            total_rev,
            prev_rev_val,
        )
        all_points = story_points + briefing_points

        with st.container():
            st.markdown(
                f"""
                <div style="background: linear-gradient(135deg, rgba(30, 58, 138, 0.08) 0%, rgba(30, 27, 75, 0.05) 100%);
                            border-left: 5px solid #3b82f6; border-radius: 12px; padding: 24px; margin: 20px 0;
                            border: 1px solid rgba(59, 130, 246, 0.1); box-shadow: 0 4px 15px rgba(0,0,0,0.15);">
                    <div style="display: flex; align-items: center; margin-bottom: 15px;">
                        <div style="font-size: 1.2rem; margin-right: 12px;">💎</div>
                        <div style="font-weight: 800; color: #3b82f6; font-size: 0.85rem; letter-spacing: 1.5px; text-transform: uppercase;">
                            Strategic Executive Intelligence
                        </div>
                    </div>
                    <div style="font-size: 0.95rem; line-height: 1.7; color: var(--text-color);">
                        {"<div style='margin-bottom:10px;'>" + "</div><div style='margin-bottom:10px;'>".join([f"• {point}" for point in all_points]) + "</div>"}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        st.divider()
        
        from .dashboard_lib.bi_analytics import render_sales_overview_timeseries
        render_sales_overview_timeseries(data["sales_active"], ml_bundle=data.get("ml") or {})

        st.divider()

        # --- Strategic Export ---
        ex_col1, ex_col2 = st.columns([3, 1])
        with ex_col1:
            st.markdown("#### 💎 Executive Strategic Export")
            st.caption("Generate a professional multi-sheet report containing the current filtered dataset and key performance metrics.")
        with ex_col2:
            summary_metrics = {
                "Report Window": window,
                "Gross Revenue (৳)": f"{total_rev:,.2f}",
                "Order Volume": f"{order_count:,}",
                "Customer Count": f"{cust_count:,}",
                "Units Sold": f"{total_items:,}",
                "AOV (৳)": f"{total_rev/order_count:,.2f}" if order_count > 0 else "0",
                "Growth vs Prev": d_rev_label,
                "Generated At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Additional analysis: Top Categories
            from BackEnd.core.categories import get_subcategory_name
            cat_df = data["sales_active"].copy()
            cat_df["Sub-Category"] = cat_df["Category"].apply(get_subcategory_name)
            top_cats = cat_df.groupby("Sub-Category")["item_revenue"].sum().reset_index().sort_values("item_revenue", ascending=False).head(10)
            
            ai_insights = pd.DataFrame({"Executive Narrative & Intelligence Briefing": all_points})
            
            additional_sheets = {
                "Top Categories": top_cats,
                "AI Briefing": ai_insights
            }
            
            if data.get("ml") and "forecast" in data["ml"]:
                fc = data["ml"]["forecast"]
                if not fc.empty:
                    additional_sheets["ML Forecasts"] = fc[["item_name", "forecast_7d_units", "risk_level", "reorder_comment"]]
            if data.get("ml") and "anomalies" in data["ml"]:
                an = data["ml"]["anomalies"]
                if not an.empty:
                    additional_sheets["Detected Anomalies"] = an[["order_day", "metric", "direction", "commentary"]]
            
            report_bytes = ui.export_to_excel(
                data["sales_active"].drop(columns=[c for c in data["sales_active"].columns if c.startswith("_")], errors="ignore"),
                sheet_name="Sales Data",
                summary_metrics=summary_metrics,
                additional_sheets=additional_sheets
            )
            
            st.download_button(
                label="📊 Export Full Report",
                data=report_bytes,
                file_name=f"deen_sales_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="sales_overview_export_btn"
            )
        st.divider()

    elif selection == "📊 Traffic & Acquisition":
        from .dashboard_lib.acquisition import render_acquisition_analytics

        render_acquisition_analytics(data["sales_active"])

    elif selection == "👥 Customer Insight":
        st.subheader("Customer Insight")
        if "customer_key" in df_exec.columns:
            is_registered = df_exec["customer_key"].str.startswith("reg_", na=False)
            reg_val = df_exec[is_registered]["item_revenue"].sum() if is_registered.any() else 0
            guest_val = df_exec[~is_registered]["item_revenue"].sum() if (~is_registered).any() else 0
        else:
            reg_val = 0
            guest_val = 0
        render_customer_insight_tab(reg_val, guest_val, data.get("customer_count", 0), data["sales_active"])

    elif selection == "🔄 Returns Insights":
        from .dashboard_lib.returns_tracker import render_returns_tracker_page

        render_returns_tracker_page()

    elif selection == "📥 Sales Data Ingestion":
        from .dashboard_lib.deep_dive import render_deep_dive_tab

        render_deep_dive_tab(data["sales_active"], data["stock"], data["prev_sales_active"], window_label=data["window_label"])

    elif selection == "📦 Stock Insight":
        from .dashboard_lib.inventory import render_inventory_health

        render_inventory_health(data["stock"], (data.get("ml") or {}).get("forecast"), data["sales"])

    elif selection == "🚀 Data Pilot":
        render_data_pilot_page(data["sales"], data["stock"], data.get("customers"))


# --- MERGED COMPONENT LOGIC ---

def render_customer_insight_tab(reg_rev: float, guest_rev: float, total_accounts: int, df_sales: pd.DataFrame = None):
    """Enhanced Customer Intelligence Component with deep-dive analysis.
    
    This enhanced version combines the original segment analysis with
    the new Customer Insight module for dynamic filtering and detailed
    customer reports.
    """
    from .dashboard_lib.customer_insight_page import render_enhanced_customer_insight_tab

    render_enhanced_customer_insight_tab(reg_rev, guest_rev, total_accounts, df_sales)


# End of Dashboard controller logic

def render_data_pilot_page(sales_df: pd.DataFrame, stock_df: pd.DataFrame, customers_df: pd.DataFrame = None):
    """The AI-first command interface for natural language operations and Strategic Intelligence."""
    st.markdown('<div class="live-indicator"><span class="live-dot" style="background:#4f46e5; box-shadow: 0 0 10px #4f46e5;"></span>Intelligence Center Active | Data Pilot v11.0</div>', unsafe_allow_html=True)
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "🚀 Ask Data Pilot", 
        "🚨 Strategic War-Room", 
        "📦 Market Basket Intel",
        "🛡️ Data Trust"
    ])
    
    with tab1:
        st.markdown("### 🤖 Operations Data Pilot")
        st.caption("Ask natural language questions about your e-commerce health, stockouts, or revenue trends.")
        from FrontEnd.components.insights import render_ai_pilot_chat
        render_ai_pilot_chat(sales_df)
    
    with tab2:
        from .dashboard_lib.war_room import render_war_room_page
        returns_df = st.session_state.get("returns_data", pd.DataFrame())
        render_war_room_page(sales_df, returns_df)

    with tab3:
        st.markdown("#### 🔍 Automated Market Basket Intelligence")
        from BackEnd.services.affinity_engine import MarketBasketEngine
        engine = MarketBasketEngine(sales_df)
        rules = engine.get_associations(min_lift=1.2)
        
        if not rules.empty:
            st.markdown("##### Detected High-Lift Product Affinities")
            st.dataframe(rules[["Antecedent", "Consequent", "Lift", "Confidence", "Frequency"]].head(10), width="stretch", hide_index=True)
            
            # Bundle Fulfillment
            st.markdown("##### 📦 Bundle Fulfillment Analysis")
            from BackEnd.services.inventory_intel import InventoryIntelligence
            if stock_df.empty:
                st.info("Stock data unavailable. Sync inventory to see fulfillment analysis.")
            else:
                inv_intel = InventoryIntelligence(sales_df, stock_df)
                pairs = rules.head(5).apply(lambda x: {'A': x['Antecedent'], 'B': x['Consequent']}, axis=1).tolist()
                bundles = inv_intel.calculate_bundle_fulfillment(pairs)
                st.dataframe(bundles, width="stretch", hide_index=True)
        else:
            st.info("Insufficient transaction density to discover complex product associations. Check back after more orders.")

    with tab4:
        from .dashboard_lib.audit import render_data_audit, render_data_trust_panel

        st.markdown("### 🛡️ System Reliability Audit")
        render_data_trust_panel(sales_df)
        if customers_df is not None:
            render_data_audit(sales_df, customers_df)
        else:
            st.warning("Customer data unavailable for deep audit.")
