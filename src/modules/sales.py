import json
import os
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

from src.core.categories import get_category_for_sales
from src.core.paths import prepare_data_dirs, SYSTEM_LOG_FILE
from src.core.sync import (
    DEFAULT_GSHEET_URL,
    LIVE_SALES_TAB_NAME,
    clear_sync_cache,
    load_published_sheet_tabs,
    load_shared_gsheet,
)
from src.services.live_ops import (
    LIVE_STREAM_REFRESH_SECONDS,
    load_live_queue,
    run_archive_if_requested,
)
from src.services.master_sales import load_master_sales_dataset
from src.data.normalized_sales import (
    CANONICAL_COLUMNS,
    compute_sales_analytics,
    normalize_sales_dataframe,
)
from src.ui.components import (
    render_ops_hero,
    render_ops_kpi,
    render_ops_list,
    section_card,
)
from src.utils.data import find_columns, parse_dates

# CONFIGURATION
TOTAL_SALES_EXCLUDED_TABS = {"lastdaysales", "latestsales"}
prepare_data_dirs()

# --- SYSTEM HELPERS ---


def log_system_event(event_type, details):
    log_file = SYSTEM_LOG_FILE
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = {"timestamp": timestamp, "type": event_type, "details": details}
    try:
        logs = []
        if os.path.exists(log_file):
            with open(log_file, "r", encoding="utf-8") as f:
                logs = json.load(f)
        logs.append(log_entry)
        with open(log_file, "w", encoding="utf-8") as f:
            json.dump(logs, f, indent=4)
    except Exception:
        pass


def get_setting(key, default=None):
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, default)


def get_custom_report_tab_label():
    return "Total Sales Report"


# --- ANALYTICS ENGINE ---


@st.cache_data(show_spinner=False, max_entries=20)
def process_data(df, selected_cols):
    """
    Legacy wrapper that now leverages src.data.normalized_sales logic 
    for consistency across the application.
    """
    try:
        df = df.copy()
        
        # Map internal names for analytics if needed
        # Mapping logic to convert selected_cols format to normalize_sales_dataframe expectations
        # However, for simplicity and to maintain current behavior, we ensure Internal columns exist
        df["item_name"] = df[selected_cols["name"]].fillna("Unknown Product").astype(str)
        df["unit_price"] = pd.to_numeric(df[selected_cols["cost"]], errors="coerce").fillna(0)
        df["qty"] = pd.to_numeric(df[selected_cols["qty"]], errors="coerce").fillna(0)
        
        c_col = selected_cols.get("customer_name")
        df["customer_name"] = (
            df[c_col].fillna("Unknown Customer").astype(str)
            if c_col and c_col in df.columns
            else "N/A"
        )
        
        # Internal aliases for backward compatibility in the rest of the script
        df["Internal_Name"] = df["item_name"]
        df["Internal_Cost"] = df["unit_price"]
        df["Internal_Qty"] = df["qty"]
        df["Internal_Customer"] = df["customer_name"]
        df["Total Amount"] = df["unit_price"] * df["qty"]
        df["line_amount"] = df["Total Amount"]
        
        # Ensure category is set
        df["Category"] = df["item_name"].apply(get_category_for_sales)
        df["category"] = df["Category"]

        # Use the centralized analytics engine
        analytics = compute_sales_analytics(df)
        
        return (
            analytics["drilldown"], 
            analytics["summary"], 
            analytics["top_products"], 
            analytics["timeframe"], 
            analytics["basket"], 
            df, 
            analytics["top_customers"]
        )
    except Exception as e:
        log_system_event("CALC_ERROR", str(e))
        return None, None, None, "", {}, None, None

# --- UI RENDERING ---


def format_period_label(start_date, end_date):
    return f"{start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}"


def compute_unique_customer_count(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    if "UID" in df.columns:
        return int(df["UID"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique())

    if "order_key" in df.columns:
        customer_key = (
            df.get("phone", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
            + "|"
            + df.get("email", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
            + "|"
            + df.get("customer_name", pd.Series("", index=df.index)).fillna("").astype(str).str.strip()
        )
        customer_key = customer_key.str.strip("|").replace("", pd.NA).dropna()
        return int(customer_key.nunique())

    if "Internal_Customer" in df.columns:
        customers = (
            df["Internal_Customer"]
            .fillna("")
            .astype(str)
            .str.strip()
            .replace({"": pd.NA, "N/A": pd.NA, "Unknown Customer": pd.NA})
            .dropna()
        )
        return int(customers.nunique())

    return 0


def render_automated_insights(df, sm, bk, deltas=None, cust_metrics=None):
    """
    Generate and render data-driven insights and business suggestions.
    """
    if df is None:
        return

    ins = []
    
    # 1. Revenue & Growth (Sales Analysis Context)
    if sm is not None and not sm.empty:
        total_rev = sm["Total Amount"].sum()
        if deltas and deltas.get("rev") is not None:
            dr = deltas["rev"]
            if dr > 15:
                ins.append(f"🟢 **Growth Alert**: Revenue is surging up **{dr:.1f}%** compared to the previous period. High momentum detected!")
            elif dr < -15:
                ins.append(f"🔴 **Sales Gap**: Revenue is down **{abs(dr):.1f}%**. Suggest reviewing pricing or running a re-engagement campaign.")
        
        # Categorical Insights
        sorted_sm = sm.sort_values("Total Amount", ascending=False)
        if not sorted_sm.empty:
            top_cat = sorted_sm.iloc[0]["Category"]
            top_share = (sorted_sm.iloc[0]["Total Amount"] / max(total_rev, 1)) * 100
            ins.append(f"📊 **Inventory Focus**: '{top_cat}' is your top performer, core to {top_share:.1f}% of your gross revenue.")

    # 2. Customer Activity (Pulse Context)
    if cust_metrics:
        retention = cust_metrics.get("retention", 0)
        if retention < 15:
            ins.append("💡 **Retention Opportunity**: Most customers are one-time buyers. Suggest a loyalty discount for the 2nd order.")
        elif retention > 35:
            ins.append("✨ **Brand Loyalty**: Your retention rate is excellent! Your core audience is returning frequently.")
        
        avg_clv = cust_metrics.get("avg_clv", 0)
        if avg_clv > 2500:
             ins.append(f"💰 **High Value Base**: Avg customer spend is **TK {avg_clv:,.0f}**. Your audience has high purchasing power.")

    # 3. Basket Size (Generic)
    if bk:
        avg_qty = bk.get("avg_basket_qty", 0)
        if avg_qty < 1.3:
            ins.append(f"🛒 **Upsell Hint**: Average basket size is low ({avg_qty:.1f} items). Consider 'Frequently Bought Together' bundles.")

    if ins:
        st.markdown(
            f"""
            <div style="background:var(--accent-soft); border:1px solid #bfdbfe; border-radius:18px; padding:1.15rem; margin-bottom:1.25rem;">
                <div style="font-size:0.72rem; color:var(--accent-strong); font-weight:800; text-transform:uppercase; letter-spacing:0.12em; margin-bottom:0.65rem;">System Insights & Suggestions</div>
                <div style="display:flex; flex-direction:column; gap:0.65rem;">
                    {"".join(f"<div style='font-size:0.88rem; color:var(--text-strong);'> {i}</div>" for i in ins)}
                </div>
            </div>
            """, 
            unsafe_allow_html=True
        )


def render_dashboard_output(
    df,
    dr,
    sm,
    top_prod,
    tf,
    bk,
    src,
    upd,
    top_cust=None,
    show_full_raw=False,
    display_period=None,
):
    pie_colors = ["#2563eb", "#0f766e", "#ea580c", "#7c3aed", "#0891b2", "#be185d"]
    from io import BytesIO
    from src.ui.components import render_plotly_chart, render_status_strip

    sorted_summ = sm.sort_values("Total Amount", ascending=False).copy()
    unique_customers = compute_unique_customer_count(df)

    render_ops_hero(
        "Sales Analysis",
        "Historical sales performance from the workbook core plus the latest 2026 delta.",
        [
            f"Period {display_period or tf or 'All Records'}",
            f"Source {src or 'Local'}",
            f"Refresh {upd or 'N/A'}",
        ],
    )
    render_status_strip(
        source=src or "Local",
        rows=len(df),
        last_refresh="N/A",
        status="Active Dataset",
    )
    
    # NEW: Automated Insights
    render_automated_insights(df, sm, bk, deltas=deltas)
    
    st.caption(f"Charts and KPIs reflect {display_period or tf or 'the selected period'}.")

    top_row = st.columns(4)
    with top_row[0]:
        render_ops_kpi(
            "Items Sold", f"{sm['Total Qty'].sum():,.0f}", "Units shipped", delta=deltas.get("qty")
        )
    with top_row[1]:
        render_ops_kpi(
            "Unique Orders",
            f"{bk['total_orders']:,}",
            f"From {bk['total_orders']:,} checkouts",
            delta=deltas.get("orders")
        )
    with top_row[2]:
        render_ops_kpi(
            "Grand Gross",
            f"TK {sm['Total Amount'].sum():,.0f}",
            "Gross line revenue",
            delta=deltas.get("rev")
        )
    with top_row[3]:
        render_ops_kpi(
            "Basket Analysis",
            f"TK {bk['avg_basket_value']:,.0f}",
            f"Avg {bk['avg_basket_qty']:.1f} items per basket",
        )

    chart_a, chart_b = st.columns(2)
    with chart_a:
        fig_pie = px.pie(
            sm.sort_values("Total Amount", ascending=False),
            values="Total Amount",
            names="Category",
            hole=0.55,
            title="Revenue Share by Category",
            color_discrete_sequence=pie_colors,
        )
        render_plotly_chart(fig_pie, key=f"sales_pie_{src or 'default'}")

    with chart_b:
        fig_bar = px.bar(
            sm.sort_values("Total Qty", ascending=True),
            x="Total Qty",
            y="Category",
            orientation="h",
            title="Volume by Category",
            color="Total Qty",
            color_continuous_scale="Blues",
        )
        render_plotly_chart(fig_bar, key=f"sales_bar_{src or 'default'}")

    side_a, side_b = st.columns([1.35, 1])
    with side_a:
        st.markdown("#### Product View")
        st.dataframe(
            top_prod.head(25),
            use_container_width=True,
            hide_index=True,
        )
    with side_b:
        render_ops_list(
            [
                ("Avg Basket Qty", f"{bk['avg_basket_qty']:.1f}"),
                ("Avg basket TK", f"TK {bk['avg_basket_value']:,.0f}"),
                ("Total Item", f"{sm['Total Qty'].sum():,.0f}"),
                ("Total Unique Order", f"{bk['total_orders']:,}"),
                ("Total Unique Customer", f"{compute_unique_customer_count(df):,}"),
            ]
        )

    if display_period:
        date_col = "_p_date" if "_p_date" in df.columns else None
        if date_col is None and "order_date" in df.columns:
            date_col = "order_date"
        if date_col is not None:
            trend_df = df.copy()
            trend_df[date_col] = pd.to_datetime(trend_df[date_col], errors="coerce")
            trend_df = trend_df.dropna(subset=[date_col])
            if not trend_df.empty and "Total Amount" in trend_df.columns:
                bucket = "D"
                if trend_df[date_col].dt.date.nunique() > 62:
                    bucket = "M"
                revenue_trend = (
                    trend_df.groupby(trend_df[date_col].dt.to_period(bucket))["Total Amount"]
                    .sum()
                    .reset_index()
                )
                revenue_trend[date_col] = revenue_trend[date_col].astype(str)
                fig_trend = px.line(
                    revenue_trend,
                    x=date_col,
                    y="Total Amount",
                    title="Revenue Over Selected Range",
                    markers=True,
                )
                render_plotly_chart(fig_trend, key=f"sales_trend_{src or 'default'}")

    try:
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            sm.to_excel(writer, sheet_name="Summary", index=False)
            top_prod.to_excel(writer, sheet_name="Product Rankings", index=False)
            dr.to_excel(writer, sheet_name="Drilldown", index=False)
            if top_cust is not None:
                top_cust.to_excel(writer, sheet_name="VIP Pulse", index=False)
            df.head(500).to_excel(writer, sheet_name="Sample Raw Data", index=False)

        clean_source = str(src).replace(" ", "_") if src else "Report"
        clean_tf = str(tf).replace("/", "-") if tf else "Overview"
        final_filename = f"Report_{clean_source}_{clean_tf}.xlsx"

        st.download_button(
            label="Export Analysis Workbook",
            data=buf.getvalue(),
            file_name=final_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=False,
            key=f"dl_{src}_{tf}",
        )
    except Exception as e:
        st.info(f"Export is unavailable right now. ({e})")

    detail_tabs = st.tabs(["Category Summary", "Products", "Normalized Detail", "Source Data"])

    with detail_tabs[0]:
        st.dataframe(
            sm.sort_values("Total Amount", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

    with detail_tabs[1]:
        st.dataframe(
            top_prod.head(50),
            use_container_width=True,
            hide_index=True,
        )

    with detail_tabs[2]:
        # Extract normalized view using CANONICAL_COLUMNS
        # Need to check if columns exist or have _p_ prefixes
        disp = df.copy()
        
        # Mapping from _p_ prefix (master dataset) or Internal prefix to canonical
        mapping = {
            "_p_order": "order_id",
            "_p_date": "order_date",
            "_p_cust_name": "customer_name",
            "_p_phone": "phone",
            "_p_email": "email",
            "_p_state": "state",
            "_p_sku": "sku",
            "_p_name": "item_name",
            "_p_cost": "unit_price",
            "_p_qty": "qty",
            "_p_order_total": "order_total",
            "_p_status": "order_status",
            "_p_archive_status": "archive_status",
            "Category": "category",
            "Total Amount": "line_amount",
        }
        
        for k, v in mapping.items():
            if k in disp.columns and v not in disp.columns:
                disp[v] = disp[k]
        
        cols_present = [c for c in CANONICAL_COLUMNS if c in disp.columns]
        if not cols_present:
             # Fallback to some basic columns if canonicals not found
             cols_present = [c for c in disp.columns if not str(c).startswith('_')]
        
        search_norm = st.text_input(
            "Filter normalized records",
            key=f"search_norm_{src}_{tf}",
            placeholder="Search by any field...",
        ).lower()
        
        if search_norm:
            mask = disp[cols_present].astype(str).apply(
                lambda x: x.str.contains(search_norm, case=False, na=False)
            ).any(axis=1)
            disp = disp[mask]
        
        st.caption(f"Showing {len(disp):,} canonical records.")
        st.dataframe(disp[cols_present], use_container_width=True, hide_index=True)

    with detail_tabs[3]:
        search_query = st.text_input(
            "Search full source records",
            key=f"search_{src}_{tf}",
            placeholder="Product, category, customer, order...",
        ).lower()
        if search_query:
            mask = (
                df.astype(str)
                .apply(lambda x: x.str.contains(search_query, case=False, na=False))
                .any(axis=1)
            )
            results = df[mask]
            st.caption(f"Showing {len(results):,} matching rows.")
            st.dataframe(results, use_container_width=True)
        else:
            if show_full_raw:
                st.caption("Showing the full dataset.")
                st.dataframe(df, use_container_width=True)
            else:
                st.caption("Showing the first 20 rows.")
                st.dataframe(df.head(20), use_container_width=True)


def enable_live_auto_refresh(interval_seconds=LIVE_STREAM_REFRESH_SECONDS):
    st.caption(f"Auto-refresh is active every {interval_seconds} seconds.")
    components.html(
        f"""
        <script>
        window.setTimeout(function() {{
            window.parent.location.reload();
        }}, {interval_seconds * 1000});
        </script>
        """,
        height=0,
    )


# --- TABS ---


def _render_live_tab_legacy():
    from src.ui.components import render_status_strip, render_action_bar
    section_card(
        "Live Stream", f"Real-time performance synchronized with {LIVE_SALES_TAB_NAME}."
    )
    enable_live_auto_refresh()
    
    p_click, _ = render_action_bar("Refresh Queue", "live_sync_btn")
    if p_click:
        from src.core.sync import clear_sync_cache
        clear_sync_cache()
        st.toast("Syncing live records...")
        st.rerun()
    try:
        from src.core.sync import load_shared_gsheet
        df, src, upd = load_shared_gsheet(LIVE_SALES_TAB_NAME, force_refresh=False)
        
        mc = find_columns(df)
        
        # --- NEW: FILTER TO LAST DAY ONLY ---
        if mc.get("date") in df.columns:
            df[mc["date"]] = parse_dates(df[mc["date"]])
            latest_date = df[mc["date"]].max()
            if pd.notna(latest_date):
                target_date = latest_date.date()
                df = df[df[mc["date"]].dt.date == target_date].copy()
                st.info(
                    f"Showing live data for {target_date.strftime('%d %b %Y')}."
                )
        
        # Precomputed KPI Snapshot Check
        from src.core.paths import CACHE_DIR
        kpi_cache_file = CACHE_DIR / "live_kpi_snapshot.json"
        
        dr, sm, tp, tf, bk, df_processed, tc = process_data(df, mc)

        # Save KPI snapshot for even faster cold starts
        if sm is not None:
             try:
                 snapshot = {
                     "upd": upd,
                     "items": int(sm['Total Qty'].sum()),
                     "revenue": float(sm['Total Amount'].sum()),
                     "orders": int(bk['total_orders']),
                     "avg_basket": float(bk['avg_basket_value'])
                 }
                 with open(kpi_cache_file, "w") as f:
                     json.dump(snapshot, f)
             except Exception:
                 pass

        if df_processed is not None:
            render_dashboard_output(
                df_processed,
                dr,
                sm,
                tp,
                tf,
                bk,
                src=src,
                top_cust=tc,
                show_full_raw=True,
            )
    except Exception as e:
        # Try to show last known KPI if sync fails
        from src.core.paths import CACHE_DIR
        kpi_cache_file = CACHE_DIR / "live_kpi_snapshot.json"
        if os.path.exists(kpi_cache_file):
            with open(kpi_cache_file, "r") as f:
                s = json.load(f)
            st.warning(f"Live sync offline. Showing snapshot from {s.get('upd', 'Unknown')}")
            c1, c2, c3, c4 = st.columns(4)
            from src.ui.components import render_metric_hud
            render_metric_hud("Items (Cached)", f"{s.get('items', 0):,}")
            render_metric_hud("Orders (Cached)", f"{s.get('orders', 0):,}")
            render_metric_hud("Revenue (Cached)", f"TK {s.get('revenue', 0):,.0f}")
            render_metric_hud("Avg Basket (Cached)", f"TK {s.get('avg_basket', 0):,.0f}")
        
        st.error(f"Live sync error: {e}")


def _render_live_tab_transition():
    from src.ui.components import render_action_bar
    from src.core.paths import CACHE_DIR
    from src.core.sync import clear_sync_cache, load_shared_gsheet
    from src.core.gsheet_archive import (
        has_archive_credentials,
        is_archive_auto_enabled,
        sync_live_sales_archive,
    )

    section_card(
        "Live Stream",
        f"Full {LIVE_SALES_TAB_NAME} tab for active processing and shipping operations.",
    )
    enable_live_auto_refresh()

    p_click, archive_click = render_action_bar(
        "Refresh Queue",
        "live_sync_btn",
        "Archive Ready Orders",
        "live_archive_btn",
    )
    if p_click:
        clear_sync_cache()
        st.toast("Syncing live records...")
        st.rerun()

    force_refresh = False
    archive_result = None
    auto_archive_enabled = is_archive_auto_enabled()
    archive_credentials_ready = has_archive_credentials()

    if archive_click:
        archive_result = sync_live_sales_archive()
    elif auto_archive_enabled and archive_credentials_ready:
        archive_result = sync_live_sales_archive()

    if archive_result is not None:
        if archive_result.ok and archive_result.deleted_rows:
            clear_sync_cache()
            force_refresh = True
            st.success(
                f"{archive_result.message} Control column: {archive_result.control_column}."
            )
        elif archive_result.ok:
            st.info(archive_result.message)
        else:
            st.warning(archive_result.message)

    with st.expander("Archive automation", expanded=False):
        st.write(
            f"Use {LIVE_SALES_TAB_NAME} as the live queue and move finished rows into 2026 automatically."
        )
        st.write(
            f"Add one control column in {LIVE_SALES_TAB_NAME} named `Archive Status`, `Sync Status`, `Sync to 2026`, or `Archive to 2026`."
        )
        st.write(
            "Mark rows with one of these values to archive them: `ready`, `done`, `completed`, `shipped`, `archive`, `synced`."
        )
        st.write(
            f"Auto-archive enabled: `{auto_archive_enabled}`. Credentials ready: `{archive_credentials_ready}`."
        )
        st.write(
            "Required secret/env for write access: `GSHEET_SPREADSHEET_ID` or `GSHEET_EDIT_URL`, plus `GSHEET_SERVICE_ACCOUNT_JSON` or `GOOGLE_SERVICE_ACCOUNT_EMAIL` + `GOOGLE_PRIVATE_KEY`."
        )

    try:
        df, src, upd = load_shared_gsheet(LIVE_SALES_TAB_NAME, force_refresh=force_refresh)
        mc = find_columns(df)
        kpi_cache_file = CACHE_DIR / "live_kpi_snapshot.json"

        dr, sm, tp, tf, bk, df_processed, tc = process_data(df, mc)
        tf = f"{LIVE_SALES_TAB_NAME} Live Queue"

        if sm is not None:
            try:
                snapshot = {
                    "upd": upd,
                    "items": int(sm["Total Qty"].sum()),
                    "revenue": float(sm["Total Amount"].sum()),
                    "orders": int(bk["total_orders"]),
                    "avg_basket": float(bk["avg_basket_value"]),
                }
                with open(kpi_cache_file, "w") as f:
                    json.dump(snapshot, f)
            except Exception:
                pass

        if df_processed is not None:
            st.info(f"Live dashboard is showing the full {LIVE_SALES_TAB_NAME} tab.")
            render_dashboard_output(
                df_processed,
                dr,
                sm,
                tp,
                tf,
                bk,
                src=src,
                top_cust=tc,
                show_full_raw=True,
            )
    except Exception as e:
        kpi_cache_file = CACHE_DIR / "live_kpi_snapshot.json"
        if os.path.exists(kpi_cache_file):
            with open(kpi_cache_file, "r") as f:
                s = json.load(f)
            st.warning(
                f"Live sync offline. Showing snapshot from {s.get('upd', 'Unknown')}"
            )
            from src.ui.components import render_metric_hud

            c1, c2, c3, c4 = st.columns(4)
            render_metric_hud("Items (Cached)", f"{s.get('items', 0):,}")
            render_metric_hud("Orders (Cached)", f"{s.get('orders', 0):,}")
            render_metric_hud("Revenue (Cached)", f"TK {s.get('revenue', 0):,.0f}")
            render_metric_hud("Avg Basket (Cached)", f"TK {s.get('avg_basket', 0):,.0f}")

        st.error(f"Live sync error: {e}")


def render_live_tab():
    from src.core.gsheet_archive import has_archive_credentials, is_archive_auto_enabled
    from src.ui.components import render_action_bar, render_plotly_chart, render_status_strip

    enable_live_auto_refresh()
    section_card(
        "Live Queue",
        f"Operational queue built from the full {LIVE_SALES_TAB_NAME} tab. Historical analysis now uses the core workbook plus fresh 2026 rows from Google Sheets.",
    )

    manual_sync, manual_archive = render_action_bar(
        "Refresh Queue",
        "live_sync_btn",
        "Archive Ready Orders",
        "live_archive_btn",
    )

    force_refresh = False
    if manual_sync:
        clear_sync_cache()
        force_refresh = True

    archive_result = run_archive_if_requested(
        manual_trigger=manual_archive, auto_trigger=not manual_archive
    )
    if archive_result is not None:
        if archive_result.ok and archive_result.deleted_rows:
            clear_sync_cache()
            force_refresh = True
            st.success(
                f"{archive_result.message} Control column: {archive_result.control_column}."
            )
        elif archive_result.ok:
            st.info(archive_result.message)
        else:
            st.warning(archive_result.message)

    try:
        package = load_live_queue(force_refresh=force_refresh)
        analytics = package.analytics
        metrics = package.queue_metrics

        render_ops_hero(
            f"{LIVE_SALES_TAB_NAME} Live Queue",
            "Core metrics focus on what is still waiting to be sold, packed, shipped, and archived.",
            [
                f"Source {package.source_name}",
                f"Refresh {package.last_refresh or 'N/A'}",
                f"Auto archive {'ON' if is_archive_auto_enabled() else 'OFF'}",
            ],
        )
        render_status_strip(
            source=package.source_name,
            rows=len(package.normalized_df),
            last_refresh=package.last_refresh,
            status="Active Queue",
        )

        summary = analytics["summary"].copy()
        if not summary.empty:
            summary["Volume Share (%)"] = (
                summary["Total Qty"] / max(summary["Total Qty"].sum(), 1) * 100
            ).round(2)

        top_row = st.columns(4)
        with top_row[0]:
            render_ops_kpi(
                "Items To Be Sold",
                f"{metrics['units']:,}",
                f"{metrics['line_items']:,} sellable lines currently in queue",
            )
        with top_row[1]:
            render_ops_kpi(
                "Total Orders",
                f"{metrics['unique_orders']:,}",
                f"{metrics['ready_to_archive']:,} orders already marked ready",
            )
        with top_row[2]:
            render_ops_kpi(
                "Revenue",
                f"TK {metrics['queue_value']:,.0f}",
                "Current queue value based on line totals",
            )
        with top_row[3]:
            render_ops_kpi(
                "Basket Analysis",
                f"{analytics['basket']['avg_basket_qty']:.1f} qty",
                f"Avg basket TK {analytics['basket']['avg_basket_value']:,.0f}",
            )

        chart_a, chart_b = st.columns(2)
        with chart_a:
            if not summary.empty:
                fig_pie = px.pie(
                    summary.sort_values("Total Amount", ascending=False),
                    values="Total Amount",
                    names="Category",
                    hole=0.55,
                    title="Revenue Share by Category",
                    color_discrete_sequence=px.colors.sequential.Blues_r,
                )
                render_plotly_chart(fig_pie, key="live_revenue_share")
            else:
                st.info("Revenue-share chart is not available for the current queue.")
        with chart_b:
            if not summary.empty:
                fig_volume = px.bar(
                    summary.sort_values("Total Qty", ascending=True),
                    x="Total Qty",
                    y="Category",
                    orientation="h",
                    title="Volume by Category",
                    color="Total Qty",
                    color_continuous_scale="Blues",
                )
                render_plotly_chart(fig_volume, key="live_category_volume")
            else:
                st.info("Category-volume chart is not available for the current queue.")

        side_a, side_b = st.columns([1.3, 1])
        with side_a:
            st.markdown("#### Product View")
            st.dataframe(
                analytics["top_products"].head(25),
                use_container_width=True,
                hide_index=True,
            )
        with side_b:
            render_ops_list(
                [
                    ("Avg Basket Qty", f"{analytics['basket']['avg_basket_qty']:.1f}"),
                    ("Avg Basket TK", f"TK {analytics['basket']['avg_basket_value']:,.0f}"),
                    ("Total Item", f"{metrics['units']:,}"),
                    ("Total Unique Order", f"{metrics['unique_orders']:,}"),
                    ("Total Unique Customer", f"{compute_unique_customer_count(package.normalized_df):,}"),
                ]
            )

        insight_tabs = st.tabs(
            ["Queue Summary", "Products", "Customers", "Raw Queue", "Archive Automation"]
        )

        with insight_tabs[0]:
            if not summary.empty:
                st.dataframe(
                    summary.sort_values("Total Amount", ascending=False),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No summary rows are available.")

        with insight_tabs[1]:
            st.dataframe(
                analytics["top_products"].head(50),
                use_container_width=True,
                hide_index=True,
            )

        with insight_tabs[2]:
            top_customers = analytics["top_customers"]
            if top_customers is not None and not top_customers.empty:
                st.dataframe(
                    top_customers.head(25), use_container_width=True, hide_index=True
                )
            else:
                st.info("Customer detail is not available in the current queue.")

        with insight_tabs[3]:
            display_queue = package.normalized_df.copy()
            selected_columns = [
                "order_id",
                "order_date",
                "customer_name",
                "phone",
                "state",
                "sku",
                "item_name",
                "qty",
                "unit_price",
                "order_total",
                "archive_status",
                "customer_note",
            ]
            search_query = st.text_input(
                "Search live queue",
                key="live_queue_search",
                placeholder="Order number, phone, product, customer...",
            ).strip()
            if search_query:
                mask = (
                    display_queue[selected_columns]
                    .astype(str)
                    .apply(lambda col: col.str.contains(search_query, case=False, na=False))
                    .any(axis=1)
                )
                display_queue = display_queue[mask]

            st.caption(f"Showing {len(display_queue):,} rows from {LIVE_SALES_TAB_NAME}.")
            st.dataframe(
                display_queue[selected_columns],
                use_container_width=True,
                hide_index=True,
            )

        with insight_tabs[4]:
            st.markdown(
                """
                Add one control column in `LatestSales`:
                `Archive Status`, `Sync Status`, `Sync to 2026`, or `Archive to 2026`.
                """
            )
            st.markdown(
                """
                Mark rows with one of these values to move them into `2026`:
                `ready`, `done`, `completed`, `shipped`, `archive`, `synced`.
                """
            )
            st.markdown(
                """
                Required write-access settings:
                `GSHEET_SPREADSHEET_ID` or `GSHEET_EDIT_URL`,
                plus `GSHEET_SERVICE_ACCOUNT_JSON`
                or `GOOGLE_SERVICE_ACCOUNT_EMAIL` + `GOOGLE_PRIVATE_KEY`.
                """
            )
            st.caption(
                f"Auto archive enabled: {is_archive_auto_enabled()} | Credentials ready: {has_archive_credentials()}"
            )
    except Exception as e:
        st.error(f"Live queue failed to load: {e}")


def parse_date_from_tab_name(name):
    """Helper to extract date from tab names for sorting."""
    import dateparser

    try:
        # Try cleaning the name for better parsing (remove 'Statement', 'Sync', etc)
        clean_name = (
            name.lower().replace("statement", "").replace("sync", "").replace("_", " ")
        )
        dt = dateparser.parse(clean_name)
        return dt or datetime(2000, 1, 1)
    except Exception:
        return datetime(2000, 1, 1)


def get_all_statements_master(full_history: bool = False, force_refresh: bool = False):
    """Load master sales data from the core workbook plus new 2026 rows from GSheet."""
    return load_master_sales_dataset(force_refresh=force_refresh)


def render_custom_period_tab():
    from src.ui.components import render_date_range_selector

    cur_start, cur_end = render_date_range_selector("sales_hub")
    selected_period = format_period_label(cur_start, cur_end)

    if st.button("Refresh Historical Core", use_container_width=True):
        get_all_statements_master(force_refresh=True)
        st.toast("Refreshing workbook core and 2026 delta...")
        st.rerun()

    master, msg = get_all_statements_master()
    if master is None:
        st.error(msg)
        return

    if "_p_date" in master.columns:
        filtered = master[
            (master["_p_date"].dt.date >= cur_start) & (master["_p_date"].dt.date <= cur_end)
        ].copy()

        if filtered.empty:
            st.warning(f"No records found for {cur_start} to {cur_end}.")
            return

        mc = {
            "name": "_p_name",
            "cost": "_p_cost",
            "qty": "_p_qty",
            "date": "_p_date",
            "order_id": "_p_order",
            "phone": "_p_phone",
            "email": "_p_email",
        }
        # PREVIOUS PERIOD ANALYSIS
        diff = (cur_end - cur_start).days + 1
        prev_end = cur_start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=diff - 1)
        
        prev_metrics = None
        prev_df = master[
            (master["_p_date"].dt.date >= prev_start) & (master["_p_date"].dt.date <= prev_end)
        ].copy()
        
        if not prev_df.empty:
            p_dr, p_sm, p_tp, p_tf, p_bk, _, p_tc = process_data(prev_df, mc)
            if p_sm is not None:
                prev_metrics = {"sm": p_sm, "bk": p_bk, "tc": p_tc}

        dr, sm, tp, tf, bk, filtered_df, tc = process_data(filtered, mc)
        if filtered_df is not None:
            render_dashboard_output(
                filtered_df,
                dr,
                sm,
                tp,
                tf,
                bk,
                "Workbook Core",
                "2026 Delta Sync",
                top_cust=tc,
                display_period=selected_period,
                prev_metrics=prev_metrics,
            )
    else:
        st.error("Time-series column missing in current dataset.")


def render_customer_pulse_tab():
    from src.ui.components import render_date_range_selector
    cur_start, cur_end = render_date_range_selector("cust_pulse")
    selected_period = format_period_label(cur_start, cur_end)

    if st.button("Refresh Customer Core", use_container_width=True, key="refresh_pulse_btn"):
        get_all_statements_master(force_refresh=True)
        st.toast("Refreshing customer dataset...")
        st.rerun()

    master, msg = get_all_statements_master()
    if master is None or master.empty:
        st.error(f"Failed to load customer foundation: {msg}")
        return

    master["UID"] = (
        master.get("_p_phone", pd.Series(dtype=str))
        .fillna(master.get("_p_email", pd.Series(dtype=str)))
        .astype(str).str.strip().str.lower()
    )

    db = master[
        (master["_p_date"].dt.date >= cur_start) & (master["_p_date"].dt.date <= cur_end)
    ].copy()

    if db.empty:
        st.info(f"No customer activity found between {cur_start} and {cur_end}.")
        return

    try:
        render_customer_pulse_core(db, display_period=selected_period)
    except Exception as e:
        from src.core.errors import log_error
        log_error(e, context="Customer Pulse Tab")
        st.error(f"Pulse analysis failed: {e}")
        st.info("Try Clear Cache in the sidebar if the source data changed structure.")


def render_customer_pulse_core(db, display_period: str | None = None):
    if db.empty:
        st.warning("No data found for selected pulse range.")
        return

    # Advanced Metrics
    db["Total_Amount"] = db["_p_cost"] * db["_p_qty"]
    total_revenue = db["Total_Amount"].sum()

    unique_customers = db["UID"].nunique()
    # Group by UID and take the last name seen as the most accurate
    freq = (
        db.groupby("UID")
        .agg(
            {
                "_p_cust_name": "last",
                "_p_order": "nunique",
                "Total_Amount": "sum",
                "_p_date": "max",
            }
        )
        .reset_index()
    )
    freq.columns = ["UID", "Name", "Orders", "LifetimeValue", "LastActive"]
    # Fallback for display - handle both None and placeholder strings
    freq["Name"] = (
        freq["Name"].replace(["N/A", "None", None, ""], pd.NA).fillna(freq["UID"])
    )

    returning_count = len(freq[freq["Orders"] > 1])
    retention_rate = (
        (returning_count / unique_customers * 100) if unique_customers > 0 else 0
    )
    avg_clv = total_revenue / unique_customers if unique_customers > 0 else 0
    avg_orders = db.groupby("UID")["_p_order"].nunique().mean() if unique_customers > 0 else 0
    last_orders = db.groupby("UID")["_p_date"].max()
    avg_recency = (pd.Timestamp.now() - last_orders).dt.days.mean() if not last_orders.empty else 0
    one_time_count = max(unique_customers - returning_count, 0)

    # Key customer KPIs
    render_ops_hero(
        "Customer Pulse",
        "Key customer KPIs from the workbook-backed master dataset plus the latest 2026 delta.",
        [
            f"Period {display_period or 'Selected Range'}",
            f"Customers {unique_customers:,}",
            f"Retention {retention_rate:.1f}%",
            f"Avg CLV TK {avg_clv:,.0f}",
        ],
    )
    st.caption(f"Charts and KPIs reflect {display_period or 'the selected period'}.")

    top_row = st.columns(4)
    with top_row[0]:
        render_ops_kpi(
            "Unique Customers",
            f"{unique_customers:,}",
            "Distinct customer identities in the selected period",
        )
    with top_row[1]:
        render_ops_kpi(
            "Returning Customers",
            f"{returning_count:,}",
            "Customers with more than one order",
        )
    with top_row[3]:
        render_ops_kpi("Avg CLV", f"TK {avg_clv:,.0f}", "Value per customer")
        
    # NEW: Customer Insights
    render_automated_insights(
        db, 
        sm=None, 
        bk=None, 
        cust_metrics={"retention": retention_rate, "avg_clv": avg_clv}
    )
    with top_row[3]:
        render_ops_kpi(
            "Avg CLV",
            f"TK {avg_clv:,.0f}",
            "Average lifetime value per customer",
        )

    chart_a, chart_b = st.columns(2)
    with chart_a:
        cust_acq = (
            db.sort_values("_p_date").groupby("UID")["_p_date"].min().reset_index()
        )
        cust_acq.columns = ["UID", "AcqDate"]
        cust_acq["Month"] = cust_acq["AcqDate"].dt.strftime("%Y-%m")
        trend_grp = cust_acq.groupby("Month").size().reset_index(name="New")
        trend_grp["Cumulative"] = trend_grp["New"].cumsum()

        fig_growth = px.line(
            trend_grp,
            x="Month",
            y=["Cumulative", "New"],
            title="Customer Scaling Factor",
            color_discrete_sequence=["#2563eb", "#0f766e"],
        )
        fig_growth.update_traces(mode="lines+markers")
        from src.ui.components import render_plotly_chart
        render_plotly_chart(fig_growth, key="pulse_scaling_line")

    with chart_b:
        retention_df = pd.DataFrame(
            {
                "Segment": ["Returning Loyals", "One-Time Shoppers"],
                "Count": [returning_count, one_time_count],
            }
        )
        fig_ret = px.pie(
            retention_df,
            values="Count",
            names="Segment",
            title="Retention Dynamics",
            hole=0.6,
            color_discrete_sequence=["#2563eb", "#94a3b8"],
        )
        render_plotly_chart(fig_ret, key="pulse_ret_pie")

    vip = freq.sort_values("LifetimeValue", ascending=False).head(10).copy()
    vip["Engagement Index"] = vip["Orders"].apply(
        lambda x: "High" if x > 3 else "Mid"
    )
    lower_a, lower_b = st.columns([1.35, 1])
    with lower_a:
        st.markdown("#### Top Customers")
        st.dataframe(
            vip[["Name", "UID", "Orders", "LifetimeValue", "Engagement Index"]],
            use_container_width=True,
            hide_index=True,
        )
    with lower_b:
        render_ops_list(
            [
                ("Avg Orders / Customer", f"{avg_orders:.1f}x"),
                ("Avg Days Since Last Order", f"{int(avg_recency or 0)} days"),
                ("One-Time Customers", f"{one_time_count:,}"),
                ("Unique Orders", f"{db['_p_order'].nunique():,}"),
            ]
        )

    if "_src_tab" in db.columns:
        source_grp = db.groupby("_src_tab").size().reset_index(name="Volume")
        if not source_grp.empty:
            fig_src = px.bar(
                source_grp.sort_values("Volume", ascending=True),
                x="Volume",
                y="_src_tab",
                orientation="h",
                title="Volume by Source Tab",
                color="Volume",
                color_continuous_scale="Blues",
            )
            render_plotly_chart(fig_src, key="pulse_source_bar")


def render_cache_health_panel():
    """System tool to inspect the GSheet cache status."""
    from src.core.sync import load_manifest
    from src.core.paths import GSHEETS_CACHE_DIR, GSHEETS_RAW_DIR, GSHEETS_NORM_DIR
    import os

    st.markdown("### GSheet Cache Health")
    manifest = load_manifest()

    if not manifest:
        st.info("Cache is empty. Start a sync to populate.")
        return

    # Summary Metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Manifest Entries", len(manifest))
    
    raw_size = sum(f.stat().st_size for f in GSHEETS_RAW_DIR.glob('*.csv')) / (1024 * 1024)
    norm_size = sum(f.stat().st_size for f in GSHEETS_NORM_DIR.glob('*.parquet')) / (1024 * 1024)
    
    c2.metric("Raw Storage", f"{raw_size:.2f} MB")
    c3.metric("Norm Storage", f"{norm_size:.2f} MB")

    st.markdown("#### Cached Tabs")
    cache_data = []
    for k, v in manifest.items():
        if k.startswith("tabs_"):
            continue
        
        age = "N/A"
        if "fetched_at" in v:
            dt = datetime.fromisoformat(v["fetched_at"])
            diff = datetime.now(timezone.utc) - dt
            age = f"{int(diff.total_seconds() // 60)}m ago"
        
        cache_data.append({
            "Tab": v.get("tab_name", "Unknown"),
            "GID": v.get("gid"),
            "Last Modified": v.get("last_modified", "Unknown"),
            "Rows": v.get("row_count", 0),
            "Age": age,
            "Status": "Fresh" if "m ago" in age and int(age.split('m')[0]) < 60 else "Stale"
        })
    
    if cache_data:
        st.table(pd.DataFrame(cache_data))
    
    if st.button("Export Current Pivot to Local CSVs", use_container_width=True, help="Dumps all cached data into Excel-readable files in your 'incoming' folder"):
        from src.core.paths import INCOMING_DIR
        try:
            m_df, msg = get_all_statements_master(full_history=False)
            if m_df is not None and not m_df.empty:
                csv_path = INCOMING_DIR / f"pivot_snapshot_{datetime.now().strftime('%Y%m%d')}.csv"
                m_df.to_csv(csv_path, index=False)
                st.success(f"Pivot snapshot secured: {csv_path.name}")
            else:
                st.warning("Database is currently empty. Sync first.")
        except Exception as e:
            st.error(f"Native export failed: {e}")

    if st.button("Wipe All Local Cache", type="secondary"):
        from src.core.sync import clear_sync_cache
        clear_sync_cache()
        st.toast("Cache Purged")
        st.rerun()


def render_data_completeness_report():
    """Detailed report on which months/tabs are loaded vs missing."""
    st.markdown("### Data Completeness Report")
    url = get_setting("GSHEET_URL", DEFAULT_GSHEET_URL)
    try:
        from src.core.sync import load_published_sheet_tabs, load_manifest
        tabs = load_published_sheet_tabs(url)
        manifest = load_manifest()
        
        report = []
        for t in tabs:
            if t["name"].lower() in TOTAL_SALES_EXCLUDED_TABS:
                continue
            
            cache_key = f"gid_{t['gid']}"
            cached = manifest.get(cache_key)
            status = "Missing"
            details = "Not yet synced"
            
            if cached:
                status = "Synced"
                details = f"{cached.get('row_count', 0)} rows, {cached.get('last_modified', 'No date')}"
            
            report.append({
                "Sheet Tab": t["name"],
                "Status": status,
                "Details": details
            })
        
        st.dataframe(pd.DataFrame(report), use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Failed to generate report: {e}")
