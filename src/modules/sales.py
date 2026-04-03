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
    compute_unique_customer_count,
    normalize_sales_dataframe,
)
from src.ui.components import (
    render_ops_hero,
    render_ops_kpi,
    render_ops_list,
    render_reset_confirm,
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

        tf = ""
        if "date" in selected_cols:
            ds = pd.to_datetime(df[selected_cols["date"]], errors="coerce").dropna()
            if not ds.empty:
                tf = f"{ds.min().strftime('%d%b')}_to_{ds.max().strftime('%d%b_%y')}"

        df["Category"] = df["Internal_Name"].apply(get_category_for_sales)
        df["Total Amount"] = df["Internal_Cost"] * df["Internal_Qty"]

        summ = (
            df.groupby("Category")
            .agg({"Internal_Qty": "sum", "Total Amount": "sum"})
            .reset_index()
        )
        summ.columns = ["Category", "Total Qty", "Total Amount"]
        total_rev = summ["Total Amount"].sum()
        total_qty = summ["Total Qty"].sum()
        if total_rev > 0:
            summ["Revenue Share (%)"] = (
                (summ["Total Amount"] / total_rev) * 100
            ).round(2)
        if total_qty > 0:
            summ["Quantity Share (%)"] = (
                (summ["Total Qty"] / total_qty) * 100
            ).round(2)

        drill = (
            df.groupby(["Category", "Internal_Cost"])
            .agg({"Internal_Qty": "sum", "Total Amount": "sum"})
            .reset_index()
        )
        drill.columns = ["Category", "Price (TK)", "Total Qty", "Total Amount"]

        # 🥇 Product Rankings (The requested report style)
        top_products = (
            df.groupby("Internal_Name")
            .agg({"Internal_Qty": "sum", "Total Amount": "sum", "Category": "first"})
            .reset_index()
        )
        top_products.columns = [
            "Product Name",
            "Total Qty",
            "Total Amount",
            "Category",
        ]
        top_products = top_products.sort_values("Total Amount", ascending=False)

        # 👥 Customer Highlights (Optional)
        top_customers = None
        if (
            "Internal_Customer" in df.columns
            and (df["Internal_Customer"] != "N/A").any()
        ):
            top_customers = (
                df.groupby("Internal_Customer")
                .agg({"Total Amount": "sum", "Internal_Qty": "sum"})
                .reset_index()
            )
            top_customers.columns = [
                "Customer Name",
                "Total Spent",
                "Items Purchased",
            ]
            top_customers = top_customers.sort_values("Total Spent", ascending=False)

        bk = {"avg_basket_qty": 0, "avg_basket_value": 0, "total_orders": 0}
        gc = [
            selected_cols[k]
            for k in ("order_id", "phone", "email")
            if k in selected_cols and selected_cols[k] in df.columns
        ]
        if gc:
            og = df.groupby(gc).agg({"Internal_Qty": "sum", "Total Amount": "sum"})
            bk = {
                "avg_basket_qty": og["Internal_Qty"].mean(),
                "avg_basket_value": og["Total Amount"].mean(),
                "total_orders": len(og),
            }

        return drill, summ, top_products, tf, bk, df, top_customers
    except Exception as e:
        log_system_event("CALC_ERROR", str(e))
        return None, None, None, "", {}, None, None


def render_story_summary(summ, tp, timeframe, bk, sm=None, deltas=None, cust_metrics=None):
    """Compact summary strip with automated insights."""
    if summ is None or summ.empty:
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


def render_automated_insights(df, summary, basket, deltas=None, cust_metrics=None):
    """Render automated insights panel based on data analysis."""
    insights = []
    
    # Revenue insights
    if summary is not None and not summary.empty:
        total_rev = summary["Total Amount"].sum()
        total_qty = summary["Total Qty"].sum()
        
        if deltas and deltas.get("rev") is not None:
            rev_change = deltas["rev"]
            if rev_change > 15:
                insights.append(f"🟢 **Strong Growth**: Revenue up {rev_change:.1f}% vs previous period")
            elif rev_change < -15:
                insights.append(f"🔴 **Declining**: Revenue down {abs(rev_change):.1f}% - review pricing/promotions")
        
        # Top category insight
        top_cat = summary.sort_values("Total Amount", ascending=False).iloc[0]
        cat_share = (top_cat["Total Amount"] / max(total_rev, 1)) * 100
        insights.append(f"📊 **Top Category**: {top_cat['Category']} drives {cat_share:.1f}% of revenue")
    
    # Customer insights
    if cust_metrics:
        retention = cust_metrics.get("retention", 0)
        if retention < 20:
            insights.append("💡 **Retention Alert**: Low repeat rate. Consider loyalty program")
        elif retention > 40:
            insights.append("✨ **Healthy Loyalty**: Strong customer retention detected")
        
        avg_clv = cust_metrics.get("avg_clv", 0)
        if avg_clv > 3000:
            insights.append(f"💰 **Premium Base**: High CLV of TK {avg_clv:,.0f} per customer")
    
    # Basket insights
    if basket:
        avg_qty = basket.get("avg_basket_qty", 0)
        avg_value = basket.get("avg_basket_value", 0)
        
        if avg_qty < 1.5:
            insights.append(f"🛒 **Upsell Opportunity**: Low basket size ({avg_qty:.1f} items) - bundle suggestions may help")
        
        if avg_value > 5000:
            insights.append(f"💎 **High AOV**: Average order value of TK {avg_value:,.0f}")
    
    # Data quality insight
    if df is not None:
        row_count = len(df)
        insights.append(f"📈 **Dataset**: {row_count:,} records analyzed")
    
    # Render insights if any
    if insights:
        st.markdown(
            f"""
            <div style="background:var(--accent-soft); border:1px solid #bfdbfe; border-radius:18px; padding:1.15rem; margin-bottom:1.25rem;">
                <div style="font-size:0.72rem; color:var(--accent-strong); font-weight:800; text-transform:uppercase; letter-spacing:0.12em; margin-bottom:0.65rem;">🤖 Automated Insights</div>
                <div style="display:flex; flex-direction:column; gap:0.65rem;">
                    {"".join(f"<div style='font-size:0.88rem; color:var(--text-strong);'>{i}</div>" for i in insights)}
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
    prev_metrics=None,
):
    """
    Core dashboard renderer for Sales Analysis and historical reports.
    Now supports Period-over-Period (PoP) comparison via prev_metrics.
    """
    # Calculate deltas for comparative analytics
    deltas = {}
    if prev_metrics:
        p_sm = prev_metrics.get("sm")
        p_bk = prev_metrics.get("bk")
        
        # Revenue Delta
        curr_rev = sm["Total Amount"].sum() if not sm.empty else 0
        prev_rev = p_sm["Total Amount"].sum() if (p_sm is not None and not p_sm.empty) else 0
        if prev_rev > 0:
            deltas["rev"] = (curr_rev - prev_rev) / prev_rev * 100
            
        # Quantity Delta
        curr_qty = sm["Total Qty"].sum() if not sm.empty else 0
        prev_qty = p_sm["Total Qty"].sum() if (p_sm is not None and not p_sm.empty) else 0
        if prev_qty > 0:
            deltas["qty"] = (curr_qty - prev_qty) / prev_qty * 100
            
        # Orders Delta
        curr_ords = bk.get("total_orders", 0)
        prev_ords = p_bk.get("total_orders", 0) if p_bk else 0
        if prev_ords > 0:
            deltas["orders"] = (curr_ords - prev_ords) / prev_ords * 100
    pie_colors = ["#2563eb", "#0f766e", "#ea580c", "#7c3aed", "#0891b2", "#be185d"]
    from io import BytesIO
    from src.ui.components import render_plotly_chart, render_status_strip

    sorted_summ = sm.sort_values("Total Amount", ascending=False).copy()
    unique_customers = compute_unique_customer_count(df)

    render_ops_hero(
        "Sales Analysis",
        "Historical sales performance with PoP comparative analytics active.",
        [
            f"Period {display_period or tf or 'All Records'}",
            f"PoP Analytics {'Enabled' if prev_metrics else 'Off'}",
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
            "Avg Basket Value",
            f"TK {bk['avg_basket_value']:,.0f}",
            f"Avg {bk['avg_basket_qty']:.1f} items per basket",
        )

    render_reset_confirm("Sales Analytics", "sales_ana", clear_sync_cache)

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
            "Core metrics focus on active processing. Analytics engine: ACTIVE.",
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
        
        # NEW: Automated Insights for Live Queue
        render_automated_insights(
            package.normalized_df, 
            analytics["summary"], 
            analytics["basket"]
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
                "Avg Basket Value",
                f"TK {analytics['basket']['avg_basket_value']:,.0f}",
                f"Avg {analytics['basket']['avg_basket_qty']:.1f} items per basket",
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
            top_products_df = analytics.get("top_products", pd.DataFrame())
            if not top_products_df.empty:
                st.dataframe(
                    top_products_df.head(25),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No product data available.")
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

    render_reset_confirm("Live Queue", "sales_live", clear_sync_cache)


def parse_date_from_tab_name(name):
    """Helper to extract date from tab names for sorting."""
    try:
        import dateparser
        has_dateparser = True
    except ImportError:
        has_dateparser = False

    try:
        # Try cleaning the name for better parsing (remove 'Statement', 'Sync', etc)
        clean_name = (
            name.lower().replace("statement", "").replace("sync", "").replace("_", " ")
        )
        if has_dateparser:
            dt = dateparser.parse(clean_name)
            return dt or datetime(2000, 1, 1)
        # Fallback to basic parsing if dateparser not available
        try:
            return datetime.strptime(clean_name.strip(), "%Y-%m")
        except ValueError:
            return datetime(2000, 1, 1)
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
    section_card("👥 Customer Pulse", "Cross-statement unique customer insights and loyalty metrics.")
    if st.button("🔄 Refresh Pulse Data", use_container_width=True, key="refresh_pulse_btn"): get_all_statements_master.clear(); st.rerun()
    
    # Check if we need full history for Pulse too
    full_hist = st.toggle("Enable Full History Analytics", value=False, key="pulse_hist_toggle")
    
    master, msg = get_all_statements_master(full_history=full_hist)
    if master is None: st.info("Pulse data not yet ready."); return
    
    # Default to last 30 days for Pulse overview
    if '_p_date' in master.columns:
        valid_dates = master[master['_p_date'].notna()]
        if not valid_dates.empty:
            min_d = valid_dates['_p_date'].min().date()
            abs_min = date(2022, 1, 1)
            f1, f2 = st.columns(2)
            default_pulse_start = max(min_d, date.today() - timedelta(days=180))
            p_start = f1.date_input("Pulse From", default_pulse_start, min_value=abs_min, max_value=date.today(), key="pulse_start")
            p_end = f2.date_input("Pulse To", date.today(), min_value=abs_min, max_value=date.today(), key="pulse_end")
            db = master[(master['_p_date'].dt.date >= p_start) & (master['_p_date'].dt.date <= p_end)].copy()
        else:
            db = master.copy()
    else:
        db = master.copy()

    if db.empty: st.warning("No data found for selected pulse range."); return
    
    # Identify unique customers across the ENTIRE current master (Global View)
    master['UID'] = master.get('_p_phone', pd.Series(dtype=str)).fillna(master.get('_p_email', pd.Series(dtype=str))).astype(str).str.strip().str.lower()
    global_unique = master[(master['UID'] != "") & (master['UID'] != "nan") & (master['UID'].notna())]['UID'].nunique()
    
    # Process Filtered View for New/Growth analysis
    db['UID'] = db.get('_p_phone', pd.Series(dtype=str)).fillna(db.get('_p_email', pd.Series(dtype=str))).astype(str).str.strip().str.lower()
    db = db[(db['UID'] != "") & (db['UID'] != "nan") & (db['UID'].notna())]
    
    cust = db.sort_values('_p_date').groupby('UID')['_p_date'].min().reset_index()
    cust.columns = ['UID', 'AcqDate']
    
    # Calculate Acquisition Metrics
    today = date.today()
    this_m = date(today.year, today.month, 1)
    last_m_end = this_m - timedelta(days=1)
    last_m_start = date(last_m_end.year, last_m_end.month, 1)
    
    new_lm = len(cust[(cust['AcqDate'].dt.date >= last_m_start) & (cust['AcqDate'].dt.date <= last_m_end)])
    new_tm = len(cust[cust['AcqDate'].dt.date >= this_m])
    
    m0, m1, m2, m3 = st.columns(4)
    m0.metric("Total Customers (Current Data)", f"{global_unique:,}")
    m1.metric("Range Unique Customers", f"{len(cust):,}")
    m2.metric("New (Last Month)", f"{new_lm:,}")
    m3.metric("New (This Month)", f"{new_tm:,}")
    
    # Visuals
    trend = cust.dropna(subset=['AcqDate']).copy()
    trend['Month'] = trend['AcqDate'].dt.strftime('%Y-%m')
    trend_grp = trend.groupby('Month').size().reset_index(name='New')
    trend_grp['Total (Cumulative)'] = trend_grp['New'].cumsum()
    
    col1, col2 = st.columns(2)
    with col1:
        # Dual-series graph: Line for monthly new and total growth
        fig_trend = px.line(
            trend_grp, 
            x='Month', 
            y=['Total (Cumulative)', 'New'], 
            title="Customer Growth & Acquisition", 
            template="plotly_dark", 
            color_discrete_sequence=['#3b82f6', '#10b981']
        ).update_layout(hovermode="x unified")
        # Add markers for better clarity on data points
        fig_trend.update_traces(mode='lines+markers')
        st.plotly_chart(fig_trend, use_container_width=True)
    
    with col2:
        freq = db.groupby('UID').agg({'_p_order': 'nunique', '_p_cost': 'sum'}).reset_index()
        freq.columns = ['UID', 'Orders', 'Revenue']
        returning = len(freq[freq['Orders'] > 1])
        one_time = len(freq[freq['Orders'] == 1])
        retention_df = pd.DataFrame({'Type': ['Returning', 'One-time'], 'Count': [returning, one_time]})
        st.plotly_chart(px.pie(retention_df, values='Count', names='Type', title="Retention Snapshot", hole=0.5, color_discrete_sequence=['#10b981', '#334155']), use_container_width=True)

    # NEW FEATURES: VIP Leaderboard
    st.markdown("### 🏆 VIP Leaderboard (Top Spenders)")
    vip = freq.sort_values('Revenue', ascending=False).head(10).copy()
    # Mask UID for privacy if needed, but here we show it for internal use
    st.table(vip.style.format({'Revenue': 'TK {:,.0f}'}))
    
    with st.expander("🔍 Customer Geography & Channel Hint"):
        if '_p_phone' in db.columns:
            total_with_phone = len(db[db['_p_phone'].str.startswith('01', na=False)])
            st.info(f"Verified mobile contacts in database: {total_with_phone:,}")
        
        # Acquisition source if multiple tabs
        if '_src_tab' in db.columns:
            source_grp = db.groupby('_src_tab').size().reset_index(name='Records')
            st.plotly_chart(px.bar(source_grp, x='Records', y='_src_tab', orientation='h', title="Records by Statement Source"), use_container_width=True)
