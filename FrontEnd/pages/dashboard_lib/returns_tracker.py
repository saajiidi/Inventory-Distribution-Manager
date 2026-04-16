"""Returns & Net Sales Tracker - Dashboard Page.

Premium Streamlit UI for tracking returns, partials, exchanges,
and calculating Net Sales intelligence.
"""

from __future__ import annotations

from datetime import datetime, date, timedelta

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from BackEnd.services.returns_tracker import (
    load_returns_data,
    calculate_net_sales_metrics,
    get_issue_type_color,
    DEFAULT_SHEET_URL,
)
from BackEnd.core.logging_config import get_logger

logger = get_logger("returns_tracker_page")


def render_returns_tracker_page() -> None:
    """Main entry point for the Returns & Net Sales Tracker page."""

    st.markdown("### 🔄 Returns & Net Sales Tracker")
    st.caption(
        "Track returns, partial orders, and exchanges. "
        "Calculate Net Sales from delivery-issue intelligence."
    )

    # ── Data Sync ──
    _render_data_sync_panel()

    if "returns_data" not in st.session_state or st.session_state.returns_data.empty:
        st.info(
            "📊 Click **Sync Returns Data** above to load delivery-issue data "
            "from Google Sheets."
        )
        return

    df = st.session_state.returns_data.copy()

    # ── Date Range Filter ──
    df = _render_date_filter(df)

    if df.empty:
        st.warning("No returns data in the selected date range.")
        return

    # ── WooCommerce Gross Sales Link ──
    gross_sales, total_orders = _get_gross_sales_context()

    # ── Compute Metrics ──
    metrics = calculate_net_sales_metrics(df, gross_sales, total_orders)

    # ── KPI Cards ──
    _render_kpi_cards(metrics)

    # ── Charts ──
    st.markdown("---")
    _render_charts(df, metrics)

    # ── Detailed Table ──
    st.markdown("---")
    _render_details_table(df)

    # ── Export ──
    st.markdown("---")
    _render_export(df, metrics)


# ═══════════════════════════════════════════════════════════════════
# SYNC PANEL
# ═══════════════════════════════════════════════════════════════════

def _render_data_sync_panel() -> None:
    """Render the data sync controls."""
    with st.expander("🔗 Data Source", expanded=False):
        c1, c2 = st.columns([3, 1])
        with c1:
            st.text_input(
                "Google Sheets CSV URL",
                value=DEFAULT_SHEET_URL,
                key="returns_sheet_url",
                label_visibility="collapsed",
                disabled=True,
            )
        with c2:
            if st.button("🔄 Sync Returns Data", type="primary", use_container_width=True):
                with st.spinner("Syncing delivery-issue data..."):
                    df = load_returns_data(url=st.session_state.get("returns_sheet_url"))
                    if not df.empty:
                        st.session_state.returns_data = df
                        st.success(f"✅ Loaded {len(df)} delivery-issue records")
                        st.rerun()
                    else:
                        st.error("❌ Failed to load data. Check the URL.")

        # Optional file upload
        uploaded = st.file_uploader(
            "Or upload CSV/Excel",
            type=["csv", "xlsx"],
            key="returns_upload",
        )
        if uploaded is not None:
            with st.spinner("Processing upload..."):
                df = load_returns_data(uploaded_file=uploaded)
                if not df.empty:
                    st.session_state.returns_data = df
                    st.success(f"✅ Loaded {len(df)} records from upload")
                    st.rerun()


# ═══════════════════════════════════════════════════════════════════
# DATE FILTER
# ═══════════════════════════════════════════════════════════════════

def _render_date_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Render date range filter and return filtered DataFrame."""
    c1, c2, c3 = st.columns([1, 1, 2])

    min_date = df["date"].min().date() if not df["date"].isna().all() else date(2025, 8, 1)
    max_date = df["date"].max().date() if not df["date"].isna().all() else date.today()

    with c1:
        start = st.date_input(
            "From", value=max(min_date, date(2025, 8, 1)),
            min_value=min_date, max_value=max_date,
            key="returns_start_date"
        )
    with c2:
        end = st.date_input(
            "To", value=max_date,
            min_value=min_date, max_value=max_date,
            key="returns_end_date"
        )
    with c3:
        # Issue type filter
        all_types = sorted(df["issue_type"].unique().tolist())
        # Default: show actionable types
        default_types = [
            t for t in all_types
            if t not in ("Delivered", "Unknown", "Delivery Issue")
        ]
        selected_types = st.multiselect(
            "Issue Types", all_types,
            default=default_types,
            key="returns_type_filter"
        )

    mask = (df["date"].dt.date >= start) & (df["date"].dt.date <= end)
    if selected_types:
        mask &= df["issue_type"].isin(selected_types)

    return df[mask]


# ═══════════════════════════════════════════════════════════════════
# GROSS SALES CONTEXT
# ═══════════════════════════════════════════════════════════════════

def _get_gross_sales_context():
    """Try to pull gross sales from the existing dashboard data."""
    gross_sales = 0.0
    total_orders = 0

    if "dashboard_data" in st.session_state:
        data = st.session_state.dashboard_data
        sales_df = data.get("sales_active", pd.DataFrame())
        if not sales_df.empty:
            if "item_revenue" in sales_df.columns:
                gross_sales = sales_df["item_revenue"].sum()
            if "order_id" in sales_df.columns:
                total_orders = sales_df["order_id"].nunique()

    return gross_sales, total_orders


# ═══════════════════════════════════════════════════════════════════
# KPI CARDS
# ═══════════════════════════════════════════════════════════════════

def _render_kpi_cards(metrics: dict) -> None:
    """Render the executive KPI cards."""

    # Row 1: Primary metrics
    cols = st.columns(5)

    with cols[0]:
        st.markdown(_kpi_card(
            "📦 TOTAL ISSUES",
            f"{metrics['total_issues']:,}",
            "All tracked delivery issues",
            "#3b82f6"
        ), unsafe_allow_html=True)

    with cols[1]:
        st.markdown(_kpi_card(
            "🔴 RETURNS",
            f"{metrics['return_count']:,}",
            f"Paid: {metrics.get('paid_return_count', 0)} | "
            f"Non-Paid: {metrics.get('non_paid_return_count', 0)}",
            "#ef4444"
        ), unsafe_allow_html=True)

    with cols[2]:
        st.markdown(_kpi_card(
            "🟡 PARTIAL",
            f"{metrics['partial_count']:,}",
            f"৳{metrics['partial_amounts']:,.0f} extracted",
            "#eab308"
        ), unsafe_allow_html=True)

    with cols[3]:
        st.markdown(_kpi_card(
            "🟣 EXCHANGES",
            f"{metrics['exchange_count']:,}",
            "Size/Product changes",
            "#8b5cf6"
        ), unsafe_allow_html=True)

    with cols[4]:
        st.markdown(_kpi_card(
            "📊 RETURN RATE",
            f"{metrics.get('return_rate', 0):.1f}%",
            f"of {metrics.get('total_orders', 0):,} total orders"
            if metrics.get("total_orders") else "Sync BI data for rate",
            "#f97316"
        ), unsafe_allow_html=True)

    # Row 2: Secondary
    cols2 = st.columns(4)
    with cols2[0]:
        st.markdown(_kpi_card(
            "💵 REFUNDS",
            f"{metrics['refund_count']:,}",
            "QC / OOS refunds",
            "#ec4899"
        ), unsafe_allow_html=True)

    with cols2[1]:
        st.markdown(_kpi_card(
            "🚫 CANCELS",
            f"{metrics['cancel_count']:,}",
            "Customer-initiated",
            "#6b7280"
        ), unsafe_allow_html=True)

    with cols2[2]:
        st.markdown(_kpi_card(
            "📦 ITEMS LOST",
            f"{metrics['items_lost_count']:,}",
            "In transit",
            "#dc2626"
        ), unsafe_allow_html=True)

    with cols2[3]:
        st.markdown(_kpi_card(
            "⚠️ DELIVERY ISSUES",
            f"{metrics['delivery_issue_count']:,}",
            "Courier problems",
            "#f59e0b"
        ), unsafe_allow_html=True)


def _kpi_card(label: str, value: str, subtitle: str, color: str) -> str:
    """Generate a premium KPI card HTML."""
    return f"""
    <div style="
        background: linear-gradient(135deg, {color}15, {color}08);
        border: 1px solid {color}30;
        border-left: 4px solid {color};
        border-radius: 12px;
        padding: 16px;
        text-align: center;
    ">
        <div style="font-size: 0.72rem; font-weight: 700; color: {color};
                     letter-spacing: 0.5px; text-transform: uppercase;">
            {label}
        </div>
        <div style="font-size: 1.8rem; font-weight: 800; color: var(--text-color);
                     margin: 4px 0;">
            {value}
        </div>
        <div style="font-size: 0.7rem; color: #9ca3af;">
            {subtitle}
        </div>
    </div>
    """


# ═══════════════════════════════════════════════════════════════════
# CHARTS
# ═══════════════════════════════════════════════════════════════════

def _render_charts(df: pd.DataFrame, metrics: dict) -> None:
    """Render the analytics charts."""

    tab1, tab2, tab3 = st.tabs([
        "📈 Monthly Trends",
        "🥧 Return Reasons",
        "📦 Product Heatmap",
    ])

    with tab1:
        _render_monthly_trend(df)

    with tab2:
        _render_reason_charts(metrics)

    with tab3:
        _render_product_heatmap(df)


def _render_monthly_trend(df: pd.DataFrame) -> None:
    """Monthly issue count trend with type breakdown."""
    monthly = (
        df.groupby([pd.Grouper(key="date", freq="ME"), "issue_type"])
        .agg(count=("order_id", "nunique"))
        .reset_index()
    )

    if monthly.empty:
        st.info("Not enough data for monthly trends.")
        return

    monthly["month_label"] = monthly["date"].dt.strftime("%b %Y")

    color_map = {t: get_issue_type_color(t) for t in monthly["issue_type"].unique()}

    fig = px.bar(
        monthly,
        x="month_label", y="count",
        color="issue_type",
        color_discrete_map=color_map,
        title="Monthly Issue Breakdown",
        labels={"count": "Orders", "month_label": "Month", "issue_type": "Type"},
        barmode="stack",
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif"),
        legend=dict(orientation="h", y=-0.2),
        margin=dict(l=20, r=20, t=50, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Total trend line
    total_monthly = (
        df.drop_duplicates(subset=["order_id"])
        .groupby(pd.Grouper(key="date", freq="ME"))
        .size()
        .reset_index(name="total_issues")
    )
    total_monthly["month_label"] = total_monthly["date"].dt.strftime("%b %Y")

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=total_monthly["month_label"],
        y=total_monthly["total_issues"],
        mode="lines+markers",
        name="Total Issues",
        line=dict(color="#3b82f6", width=3),
        marker=dict(size=8),
    ))
    fig2.update_layout(
        title="Total Issues Over Time",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif"),
        margin=dict(l=20, r=20, t=50, b=20),
        yaxis_title="Count",
    )
    st.plotly_chart(fig2, use_container_width=True)


def _render_reason_charts(metrics: dict) -> None:
    """Return reason distribution charts."""
    reasons = metrics.get("reason_counts", {})

    if not reasons:
        st.info("No return reason data available.")
        return

    c1, c2 = st.columns(2)

    with c1:
        reason_df = pd.DataFrame([
            {"Reason": k, "Count": v} for k, v in reasons.items()
        ]).sort_values("Count", ascending=False)

        fig = px.pie(
            reason_df, values="Count", names="Reason",
            title="Return Reasons Distribution",
            hole=0.45,
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter, sans-serif"),
            margin=dict(l=20, r=20, t=50, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig2 = px.bar(
            reason_df,
            x="Count", y="Reason",
            orientation="h",
            title="Return Reasons (Count)",
            color="Count",
            color_continuous_scale="Reds",
        )
        fig2.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter, sans-serif"),
            margin=dict(l=20, r=20, t=50, b=20),
            showlegend=False,
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig2, use_container_width=True)


def _render_product_heatmap(df: pd.DataFrame) -> None:
    """Product-level return frequency heatmap."""
    # Extract product category from product_details
    issue_df = df[df["issue_type"].isin([
        "Paid Return", "Non Paid Return", "Partial", "Exchange"
    ])].copy()

    if issue_df.empty:
        st.info("No product-level data available.")
        return

    # Extract rough category from product_details
    issue_df["product_category"] = issue_df["product_details"].apply(_extract_product_category)
    issue_df = issue_df[issue_df["product_category"] != "Unknown"]

    if issue_df.empty:
        st.info("Could not extract product categories from details.")
        return

    heatmap_data = (
        issue_df
        .groupby(["product_category", "issue_type"])
        .agg(count=("order_id", "nunique"))
        .reset_index()
    )

    pivot = heatmap_data.pivot_table(
        index="product_category", columns="issue_type",
        values="count", fill_value=0, aggfunc="sum"
    )

    # Sort by total descending
    pivot["_total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("_total", ascending=True).drop(columns=["_total"])

    fig = px.imshow(
        pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        title="Return Frequency by Product Category",
        color_continuous_scale="YlOrRd",
        aspect="auto",
        labels=dict(color="Count"),
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif"),
        margin=dict(l=20, r=20, t=50, b=20),
        height=max(400, len(pivot) * 30 + 100),
    )
    st.plotly_chart(fig, use_container_width=True)


def _extract_product_category(details: str) -> str:
    """Extract a rough product category from product details text."""
    if not details or details.strip() == "":
        return "Unknown"

    det = details.lower()

    categories = [
        ("Jeans", ["jeans", "denim pant"]),
        ("Flannel Shirt", ["flannel"]),
        ("Kaftan Shirt", ["kaftan"]),
        ("Contrast Shirt", ["contrast stitch"]),
        ("HS Shirt", ["half shirt", "hs-shirt", "hs shirt", "cotton shirt"]),
        ("FS Shirt", ["full sleeve shirt", "fs shirt", "polka dot", "denim shirt"]),
        ("Polo Shirt", ["polo"]),
        ("T-Shirt", ["tshirt", "t-shirt", "active wear"]),
        ("FS T-Shirt", ["fs-tshirt", "fs tshirt", "full sleeve.*t-shirt",
                         "full sleeve.*tshirt", "fs-t-shirt"]),
        ("Sweatshirt", ["sweatshirt"]),
        ("Turtleneck", ["turtleneck"]),
        ("Panjabi", ["panjabi"]),
        ("Trouser", ["trousers", "trouser"]),
        ("Twill/Chino", ["twill", "chino", "jogger"]),
        ("Wallet", ["wallet"]),
        ("Belt", ["belt"]),
        ("Bag", ["bagpack", "bag"]),
        ("Boxer", ["boxer"]),
        ("Bundle", ["pack", "combo", "deal", "dbb"]),
    ]

    for cat_name, keywords in categories:
        for kw in keywords:
            if kw in det:
                return cat_name

    return "Other"


# ═══════════════════════════════════════════════════════════════════
# DETAILS TABLE
# ═══════════════════════════════════════════════════════════════════

def _render_details_table(df: pd.DataFrame) -> None:
    """Render the detailed issue ledger."""
    st.markdown("### 📋 Issue Ledger")

    # Prepare display df
    display_cols = [
        "date", "order_id_raw", "issue_type", "return_reason",
        "product_details", "customer_reason", "courier_reason",
        "fu_status", "inventory_updated", "partial_amount",
    ]
    available_cols = [c for c in display_cols if c in df.columns]
    display_df = df[available_cols].copy()

    # Format
    col_rename = {
        "date": "Date",
        "order_id_raw": "Order ID",
        "issue_type": "Type",
        "return_reason": "Reason",
        "product_details": "Product Details",
        "customer_reason": "Customer Reason",
        "courier_reason": "Courier Reason",
        "fu_status": "Follow-Up",
        "inventory_updated": "Inventory",
        "partial_amount": "Partial ৳",
    }
    display_df = display_df.rename(columns=col_rename)

    if "Date" in display_df.columns:
        display_df["Date"] = display_df["Date"].dt.strftime("%Y-%m-%d")

    st.dataframe(
        display_df,
        use_container_width=True,
        height=500,
        column_config={
            "Partial ৳": st.column_config.NumberColumn(format="৳%.0f"),
        },
    )

    st.caption(f"Showing {len(display_df)} records")


# ═══════════════════════════════════════════════════════════════════
# EXPORT
# ═══════════════════════════════════════════════════════════════════

def _render_export(df: pd.DataFrame, metrics: dict) -> None:
    """Export returns data to Excel with summary sheet."""
    from io import BytesIO

    st.markdown("### 📤 Export Report")

    if st.button("📊 Export Returns Report", type="primary", use_container_width=False):
        with st.spinner("Generating report..."):
            buffer = _generate_excel_report(df, metrics)
            st.download_button(
                label="⬇️ Download Excel Report",
                data=buffer,
                file_name=f"deen_returns_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )


def _generate_excel_report(df: pd.DataFrame, metrics: dict) -> bytes:
    """Generate a multi-sheet Excel report."""
    from io import BytesIO

    try:
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    except ImportError:
        pass

    buffer = BytesIO()

    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        # ── Summary Sheet ──
        summary_data = {
            "Metric": [
                "Total Issues Tracked",
                "Total Returns (Paid + Non-Paid)",
                "  ├─ Paid Returns",
                "  └─ Non-Paid Returns",
                "Partial Orders",
                "Exchanges",
                "Refunds",
                "Cancellations",
                "Items Lost",
                "Delivery Issues",
                "",
                "Return Rate (%)",
                "Partial Amounts Extracted (৳)",
            ],
            "Value": [
                metrics["total_issues"],
                metrics["return_count"],
                metrics.get("paid_return_count", 0),
                metrics.get("non_paid_return_count", 0),
                metrics["partial_count"],
                metrics["exchange_count"],
                metrics["refund_count"],
                metrics["cancel_count"],
                metrics["items_lost_count"],
                metrics["delivery_issue_count"],
                "",
                metrics.get("return_rate", 0),
                metrics["partial_amounts"],
            ],
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

        # ── Reason Breakdown ──
        reasons = metrics.get("reason_counts", {})
        if reasons:
            reason_df = pd.DataFrame([
                {"Reason": k, "Count": v} for k, v in reasons.items()
            ]).sort_values("Count", ascending=False)
            reason_df.to_excel(writer, sheet_name="Reason Analysis", index=False)

        # ── Detailed Data ──
        export_cols = [
            "date", "order_id_raw", "order_id", "issue_type", "return_reason",
            "product_details", "customer_reason", "courier_reason",
            "courier", "fu_status", "inventory_updated", "partial_amount",
        ]
        available = [c for c in export_cols if c in df.columns]
        detail_df = df[available].copy()
        if "date" in detail_df.columns:
            detail_df["date"] = detail_df["date"].dt.strftime("%Y-%m-%d")
        detail_df.to_excel(writer, sheet_name="Detailed Ledger", index=False)

        # ── Style sheets ──
        try:
            wb = writer.book
            header_fill = PatternFill(start_color="1a1a2e", end_color="1a1a2e", fill_type="solid")
            header_font = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
            thin_border = Border(
                left=Side(style="thin"),
                right=Side(style="thin"),
                top=Side(style="thin"),
                bottom=Side(style="thin"),
            )

            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                for cell in ws[1]:
                    cell.fill = header_fill
                    cell.font = header_font
                    cell.alignment = Alignment(horizontal="center")
                    cell.border = thin_border

                for col in ws.columns:
                    max_len = max(len(str(cell.value or "")) for cell in col)
                    ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 50)

                ws.freeze_panes = "A2"
        except Exception:
            pass

    return buffer.getvalue()
