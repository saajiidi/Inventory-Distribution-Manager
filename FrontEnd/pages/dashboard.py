"""Main retail dashboard powered by the normalized hybrid sales schema."""

from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from BackEnd.services.customer_insights import generate_customer_insights
from BackEnd.services.hybrid_data_loader import get_data_summary, load_hybrid_data
from BackEnd.utils.sales_schema import ensure_sales_schema
from FrontEnd.components.ui_components import (
    render_bi_hero,
    render_commentary_panel,
    render_section_card,
)
from FrontEnd.utils.error_handler import log_error



def render_dashboard_tab():
    render_bi_hero(
        "Commerce Command Center",
        "A cleaner BI-style operating view for revenue, demand, customer health, and geographic performance. The dashboard is now optimized for WooCommerce-first analysis with less visual noise and clearer executive signals.",
        chips=[
            "WooCommerce-first",
            "Hybrid live data",
            "Customer intelligence",
            "Modern BI layout",
        ],
    )

    with st.sidebar:
        st.subheader("Data Connectors")
        live_source = st.radio(
            "Primary Live Source",
            ["WooCommerce API Only", "Merged (Woo + Sheets)", "Google Sheets Only"],
            index=0,
            key="dashboard_live_source",
        )

    include_gsheet = live_source in {"Merged (Woo + Sheets)", "Google Sheets Only"}
    include_woo = live_source in {"Merged (Woo + Sheets)", "WooCommerce API Only"}

    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        start_date = st.date_input("From", value=date(2022, 8, 1), key="dashboard_start_date")
    with col2:
        end_date = st.date_input("To", value=date.today(), key="dashboard_end_date")
    with col3:
        st.markdown("<div style='height: 1.75rem;'></div>", unsafe_allow_html=True)
        load_clicked = st.button("Refresh Data", use_container_width=True, type="primary")

    if load_clicked or "dashboard_data" in st.session_state:
        try:
            df_sales = ensure_sales_schema(
                load_hybrid_data(
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                    include_gsheet=include_gsheet,
                    include_woocommerce=include_woo,
                )
            )
            df_customers = generate_customer_insights(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                include_gsheet=include_gsheet,
                include_woocommerce=include_woo,
            )
            if df_sales.empty:
                st.warning("No sales data found for the selected date range.")
                return
            st.session_state.dashboard_data = {
                "sales": df_sales,
                "customers": df_customers,
                "summary": get_data_summary(),
            }
        except Exception as exc:
            log_error(exc, context="Dashboard Load")
            st.error(f"Error loading dashboard data: {exc}")
            return

    if "dashboard_data" not in st.session_state:
        st.info("Select a date range and click Refresh Data to view insights.")
        return

    data = st.session_state.dashboard_data
    df_sales = ensure_sales_schema(data["sales"])
    df_customers = data["customers"]
    summary = data.get("summary", {})

    tabs = st.tabs(["Executive Summary", "Sales Trends", "Product Performance", "Customer Behavior", "Geographic"])
    with tabs[0]:
        render_executive_summary(df_sales, df_customers, summary)
    with tabs[1]:
        render_sales_trends(df_sales)
    with tabs[2]:
        render_product_performance(df_sales)
    with tabs[3]:
        render_customer_behavior(df_sales, df_customers)
    with tabs[4]:
        render_geographic_insights(df_sales)



def render_executive_summary(df_sales: pd.DataFrame, df_customers: pd.DataFrame, summary: dict):
    st.subheader("Executive Summary")
    df = df_sales[df_sales["order_date"].notna()].copy()
    df["order_day"] = df["order_date"].dt.normalize()
    today = pd.Timestamp.now().normalize()
    yesterday = today - pd.Timedelta(days=1)

    today_data = df[df["order_day"] == today]
    yesterday_data = df[df["order_day"] == yesterday]

    total_revenue = float(df["order_total"].sum())
    total_orders = df["order_id"].replace("", pd.NA).dropna().nunique()
    active_customers = df["customer_key"].replace("", pd.NA).dropna().nunique()
    total_items = float(df["qty"].sum())
    pending_count = len(df[df["order_status"].str.lower().isin(["pending", "processing", "on-hold"])])

    today_revenue = float(today_data["order_total"].sum())
    yesterday_revenue = float(yesterday_data["order_total"].sum())
    today_orders = today_data["order_id"].replace("", pd.NA).dropna().nunique()
    yesterday_orders = yesterday_data["order_id"].replace("", pd.NA).dropna().nunique()
    today_aov = today_revenue / today_orders if today_orders else 0
    yesterday_aov = yesterday_revenue / yesterday_orders if yesterday_orders else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("Revenue", f"TK {total_revenue:,.0f}", _pct_delta(today_revenue, yesterday_revenue, "today vs yesterday"))
    with k2:
        st.metric("Orders", f"{total_orders:,}", _pct_delta(today_orders, yesterday_orders, "today vs yesterday"))
    with k3:
        overall_aov = total_revenue / total_orders if total_orders else 0
        st.metric("AOV", f"TK {overall_aov:,.0f}", _pct_delta(today_aov, yesterday_aov, "today vs yesterday"))
    with k4:
        st.metric("Customers", f"{active_customers:,}")
    with k5:
        st.metric("Pending", f"{pending_count:,}", "Needs action" if pending_count > 5 else "Healthy")

    s1, s2, s3 = st.columns(3)
    with s1:
        st.metric("Items Sold", f"{total_items:,.0f}")
        st.caption(f"Historical rows: {summary.get('historical', 0):,} | Woo live rows: {summary.get('woocommerce_live', 0):,}")
    with s2:
        repeat_rate = 0.0
        if isinstance(df_customers, pd.DataFrame) and not df_customers.empty and "total_orders" in df_customers.columns:
            repeat_rate = float((df_customers["total_orders"] > 1).mean() * 100)
        st.metric("Repeat Rate", f"{repeat_rate:.1f}%")
    with s3:
        latest_date = df["order_date"].max()
        st.metric("Latest Order", latest_date.strftime("%Y-%m-%d %H:%M") if pd.notna(latest_date) else "N/A")

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
    render_commentary_panel("Intelligence Commentary", insights)



def render_sales_trends(df: pd.DataFrame):
    st.subheader("Sales Trends")
    df = df[df["order_date"].notna()].copy()
    if df.empty:
        st.info("No date data available for trend analysis.")
        return

    trend_df = df.copy()
    trend_df["order_day"] = trend_df["order_date"].dt.date
    trend_df["day_name"] = trend_df["order_date"].dt.day_name()
    trend_df["day_num"] = trend_df["order_date"].dt.dayofweek
    trend_df["hour"] = trend_df["order_date"].dt.hour

    daily = trend_df.groupby("order_day", as_index=False).agg(Revenue=("order_total", "sum"), Orders=("order_id", "nunique"))
    fig_line = px.line(daily, x="order_day", y="Revenue", title="Daily Revenue", markers=True)
    fig_line.update_layout(height=350, xaxis_title="Date")
    st.plotly_chart(fig_line, use_container_width=True)

    commentary = []
    if not daily.empty:
        peak_day = daily.loc[daily["Revenue"].idxmax()]
        commentary.append(
            f"Peak daily revenue in this selection was TK {peak_day['Revenue']:,.0f} on {peak_day['order_day']}."
        )
        if len(daily) > 1:
            recent_avg = daily["Revenue"].tail(min(7, len(daily))).mean()
            commentary.append(
                f"Recent run-rate is about TK {recent_avg:,.0f} per day based on the latest visible period."
            )
    render_commentary_panel("Trend Commentary", commentary)

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
    if df.empty:
        st.info("No product data available.")
        return

    grouped = df.groupby("item_name").agg(Revenue=("order_total", "sum"), Units=("qty", "sum"), Orders=("order_id", "nunique")).reset_index()
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
            f"The top 10 products contribute {concentration * 100:.1f}% of visible product revenue, which helps show catalog concentration risk."
        )
    render_commentary_panel("Merchandising Commentary", product_notes)

    c1, c2 = st.columns(2)
    with c1:
        fig_top = px.bar(top_products.sort_values("Revenue"), x="Revenue", y="item_name", orientation="h", title="Top Products by Revenue", color="Revenue", color_continuous_scale="Greens")
        fig_top.update_layout(height=400, yaxis_title="Product")
        st.plotly_chart(fig_top, use_container_width=True)
    with c2:
        fig_units = px.bar(top_products.sort_values("Units"), x="Units", y="item_name", orientation="h", title="Top Products by Units Sold", color="Units", color_continuous_scale="Blues")
        fig_units.update_layout(height=400, yaxis_title="Product")
        st.plotly_chart(fig_units, use_container_width=True)

    st.dataframe(top_products.rename(columns={"item_name": "Product"}), use_container_width=True, hide_index=True)



def render_customer_behavior(df_sales: pd.DataFrame, df_customers: pd.DataFrame):
    st.subheader("Customer Behavior")
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
    render_commentary_panel("Retention Commentary", customer_notes)

    c1, c2 = st.columns(2)
    with c1:
        fig_donut = go.Figure(data=[go.Pie(labels=["New", "Returning"], values=[new_customers, returning_customers], hole=0.45)])
        fig_donut.update_layout(title="New vs Returning Customers", height=320)
        st.plotly_chart(fig_donut, use_container_width=True)
    with c2:
        segment_counts = df_customers["segment"].value_counts().reset_index()
        segment_counts.columns = ["Segment", "Count"]
        fig_segments = px.bar(segment_counts, x="Segment", y="Count", color="Count", title="Customer Segments")
        fig_segments.update_layout(height=320)
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
        fig_scatter.update_layout(height=420)
        st.plotly_chart(fig_scatter, use_container_width=True)



def render_geographic_insights(df: pd.DataFrame):
    st.subheader("Geographic Insights")
    geo = df.copy()
    geo["region"] = geo["state"].where(geo["state"] != "", geo["city"])
    geo = geo[geo["region"].astype(str).str.strip() != ""]
    if geo.empty:
        st.info("No geographic data found in the selected dataset.")
        return

    geo_sales = geo.groupby("region").agg(Revenue=("order_total", "sum"), Orders=("order_id", "nunique")).reset_index().sort_values("Revenue", ascending=False).head(15)
    geo_notes = []
    if not geo_sales.empty:
        leader = geo_sales.iloc[0]
        geo_notes.append(
            f"Top visible region is {leader['region']} with TK {leader['Revenue']:,.0f} revenue from {leader['Orders']:,} orders."
        )
        geo_notes.append(
            "Use this view to prioritize delivery reliability, ad targeting, and stock placement by region."
        )
    render_commentary_panel("Regional Commentary", geo_notes)
    c1, c2 = st.columns(2)
    with c1:
        fig_geo = px.bar(geo_sales.sort_values("Revenue"), x="Revenue", y="region", orientation="h", title="Revenue by Region", color="Revenue", color_continuous_scale="Teal")
        fig_geo.update_layout(height=400, yaxis_title="Region")
        st.plotly_chart(fig_geo, use_container_width=True)
    with c2:
        fig_orders = px.bar(geo_sales.sort_values("Orders"), x="Orders", y="region", orientation="h", title="Orders by Region", color="Orders", color_continuous_scale="Oranges")
        fig_orders.update_layout(height=400, yaxis_title="Region")
        st.plotly_chart(fig_orders, use_container_width=True)



def _pct_delta(current: float, previous: float, suffix: str = "") -> str | None:
    if previous in (0, None):
        return None
    delta = ((current - previous) / previous) * 100
    return f"{delta:+.1f}% {suffix}".strip()
