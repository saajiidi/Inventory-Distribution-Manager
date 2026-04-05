"""E-commerce Retail Dashboard Tab.

A comprehensive dashboard with four main sections:
1. Executive Summary - KPI cards and business metrics
2. Sales Trends - Time-based analysis and patterns
3. Product Performance - Catalog and inventory insights
4. Customer Behavior - Retention and segmentation analysis
"""

from datetime import date, datetime, timedelta
from typing import Optional, Dict, List, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from FrontEnd.components.ui_components import render_section_card, to_excel_bytes
from BackEnd.services.hybrid_data_loader import load_hybrid_data
from BackEnd.services.customer_insights import generate_customer_insights


def render_dashboard_tab():
    """Render the comprehensive E-commerce Dashboard."""
    render_section_card(
        "📊 Retail Dashboard",
        "Executive insights into sales trends, product performance, and customer behavior",
    )

    # Source Selector
    with st.sidebar:
        st.subheader("🔌 Data Connectors")
        live_source = st.radio(
            "Primary Live Source",
            ["WooCommerce API Only", "Merged (Woo + Sheets)", "Google Sheets Only"],
            index=0,
            key="dashboard_live_source",
            help="Choose where the live dashboard fetches its latest 2026 data."
        )
        
    include_gsheet = "Sheets" in live_source or "Merged" in live_source
    include_woo = "Woo" in live_source or "Merged" in live_source

    # Date range selector
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        start_date = st.date_input("From", value=date(2022, 8, 1), key="dashboard_start_date")
    with col2:
        end_date = st.date_input("To", value=date.today(), key="dashboard_end_date")
    with col3:
        # Add vertical spacing to align button with date input fields
        st.markdown("<div style='height: 1.75rem;'></div>", unsafe_allow_html=True)
        load_clicked = st.button("🔄 Refresh Data", use_container_width=True, type="primary")

    if load_clicked or "dashboard_data" in st.session_state:
        with st.spinner("Syncing latest data..."):
            try:
                # Load sales data
                df_sales = load_hybrid_data(
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                    include_gsheet=include_gsheet,
                    include_woocommerce=include_woo
                )

                # Load customer insights
                df_customers = generate_customer_insights(
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                )

                if df_sales.empty:
                    st.warning("No sales data found for the selected date range.")
                    return

                # Store in session
                st.session_state.dashboard_data = {
                    "sales": df_sales,
                    "customers": df_customers,
                    "start_date": start_date,
                    "end_date": end_date,
                }

            except Exception as e:
                st.error(f"Error loading dashboard data: {e}")
                return

    # Display dashboard if data available
    if "dashboard_data" in st.session_state:
        data = st.session_state.dashboard_data
        df_sales = data["sales"]
        df_customers = data["customers"]

        # Create tabs for each section
        dash_tabs = st.tabs(
            [
                "📈 Executive Summary",
                "📉 Sales Trends",
                "🛍️ Product Performance",
                "👥 Customer Behavior",
                "🗺️ Geographic",
            ]
        )

        with dash_tabs[0]:
            render_executive_summary(df_sales, df_customers, data["start_date"], data["end_date"])

        with dash_tabs[1]:
            render_sales_trends(df_sales)

        with dash_tabs[2]:
            render_product_performance(df_sales)

        with dash_tabs[3]:
            render_customer_behavior(df_sales, df_customers)

        with dash_tabs[4]:
            render_geographic_insights(df_sales)
    else:
        st.info("👆 Select a date range and click 'Load Dashboard' to view insights.")


def render_executive_summary(
    df_sales: pd.DataFrame, df_customers: pd.DataFrame, start_date: date, end_date: date
):
    """Render Executive Summary with Smart KPI cards featuring Delta comparisons."""
    st.subheader("Executive Summary - Today's Performance")

    # Ensure Order Date is datetime
    date_col = "Order Date" if "Order Date" in df_sales.columns else "order_date"
    if date_col in df_sales.columns:
        df_sales[date_col] = pd.to_datetime(df_sales[date_col], errors="coerce")

    # Calculate TODAY's metrics
    today = pd.Timestamp.now().normalize()
    today_data = (
        df_sales[df_sales[date_col] == today] if date_col in df_sales.columns else pd.DataFrame()
    )

    # Calculate YESTERDAY's metrics for comparison
    yesterday = today - pd.Timedelta(days=1)
    yesterday_data = (
        df_sales[df_sales[date_col] == yesterday]
        if date_col in df_sales.columns
        else pd.DataFrame()
    )

    # Revenue metrics
    revenue_col = (
        "Order Total Amount" if "Order Total Amount" in df_sales.columns else "order_total"
    )

    # Today's numbers
    today_revenue = (
        pd.to_numeric(today_data[revenue_col], errors="coerce").sum() if not today_data.empty else 0
    )
    today_orders = (
        today_data["Order Number"].nunique() if "Order Number" in today_data.columns else 0
    )

    # Yesterday's numbers
    yesterday_revenue = (
        pd.to_numeric(yesterday_data[revenue_col], errors="coerce").sum()
        if not yesterday_data.empty
        else 0
    )
    yesterday_orders = (
        yesterday_data["Order Number"].nunique() if "Order Number" in yesterday_data.columns else 0
    )

    # AOV calculations
    today_aov = today_revenue / today_orders if today_orders > 0 else 0
    yesterday_aov = yesterday_revenue / yesterday_orders if yesterday_orders > 0 else 0

    # Pending Orders (orders without shipped_date or status == 'Pending')
    pending_count = 0
    if "Status" in df_sales.columns:
        pending_count = len(df_sales[df_sales["Status"].str.lower() == "pending"])
    elif "shipped_date" in df_sales.columns:
        pending_count = df_sales["shipped_date"].isna().sum()

    # Row 1: Main KPIs with Delta (Today vs Yesterday) - Responsive layout
    st.markdown("#### 📊 Today's Performance vs Yesterday")
    # Use responsive column ratios for better alignment
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns([3, 3, 3, 2])

    with kpi_col1:
        # Revenue with delta vs yesterday
        if yesterday_revenue > 0:
            revenue_delta_pct = ((today_revenue - yesterday_revenue) / yesterday_revenue) * 100
            revenue_delta_label = f"{revenue_delta_pct:+.1f}% vs yesterday"
        else:
            revenue_delta_label = "No data yesterday"

        st.metric(
            label="💰 Today's Revenue",
            value=f"TK {today_revenue:,.0f}",
            delta=revenue_delta_label,
            delta_color="normal",
        )
        st.caption(f"Yesterday: TK {yesterday_revenue:,.0f}")

    with kpi_col2:
        # Orders with delta
        if yesterday_orders > 0:
            orders_delta_pct = ((today_orders - yesterday_orders) / yesterday_orders) * 100
            orders_delta_label = f"{orders_delta_pct:+.0f}% vs yesterday"
        else:
            orders_delta_label = "No data yesterday"

        st.metric(label="📦 Today's Orders", value=f"{today_orders:,}", delta=orders_delta_label)
        st.caption(f"Yesterday: {yesterday_orders:,}")

    with kpi_col3:
        # AOV with delta
        if yesterday_aov > 0:
            aov_delta_pct = ((today_aov - yesterday_aov) / yesterday_aov) * 100
            aov_delta_label = f"{aov_delta_pct:+.1f}% vs yesterday"
        else:
            aov_delta_label = None

        st.metric(
            label="🛒 AOV (Avg Order Value)", value=f"TK {today_aov:,.0f}", delta=aov_delta_label
        )
        if yesterday_aov > 0:
            st.caption(f"Yesterday: TK {yesterday_aov:,.0f}")

    with kpi_col4:
        # Pending Orders Alert
        delta_color = "inverse" if pending_count > 10 else "normal"
        st.metric(
            label="⏳ Pending Orders",
            value=f"{pending_count}",
            delta="Action needed" if pending_count > 5 else "On track",
            delta_color=delta_color,
        )
        if pending_count > 5:
            st.warning(f"⚠️ You have {pending_count} orders waiting to be shipped!")

    # Row 2: Financial Health & Retention Metrics
    st.divider()
    st.markdown("#### 💵 Financial Health & Customer Retention")

    # Calculate period metrics
    total_revenue = calculate_revenue(df_sales)
    total_orders = calculate_orders(df_sales)
    active_customers = calculate_active_customers(df_sales)

    # Sales Velocity (Revenue per day this month)
    days_in_month = pd.Timestamp.now().day
    month_revenue = (
        pd.to_numeric(
            df_sales[df_sales[date_col] >= pd.Timestamp.now().replace(day=1)][revenue_col],
            errors="coerce",
        ).sum()
        if date_col in df_sales.columns
        else 0
    )
    sales_velocity = month_revenue / days_in_month if days_in_month > 0 else 0

    # Monthly target tracking
    monthly_goal = 100000  # TK
    progress = min(month_revenue / monthly_goal, 1.0) if monthly_goal > 0 else 0
    projected_monthly = sales_velocity * 30

    # Customer Retention metrics
    if not df_customers.empty and "total_orders" in df_customers.columns:
        repeat_customers = len(df_customers[df_customers["total_orders"] > 1])
        total_customers = len(df_customers)
        repeat_rate = (repeat_customers / total_customers * 100) if total_customers > 0 else 0

        new_customers = len(df_customers[df_customers["total_orders"] == 1])
        new_pct = (new_customers / total_customers * 100) if total_customers > 0 else 0
    else:
        repeat_rate = 0
        new_pct = 0

    # Use responsive column ratios for better alignment
    health_col1, health_col2, health_col3, health_col4 = st.columns([3, 3, 3, 2])

    with health_col1:
        st.metric(
            label="📈 Sales Velocity",
            value=f"TK {sales_velocity:,.0f}/day",
            delta=f"Projected: TK {projected_monthly:,.0f}/mo",
        )
        st.progress(progress, text=f"Monthly Goal: {progress*100:.1f}%")

    with health_col2:
        st.metric(
            label="🔄 Repeat Customer Rate",
            value=f"{repeat_rate:.1f}%",
            delta="Healthy" if repeat_rate > 40 else "Needs attention",
            delta_color="normal" if repeat_rate > 40 else "inverse",
        )
        st.caption(
            f"{repeat_customers if 'repeat_customers' in locals() else 0} returning customers"
        )

    with health_col3:
        st.metric(
            label="🆕 New Customer %",
            value=f"{new_pct:.1f}%",
            delta="High acquisition" if new_pct > 70 else "Balanced",
            delta_color="normal" if new_pct < 70 else "off",
        )
        if new_pct > 70:
            st.info("💡 Focus on retention strategies")

    with health_col4:
        # Period AOV comparison
        period_aov = total_revenue / total_orders if total_orders > 0 else 0
        st.metric(
            label="💎 Period AOV",
            value=f"TK {period_aov:,.0f}",
            delta=f"{total_orders:,} total orders",
        )

    # Smart Insights Section
    st.divider()
    st.markdown("#### 🧠 Smart Insights")

    insights = []

    # AOV Trend Insight
    if today_aov < yesterday_aov * 0.95:  # 5% drop
        insights.append(
            (
                "warning",
                "💡 **AOV Alert:** Customers buying cheaper items today. Consider 'Frequently Bought Together' bundles.",
            )
        )
    elif today_aov > yesterday_aov * 1.1:  # 10% increase
        insights.append(
            ("success", "💡 **AOV Growth:** Customers spending more! Upselling is working.")
        )

    # Pending Orders Insight
    if pending_count > 10:
        insights.append(
            (
                "error",
                f"⚠️ **Shipping Alert:** {pending_count} orders pending. Process immediately to avoid customer complaints.",
            )
        )

    # Sales Velocity Insight
    if projected_monthly < monthly_goal * 0.9:
        shortfall = monthly_goal - projected_monthly
        insights.append(
            (
                "warning",
                f"📉 **Goal Alert:** Projected to miss monthly target by TK {shortfall:,.0f}. Ramp up marketing!",
            )
        )
    elif projected_monthly > monthly_goal:
        insights.append(
            (
                "success",
                f"✅ **Goal On Track:** Projected to exceed monthly target by TK {projected_monthly - monthly_goal:,.0f}!",
            )
        )

    # Retention Insight
    if repeat_rate < 30 and new_pct > 60:
        insights.append(
            (
                "warning",
                "🔄 **Retention Issue:** Too many new customers vs returning. Implement loyalty program.",
            )
        )

    # Display insights
    if insights:
        for severity, message in insights:
            if severity == "error":
                st.error(message)
            elif severity == "warning":
                st.warning(message)
            else:
                st.success(message)
    else:
        st.info("📊 Business metrics are stable. No immediate actions required.")


def render_sales_trends(df: pd.DataFrame):
    """Render Sales Trends section with time-based analysis."""
    st.subheader("Sales Trends")

    # Ensure date column exists and is datetime
    date_col = "Order Date" if "Order Date" in df.columns else "order_date"
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])

    if df.empty:
        st.info("No date data available for trend analysis.")
        return

    # Revenue Over Time - Line Chart
    st.markdown("#### Revenue Trend")
    daily_revenue = (
        df.groupby(df[date_col].dt.date)
        .agg(
            {
                "Order Total Amount": (
                    "sum"
                    if "Order Total Amount" in df.columns
                    else lambda x: pd.to_numeric(x, errors="coerce").sum()
                )
            }
        )
        .reset_index()
    )
    daily_revenue.columns = ["Date", "Revenue"]

    fig_line = px.line(
        daily_revenue,
        x="Date",
        y="Revenue",
        title="Daily Revenue Over Time",
        labels={"Revenue": "Revenue (TK)", "Date": ""},
    )
    fig_line.update_layout(
        height=350, xaxis_rangeslider_visible=True, margin=dict(l=40, r=40, t=50, b=40)
    )
    st.plotly_chart(fig_line, use_container_width=True)

    # Orders by Day of Week
    st.markdown("#### Orders by Day of Week")
    df["day_of_week"] = df[date_col].dt.day_name()
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    orders_by_day = df.groupby("day_of_week").size().reindex(day_order, fill_value=0).reset_index()
    orders_by_day.columns = ["Day", "Orders"]

    col1, col2 = st.columns(2)
    with col1:
        fig_bar = px.bar(
            orders_by_day,
            x="Day",
            y="Orders",
            color="Orders",
            color_continuous_scale="blues",
            title="Orders by Day of Week",
        )
        fig_bar.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig_bar, use_container_width=True)

    with col2:
        # Find lowest sales day for promotion insight
        min_day = orders_by_day.loc[orders_by_day["Orders"].idxmin(), "Day"]
        st.info(
            f"💡 **Insight:** Sales are lowest on **{min_day}**. Consider running promotions on this day."
        )

        # Show peak day
        max_day = orders_by_day.loc[orders_by_day["Orders"].idxmax(), "Day"]
        st.write(f"**Peak Sales Day:** {max_day}")
        st.write(f"**Lowest Sales Day:** {min_day}")

    # Heatmap: Peak Hours/Days
    st.markdown("#### Peak Activity Heatmap")
    df["hour"] = df[date_col].dt.hour
    df["day_num"] = df[date_col].dt.dayofweek
    df["day_name"] = df[date_col].dt.day_name()

    heatmap_data = df.groupby(["day_num", "hour"]).size().reset_index(name="Orders")
    heatmap_pivot = heatmap_data.pivot(index="day_num", columns="hour", values="Orders").fillna(0)

    # Reindex to force all 7 days (0-6) and all 24 hours (0-23) to exist in the matrix
    heatmap_pivot = heatmap_pivot.reindex(index=range(7), columns=range(24), fill_value=0)

    day_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    fig_heatmap = px.imshow(
        heatmap_pivot.values,
        labels=dict(x="Hour of Day", y="Day of Week", color="Orders"),
        x=list(range(24)),
        y=day_labels,
        color_continuous_scale="YlOrRd",
        title="Order Volume by Hour and Day",
    )
    fig_heatmap.update_layout(height=350)
    st.plotly_chart(fig_heatmap, use_container_width=True)

    st.caption("💡 **Use this to:** Schedule customer support staffing and plan ad campaign timing")


def render_product_performance(df: pd.DataFrame):
    """Render Product Performance section."""
    st.subheader("Product Performance")

    if df.empty:
        st.info("No product data available.")
        return

    # Top 10 Products by Revenue
    st.markdown("#### Top 10 Products by Revenue")

    product_col = "Item Name" if "Item Name" in df.columns else "item_name"
    revenue_col = "Order Total Amount" if "Order Total Amount" in df.columns else "order_total"

    if product_col in df.columns and revenue_col in df.columns:
        # Ensure numeric
        df[revenue_col] = pd.to_numeric(df[revenue_col], errors="coerce")

        top_products = (
            df.groupby(product_col)[revenue_col]
            .sum()
            .sort_values(ascending=False)
            .head(10)
            .reset_index()
        )
        top_products.columns = ["Product", "Revenue"]

        col1, col2 = st.columns(2)
        with col1:
            fig_top = px.bar(
                top_products,
                y="Product",
                x="Revenue",
                orientation="h",
                color="Revenue",
                color_continuous_scale="greens",
                title="Top 10 Products",
            )
            fig_top.update_layout(height=400, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_top, use_container_width=True)

        with col2:
            st.info("📦 **Action Items:**")
            st.write("- Ensure top products are always in stock")
            st.write("- Consider featuring these in marketing")
            st.write("- Bundle slower products with top sellers")

            # Stock alert simulation
            st.divider()
            st.warning("⚠️ **Low Stock Alert** (simulated)")
            low_stock = top_products.head(3)["Product"].tolist()
            for product in low_stock:
                st.write(f"- {product}")

    # Category Performance (if category data exists)
    st.markdown("#### Sales by Category")
    # Try to extract categories from product names or use existing category column
    if "Category" in df.columns or "category" in df.columns:
        cat_col = "Category" if "Category" in df.columns else "category"
    else:
        # Create simple categories from product names
        df["Category"] = df[product_col].apply(extract_category)
        cat_col = "Category"

    if cat_col in df.columns:
        category_sales = (
            df.groupby(cat_col)[revenue_col].sum().sort_values(ascending=False).reset_index()
        )
        category_sales.columns = ["Category", "Revenue"]

        col1, col2 = st.columns(2)
        with col1:
            fig_treemap = px.treemap(
                category_sales, path=["Category"], values="Revenue", title="Revenue by Category"
            )
            fig_treemap.update_layout(height=350)
            st.plotly_chart(fig_treemap, use_container_width=True)

        with col2:
            # Category pie chart
            fig_pie = px.pie(
                category_sales.head(8),
                values="Revenue",
                names="Category",
                title="Category Distribution",
            )
            fig_pie.update_layout(height=350)
            st.plotly_chart(fig_pie, use_container_width=True)


def render_customer_behavior(df_sales: pd.DataFrame, df_customers: pd.DataFrame):
    """Render Customer Behavior section."""
    st.subheader("Customer Behavior")

    # New vs Returning Customers
    st.markdown("#### Customer Acquisition vs Retention")

    if not df_customers.empty and "total_orders" in df_customers.columns:
        new_customers = len(df_customers[df_customers["total_orders"] == 1])
        returning_customers = len(df_customers[df_customers["total_orders"] > 1])
        total = new_customers + returning_customers

        if total > 0:
            new_pct = (new_customers / total) * 100
            returning_pct = (returning_customers / total) * 100

            col1, col2, col3 = st.columns(3)
            with col1:
                fig_donut = go.Figure(
                    data=[
                        go.Pie(
                            labels=["New Customers", "Returning Customers"],
                            values=[new_customers, returning_customers],
                            hole=0.4,
                            marker_colors=["#3b82f6", "#10b981"],
                        )
                    ]
                )
                fig_donut.update_layout(title="New vs Returning", height=300, showlegend=True)
                st.plotly_chart(fig_donut, use_container_width=True)

            with col2:
                st.metric("New Customers", f"{new_customers:,}", f"{new_pct:.1f}%")
                st.metric(
                    "Returning Customers", f"{returning_customers:,}", f"{returning_pct:.1f}%"
                )

            with col3:
                if new_pct > 70:
                    st.warning("⚠️ **High acquisition, low retention.** Focus on loyalty programs.")
                elif returning_pct > 60:
                    st.success("✅ **Strong retention!** Your customers are loyal.")
                else:
                    st.info("💡 **Balanced mix.** Good acquisition with decent retention.")

    # RFM Segments
    if not df_customers.empty and "segment" in df_customers.columns:
        st.markdown("#### Customer Segments (RFM Analysis)")

        segment_counts = df_customers["segment"].value_counts().reset_index()
        segment_counts.columns = ["Segment", "Count"]

        col1, col2 = st.columns(2)
        with col1:
            fig_segments = px.bar(
                segment_counts,
                x="Segment",
                y="Count",
                color="Count",
                color_continuous_scale="viridis",
                title="Customers by RFM Segment",
            )
            fig_segments.update_layout(height=350)
            st.plotly_chart(fig_segments, use_container_width=True)

        with col2:
            st.write("**Segment Actions:**")
            segment_actions = {
                "⭐ VIP": "Offer exclusive early access and VIP support",
                "💰 Potential Loyalist": "Invite to loyalty program",
                "🆕 New": "Send welcome email with 2nd order discount",
                "⚠️ At Risk": 'Send "We miss you" coupon',
                "💀 Churned": "Re-engagement campaign or ignore",
                "📦 Regular": "Maintain good service",
            }
            for segment, action in segment_actions.items():
                if segment in df_customers["segment"].values:
                    st.write(f"- **{segment}:** {action}")

    # Customer value scatter plot
    if not df_customers.empty and all(
        col in df_customers.columns for col in ["total_orders", "total_revenue"]
    ):
        st.markdown("#### Customer Value Matrix")

        fig_scatter = px.scatter(
            df_customers,
            x="total_orders",
            y="total_revenue",
            color="segment" if "segment" in df_customers.columns else None,
            size="avg_order_value" if "avg_order_value" in df_customers.columns else None,
            hover_name="primary_name" if "primary_name" in df_customers.columns else None,
            title="Customer Value: Frequency vs Monetary",
            labels={"total_orders": "Number of Orders", "total_revenue": "Total Revenue (TK)"},
        )
        fig_scatter.update_layout(height=400)
        st.plotly_chart(fig_scatter, use_container_width=True)

        st.caption(
            "💡 **Stars** (top-right) = High value customers | **Dogs** (bottom-left) = Low engagement"
        )


def render_geographic_insights(df: pd.DataFrame):
    """Render Geographic Insights section."""
    st.subheader("Geographic Insights")

    # Find state/location column
    state_col = None
    for col in [
        "State",
        "State Name (Billing)",
        "state",
        "Customer State",
        "City",
        "City (Billing)",
    ]:
        if col in df.columns:
            state_col = col
            break

    if state_col:
        st.markdown(f"#### Sales by {state_col.replace('_', ' ').title()}")

        revenue_col = "Order Total Amount" if "Order Total Amount" in df.columns else "order_total"
        df[revenue_col] = pd.to_numeric(df[revenue_col], errors="coerce")

        geo_sales = df.groupby(state_col)[revenue_col].agg(["sum", "count"]).reset_index()
        geo_sales.columns = ["Location", "Revenue", "Orders"]
        geo_sales = geo_sales.sort_values("Revenue", ascending=False).head(15)

        col1, col2 = st.columns(2)
        with col1:
            fig_geo = px.bar(
                geo_sales,
                y="Location",
                x="Revenue",
                orientation="h",
                color="Revenue",
                color_continuous_scale="teal",
                title="Revenue by Location",
            )
            fig_geo.update_layout(height=400, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_geo, use_container_width=True)

        with col2:
            fig_orders = px.bar(
                geo_sales,
                y="Location",
                x="Orders",
                orientation="h",
                color="Orders",
                color_continuous_scale="oranges",
                title="Order Count by Location",
            )
            fig_orders.update_layout(height=400, yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_orders, use_container_width=True)

        # Geographic insights
        st.divider()
        top_location = geo_sales.iloc[0]["Location"] if not geo_sales.empty else "Unknown"
        total_locations = df[state_col].nunique()

        insight_col1, insight_col2 = st.columns(2)
        with insight_col1:
            st.success(f"🏆 **Top Region:** {top_location}")
            st.write(f"Consider targeted ads in weaker regions to expand reach.")
        with insight_col2:
            st.info(f"🌍 **Total Locations:** {total_locations}")
            st.write(f"Monitor shipping logistics for top regions.")
    else:
        st.info("No geographic data (State/City columns) found in dataset.")


# Helper functions
def calculate_revenue(df: pd.DataFrame) -> float:
    """Calculate total revenue from dataframe."""
    col = "Order Total Amount" if "Order Total Amount" in df.columns else "order_total"
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").sum()
    return 0


def calculate_orders(df: pd.DataFrame) -> int:
    """Calculate total unique orders."""
    col = "Order Number" if "Order Number" in df.columns else "order_id"
    if col in df.columns:
        return df[col].nunique()
    return 0


def calculate_active_customers(df: pd.DataFrame) -> int:
    """Calculate number of active customers."""
    # Look for customer identifier columns
    for col in ["Customer Name", "customer_name", "Phone (Billing)", "phone", "Email", "email"]:
        if col in df.columns:
            return df[col].nunique()
    return 0


def calculate_total_items(df: pd.DataFrame) -> int:
    """Calculate total items sold."""
    col = "Qty" if "Qty" in df.columns else "qty" if "qty" in df.columns else "Quantity"
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").sum()
    return 0


def extract_category(product_name: str) -> str:
    """Extract category from product name."""
    name = str(product_name).lower()

    category_map = [
        ("t-shirt", "T-Shirts"),
        ("shirt", "Shirts"),
        ("polo", "Polos"),
        ("panjabi", "Panjabi"),
        ("jeans", "Jeans"),
        ("trouser", "Trousers"),
        ("boxer", "Boxers"),
        ("mask", "Masks"),
        ("wallet", "Accessories"),
        ("bag", "Accessories"),
        ("bottle", "Accessories"),
        ("belt", "Accessories"),
    ]

    for keyword, category in category_map:
        if keyword in name:
            return category

    return "Other"
