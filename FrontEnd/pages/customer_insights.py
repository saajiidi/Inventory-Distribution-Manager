"""Customer Insight Tab - Virtual customer insights powered by DuckDB.

This module provides customer insights generated on-the-fly from the hybrid
data loading system using DuckDB aggregation. No persistent parquet files needed.
"""

from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from FrontEnd.components.ui_components import render_action_bar, render_section_card, to_excel_bytes
from BackEnd.services.customer_insights import (
    generate_customer_insights,
    get_customer_segments,
    get_segment_summary,
    search_customers,
)


def render_customer_insight_tab():
    """Render the Customer Insight tab with RFM-powered insights."""
    render_section_card(
        "🎯 Customer Insight",
        "RFM Analysis (Recency, Frequency, Monetary) with DuckDB-powered customer segmentation",
    )

    # Source Selector
    with st.sidebar:
        st.subheader("🔌 Data Connectors")
        live_source = st.radio(
            "Primary Live Source",
            ["WooCommerce API Only", "Merged (Woo + Sheets)", "Google Sheets Only"],
            index=0,
            key="insight_live_source",
            help="Choose where the live dashboard fetches its latest 2026 data."
        )
        
    include_gsheet = "Sheets" in live_source or "Merged" in live_source
    include_woo = "Woo" in live_source or "Merged" in live_source

    # Date range selector
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        start_date = st.date_input("From", value=date(2022, 8, 1), key="insight_start_date")
    with col2:
        end_date = st.date_input("To", value=date.today(), key="insight_end_date")
    with col3:
        # Add vertical spacing to align button with date input fields
        st.markdown("<div style='height: 1.75rem;'></div>", unsafe_allow_html=True)
        load_clicked = st.button("🔍 Refresh Insights", use_container_width=True, type="primary")

    # Search box
    search_col1, search_col2 = st.columns([3, 1])
    with search_col1:
        search_query = st.text_input(
            "🔎 Search by name, email, phone, segment, or RFM score (e.g., 'vip', 'rfm:555')",
            "",
            key="customer_search",
            help="Search for customers by name, email, phone, segment (VIP, New, At Risk, Churned), or RFM score",
        )
    with search_col2:
        selected_segment = st.selectbox(
            "Filter by Segment",
            [
                "All",
                "⭐ VIP",
                "💰 Potential Loyalist",
                "📦 Regular",
                "🆕 New",
                "⚠️ At Risk",
                "💀 Churned",
            ],
            key="segment_filter",
        )

    if load_clicked or "customer_insights_df" in st.session_state:
        with st.spinner("Generating customer insights with DuckDB..."):
            try:
                # Generate insights
                df_insights = generate_customer_insights(
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                    include_gsheet=include_gsheet,
                    include_woocommerce=include_woo
                )

                if df_insights.empty:
                    st.warning("No customer data found for the selected date range.")
                    return

                # Store in session
                st.session_state.customer_insights_df = df_insights

            except Exception as e:
                st.error(f"Error generating insights: {e}")
                return

    # Display insights if available
    if "customer_insights_df" in st.session_state:
        df = st.session_state.customer_insights_df

        # Apply search filter
        if search_query:
            df = search_customers(search_query, df)

        # Apply segment filter
        if selected_segment != "All" and "segment" in df.columns:
            df = df[df["segment"] == selected_segment]

        if df.empty:
            st.info(f"No customers found matching your filters")
            return

        # Summary metrics - Top row: Business metrics
        st.subheader("Customer Summary")
        m1, m2, m3, m4, m5 = st.columns(5)
        with m1:
            st.metric("Total Customers", f"{len(df):,}")
        with m2:
            total_revenue = df["total_revenue"].sum()
            st.metric("Total Revenue", f"TK {total_revenue:,.0f}")
        with m3:
            avg_revenue = df["total_revenue"].mean()
            st.metric("Avg Revenue/Customer", f"TK {avg_revenue:,.0f}")
        with m4:
            total_orders = df["total_orders"].sum()
            st.metric("Total Orders", f"{total_orders:,}")
        with m5:
            avg_orders = df["total_orders"].mean()
            st.metric("Avg Orders/Customer", f"{avg_orders:.1f}")

        # RFM Metrics row
        if "r_score" in df.columns:
            st.subheader("RFM Overview")
            r1, r2, r3, r4 = st.columns(4)
            with r1:
                avg_recency = df["recency_days"].mean()
                st.metric("Avg Recency", f"{avg_recency:.0f} days")
            with r2:
                avg_frequency = df["total_orders"].mean()
                st.metric("Avg Frequency", f"{avg_frequency:.1f} orders")
            with r3:
                avg_monetary = df["total_revenue"].mean()
                st.metric("Avg Monetary", f"TK {avg_monetary:,.0f}")
            with r4:
                avg_rfm = df["rfm_avg"].mean() if "rfm_avg" in df.columns else 0
                st.metric("Avg RFM Score", f"{avg_rfm:.1f}/5")

        st.divider()

        # Customer segments with visual cards
        with st.expander("📊 RFM Customer Segments", expanded=True):
            segments = get_customer_segments(df)

            if segments:
                # Create segment cards
                seg_cols = st.columns(min(len(segments), 6))
                for idx, (seg_name, seg_df) in enumerate(segments.items()):
                    with seg_cols[idx % 6]:
                        count = len(seg_df)
                        revenue = seg_df["total_revenue"].sum() if not seg_df.empty else 0
                        pct = (count / len(df)) * 100 if len(df) > 0 else 0

                        st.metric(seg_name, f"{count:,} ({pct:.1f}%)", f"TK {revenue:,.0f}")

                # Segment summary table
                st.divider()
                summary_df = get_segment_summary(df)
                if not summary_df.empty:
                    st.caption("Segment Details")
                    # Format currency columns
                    for col in ["Total Revenue", "Avg Revenue"]:
                        if col in summary_df.columns:
                            summary_df[col] = summary_df[col].apply(lambda x: f"TK {x:,.0f}")
                    st.dataframe(summary_df, use_container_width=True, hide_index=True)
            else:
                st.info("No segment data available")

        st.divider()

        # Customer table with RFM columns
        st.subheader("Customer Details")

        # Format for display
        display_df = df.copy()
        display_df["total_revenue"] = display_df["total_revenue"].apply(lambda x: f"TK {x:,.0f}")
        display_df["avg_order_value"] = display_df["avg_order_value"].apply(
            lambda x: f"TK {x:,.0f}"
        )

        # Reorder columns for display - include RFM if available
        base_cols = [
            "primary_name",
            "all_emails",
            "all_phones",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "first_order",
            "last_order",
        ]

        # Add RFM columns if they exist
        rfm_cols = [
            "segment",
            "rfm_score",
            "recency_days",
            "purchase_cycle_days",
            "favorite_product",
        ]
        for col in rfm_cols:
            if col in df.columns:
                base_cols.append(col)

        base_cols.append("customer_id")

        display_df = display_df[[c for c in base_cols if c in display_df.columns]]

        # Rename columns for display
        col_rename = {
            "primary_name": "Name",
            "all_emails": "Email(s)",
            "all_phones": "Phone(s)",
            "total_orders": "Orders",
            "total_revenue": "Revenue",
            "avg_order_value": "AOV",
            "first_order": "First Order",
            "last_order": "Last Order",
            "segment": "Segment",
            "rfm_score": "RFM Score",
            "recency_days": "Recency (days)",
            "purchase_cycle_days": "Purchase Cycle",
            "favorite_product": "Favorite Product",
            "customer_id": "ID",
        }
        display_df.columns = [col_rename.get(c, c) for c in display_df.columns]

        st.dataframe(display_df, use_container_width=True, height=500)

        # Export button
        excel_bytes = to_excel_bytes(df, "Customer Insights")
        st.download_button(
            label="📥 Download Excel",
            data=excel_bytes,
            file_name=f"customer_insights_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # Customer detail view with RFM metrics
        st.divider()
        st.subheader("🔍 Customer Detail View")

        selected_customer = st.selectbox(
            "Select customer to view details",
            options=df["primary_name"].tolist(),
            key="customer_detail_select",
        )

        if selected_customer:
            customer_row = df[df["primary_name"] == selected_customer].iloc[0]

            # Top info row
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write(f"**Name:** {customer_row['primary_name']}")
                st.write(f"**Customer ID:** `{customer_row['customer_id']}`")
                if "segment" in customer_row:
                    st.write(f"**Segment:** {customer_row['segment']}")
            with col2:
                st.write(f"**Emails:** {customer_row['all_emails'] or 'N/A'}")
                if "favorite_product" in customer_row and pd.notna(
                    customer_row["favorite_product"]
                ):
                    st.write(f"**Favorite Product:** {customer_row['favorite_product']}")
            with col3:
                st.write(f"**Phones:** {customer_row['all_phones'] or 'N/A'}")

            # RFM Scores row
            if "r_score" in customer_row:
                st.divider()
                st.subheader("📊 RFM Scores")
                rfm_col1, rfm_col2, rfm_col3, rfm_col4 = st.columns(4)
                with rfm_col1:
                    st.metric(
                        "Recency (R)",
                        f"{customer_row['r_score']}/5",
                        f"{customer_row['recency_days']:.0f} days ago",
                    )
                with rfm_col2:
                    st.metric(
                        "Frequency (F)",
                        f"{customer_row['f_score']}/5",
                        f"{customer_row['total_orders']} orders",
                    )
                with rfm_col3:
                    st.metric(
                        "Monetary (M)",
                        f"{customer_row['m_score']}/5",
                        f"TK {customer_row['total_revenue']:,.0f}",
                    )
                with rfm_col4:
                    rfm_score = customer_row.get("rfm_score", "N/A")
                    st.metric("RFM Score", rfm_score)

                # Purchase cycle info
                if "purchase_cycle_days" in customer_row and pd.notna(
                    customer_row["purchase_cycle_days"]
                ):
                    st.info(
                        f"📅 Average Purchase Cycle: Every {customer_row['purchase_cycle_days']:.0f} days"
                    )

                    # Check if overdue
                    days_since_last = customer_row["recency_days"]
                    avg_cycle = customer_row["purchase_cycle_days"]
                    if days_since_last > avg_cycle * 1.5:
                        st.warning(
                            f"⚠️ Customer is overdue! Expected order {days_since_last - avg_cycle:.0f} days ago"
                        )

            # Order timeline
            st.divider()
            st.subheader("📈 Order History")
            hist_col1, hist_col2, hist_col3, hist_col4 = st.columns(4)
            with hist_col1:
                st.write(f"**Total Orders:** {customer_row['total_orders']}")
            with hist_col2:
                st.write(f"**First Order:** {customer_row['first_order']}")
            with hist_col3:
                st.write(f"**Last Order:** {customer_row['last_order']}")
            with hist_col4:
                st.write(f"**AOV:** TK {customer_row['avg_order_value']:,.0f}")

            st.write(
                f"**Customer Lifetime Value:** TK {customer_row.get('clv', customer_row['total_revenue']):,.0f}"
            )
    else:
        st.info("👆 Select a date range and click 'Load Insights' to generate customer data.")
