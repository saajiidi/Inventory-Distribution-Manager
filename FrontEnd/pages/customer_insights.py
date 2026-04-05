from datetime import date, datetime

import pandas as pd
import streamlit as st

from BackEnd.services.customer_insights import (
    generate_customer_insights,
    get_customer_segments,
    get_segment_summary,
    search_customers,
)
from FrontEnd.components.ui_components import render_section_card, to_excel_bytes
from FrontEnd.utils.error_handler import log_error



def render_customer_insight_tab():
    render_section_card(
        "Customer Insight",
        "RFM analysis for retention, churn, loyalty, and customer lifetime value.",
    )

    with st.sidebar:
        st.subheader("Data Connectors")
        live_source = st.radio(
            "Primary Live Source",
            ["WooCommerce API Only", "Merged (Woo + Sheets)", "Google Sheets Only"],
            index=0,
            key="insight_live_source",
        )

    include_gsheet = live_source in {"Merged (Woo + Sheets)", "Google Sheets Only"}
    include_woo = live_source in {"Merged (Woo + Sheets)", "WooCommerce API Only"}

    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        start_date = st.date_input("From", value=date(2022, 8, 1), key="insight_start_date")
    with col2:
        end_date = st.date_input("To", value=date.today(), key="insight_end_date")
    with col3:
        st.markdown("<div style='height: 1.75rem;'></div>", unsafe_allow_html=True)
        load_clicked = st.button("Refresh Insights", use_container_width=True, type="primary")

    search_col1, search_col2 = st.columns([3, 1])
    with search_col1:
        search_query = st.text_input(
            "Search by name, email, phone, segment, or RFM score",
            "",
            key="customer_search",
        )
    with search_col2:
        selected_segment = st.selectbox(
            "Filter by Segment",
            ["All", "VIP", "Potential Loyalist", "Regular", "New", "At Risk", "Churned"],
            key="segment_filter",
        )

    if load_clicked or "customer_insights_df" in st.session_state:
        try:
            df_insights = generate_customer_insights(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                include_gsheet=include_gsheet,
                include_woocommerce=include_woo,
            )
            if df_insights.empty:
                st.warning("No customer data found for the selected date range.")
                return
            st.session_state.customer_insights_df = df_insights
        except Exception as exc:
            log_error(exc, context="Customer Insight Page")
            st.error(f"Error generating insights: {exc}")
            return

    if "customer_insights_df" not in st.session_state:
        st.info("Select a date range and click Refresh Insights to generate customer data.")
        return

    df = st.session_state.customer_insights_df.copy()
    if search_query:
        df = search_customers(search_query, df)
    if selected_segment != "All" and "segment" in df.columns:
        df = df[df["segment"] == selected_segment]
    if df.empty:
        st.info("No customers found matching your filters.")
        return

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Customers", f"{len(df):,}")
    with m2:
        st.metric("Revenue", f"TK {df['total_revenue'].sum():,.0f}")
    with m3:
        st.metric("Avg Orders", f"{df['total_orders'].mean():.1f}")
    with m4:
        st.metric("Avg AOV", f"TK {df['avg_order_value'].mean():,.0f}")

    with st.expander("RFM Segments", expanded=True):
        segments = get_customer_segments(df)
        if segments:
            seg_cols = st.columns(min(len(segments), 6))
            for idx, (seg_name, seg_df) in enumerate(segments.items()):
                with seg_cols[idx % len(seg_cols)]:
                    st.metric(seg_name, f"{len(seg_df):,}", f"TK {seg_df['total_revenue'].sum():,.0f}")
            summary_df = get_segment_summary(df)
            if not summary_df.empty:
                st.dataframe(summary_df, use_container_width=True, hide_index=True)

    display_df = df.copy()
    display_df["total_revenue"] = display_df["total_revenue"].map(lambda x: f"TK {x:,.0f}")
    display_df["avg_order_value"] = display_df["avg_order_value"].map(lambda x: f"TK {x:,.0f}")
    wanted = [
        "primary_name",
        "all_emails",
        "all_phones",
        "segment",
        "rfm_score",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "recency_days",
        "purchase_cycle_days",
        "favorite_product",
        "first_order",
        "last_order",
        "customer_id",
    ]
    display_df = display_df[[col for col in wanted if col in display_df.columns]]
    display_df = display_df.rename(
        columns={
            "primary_name": "Name",
            "all_emails": "Emails",
            "all_phones": "Phones",
            "segment": "Segment",
            "rfm_score": "RFM Score",
            "total_orders": "Orders",
            "total_revenue": "Revenue",
            "avg_order_value": "AOV",
            "recency_days": "Recency (days)",
            "purchase_cycle_days": "Purchase Cycle",
            "favorite_product": "Favorite Product",
            "first_order": "First Order",
            "last_order": "Last Order",
            "customer_id": "Customer ID",
        }
    )
    st.dataframe(display_df, use_container_width=True, height=500, hide_index=True)

    excel_bytes = to_excel_bytes(df, "Customer Insights")
    st.download_button(
        label="Download Excel",
        data=excel_bytes,
        file_name=f"customer_insights_{datetime.now().strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
