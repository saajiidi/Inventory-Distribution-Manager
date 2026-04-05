from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from BackEnd.services.customer_insights import (
    generate_customer_insights,
    get_customer_segments,
    get_segment_summary,
    search_customers,
)
from BackEnd.services.hybrid_data_loader import (
    get_woocommerce_full_history_status,
    start_full_history_background_refresh,
)
from FrontEnd.components.ui_components import (
    render_highlight_stat,
    render_loaded_date_context,
    render_section_card,
    to_excel_bytes,
)
from FrontEnd.utils.config import APP_DATA_START_DATE
from FrontEnd.utils.error_handler import log_error



def render_customer_insight_tab():
    render_section_card(
        "Customer Insight",
        "RFM analysis for retention, churn, loyalty, and customer lifetime value.",
    )

    with st.sidebar:
        st.subheader("Data Connectors")
        st.info("Customer insights are exclusively powered by WooCommerce order history for maximum accuracy.")
        include_woo = True

    # Fixed rolling 120-day window
    end_date = date.today()
    start_date = end_date - timedelta(days=120)

    st.markdown("<div style='margin-bottom: 1rem;'></div>", unsafe_allow_html=True)
    load_clicked = st.button("Sync Insights", use_container_width=True, type="primary")

    end_date_str = end_date.strftime("%Y-%m-%d")
    history_status = get_woocommerce_full_history_status(end_date=end_date_str)
    history_started = start_full_history_background_refresh(end_date=end_date_str, force=load_clicked)
    if history_started:
        history_status = get_woocommerce_full_history_status(end_date=end_date_str)
    st.caption(history_status.get("status_message", ""))

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
                end_date=end_date_str,
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
        st.info("Click 'Sync Insights' to generate customer data for the last 120 days.")
        return

    df = st.session_state.customer_insights_df.copy()
    if search_query:
        df = search_customers(search_query, df)
    if selected_segment != "All" and "segment" in df.columns:
        df = df[df["segment"] == selected_segment]
    if df.empty:
        st.info("No customers found matching your filters.")
        return

    render_highlight_stat(
        "Total Unique Customers",
        f"{len(df):,}",
        "This is the distinct customer count in the current result after search, segment filters, and customer identity normalization.",
    )

    current_first_col = "current_first_order" if "current_first_order" in df.columns else "first_order"
    current_last_col = "current_last_order" if "current_last_order" in df.columns else "last_order"
    loaded_start = pd.to_datetime(df.get(current_first_col), errors="coerce").min()
    loaded_end = pd.to_datetime(df.get(current_last_col), errors="coerce").max()
    render_loaded_date_context(
        requested_start=start_date,
        requested_end=end_date,
        loaded_start=loaded_start,
        loaded_end=loaded_end,
        label="Loaded customer activity",
    )

    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Revenue", f"TK {df['total_revenue'].sum():,.0f}")
    with m2:
        st.metric("Avg Orders", f"{df['total_orders'].mean():.1f}")
    with m3:
        st.metric("Avg AOV", f"TK {df['avg_order_value'].mean():,.0f}")

    st.caption(f"Total Customers shows the distinct customer count in the current date range and active filters: {len(df):,}.")

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

    st.divider()
    from FrontEnd.pages.dashboard import render_dashboard_customers_section
    render_dashboard_customers_section()
