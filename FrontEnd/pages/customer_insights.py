from datetime import date, datetime, timedelta

import pandas as pd
import plotly.express as px
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
    apply_plotly_theme,
    build_adaptive_donut,
    build_spotlight_bar,
    render_audit_card,
    render_bi_hero,
    render_commentary_panel,
    render_highlight_stat,
    render_kpi_note,
    render_loaded_date_context,
    to_excel_bytes,
)
from FrontEnd.utils.error_handler import log_error


def _format_currency(value: float) -> str:
    return f"Tk {value:,.0f}"


def _segment_mix(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "segment" not in df.columns:
        return pd.DataFrame(columns=["Segment", "Customers"])
    return df["segment"].fillna("Unknown").value_counts().rename_axis("Segment").reset_index(name="Customers")


def _segment_revenue(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "segment" not in df.columns:
        return pd.DataFrame(columns=["Segment", "Revenue", "Customers"])
    return (
        df.groupby("segment", as_index=False)
        .agg(Revenue=("total_revenue", "sum"), Customers=("customer_id", "count"))
        .rename(columns={"segment": "Segment"})
        .sort_values("Revenue", ascending=False)
    )


def _favorite_products(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "favorite_product" not in df.columns:
        return pd.DataFrame(columns=["Product", "Customers"])
    favorites = df[df["favorite_product"].astype(str).str.strip() != ""]
    if favorites.empty:
        return pd.DataFrame(columns=["Product", "Customers"])
    return (
        favorites.groupby("favorite_product", as_index=False)
        .agg(Customers=("customer_id", "count"), Revenue=("total_revenue", "sum"))
        .rename(columns={"favorite_product": "Product"})
        .sort_values(["Customers", "Revenue"], ascending=[False, False])
        .head(10)
    )


def _crm_priority_queue(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    queue = df.copy()
    queue["priority"] = 0
    queue.loc[queue["segment"] == "VIP", "priority"] += 3
    queue.loc[queue["segment"] == "At Risk", "priority"] += 2
    queue.loc[queue["segment"] == "Churned", "priority"] += 3
    queue.loc[pd.to_numeric(queue.get("recency_days"), errors="coerce").fillna(0) > 60, "priority"] += 1
    queue = queue.sort_values(["priority", "total_revenue"], ascending=[False, False])
    queue["recommended_action"] = queue["segment"].map(
        {
            "VIP": "White-glove support and recovery outreach",
            "At Risk": "Retention offer with fast follow-up",
            "Churned": "Win-back sequence or human callback",
            "Potential Loyalist": "Cross-sell with product recommendations",
            "New": "Onboarding and second-order nudges",
            "Regular": "Routine nurture and remarketing",
        }
    ).fillna("Review manually")
    return queue.head(20)


def render_customer_insight_tab():
    render_bi_hero(
        "Customer Intelligence",
        (
            "A CRM-led view of the customer base: who buys, who is slipping, which segments create value, "
            "and where ShopAI support can drive retention and conversion."
        ),
        chips=[
            "WooCommerce-only identity",
            "RFM segmentation",
            "CRM actions",
            "ShopAI-linked support",
        ],
    )

    with st.sidebar:
        st.subheader("Customer Data")
        st.info("Customer intelligence is built from WooCommerce order history with identity normalization across names, phones, and emails.")

    end_date = date.today()
    start_date = end_date - timedelta(days=120)
    end_date_str = end_date.strftime("%Y-%m-%d")

    action_col_1, action_col_2 = st.columns([1.3, 1])
    with action_col_1:
        load_clicked = st.button("Sync Customer Intelligence", use_container_width=True, type="primary")
    with action_col_2:
        load_history_clicked = st.button("Refresh Lifetime History", use_container_width=True)

    history_status = get_woocommerce_full_history_status(end_date=end_date_str)
    history_started = start_full_history_background_refresh(end_date=end_date_str, force=load_clicked or load_history_clicked)
    if history_started:
        history_status = get_woocommerce_full_history_status(end_date=end_date_str)

    render_audit_card(
        "History readiness",
        history_status.get(
            "status_message",
            "Lifetime WooCommerce history sync status is not available yet.",
        ),
    )

    filter_col_1, filter_col_2 = st.columns([3, 1.2])
    with filter_col_1:
        search_query = st.text_input("Search by name, email, phone, segment, or RFM score", "", key="customer_search")
    with filter_col_2:
        selected_segment = st.selectbox(
            "Filter by Segment",
            ["All", "VIP", "Potential Loyalist", "Regular", "New", "At Risk", "Churned"],
            key="segment_filter",
        )

    if load_clicked or "customer_insights_df" not in st.session_state:
        try:
            df_insights = generate_customer_insights(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date_str,
                include_woocommerce=True,
            )
            if df_insights.empty:
                st.warning("No customer data found for the selected date range.")
                return
            st.session_state.customer_insights_df = df_insights
        except Exception as exc:
            log_error(exc, context="Customer Insight Page")
            st.error(f"Error generating customer intelligence: {exc}")
            return

    df = st.session_state.get("customer_insights_df", pd.DataFrame()).copy()
    if df.empty:
        st.info("Sync customer intelligence to load the CRM view.")
        return

    if search_query:
        df = search_customers(search_query, df)
    if selected_segment != "All" and "segment" in df.columns:
        df = df[df["segment"] == selected_segment]
    if df.empty:
        st.info("No customers found for the current filters.")
        return

    current_first_col = "current_first_order" if "current_first_order" in df.columns else "first_order"
    current_last_col = "current_last_order" if "current_last_order" in df.columns else "last_order"
    render_loaded_date_context(
        requested_start=start_date,
        requested_end=end_date,
        loaded_start=pd.to_datetime(df.get(current_first_col), errors="coerce"),
        loaded_end=pd.to_datetime(df.get(current_last_col), errors="coerce"),
        label="Loaded customer activity",
    )

    total_customers = len(df)
    vip_count = int((df["segment"] == "VIP").sum()) if "segment" in df.columns else 0
    at_risk_count = int((df["segment"] == "At Risk").sum()) if "segment" in df.columns else 0
    churned_count = int((df["segment"] == "Churned").sum()) if "segment" in df.columns else 0
    repeat_share = ((df["total_orders"] > 1).sum() / max(total_customers, 1)) * 100

    render_highlight_stat(
        "Customer story",
        f"{total_customers:,} customers in view",
        f"{repeat_share:.0f}% of the visible base has purchased more than once.",
    )

    story_points = [
        f"Visible revenue totals {_format_currency(float(df['total_revenue'].sum()))} across the current customer selection.",
        f"{vip_count} customers are in the VIP segment and {at_risk_count} are flagged as at risk.",
        f"{churned_count} customers are currently tagged as churned, which makes recovery flows a priority.",
    ]
    if "favorite_product" in df.columns and df["favorite_product"].astype(str).str.strip().ne("").any():
        favorite = df["favorite_product"].astype(str).value_counts().idxmax()
        story_points.append(f"The most common favorite product in this cohort is {favorite}.")
    render_commentary_panel("Customer narrative", story_points)

    metric_cols = st.columns(4)
    metric_cols[0].metric("Revenue", _format_currency(float(df["total_revenue"].sum())))
    metric_cols[1].metric("Avg Orders", f"{df['total_orders'].mean():.1f}")
    metric_cols[2].metric("Avg AOV", _format_currency(float(df["avg_order_value"].mean())))
    metric_cols[3].metric("Avg Recency", f"{pd.to_numeric(df['recency_days'], errors='coerce').mean():.0f} days")
    render_kpi_note("Metrics respect the current search and segment filters.")

    tabs = st.tabs(["Customer Story", "Segments", "CRM + ShopAI", "Customer Ledger"])

    with tabs[0]:
        segment_mix = _segment_mix(df)
        segment_revenue = _segment_revenue(df)
        left_col, right_col = st.columns(2)
        with left_col:
            segment_chart = build_adaptive_donut(
                segment_mix,
                values="Customers",
                names="Segment",
                title="Customer Segment Mix",
                color_scale="Plasma",
            )
            st.plotly_chart(segment_chart, use_container_width=True)
        with right_col:
            revenue_chart = build_spotlight_bar(
                segment_revenue.sort_values("Revenue"),
                x="Revenue",
                y="Segment",
                title="Revenue by Segment",
                color="Revenue",
                color_scale="Tealgrn",
                text_auto=".2s",
            )
            st.plotly_chart(revenue_chart, use_container_width=True)

        if all(col in df.columns for col in ["total_orders", "total_revenue"]):
            scatter = px.scatter(
                df,
                x="total_orders",
                y="total_revenue",
                color="segment" if "segment" in df.columns else None,
                size="avg_order_value" if "avg_order_value" in df.columns else None,
                hover_name="primary_name" if "primary_name" in df.columns else None,
                title="Customer Value Matrix",
                labels={"total_orders": "Orders", "total_revenue": "Revenue"},
            )
            st.plotly_chart(apply_plotly_theme(scatter, height=420), use_container_width=True)

    with tabs[1]:
        summary_df = get_segment_summary(df)
        favorite_products = _favorite_products(df)

        seg_left, seg_right = st.columns([1.1, 1])
        with seg_left:
            if not summary_df.empty:
                st.dataframe(summary_df, use_container_width=True, hide_index=True, height=340)
            else:
                st.info("Segment summary is not available yet.")
        with seg_right:
            if not favorite_products.empty:
                favorite_chart = build_spotlight_bar(
                    favorite_products.sort_values("Customers"),
                    x="Customers",
                    y="Product",
                    title="Most Common Favorite Products",
                    color="Customers",
                    color_scale="Oranges",
                    text_auto=".0f",
                )
                st.plotly_chart(favorite_chart, use_container_width=True)
            else:
                st.info("Favorite-product data is not available yet.")

        segments = get_customer_segments(df)
        if segments:
            st.markdown("#### Segment roster")
            segment_cols = st.columns(min(len(segments), 6))
            for index, (segment_name, segment_df) in enumerate(segments.items()):
                with segment_cols[index % len(segment_cols)]:
                    st.metric(segment_name, f"{len(segment_df):,}", _format_currency(float(segment_df["total_revenue"].sum())))

    with tabs[2]:
        priority_queue = _crm_priority_queue(df)
        render_audit_card(
            "CRM action model",
            "Priority is based on segment risk, lifetime revenue, and recency so support and retention teams can work from the same queue.",
        )

        if not priority_queue.empty:
            st.dataframe(
                priority_queue[
                    [
                        "primary_name",
                        "segment",
                        "total_revenue",
                        "total_orders",
                        "recency_days",
                        "favorite_product",
                        "recommended_action",
                    ]
                ].rename(
                    columns={
                        "primary_name": "Customer",
                        "segment": "Segment",
                        "total_revenue": "Revenue",
                        "total_orders": "Orders",
                        "recency_days": "Recency (days)",
                        "favorite_product": "Favorite Product",
                        "recommended_action": "Recommended Action",
                    }
                ),
                column_config={
                    "Revenue": st.column_config.NumberColumn("Revenue", format="Tk %.0f"),
                    "Orders": st.column_config.NumberColumn("Orders", format="%d"),
                },
                use_container_width=True,
                hide_index=True,
                height=340,
            )
        else:
            st.info("No CRM action queue is available yet.")

        from FrontEnd.pages.shopai import render_shopai_crm_snapshot

        render_shopai_crm_snapshot(df)

    with tabs[3]:
        display_df = df.copy()
        display_df["total_revenue"] = display_df["total_revenue"].map(lambda x: _format_currency(float(x)))
        display_df["avg_order_value"] = display_df["avg_order_value"].map(lambda x: _format_currency(float(x)))
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
        display_df = display_df[[col for col in wanted if col in display_df.columns]].rename(
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
        st.dataframe(display_df, use_container_width=True, height=480, hide_index=True)

        excel_bytes = to_excel_bytes(df, "Customer Intelligence")
        st.download_button(
            label="Download Customer Intelligence",
            data=excel_bytes,
            file_name=f"customer_intelligence_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
