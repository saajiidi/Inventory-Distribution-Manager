from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from BackEnd.services.woocommerce_service import (
    WooCommerceService,
    get_woocommerce_credentials,
    get_woocommerce_store_label,
)
from BackEnd.services.duckdb_loader import load_partitioned_data
from BackEnd.utils.sales_schema import ensure_sales_schema
from FrontEnd.components import ui
def _resolve_preview_columns(df: pd.DataFrame) -> list[str]:
    """Return the best available preview columns across old and new schemas."""
    preferred_groups = [
        ["Order ID", "Order Number"],
        ["Order Date"],
        ["Shipped Date"],
        ["Full Name (Billing)", "Customer Name"],
        ["Tracking"],
        ["Product Name (main)", "Item Name"],
        ["Quantity", "Qty"],
        ["Order Status", "Status"],
        ["Order Total Amount"],
    ]

    preview_cols = []
    for candidates in preferred_groups:
        match = next((col for col in candidates if col in df.columns), None)
        if match:
            preview_cols.append(match)

    return preview_cols or list(df.columns[:8])


def _render_preview_chart_block(title: str, chart_builders: list[tuple[str, callable]]):
    available = [(label, builder) for label, builder in chart_builders if builder is not None]
    if not available:
        return

    st.markdown(f"#### {title}")
    cols = st.columns(len(available))
    for column, (label, builder) in zip(cols, available):
        with column:
            fig = builder()
            if fig is not None:
                fig.update_layout(height=320, margin=dict(l=12, r=12, t=48, b=12))
                st.plotly_chart(fig, use_container_width=True)


def _build_order_charts(df: pd.DataFrame) -> list[tuple[str, callable]]:
    charts: list[tuple[str, callable]] = []
    if df.empty:
        return charts

    order_df = df.copy()
    if "Order Date" in order_df.columns:
        order_df["Order Date"] = pd.to_datetime(order_df["Order Date"], errors="coerce")
        daily = (
            order_df[order_df["Order Date"].notna()]
            .assign(order_day=lambda frame: frame["Order Date"].dt.date)
            .groupby("order_day", as_index=False)
            .agg(Revenue=("Order Total Amount", "sum"), Units=("Qty", "sum"))
        )
        if not daily.empty:
            charts.append(
                (
                    "Daily Revenue",
                    lambda daily=daily: px.line(
                        daily,
                        x="order_day",
                        y="Revenue",
                        markers=True,
                        title="Daily Revenue",
                    ),
                )
            )

    if "Order Status" in order_df.columns:
        status_counts = order_df["Order Status"].fillna("unknown").astype(str).value_counts().reset_index()
        status_counts.columns = ["Status", "Rows"]
        charts.append(
            (
                "Order Status Mix",
                lambda status_counts=status_counts: px.bar(
                    status_counts,
                    x="Status",
                    y="Rows",
                    color="Rows",
                    title="Order Status Mix",
                    color_continuous_scale="Blues",
                ),
            )
        )

    if "Item Name" in order_df.columns:
        top_items = (
            order_df.groupby("Item Name", as_index=False)
            .agg(Revenue=("Order Total Amount", "sum"), Units=("Qty", "sum"))
            .sort_values("Revenue", ascending=False)
            .head(10)
        )
        if not top_items.empty:
            charts.append(
                (
                    "Top Products",
                    lambda top_items=top_items: px.bar(
                        top_items.sort_values("Revenue"),
                        x="Revenue",
                        y="Item Name",
                        orientation="h",
                        title="Top Products by Revenue",
                        color="Revenue",
                        color_continuous_scale="Tealgrn",
                    ),
                )
            )

    return charts[:3]


def _build_inventory_charts(df: pd.DataFrame) -> list[tuple[str, callable]]:
    charts: list[tuple[str, callable]] = []
    if df.empty:
        return charts

    inventory_df = df.copy()
    if "Stock Status" in inventory_df.columns:
        status_counts = inventory_df["Stock Status"].fillna("unknown").astype(str).value_counts().reset_index()
        status_counts.columns = ["Status", "Products"]
        charts.append(
            (
                "Inventory Status",
                lambda status_counts=status_counts: px.pie(
                    status_counts,
                    names="Status",
                    values="Products",
                    hole=0.5,
                    title="Inventory Status Mix",
                ),
            )
        )

    if "Category" in inventory_df.columns:
        category_value = (
            inventory_df.groupby("Category", as_index=False)
            .agg(Inventory_Value=("Inventory Value", "sum"))
            .sort_values("Inventory_Value", ascending=False)
            .head(10)
        )
        if not category_value.empty:
            charts.append(
                (
                    "Category Value",
                    lambda category_value=category_value: px.bar(
                        category_value.sort_values("Inventory_Value"),
                        x="Inventory_Value",
                        y="Category",
                        orientation="h",
                        title="Top Inventory Value by Category",
                        color="Inventory_Value",
                        color_continuous_scale="Greens",
                    ),
                )
            )

    if "Stock Quantity" in inventory_df.columns and "Name" in inventory_df.columns:
        low_stock = inventory_df.sort_values(["Stock Quantity", "Name"], ascending=[True, True]).head(12)
        if not low_stock.empty:
            charts.append(
                (
                    "Low Stock Watch",
                    lambda low_stock=low_stock: px.bar(
                        low_stock.sort_values("Stock Quantity"),
                        x="Stock Quantity",
                        y="Name",
                        orientation="h",
                        title="Lowest Stock Products",
                        color="Stock Quantity",
                        color_continuous_scale="OrRd",
                    ),
                )
            )

    return charts[:3]


def _render_live_comparison(live_df: pd.DataFrame):
    """Compare freshly fetched 'Live' data against the previous day's local cache."""
    if live_df.empty:
        return

    st.subheader("Performance Intelligence")
    st.caption("Comparing the freshly fetched live orders against the previous business day's local data.")

    # Calculate Live metrics (assuming the fetched DF is 'Live')
    live = ensure_sales_schema(live_df)
    live_revenue = float(live["order_total"].sum())
    live_count = int(live["order_id"].nunique())
    live_aov = live_revenue / live_count if live_count else 0.0

    # Load Yesterday (simplified: today-1) from cache for comparison
    try:
        now = datetime.now()
        yesterday_year = (now - timedelta(days=1)).year
        cached_df = load_partitioned_data(year=yesterday_year)
        
        if cached_df.empty:
            st.info("No local data found for the previous day to perform a comparison.")
            return

        baseline = ensure_sales_schema(cached_df)
        # Filter baseline to just the previous 24h cycle
        yesterday_cutoff = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0)
        baseline = baseline[baseline["order_date"] >= pd.Timestamp(yesterday_cutoff)]
        
        baseline_revenue = float(baseline["order_total"].sum())
        baseline_count = int(baseline["order_id"].nunique())
        baseline_aov = baseline_revenue / baseline_count if baseline_count else 0.0
        
        # Display Metrics
        m1, m2, m3 = st.columns(3)
        with m1:
            ui.metric_highlight("Revenue delta", f"TK {live_revenue:,.0f}", f"{((live_revenue/baseline_revenue)-1)*100:+.1f}% vs Yesterday" if baseline_revenue else "New Session")
        with m2:
            ui.metric_highlight("Order volume", f"{live_count:,} orders", f"{live_count - baseline_count:+} vs Yesterday")
        with m3:
            ui.metric_highlight("AOV focus", f"TK {live_aov:,.0f}", f"{live_aov - baseline_aov:+.0f} change")
            
        if live_revenue < baseline_revenue * 0.5:
             st.warning(f"Live revenue is significantly lower than yesterday's baseline ({((live_revenue/baseline_revenue)*100):.0f}% of goal). Check if the sync window matches.")
        elif live_revenue > baseline_revenue:
             st.balloons()
             st.success(f"Live performance has outpaced yesterday's full day by {live_revenue - baseline_revenue:,.0f} TK!")

    except Exception as e:
        st.caption(f"Could not calculate comparison metrics: {e}")


def _render_order_sync(wc_service: WooCommerceService):
    st.subheader("Order Sync")
    st.caption("Pull WooCommerce orders, review the preview, then save them into local storage.")

    with st.expander("Fetch Settings", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            sync_mode = st.selectbox(
                "Sync Mode",
                ["Standard (Date Range)", "Current Cycle (Since 5 PM)", "Fulfillment Queue (Processing)", "Recent Shipped"],
                index=0,
                help="Select a preset to automatically set date/time ranges based on business cycles."
            )
            
        with col2:
            status_filter = st.selectbox(
                "Order Status",
                ["any", "processing", "on-hold", "completed", "cancelled", "refunded", "failed"],
                index=0,
                help="Select the exact order status to fetch from the WooCommerce database.",
            )

        # Operational calculation for the 5 PM cutoff
        now = datetime.now()
        yesterday_5pm = (now - timedelta(days=1)).replace(hour=17, minute=0, second=0, microsecond=0)
        
        if sync_mode == "Current Cycle (Since 5 PM)":
            start_date_val = yesterday_5pm.date()
            start_time_val = yesterday_5pm.time()
            end_date_val = now.date()
            end_time_val = now.time()
            status_filter = "any"
        elif sync_mode == "Fulfillment Queue (Processing)":
            start_date_val = yesterday_5pm.date()
            start_time_val = yesterday_5pm.time()
            end_date_val = now.date()
            end_time_val = now.time()
            status_filter = "processing"
        elif sync_mode == "Recent Shipped":
            start_date_val = yesterday_5pm.date()
            start_time_val = yesterday_5pm.time()
            end_date_val = now.date()
            end_time_val = now.time()
            status_filter = "completed"
        else:
            # Fallback to standard 120-day range
            start_date_val = now.date() - timedelta(days=120)
            start_time_val = yesterday_5pm.time().replace(hour=0)
            end_date_val = now.date()
            end_time_val = yesterday_5pm.time().replace(hour=23, minute=59)

        if sync_mode == "Standard (Date Range)":
            c1, c2 = st.columns(2)
            with c1:
                start_date = st.date_input("From Date", value=start_date_val)
            with c2:
                end_date = st.date_input("To Date", value=end_date_val)
            after = datetime.combine(start_date, start_time_val).strftime("%Y-%m-%dT%H:%M:%SZ")
            before = datetime.combine(end_date, end_time_val).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            st.info(f"Targeting window: **{yesterday_5pm.strftime('%d %b %I:%M %p')}** to **Now**")
            after = yesterday_5pm.strftime("%Y-%m-%dT17:00:00Z")
            before = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            start_date = start_date_val # For UI date_context
            end_date = end_date_val

        require_tracking = st.checkbox(
            "Only tracked orders",
            value=False,
            help="Keep only orders that include a tracking reference.",
        )

        st.markdown("<div style='margin-bottom: 0.5rem;'></div>", unsafe_allow_html=True)
        fetch_btn = st.button("Fetch Fresh Data", use_container_width=True, type="primary")

    if not fetch_btn:
        return

    with st.status("Fetching WooCommerce orders...", expanded=True) as status:
        try:
            df = wc_service.fetch_all_historical_orders(after=after, before=before, status=status_filter)
            if require_tracking and not df.empty and "Tracking" in df.columns:
                df = df[df["Tracking"] != "N/A"]

            st.session_state.woo_orders_preview_df = df
            if df.empty:
                status.update(label="No matching orders found for this range.", state="error")
                st.info("Try adjusting the date range or order status.")
                return

            status.update(label=f"Fetched {len(df):,} order rows.", state="complete")
        except Exception as exc:
            status.update(label="Order fetch failed.", state="error")
            st.error(f"Order fetch failed: {exc}")
            return

    df = st.session_state.woo_orders_preview_df
    preview_cols = _resolve_preview_columns(df)
    loaded_order_dates = pd.to_datetime(df.get("Order Date"), errors="coerce")
    has_loaded_order_dates = isinstance(loaded_order_dates, pd.Series) and not loaded_order_dates.empty and loaded_order_dates.notna().any()
    ui.date_context(
        requested_start=start_date,
        requested_end=end_date,
        loaded_start=loaded_order_dates.min() if has_loaded_order_dates else None,
        loaded_end=loaded_order_dates.max() if has_loaded_order_dates else None,
        label="Fetched order activity",
    )
    _render_preview_chart_block("Order Charts", _build_order_charts(df))
    st.markdown("#### Order Preview")
    st.dataframe(df[preview_cols].head(50), use_container_width=True, hide_index=True)
    st.caption(f"Previewing {min(len(df), 50):,} of {len(df):,} fetched rows.")

    if st.button("Save Orders to Local Storage", use_container_width=True, type="secondary"):
        wc_service.save_to_parquet(df)
        st.success("Orders were saved and are now available to the dashboard.")
    
    st.divider()
    _render_live_comparison(df)


def _render_inventory_sync(wc_service: WooCommerceService):
    st.subheader("Inventory Sync")
    st.caption("Fetch the latest stock counts directly from the WooCommerce REST API and review the preview below.")

    if st.button("Fetch Inventory", use_container_width=True):
        with st.spinner("Fetching WooCommerce inventory..."):
            stock_df = wc_service.get_stock_report()
        st.session_state.woo_stock_df = stock_df
        if stock_df.empty:
            st.warning("No stock data was returned from WooCommerce.")
        else:
            st.success(f"Fetched {len(stock_df):,} products from WooCommerce.")

    stock_df = st.session_state.get("woo_stock_df")
    if stock_df is None or stock_df.empty:
        return

    df_full = stock_df.copy()
    df_full["Stock Quantity"] = pd.to_numeric(df_full.get("Stock Quantity", 0), errors="coerce").fillna(0)
    df_full["Price"] = pd.to_numeric(df_full.get("Price", 0), errors="coerce").fillna(0)
    df_full["Inventory Value"] = df_full["Stock Quantity"] * df_full["Price"]
    imported_at = pd.to_datetime(df_full.get("_imported_at"), errors="coerce")
    has_imported_at = isinstance(imported_at, pd.Series) and not imported_at.empty and imported_at.notna().any()
    ui.date_context(
        requested_start=None,
        requested_end=datetime.now(),
        loaded_start=imported_at.min() if has_imported_at else None,
        loaded_end=imported_at.max() if has_imported_at else None,
        label="Fetched inventory snapshot",
    )

    f1, f2 = st.columns([1, 2])
    with f1:
        status_filter = st.selectbox(
            "Inventory Status",
            ["All"] + sorted(df_full["Stock Status"].dropna().astype(str).unique().tolist()),
            key="stock_status_filter",
        )
    with f2:
        search_query = st.text_input("Search Product / SKU", "", key="stock_search_query")

    filtered_df = df_full.copy()
    if status_filter != "All":
        filtered_df = filtered_df[filtered_df["Stock Status"] == status_filter]
    if search_query:
        filtered_df = filtered_df[
            filtered_df["Name"].astype(str).str.contains(search_query, case=False, na=False)
            | filtered_df["SKU"].astype(str).str.contains(search_query, case=False, na=False)
        ]

    m1, m2, m3, m4 = st.columns(4)
    total_p = len(filtered_df)
    low_p = len(filtered_df[filtered_df["Stock Quantity"] <= 5])
    out_p = len(filtered_df[filtered_df["Stock Status"] == "outofstock"])
    total_v = (filtered_df["Stock Quantity"] * filtered_df["Price"]).sum()

    with m1:
        st.metric("Products", total_p)
    with m2:
        st.metric("Low Stock", low_p)
    with m3:
        st.metric("Out of Stock", out_p)
    with m4:
        st.metric("Inventory Value", f"TK {total_v:,.0f}")

    _render_preview_chart_block("Inventory Charts", _build_inventory_charts(filtered_df))
    st.markdown("#### Inventory Preview")
    st.dataframe(filtered_df, use_container_width=True, height=420, hide_index=True)
    st.caption(f"Previewing {len(filtered_df):,} inventory rows from the latest fetch.")

    csv_data = filtered_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download Inventory CSV",
        data=csv_data,
        file_name=f"inventory_snapshot_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )


def render_woocommerce_tab():
    """Render the WooCommerce operations page."""
    ui.page_header("Commerce Hub")
    
    st.markdown(
        """
        <div class="bi-commentary">
            <div class="bi-commentary-label">Pipeline Intelligence</div>
            <p class="bi-audit-body">
                The Commerce Hub is the bridge between your live WooCommerce store and the BI engine. 
                Data flows from the API into your <b>local cache</b>, which powers the high-speed dashboard.
            </p>
        </div>
        """, 
        unsafe_allow_html=True
    )
    
    st.caption("Sync orders, monitor inventory levels, and manage data completeness from this operational command center.")

    credentials = get_woocommerce_credentials()
    if not credentials:
        st.warning("WooCommerce connection is not configured in `.streamlit/secrets.toml`.")
        st.info("Add the `[woocommerce]` block with `store_url`, `consumer_key`, and `consumer_secret` to enable this page.")
        st.code(
            """
[woocommerce]
store_url = "https://your-store.com"
consumer_key = "ck_your_consumer_key"
consumer_secret = "cs_your_consumer_secret"
            """
        )
        return

    wc_service = WooCommerceService()
    st.success(f"Connected to {get_woocommerce_store_label()}. API keys stay hidden in Streamlit secrets.")

    order_tab, inventory_tab = st.tabs([
        "Order Sync",
        "Inventory Control",
    ])

    with order_tab:
        _render_order_sync(wc_service)

    with inventory_tab:
        _render_inventory_sync(wc_service)

        st.divider()
        st.subheader("Storage Status")
        from BackEnd.services.duckdb_loader import get_data_completeness

        try:
            completeness = get_data_completeness()
            if completeness.empty:
                st.info("No local data has been indexed yet.")
            else:
                st.dataframe(completeness, use_container_width=True, hide_index=True)
        except Exception as exc:
            st.caption(f"Could not load storage status: {exc}")

    st.divider()
    from FrontEnd.pages.dashboard import render_dashboard_products_section
    render_dashboard_products_section()
