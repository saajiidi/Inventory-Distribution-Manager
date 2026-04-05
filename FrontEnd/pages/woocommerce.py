import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from BackEnd.services.woocommerce_service import (
    WooCommerceService,
    get_woocommerce_credentials,
)


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
        ["Order Total Amount"],
    ]

    preview_cols = []
    for candidates in preferred_groups:
        match = next((col for col in candidates if col in df.columns), None)
        if match:
            preview_cols.append(match)

    if preview_cols:
        return preview_cols

    return list(df.columns[:8])



def render_woocommerce_tab():
    """Render the WooCommerce data pull tab."""
    st.header("🛍️ WooCommerce Connector")
    
    # 1. UI: Configuration Status
    if not get_woocommerce_credentials():
        st.warning("⚠️ WooCommerce secrets not found in `.streamlit/secrets.toml`.")
        st.info("To enable this integration, add your API keys to `.streamlit/secrets.toml`:")
        st.code("""
[woocommerce]
store_url = "https://your-store.com"
consumer_key = "ck_your_consumer_key"
consumer_secret = "cs_your_consumer_secret"
        """)
        return

    # 2. Connection Check
    wc_service = WooCommerceService()
    
    st.success("✅ WooCommerce Secrets Loaded.")

    # 3. Pull Functionality
    st.subheader("📥 Historical Data Pull")
    st.info("Fetch orders from your WooCommerce store and save them as manageable data files.")
    
    with st.expander("Configure Fetch Parameters", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=datetime.now() - timedelta(days=30))
        with col2:
            end_date = st.date_input("End Date", value=datetime.now())
            
        status_filter = st.selectbox(
            "Order Status",
            ["any", "completed", "shipped", "processing", "on-hold", "pending"],
            index=0,
            help="Select the order status to fetch. 'shipped' is often a custom status."
        )
        
        require_tracking = st.checkbox("Require Tracking Info", value=False, help="Only show orders that have tracking information in meta-data.")
        
        fetch_btn = st.button("Start Historical Sync", use_container_width=True, type="primary")
        
        if fetch_btn:
            # Format dates for API (WooCommerce expects ISO8601)
            after = start_date.strftime("%Y-%m-%dT00:00:00Z")
            before = end_date.strftime("%Y-%m-%dT23:59:59Z")
            
            with st.status("Fetching orders from WooCommerce API...", expanded=True) as status:
                try:
                    # Recursive fetch
                    df = wc_service.fetch_all_historical_orders(after=after, before=before, status=status_filter)
                    
                    if not df.empty:
                        # Post-fetch tracking filter if requested
                        if require_tracking and "Tracking" in df.columns:
                            df = df[df["Tracking"] != "N/A"]
                            
                        if not df.empty:
                            status.update(label=f"✅ Successfully processed {len(df)} line items!", state="complete")
                            
                            # Display preview
                            st.subheader("Data Preview")
                            preview_cols = _resolve_preview_columns(df)
                            st.dataframe(df[preview_cols].head(50), use_container_width=True)
                            
                            # Save
                            if st.button("Save to System Data Storage", use_container_width=True, type="secondary"):
                                wc_service.save_to_parquet(df)
                                st.balloons()
                                st.success("Data committed to system storage. It will now be available in the Dashboard and analytics.")
                        else:
                            status.update(label="❌ No matching orders with tracking found.", state="error")
                    else:
                        status.update(label="❌ No orders found in the selected range.", state="error")
                        st.info("Check your date filters or WooCommerce store status.")
                except Exception as e:
                    status.update(label=f"⚠️ Error: {str(e)}", state="error")
                    st.error(f"Sync failed: {e}")

    # 4. Stock Dashboard & AI Assistant
    st.divider()
    st.header("📦 Stock Dashboard & AI Assistant")
    st.info("Monitor inventory levels and chat with your stock assistant for deep analysis.")
    
    if "woo_stock_df" not in st.session_state:
        st.session_state.woo_stock_df = None
    if "stock_messages" not in st.session_state:
        st.session_state.stock_messages = [
            {"role": "assistant", "content": "Hi! I'm your stock assistant. Ask me about your inventory, like 'Which items are low?' or 'What's my total stock value?'"}
        ]

    fetch_stock_btn = st.button("🔄 Sync Live Stock", use_container_width=True, type="secondary")
    
    if fetch_stock_btn:
        with st.spinner("Fetching product data from WooCommerce..."):
            stock_data = wc_service.get_stock_report()
            if not stock_data.empty:
                st.session_state.woo_stock_df = stock_data
                st.success(f"✅ Successfully fetched {len(stock_data)} products!")
            else:
                st.warning("⚠️ No product data could be retrieved.")

    if st.session_state.woo_stock_df is not None:
        df_full = st.session_state.woo_stock_df
        
        # Filters
        f1, f2 = st.columns([1, 2])
        with f1:
            status_filter = st.selectbox("Filter Status", ["All"] + sorted(df_full["Stock Status"].unique().tolist()), key="stock_status_filter")
        with f2:
            search_query = st.text_input("Find Product / SKU", "", key="stock_search_query")
            
        filtered_df = df_full.copy()
        if status_filter != "All":
            filtered_df = filtered_df[filtered_df["Stock Status"] == status_filter]
        if search_query:
            filtered_df = filtered_df[
                filtered_df["Name"].str.contains(search_query, case=False) | 
                filtered_df["SKU"].str.contains(search_query, case=False)
            ]
            
        # Metrics Row
        m1, m2, m3, m4 = st.columns(4)
        total_p = len(filtered_df)
        low_p = len(filtered_df[filtered_df["Stock Quantity"] <= 5])
        out_p = len(filtered_df[filtered_df["Stock Status"] == "outofstock"])
        total_v = (filtered_df["Stock Quantity"] * filtered_df["Price"]).sum()
        
        with m1: st.metric("Products", total_p)
        with m2: st.metric("Low Stock (≤5)", low_p, delta=f"{low_p} items" if low_p > 0 else None, delta_color="inverse")
        with m3: st.metric("Out of Stock", out_p, delta=f"{out_p} items" if out_p > 0 else None, delta_color="inverse")
        with m4: st.metric("Value", f"TK {total_v:,.0f}")
        
        # Dual Column Layout
        left, right = st.columns([1.5, 1])
        
        with left:
            st.subheader("📋 Inventory Data")
            st.dataframe(filtered_df, use_container_width=True, height=450, hide_index=True)
            
            # Export
            csv_data = filtered_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Report (CSV)", data=csv_data, file_name=f"stock_report_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")
            
        with right:
            st.subheader("🤖 Stock AI Assistant")
            st.caption("Context: Currently Filtered Products")
            
            chat_container = st.container(height=400)
            with chat_container:
                for msg in st.session_state.stock_messages:
                    with st.chat_message(msg["role"]):
                        st.markdown(msg["content"])
            
            if prompt := st.chat_input("Ask about stock levels...", key="stock_assistant_input"):
                st.session_state.stock_messages.append({"role": "user", "content": prompt})
                with chat_container:
                    with st.chat_message("user"):
                        st.markdown(prompt)
                    
                    with st.chat_message("assistant"):
                        with st.spinner("Analyzing..."):
                            response = wc_service.query_stock_assistant(prompt, filtered_df)
                            st.markdown(response)
                            st.session_state.stock_messages.append({"role": "assistant", "content": response})
                st.rerun()

    # 5. Data Management Summary
    st.divider()
    st.subheader("📊 Storage Audit")
    
    from BackEnd.services.duckdb_loader import get_data_completeness
    try:
        completeness = get_data_completeness()
        if not completeness.empty:
            st.write("Current data availability by year:")
            st.dataframe(completeness, use_container_width=True)
        else:
            st.info("No data currently indexed in the system.")
    except Exception as e:
        st.caption(f"Could not load storage audit: {e}")
