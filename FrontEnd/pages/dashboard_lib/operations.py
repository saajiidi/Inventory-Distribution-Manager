import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

def render_operational_health(df_sales: pd.DataFrame, stock_df: pd.DataFrame):
    """
    Renders the Operational Health tab.
    Metrics: Shipping Latency, Refund Rate, Stock-out Rate.
    """
    st.markdown("### 📋 Operational Health & Logistics")
    
    # 1. Logistics Efficiency: Shipping Latency
    st.markdown("#### 🚚 Logistics Velocity")
    
    # Ensure date types and strip timezones to prevent subtraction crashes
    df = df_sales.copy()
    df['order_date'] = pd.to_datetime(df.get('order_date'), errors='coerce').dt.tz_localize(None)
    df['shipped_date'] = pd.to_datetime(df.get('shipped_date'), errors='coerce').dt.tz_localize(None)
    
    # Filter for shipped/completed orders to calculate latency
    shipped_df = df[df['shipped_date'].notna()].copy()
    if not shipped_df.empty:
        shipped_df['latency'] = (shipped_df['shipped_date'] - shipped_df['order_date']).dt.days
        avg_latency = shipped_df['latency'].mean()
        
        c1, c2 = st.columns([1, 2])
        with c1:
            st.metric("Avg. Shipping Time", f"{avg_latency:.1f} Days", 
                      delta=f"{avg_latency - 3:.1f} vs Target (3d)", delta_color="inverse")
            st.caption("Target dispatch: 72 hours.")
        
        with c2:
            # Latency Distribution
            latency_counts = shipped_df['latency'].value_counts().reset_index()
            latency_counts.columns = ['Days', 'Count']
            fig = px.bar(latency_counts.sort_values('Days'), x='Days', y='Count', 
                         title="Shipping Velocity Distribution",
                         labels={'Count': 'Orders'},
                         color_discrete_sequence=['#F59E0B'])
            st.plotly_chart(fig, width="stretch")
    else:
        st.info("No 'shipped_date' data available in this window to calculate velocity.")

    st.divider()

    # 2. Refund Analytics
    st.markdown("#### 🔄 Returns & Refund Control")
    
    refund_df = df[df['order_status'].astype(str).str.lower() == 'refunded'] if 'order_status' in df.columns else pd.DataFrame()
    total_orders = df['order_id'].nunique() if 'order_id' in df.columns else 0
    refund_count = refund_df['order_id'].nunique() if 'order_id' in refund_df.columns else 0
    refund_rate = (refund_count / total_orders * 100) if total_orders > 0 else 0
    
    m1, m2 = st.columns(2)
    with m1:
        st.metric("Refund Rate", f"{refund_rate:.1f}%", help="Percentage of total orders that resulted in a refund.")
    with m2:
        target = 5.0
        st.progress(min(refund_rate / 15.0, 1.0), text=f"Tolerance: {target}%")

    # Weekly Refund Trend
    if 'order_date' in df.columns and 'order_status' in df.columns and 'order_id' in df.columns:
        df['week'] = df['order_date'].dt.to_period('W').apply(lambda r: r.start_time)
        weekly_refunds = df.groupby('week').apply(
            lambda x: (x[x['order_status'].str.lower() == 'refunded']['order_id'].nunique() / x['order_id'].nunique() * 100) if x['order_id'].nunique() > 0 else 0
        ).reset_index()
        weekly_refunds.columns = ['Week', 'Refund Rate']
        
        fig_ref = px.line(weekly_refunds, x='Week', y='Refund Rate', title="Weekly Refund Rate Trend",
                          markers=True, color_discrete_sequence=['#EF4444'])
        fig_ref.add_hline(y=5.0, line_dash="dash", line_color="green", annotation_text="Target")
        st.plotly_chart(fig_ref, width="stretch")

    st.divider()

    # 3. Inventory Pressure
    st.markdown("#### 📦 Inventory Health & Availability")
    
    if not stock_df.empty:
        total_skus = len(stock_df)
        out_of_stock = len(stock_df[stock_df['Stock Status'] == 'outofstock'])
        stockout_rate = (out_of_stock / total_skus * 100) if total_skus > 0 else 0
        
        low_stock = len(stock_df[stock_df['Stock Quantity'] <= 5])
        
        i1, i2, i3 = st.columns(3)
        i1.metric("Stock-out Rate", f"{stockout_rate:.1f}%", delta=f"{out_of_stock} SKUs Unavailable", delta_color="inverse")
        i2.metric("Low Stock Alert", f"{low_stock} Items", help="Items with <= 5 units remaining.")
        i3.metric("Inventory Value", f"৳{(stock_df['Stock Quantity'] * stock_df['Price']).sum():,.0f}")
        
        # Categorical Health
        cat_stock = stock_df.groupby('Category').agg({
            'ID': 'count',
            'Stock Quantity': 'sum'
        }).reset_index()
        cat_stock.columns = ['Category', 'Product Count', 'Total Stock']
        
        # Guard against NaNs in Plotly Treemap path
        plot_stock = stock_df.copy()
        plot_stock['Category'] = plot_stock.get('Category', 'Unknown').fillna('Unknown')
        plot_stock['Name'] = plot_stock.get('Name', 'Unnamed').fillna('Unnamed')
        
        st.plotly_chart(px.treemap(plot_stock, path=['Category', 'Name'], values='Stock Quantity', 
                                   title="Inventory Volume by Category"), width="stretch")
    else:
        st.warning("Inventory data currently unavailable.")
