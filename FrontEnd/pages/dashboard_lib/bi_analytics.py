import pandas as pd
import plotly.express as px
import streamlit as st
from BackEnd.utils.sales_schema import ensure_sales_schema
from .data_helpers import build_order_level_dataset

def build_period_business_metrics(df_sales: pd.DataFrame, df_customers: pd.DataFrame, view_mode: str) -> pd.DataFrame:
    sales = ensure_sales_schema(df_sales).copy()
    sales = sales[sales["order_date"].notna()].copy()
    if sales.empty: return pd.DataFrame()
    freq_map = {"Quarter": "Q", "Month": "M", "Week": "W", "Year": "Y"}
    sales["order_date"] = pd.to_datetime(sales["order_date"], errors="coerce")
    sales["period"] = sales["order_date"].dt.to_period(freq_map.get(view_mode, "Q"))
    order_metrics = build_order_level_dataset(sales)
    if order_metrics.empty: return pd.DataFrame()
    order_metrics["order_date"] = pd.to_datetime(order_metrics["order_date"], errors="coerce")
    order_metrics["period"] = order_metrics["order_date"].dt.to_period(freq_map.get(view_mode, "Q"))
    order_metrics["period_label"] = order_metrics["period"].astype(str)
    metrics = order_metrics.groupby(["period", "period_label"], as_index=False).agg(
        revenue=("order_total", "sum"),
        orders=("order_id", lambda s: s.replace("", pd.NA).dropna().nunique()),
        unique_customers=("customer_key", lambda s: s.replace("", pd.NA).dropna().nunique()),
    ).sort_values("period").reset_index(drop=True)
    if isinstance(df_customers, pd.DataFrame) and not df_customers.empty and "first_order" in df_customers.columns:
        customer_df = df_customers.copy()
        customer_df["first_order"] = pd.to_datetime(customer_df["first_order"], errors="coerce")
        customer_df = customer_df[customer_df["first_order"].notna()].copy()
        if not customer_df.empty:
            customer_df["period"] = customer_df["first_order"].dt.to_period(freq_map.get(view_mode, "Q"))
            new_customer_counts = customer_df.groupby("period").size().reset_index(name="new_customers")
            metrics = metrics.merge(new_customer_counts, on="period", how="left")
    metrics["new_customers"] = pd.to_numeric(metrics.get("new_customers", 0), errors="coerce").fillna(0).astype(int)
    limit = {"Quarter": 4, "Month": 3, "Week": 4, "Year": 3}.get(view_mode, 4)
    return metrics.tail(limit).reset_index(drop=True)

def render_today_vs_last_day_sales_chart(df_sales: pd.DataFrame, df_customers: pd.DataFrame):
    st.markdown("#### Exact Order Status Breakdown")
    order_df = build_order_level_dataset(df_sales)
    if not order_df.empty and "order_status" in order_df.columns:
        status_map = {"completed": "Shipped", "on-hold": "Waiting", "processing": "Processing", "cancelled": "Cancelled", "refunded": "Refunded", "pending": "Pending", "failed": "Failed"}
        status_counts = order_df["order_status"].str.lower().value_counts().reset_index()
        status_counts.columns = ["Status", "Orders"]
        rows = (len(status_counts) + 3) // 4
        for r in range(rows):
            cols = st.columns(4)
            for c in range(4):
                idx = r * 4 + c
                if idx < len(status_counts):
                    row = status_counts.iloc[idx]
                    st.metric(status_map.get(row["Status"], row["Status"].title()), f"{row['Orders']:,}")
    st.divider()
    st.markdown("#### Today vs Previous Day Sales Comparison")
    sales = ensure_sales_schema(df_sales)
    sales = sales[sales["order_date"].notna()].copy()
    if sales.empty: return
    sales["order_day"] = sales["order_date"].dt.normalize()
    order_daily = build_order_level_dataset(sales).groupby("order_day", as_index=False).agg(
        revenue=("order_total", "sum"),
        orders=("order_id", lambda s: s.replace("", pd.NA).dropna().nunique()),
        unique_customers=("customer_key", lambda s: s.replace("", pd.NA).dropna().nunique()),
        units=("qty", "sum"),
    ).sort_values("order_day").tail(2).reset_index(drop=True)
    if order_daily.empty: return
    if isinstance(df_customers, pd.DataFrame) and not df_customers.empty and "first_order" in df_customers.columns:
        customer_df = df_customers.copy()
        customer_df["first_order"] = pd.to_datetime(customer_df["first_order"], errors="coerce").dt.normalize()
        new_customer_daily = customer_df[customer_df["first_order"].notna()].groupby("first_order").size().reset_index(name="new_customers").rename(columns={"first_order": "order_day"})
        order_daily = order_daily.merge(new_customer_daily, on="order_day", how="left")
    order_daily["new_customers"] = pd.to_numeric(order_daily.get("new_customers", 0), errors="coerce").fillna(0).astype(int)
    latest_day = order_daily["order_day"].max()
    order_daily["day_label"] = order_daily.apply(lambda row: f"{ {0: 'Today', 1: 'Previous'}.get((latest_day-row['order_day']).days, 'Earlier') } - {row['order_day'].strftime('%A, %d %b')}", axis=1)
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(px.bar(order_daily, x="day_label", y="revenue", color="day_label", title="Today vs Previous Day Revenue", text_auto=".2s").update_layout(height=320, showlegend=False), use_container_width=True)
    with c2:
        st.plotly_chart(px.bar(order_daily.melt(id_vars=["day_label"], value_vars=["orders", "unique_customers", "new_customers", "units"], var_name="metric", value_name="value"), x="metric", y="value", color="day_label", barmode="group", title="Today vs Previous Day Volume").update_layout(height=320), use_container_width=True)

def render_last_7_days_sales_chart(df_sales: pd.DataFrame, df_customers: pd.DataFrame):
    st.markdown("#### Daily Comparison: Today vs Last Day vs Previous 7 Days")
    sales = ensure_sales_schema(df_sales).copy()
    sales = sales[sales["order_date"].notna()].copy()
    if sales.empty: return
    daily = build_order_level_dataset(sales.assign(order_day=sales["order_date"].dt.normalize())).groupby("order_day", as_index=False).agg(
        revenue=("order_total", "sum"),
        orders=("order_id", lambda s: s.replace("", pd.NA).dropna().nunique()),
        unique_customers=("customer_key", lambda s: s.replace("", pd.NA).dropna().nunique()),
        units=("qty", "sum"),
    ).sort_values("order_day").tail(7).reset_index(drop=True)
    if daily.empty: return
    latest_day = daily["order_day"].max()
    daily["day_label"] = daily.apply(lambda row: f"{ {0:'Today', 1:'Previous', 2:'Earlier'}.get((latest_day-row['order_day']).days, row['order_day'].strftime('%A, %d %b')) }", axis=1)
    if isinstance(df_customers, pd.DataFrame) and not df_customers.empty and "first_order" in df_customers.columns:
        customer_df = df_customers.copy()
        customer_df["first_order"] = pd.to_datetime(customer_df["first_order"], errors="coerce").dt.normalize()
        new_customer_daily = customer_df[customer_df["first_order"].notna()].groupby("first_order").size().reset_index(name="new_customers").rename(columns={"first_order": "order_day"})
        daily = daily.merge(new_customer_daily, on="order_day", how="left")
    daily["new_customers"] = pd.to_numeric(daily.get("new_customers", 0), errors="coerce").fillna(0).astype(int)
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(px.bar(daily, x="day_label", y="revenue", color="revenue", title="Last 7 Days Revenue", text_auto=".2s", color_continuous_scale="Tealgrn").update_layout(height=340), use_container_width=True)
    with c2:
        st.plotly_chart(px.line(daily.melt(id_vars=["day_label"], value_vars=["orders", "unique_customers", "new_customers"], var_name="metric", value_name="value"), x="day_label", y="value", color="metric", markers=True, title="Last 7 Days Orders and Customers").update_layout(height=340), use_container_width=True)
def render_market_overview_timeseries(df_sales: pd.DataFrame):
    """Renders high-fidelity time-series analysis for Market Overview."""
    st.markdown("#### 📈 Time-Series Performance Analysis")
    sales = ensure_sales_schema(df_sales).copy()
    sales = sales[sales["order_date"].notna()].copy()
    if sales.empty:
        st.info("Insufficient data for time-series analysis.")
        return

    # Aggregate by day
    sales["order_day"] = sales["order_date"].dt.normalize()
    daily = build_order_level_dataset(sales).groupby("order_day", as_index=False).agg(
        revenue=("order_total", "sum"),
        orders=("order_id", "nunique"),
        units=("qty", "sum"),
        avg_basket=("order_total", "mean")
    ).sort_values("order_day")

    if daily.empty:
        st.info("No daily data points found.")
        return

    c1, c2 = st.columns(2)
    with c1:
        # Revenue Time Series
        fig_rev = px.line(daily, x="order_day", y="revenue", 
                          title="Daily Revenue Trend (TK)",
                          markers=True, line_shape="spline",
                          color_discrete_sequence=["#4F46E5"])
        fig_rev.update_layout(height=300, margin=dict(l=0, r=0, t=40, b=0), 
                              hovermode="x unified", template="plotly_white")
        st.plotly_chart(fig_rev, use_container_width=True)

    with c2:
        # Order Count Time Series
        fig_ord = px.line(daily, x="order_day", y="orders", 
                          title="Daily Order Volume",
                          markers=True, line_shape="spline",
                          color_discrete_sequence=["#10B981"])
        fig_ord.update_layout(height=300, margin=dict(l=0, r=0, t=40, b=0), 
                              hovermode="x unified", template="plotly_white")
        st.plotly_chart(fig_ord, use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        # Item Sold Time Series
        fig_units = px.line(daily, x="order_day", y="units", 
                          title="Daily Items Sold (Volume)",
                          markers=True, line_shape="spline",
                          color_discrete_sequence=["#F59E0B"])
        fig_units.update_layout(height=300, margin=dict(l=0, r=0, t=40, b=0), 
                              hovermode="x unified", template="plotly_white")
        st.plotly_chart(fig_units, use_container_width=True)

    with c4:
        # AOV (Basket Value) Time Series - SUGGESTED
        daily["aov"] = daily["revenue"] / daily["orders"].replace(0, 1)
        fig_aov = px.line(daily, x="order_day", y="aov", 
                          title="Average Order Value (AOV) Trend",
                          markers=True, line_shape="spline",
                          color_discrete_sequence=["#EC4899"])
        st.plotly_chart(fig_aov, use_container_width=True)

    st.divider()
    render_ml_forecast_charts(daily)

def render_ml_forecast_charts(daily: pd.DataFrame):
    st.markdown("#### 🤖 Predictive Market Forecasting (ML Pipeline)")
    
    try:
        from BackEnd.services.ts_forecast import generate_forecasts
    except ImportError:
        st.warning("Time-series forecasting module not loaded.")
        return
        
    with st.spinner("Training predictive models (ARIMA, SARIMA, Holt-Winters)..."):
        res = generate_forecasts(daily, metric="revenue", horizon=7)
        
    if "error" in res:
        st.info(f"⚠️ {res['error']}")
        return
        
    y = res["history"]
    forecasts = res["forecasts"]
    best_model = res["best_model"]
    
    st.success(f"**ML Verdict Benchmark:** The `{best_model}` algorithm successfully evaluated as the most accurate architecture for this trend based on historical trailing-window evaluation.")
    
    c1, c2 = st.columns(2)
    cols = [c1, c2, c1, c2]
    
    models_list = list(forecasts.keys())
    for i, model_name in enumerate(models_list[:4]):
        fc = forecasts[model_name]
        
        hist_df = pd.DataFrame({"Date": y.index, "Revenue": y.values, "Data Split": "Historical"})
        fc_df = pd.DataFrame({"Date": fc.index, "Revenue": fc.values, "Data Split": "Prediction"})
        plot_df = pd.concat([hist_df, fc_df])
        
        star = "⭐ Best Evaluated Fit: " if model_name == best_model else ""
        title = f"{star}{model_name} Forecast (7 Days)"
        
        fig = px.line(plot_df, x="Date", y="Revenue", color="Data Split", title=title, 
                      color_discrete_map={"Historical": "#4F46E5", "Prediction": "#F59E0B"})
        
        if model_name == best_model:
             fig.update_traces(line=dict(width=3))
             
        fig.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=0), hovermode="x unified", template="plotly_white", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        cols[i].plotly_chart(fig, use_container_width=True)
