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
        orders=("order_id", "nunique"),
        unique_customers=("customer_key", "nunique"),
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
        orders=("order_id", "nunique"),
        unique_customers=("customer_key", "nunique"),
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
        fig1 = px.bar(order_daily, x="day_label", y="revenue", color="day_label", title="Today vs Previous Day Revenue", text_auto=".2s")
        st.plotly_chart(fig1.update_layout(height=320, showlegend=False, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"), width="stretch")
    with c2:
        fig2 = px.bar(order_daily.melt(id_vars=["day_label"], value_vars=["orders", "unique_customers", "new_customers", "units"], var_name="metric", value_name="value"), x="metric", y="value", color="day_label", barmode="group", title="Today vs Previous Day Volume")
        st.plotly_chart(fig2.update_layout(height=320, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"), width="stretch")

def render_last_7_days_sales_chart(df_sales: pd.DataFrame, df_customers: pd.DataFrame):
    st.markdown("#### Daily Comparison: Today vs Last Day vs Previous 7 Days")
    sales = ensure_sales_schema(df_sales).copy()
    sales = sales[sales["order_date"].notna()].copy()
    if sales.empty: return
    daily = build_order_level_dataset(sales.assign(order_day=sales["order_date"].dt.normalize())).groupby("order_day", as_index=False).agg(
        revenue=("order_total", "sum"),
        orders=("order_id", "nunique"),
        unique_customers=("customer_key", "nunique"),
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
        st.plotly_chart(px.bar(daily, x="day_label", y="revenue", color="revenue", title="Last 7 Days Revenue", text_auto=".2s", color_continuous_scale="Tealgrn").update_layout(height=340), width="stretch")
    with c2:
        st.plotly_chart(px.line(daily.melt(id_vars=["day_label"], value_vars=["orders", "unique_customers", "new_customers"], var_name="metric", value_name="value"), x="day_label", y="value", color="metric", markers=True, title="Last 7 Days Orders and Customers").update_layout(height=340), width="stretch")
def render_sales_overview_timeseries(df_sales: pd.DataFrame, ml_bundle: dict = None):
    """Renders high-fidelity time-series analysis for Sales Overview."""
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
                              hovermode="x unified", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_rev, width="stretch")

    with c2:
        # Order Count Time Series
        fig_ord = px.line(daily, x="order_day", y="orders", 
                          title="Daily Order Volume",
                          markers=True, line_shape="spline",
                          color_discrete_sequence=["#10B981"])
        fig_ord.update_layout(height=300, margin=dict(l=0, r=0, t=40, b=0), 
                              hovermode="x unified", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_ord, width="stretch")

    c3, c4 = st.columns(2)
    with c3:
        # Item Sold Time Series
        fig_units = px.line(daily, x="order_day", y="units", 
                          title="Daily Items Sold (Volume)",
                          markers=True, line_shape="spline",
                          color_discrete_sequence=["#F59E0B"])
        fig_units.update_layout(height=300, margin=dict(l=0, r=0, t=40, b=0), 
                              hovermode="x unified", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_units, width="stretch")

    with c4:
        # AOV (Basket Value) Time Series - SUGGESTED
        daily["aov"] = daily["revenue"] / daily["orders"].replace(0, 1)
        fig_aov = px.line(daily, x="order_day", y="aov", 
                          title="Average Order Value (AOV) Trend",
                          markers=True, line_shape="spline",
                          color_discrete_sequence=["#EC4899"])
        fig_aov.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_aov, width="stretch")

    st.divider()
    render_ml_forecast_charts(daily, ml_bundle=ml_bundle)

def render_ml_forecast_charts(daily: pd.DataFrame, ml_bundle: dict = None):
    st.markdown("#### 🤖 Predictive Market Forecasting Ensembles")
    
    # Check if we already have pre-calculated forecasts in the bundle (Snapshot Mode)
    use_precalculated = False
    if ml_bundle and "forecasts" in ml_bundle:
        use_precalculated = True
    
    # If not pre-calculated, check for forecasting dependencies for live training
    if not use_precalculated:
        try:
            from BackEnd.services.ml_engine import run_automl_forecast as generate_forecasts
            # Pre-flight check
            import statsmodels
            import sklearn
        except (ImportError, ModuleNotFoundError):
            st.info("💡 **Predictive Insights Paused**: The advanced ML ensemble engine is currently not installed. The dashboard is running in standard BI mode without rolling forecasts.")
            return
        except Exception as e:
            st.warning(f"Forecasting unavailable: {e}")
            return
        
    metrics_to_forecast = {
        "revenue": "Revenue (TK)",
        "orders": "Order Volume",
        "units": "Items Sold",
        "aov": "Average Order Value"
    }
    
    if "aov" not in daily.columns:
        daily["aov"] = daily["revenue"] / daily["orders"].replace(0, 1)

    # Shared Indicator (Common Legend)
    st.markdown("""
        <div style='display:flex; flex-wrap:wrap; justify-content:center; gap:20px; font-size:0.9rem; font-weight:600; padding:10px 0 20px 0; color:var(--text-strong);'>
            <div><span style='color:#1E293B; font-size:1.2em;'>●</span> Historical Signal</div>
            <div><span style='color:#F59E0B; font-size:1.2em;'>●</span> ARIMA</div>
            <div><span style='color:#10B981; font-size:1.2em;'>●</span> SARIMA</div>
            <div><span style='color:#EC4899; font-size:1.2em;'>●</span> Holt-Winters</div>
            <div><span style='color:#8B5CF6; font-size:1.2em;'>●</span> Linear Trend</div>
            <div><span style='color:#3B82F6; font-size:1.2em;'>●</span> Naive Baseline</div>
            <div><span style='color:#EF4444; font-size:1.2em;'>●</span> Random Forest</div>
        </div>
    """, unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    cols = [c1, c2, c1, c2]

    for i, (metric_key, metric_title) in enumerate(metrics_to_forecast.items()):
        with cols[i]:
            if use_precalculated and metric_key in ml_bundle["forecasts"]:
                res = ml_bundle["forecasts"][metric_key]
            else:
                with st.spinner(f"Training ensembles for {metric_title}..."):
                    res = generate_forecasts(daily, metric=metric_key, horizon=7)
                
            if "error" in res or not res:
                # Error is now handled at top level, but if a specific metric still has an error (e.g. data points)
                continue
                
            y = res["history"]
            forecasts = res["forecasts"]
            best_model = res["best_model"]
            
            # Combine all history and forecasts into one unified graph
            plot_df = pd.DataFrame({"Date": y.index, metric_title: y.values, "Model": "Historical Signal"})
            
            for model_name, fc in forecasts.items():
                fc_df = pd.DataFrame({"Date": fc.index, metric_title: fc.values, "Model": model_name})
                plot_df = pd.concat([plot_df, fc_df])
                
            fig = px.line(plot_df, x="Date", y=metric_title, color="Model", 
                          title=f"{metric_title} Prediction (⭐ Best: {best_model})",
                          color_discrete_map={
                              "Historical Signal": "#1E293B", 
                              "ARIMA": "#F59E0B",
                              "SARIMA": "#10B981",
                              "Holt-Winters": "#EC4899",
                              "Linear Trend": "#8B5CF6",
                              "Naive Baseline": "#3B82F6",
                              "Random Forest": "#EF4444"
                          }, line_shape="spline")
            
            for trace in fig.data:
                if trace.name == "Historical Signal":
                    trace.line.width = 4
                elif trace.name == best_model:
                    trace.line.width = 3
                    trace.line.dash = "solid"
                else:
                    trace.line.width = 2
                    trace.line.dash = "dot"
                    trace.opacity = 0.6
                     
            fig.update_layout(height=400, margin=dict(l=0, r=0, t=60, b=0), hovermode="x unified", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=False)
            st.plotly_chart(fig, width="stretch")
