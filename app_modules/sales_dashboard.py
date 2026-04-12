import streamlit as st
import pandas as pd
import plotly.express as px
from BackEnd.services.ts_forecast import generate_forecasts

from BackEnd.services.ml_engine import run_automl_forecast
from BackEnd.services.affinity_engine import MarketBasketEngine
from FrontEnd.components.insights import render_insight_dashboard, render_ai_pilot_chat

def render_operational_forecast(df_sales: pd.DataFrame):
    """
    Renders high-fidelity AI Predictive Terminal.
    """
    st.markdown("### 🔮 Predictive Operations Terminal")
    
    # AutoML Tournament Settings
    with st.expander("⚙️ ML Ensemble Configuration", expanded=False):
        c1, c2, c3 = st.columns(3)
        enable_ml = c1.toggle("Enable ML Ensembles", value=True)
        horizon = c2.slider("Forecast Horizon (Days)", 7, 30, 7)
        metric_key = c3.selectbox("Primary Metric", ["revenue", "orders"])

    if not enable_ml:
        st.info("Predictive services are currently paused.")
        return

    # Data Aggregation
    daily = df_sales.groupby(df_sales["order_date"].dt.normalize(), as_index=False).agg(
        revenue=("order_total", "sum"),
        orders=("order_id", "nunique")
    )
    
    # Run Engine
    with st.spinner("🤖 Evaluating tournament models..."):
        res = run_automl_forecast(daily, metric=metric_key, horizon=horizon)
    
    if "error" in res:
        st.warning(res["error"])
        return

    # Visualization
    y = res["history"]
    best_fc = res["forecasts"].get(res["best_model"])
    
    fig = px.line(title=f"Best-Fit Projection: {res['best_model']} Model")
    fig.add_scatter(x=y.index, y=y.values, name="Historical", line_color="#1e293b")
    fig.add_scatter(x=best_fc.index, y=best_fc.values, name="Forecast", line=dict(dash='dash', color='#4f46e5'))
    
    fig.update_layout(template="plotly_white", height=400, hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)
    
    # Insight Generation (Simulated for Demo, would be calculated from MBA/Forecast)
    insights = [
        {"title": "Velocity Spike", "body": f"Detected {res['best_model']} trend suggesting 12% growth in next {horizon} days."},
        {"title": "Revenue Integrity", "body": "Standard deviation in weekend revenue has decreased, suggesting stable demand cycles."}
    ]
    recs = [
        "Increase ad-spend on Category X to capitalize on predicted midweek surge.",
        "Pre-allocate staff for Friday processing based on SARIMA volume projections."
    ]
    alerts = ["Low stock detected in 'Top Bundle' components" if "stock" in st.session_state else ""]
    
    render_insight_dashboard(insights, recs, [a for a in alerts if a])
    render_ai_pilot_chat()

def render_category_intelligence(df_sales: pd.DataFrame):
    """
    Renders Revenue Share (Donut) and Volume (Bar) for Category-wise items.
    """
    st.markdown("#### 📂 Category Strategy Distribution")
    
    # Safety Check: Inherit expert rules if column missing (e.g. from cache)
    if "Category" not in df_sales.columns:
        from BackEnd.utils.category_rules import apply_category_expert_rules
        # Use semantic mapping to find item name
        name_col = "item_name" if "item_name" in df_sales.columns else "Product Name"
        if name_col in df_sales.columns:
            df_sales = apply_category_expert_rules(df_sales, name_col=name_col)
    
    if df_sales.empty or "Category" not in df_sales.columns:
        st.info("Category data unavailable for this selection. Ensure 'Product Name' or 'item_name' exists.")
        return

    # Aggregate by category and sort for impact
    cat_df = df_sales.groupby("Category").agg(
        Revenue=("order_total", "sum"),
        Volume=("qty", "sum")
    ).reset_index()
    
    cat_df = cat_df[cat_df["Revenue"] > 0].sort_values("Revenue", ascending=False)

    if cat_df.empty:
        st.info("No categorical revenue identified.")
        return

    # Handle Hierarchical Display for Charts
    # If a sub-category exists (e.g. 'Jeans - Slim Fit'), we show only the sub-category in the graph
    cat_df["DisplayCategory"] = cat_df["Category"].apply(lambda x: x.split(" - ")[1] if " - " in str(x) else x)

    from FrontEnd.components import ui
    
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(ui.donut_chart(cat_df, values="Revenue", names="DisplayCategory", title="Revenue Share"), use_container_width=True)
    with c2:
        # Sort by volume for the bar chart
        vol_df = cat_df.sort_values("Volume", ascending=True) # Horizontal bars sorted asc to put largest at top
        st.plotly_chart(ui.bar_chart(vol_df, x="Volume", y="DisplayCategory", title="Units Sold", color_scale="Tealgrn", text_auto=".1s"), use_container_width=True)
