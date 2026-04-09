import pandas as pd
import streamlit as st
from .data_helpers import sum_order_level_revenue, build_order_level_dataset

def render_dashboard_story(df_sales: pd.DataFrame, df_customers: pd.DataFrame, ml_bundle: dict, time_window: str = "this period"):
    if df_sales.empty:
        return
    total_revenue = sum_order_level_revenue(df_sales)
    order_df = build_order_level_dataset(df_sales)
    total_orders = order_df["order_id"].nunique()
    aov = total_revenue / total_orders if total_orders else 0
    days_in_window = (df_sales["order_date"].max() - df_sales["order_date"].min()).days + 1 if not df_sales.empty else 1
    
    narrative = []
    if total_revenue > 0:
        avg_daily = total_revenue / days_in_window if days_in_window > 0 else total_revenue
        
        # Format the time prefix elegantly
        if time_window.lower() in ["mtd", "ytd"]:
            period_prefix = f"In {time_window}"
        elif "last" in time_window.lower() or "yesterday" in time_window.lower():
            period_prefix = f"Over the {time_window.lower()}"
        else:
            period_prefix = f"In {time_window.lower()}"
            
        narrative.append(f"{period_prefix}, your store has generated <b>TK {total_revenue:,.0f}</b> in revenue, averaging <b>TK {avg_daily:,.0f}</b> per day.")
    if not df_customers.empty and "segment" in df_customers.columns:
        vips = len(df_customers[df_customers["segment"] == "VIP"])
        if vips > 0:
            narrative.append(f"Your <b>{vips} VIP customers</b> continue to represent the most stable growth lever in this window.")
    forecast = ml_bundle.get("forecast", pd.DataFrame())
    if not forecast.empty and "forecast_7d_revenue" in forecast.columns:
        next_week_rev = forecast["forecast_7d_revenue"].sum()
        narrative.append(f"The ML engine predicts a rolling 7-day revenue outlook of <b>TK {next_week_rev:,.0f}</b> based on current trajectories.")
    # 4. VIP Churn Watch (Strategic)
    at_risk_vips = []
    if not df_customers.empty and "recency_days" in df_customers.columns:
        # VIPs who haven't bought in 21+ days
        vips_raw = df_customers[df_customers["segment"] == "VIP"]
        at_risk_vips = vips_raw[vips_raw["recency_days"] > 21].copy()
        if not at_risk_vips.empty:
            narrative.append(f"<b>{len(at_risk_vips)} VIP customers</b> are currently at risk of churning (no purchase in 21+ days).")

    # 5. Bundle Discovery (Growth)
    bundle_suggestions = []
    if not df_sales.empty:
        # Find orders with > 1 item
        multi_item_orders = df_sales.groupby("order_id").filter(lambda x: x["item_name"].nunique() > 1)
        if not multi_item_orders.empty:
            # Simple co-occurrence count
            from itertools import combinations
            order_items = multi_item_orders.groupby("order_id")["item_name"].apply(list)
            pairs = []
            for items in order_items:
                pairs.extend(combinations(sorted(items), 2))
            
            if pairs:
                pair_counts = pd.Series(pairs).value_counts()
                top_pair = pair_counts.index[0]
                if pair_counts.iloc[0] > 1: # Only suggest if seen more than once
                    narrative.append(f"Growth Slot: Customers frequently buy <b>{top_pair[0]}</b> and <b>{top_pair[1]}</b> together. Consider a bundle.")
                    bundle_suggestions = [top_pair]

    combined_narrative = " ".join(narrative).replace("<b>", "").replace("</b>", "")
    import hashlib
    narrative_hash = hashlib.md5(combined_narrative.encode()).hexdigest()[:8]
    typing_duration = max(8, len(combined_narrative) // 12)

    # Advanced Multi-Line Typewriter Effect (Opacity Based avoids Reflow)
    char_delay = typing_duration / max(len(combined_narrative), 1)
    animated_chars = []
    for i, char in enumerate(combined_narrative):
        if char == " ":
            animated_chars.append(" ")
        else:
            animated_chars.append(f'<span style="opacity: 0; animation: revChar_{narrative_hash} 0.1s forwards; animation-delay: {i * char_delay:.3f}s;">{char}</span>')
    animated_html = "".join(animated_chars)

    st.markdown(
        f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@500&display=swap');

            .orthodox-typewriter {{
                font-family: 'JetBrains Mono', monospace;
                font-size: 0.9rem;
                background: var(--surface);
                padding: 12px 18px;
                border-radius: 4px;
                border-left: 4px solid #F59E0B;
                white-space: normal;
                display: block;
                width: 100%;
                margin-bottom: 8px;
                line-height: 1.5;
            }}

            /* Light/Dark adaptive colors */
            @media (prefers-color-scheme: light) {{ .orthodox-typewriter {{ color: #000000; border-left-color: #000000; }} }}
            @media (prefers-color-scheme: dark) {{ .orthodox-typewriter {{ color: #F59E0B; border-left-color: #F59E0B; }} }}

            @keyframes revChar_{narrative_hash} {{
                to {{ opacity: 1; }}
            }}
        </style>
        <div class="orthodox-typewriter" id="story-typewriter-{narrative_hash}">
            {animated_html}<span style="animation: blinkCaret 0.75s step-end infinite;">_</span>
        </div>
        <style>
            @keyframes blinkCaret {{
                0%, 100% {{ opacity: 1; }}
                50% {{ opacity: 0; }}
            }}
        </style>
        """,
        unsafe_allow_html=True
    )

    # 8. Interactive Discovery Tools (Icon Bar - Now on Next Line)
    # Ensure icons appear if any insight is available (VIP Churn or Bundles)
    has_insights = not at_risk_vips.empty or bool(bundle_suggestions)
    
    if has_insights:
        # Create a row of icons below the typewriter
        btn_col, _ = st.columns([2, 5])
        with btn_col:
            # Use nested columns for horizontal buttons
            ic1, ic2 = st.columns(2)
            # Use a container with a delayed fade-in to match typing duration
            st.markdown(
                f"""
                <style>
                    .delayed-icon {{
                        animation: fadeIn 0.5s ease-in forwards;
                        animation-delay: {typing_duration}s;
                        opacity: 0;
                    }}
                    @keyframes fadeIn {{
                        from {{ opacity: 0; }}
                        to {{ opacity: 1; }}
                    }}
                </style>
                """,
                unsafe_allow_html=True
            )
            
            # Icons now appear in horizontal sequence after the typing delay
            with ic1:
                pass # Bundle icon removed
            
            with ic2:
                if not at_risk_vips.empty:
                    if st.button("👥", key="btn_vip_churn", help="View At-Risk VIPs"):
                        st.session_state.show_vip_churn = not st.session_state.get("show_vip_churn", False)

            # VIP Churn Rescue View
            if st.session_state.get("show_vip_churn") and not at_risk_vips.empty:
                st.markdown("---")
                st.warning(f"🚨 At-Risk VIPs: These customers are high-value but haven't interacted in 21+ days.")
                st.dataframe(at_risk_vips[["primary_name", "total_revenue", "total_orders", "recency_days"]].rename(
                    columns={"primary_name": "Customer", "total_revenue": "Lifetime Value", "recency_days": "Days Since Last Order"}
                ), use_container_width=True, hide_index=True)
                st.caption("💡 Suggestion: Launch a 'We Miss You' WhatsApp campaign for these specific individuals.")
