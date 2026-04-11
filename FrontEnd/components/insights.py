import streamlit as st

def render_insight_dashboard(insights: list, recommendations: list, alerts: list):
    """Renders a premium decision-support panel."""
    
    st.markdown("### 🧠 Operational Intelligence")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown(
            """
            <div style="background: rgba(16, 185, 129, 0.05); border: 1px solid rgba(16, 185, 129, 0.2); border-radius: 12px; padding: 20px;">
                <h4 style="margin-top: 0; color: #059669; font-size: 1rem; text-transform: uppercase;">🔍 Key Insights</h4>
            """, unsafe_allow_html=True
        )
        for ins in insights:
            st.markdown(f"• **{ins['title']}**: {ins['body']}")
        st.markdown("</div>", unsafe_allow_html=True)
        
    with col2:
        st.markdown(
            """
            <div style="background: rgba(245, 158, 11, 0.05); border: 1px solid rgba(245, 158, 11, 0.2); border-radius: 12px; padding: 20px;">
                <h4 style="margin-top: 0; color: #d97706; font-size: 1rem; text-transform: uppercase;">💡 Strategic Recommendations</h4>
            """, unsafe_allow_html=True
        )
        for rec in recommendations:
            st.markdown(f"• {rec}")
        st.markdown("</div>", unsafe_allow_html=True)
        
    if alerts:
        st.markdown("<div style='margin-top:20px;'></div>", unsafe_allow_html=True)
        st.error(f"⚠️ **Critical Alerts**: {' | '.join(alerts)}")

def render_ai_pilot_chat():
    """Renders the 'Ask Your Data' input box."""
    with st.expander("🚀 Data Pilot | Natural Language Query", expanded=False):
        query = st.text_input("Ask anything about your operations (e.g., 'Why did orders drop yesterday?')", 
                             placeholder="Type your question here...")
        if query:
            with st.spinner("Analyzing operational signals..."):
                # Integration with LLM would go here
                st.info("The Data Pilot is analyzing cross-table signals to generate a synthesized response.")
                st.markdown("**Analysis Preview**: Orders dropped by 12% compared to previous Sunday. Key factor identified: Stockout in 'Product X' which usually accounts for 15% of weekend revenue.")
