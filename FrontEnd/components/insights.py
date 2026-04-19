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

def render_ai_pilot_chat(sales_df: pd.DataFrame):
    """Renders the 'Ask Your Data' input box with live NLP processing."""
    import pandas as pd
    from BackEnd.services.nlp_engine import get_nlp_response
    
    st.markdown("""
        <style>
        .stTextInput > div > div > input {
            background-color: rgba(79, 70, 229, 0.05);
            border: 1px solid rgba(79, 70, 229, 0.2);
            color: #4f46e5;
            font-weight: 500;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # AI Configuration Bar
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        agent_type = st.selectbox("🤖 Brain Type", ["Standard", "Local AI Agent"], help="Standard is fast/rule-based. AI Agent uses local LLMs.")
    with c2:
        model_name = st.text_input("📦 Model Name", value="gemma", help="Model name (e.g., gemma, llama3, mistral)")
    with c3:
        base_url = st.text_input("🔗 API Base URL", value="http://localhost:11434", help="Ollama: http://localhost:11434 | LM Studio: http://localhost:1234")

    query = st.text_input("💬 Ask Data Pilot (e.g., 'What is my top category this month?' or 'revenue yesterday')", 
                         placeholder="Type your command here...",
                         key="nlp_query_input")
    
    if query:
        with st.spinner(f"🧠 {agent_type} is querying the data streams..."):
            response = get_nlp_response(query, sales_df, agent_type=agent_type, model_name=model_name, base_url=base_url)
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, rgba(79, 70, 229, 0.1) 0%, rgba(124, 58, 237, 0.1) 100%); 
                        padding: 20px; border-radius: 12px; border: 1px solid rgba(79, 70, 229, 0.2); 
                        margin-top: 15px; box-shadow: 0 4px 12px rgba(0,0,0,0.1);">
                <div style="color: #4f46e5; font-weight: 800; font-size: 0.75rem; letter-spacing: 1px; margin-bottom: 8px;">🚀 DATA PILOT RESPONSE</div>
                <div style="font-size: 1.05rem; line-height: 1.5; color: var(--text-color);">{response}</div>
            </div>
            """, unsafe_allow_html=True)
            
            # Predictive follow-up
            if "revenue" in query.lower():
                st.caption("✨ Tip: Try asking 'Who is my top customer?' to see who contributed to this revenue.")
