import streamlit as st
import pandas as pd
import sqlite3
import json
from datetime import datetime
from BackEnd.commerce_ops.persistence import KeyManager
from BackEnd.services.nlp_engine import LLMAgent
from BackEnd.core.paths import SYSTEM_LOG_FILE
from FrontEnd.components.ui import export_to_excel

def render_advanced_sql_terminal(sales_df: pd.DataFrame = None, returns_df: pd.DataFrame = None, stock_df: pd.DataFrame = None):
    st.markdown("### 🚀 Advanced SQL Data Pilot")
    
    # Initialize in-memory database
    conn = sqlite3.connect(':memory:')
    schema_info = []
    
    if sales_df is not None and not sales_df.empty:
        sales_df.to_sql('sales', conn, index=False)
        schema_info.append(f"Table 'sales' columns: {', '.join(sales_df.columns)}")
        
    if returns_df is not None and not returns_df.empty:
        returns_df.to_sql('returns', conn, index=False)
        schema_info.append(f"Table 'returns' columns: {', '.join(returns_df.columns)}")
        
    if stock_df is not None and not stock_df.empty:
        stock_df.to_sql('stock', conn, index=False)
        schema_info.append(f"Table 'stock' columns: {', '.join(stock_df.columns)}")

    # --- Terminal UI ---
    st.markdown("""
    <style>
    .terminal-box {
        background-color: #0c0c0c;
        color: #00ff00;
        font-family: 'Courier New', Courier, monospace;
        padding: 15px;
        border-radius: 8px;
        height: 200px;
        overflow-y: auto;
        border: 1px solid #333;
        margin-bottom: 20px;
        box-shadow: inset 0 0 10px rgba(0,0,0,0.8);
    }
    .term-prefix { color: #ff00ff; font-weight: bold; }
    .term-msg { color: #00ff00; }
    .term-error { color: #ff3333; }
    </style>
    """, unsafe_allow_html=True)
    
    if "pilot_term_logs" not in st.session_state:
        loaded_logs = []
        if SYSTEM_LOG_FILE.exists():
            try:
                with open(SYSTEM_LOG_FILE, "r", encoding="utf-8") as f:
                    loaded_logs = json.load(f)
            except Exception:
                pass
        
        if not loaded_logs:
            loaded_logs = [
                "<span class='term-msg'>DEEN-OPS Data Pilot Terminal initialized.</span>",
                "<span class='term-msg'>Connecting to SQLite In-Memory Database...</span>",
                "<span class='term-msg'>Available Tables loaded: sales, returns, stock.</span>",
                "<span class='term-msg'>Ready for queries.</span>"
            ]
        st.session_state.pilot_term_logs = loaded_logs
        
    def log_to_term(msg, is_error=False):
        css_class = "term-error" if is_error else "term-msg"
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_msg = f"<span class='{css_class}'>[{timestamp}] {msg}</span>"
        st.session_state.pilot_term_logs.append(formatted_msg)
        
        try:
            SYSTEM_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(SYSTEM_LOG_FILE, "w", encoding="utf-8") as f:
                json.dump(st.session_state.pilot_term_logs[-100:], f, ensure_ascii=False)
        except Exception:
            pass
        
    terminal_html = "<div class='terminal-box'>"
    for log in st.session_state.pilot_term_logs[-20:]:
        if log.startswith("<span"):
            terminal_html += f"<div><span class='term-prefix'>root@data-pilot:~$</span> {log}</div>"
        else:
            terminal_html += f"<div><span class='term-prefix'>root@data-pilot:~$</span> <span class='term-msg'>{log}</span></div>"
    terminal_html += "</div>"
    st.markdown(terminal_html, unsafe_allow_html=True)

    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.markdown("#### 💻 Custom SQL Engine")
        st.caption("Write advanced SQL queries (JOINs, VIEWs, Aggregations) directly against your loaded data.")
        
        SAVED_QUERIES_FILE = SYSTEM_LOG_FILE.parent / "saved_queries.json"
        if "saved_queries" not in st.session_state:
            st.session_state.saved_queries = []
            if SAVED_QUERIES_FILE.exists():
                try:
                    with open(SAVED_QUERIES_FILE, "r", encoding="utf-8") as f:
                        st.session_state.saved_queries = json.load(f)
                except Exception:
                    pass

        if st.session_state.saved_queries:
            def _set_query():
                val = st.session_state[KeyManager.get_key("pilot", "query_selector")]
                if val != "-- Select a saved query --":
                    st.session_state[KeyManager.get_key("pilot", "sql_input")] = val
            
            sq_c1, sq_c2 = st.columns([5, 1])
            with sq_c1:
                st.selectbox("📂 Load Saved Query", ["-- Select a saved query --"] + st.session_state.saved_queries, key=KeyManager.get_key("pilot", "query_selector"), on_change=_set_query, label_visibility="collapsed")
            with sq_c2:
                if st.button("🗑️", help="Delete selected query", use_container_width=True):
                    sel_val = st.session_state.get(KeyManager.get_key("pilot", "query_selector"))
                    if sel_val and sel_val != "-- Select a saved query --" and sel_val in st.session_state.saved_queries:
                        st.session_state.saved_queries.remove(sel_val)
                        try:
                            with open(SAVED_QUERIES_FILE, "w", encoding="utf-8") as f:
                                json.dump(st.session_state.saved_queries, f, ensure_ascii=False)
                        except Exception: pass
                        if KeyManager.get_key("pilot", "query_selector") in st.session_state:
                            del st.session_state[KeyManager.get_key("pilot", "query_selector")]
                        st.rerun()

        query = st.text_area("SQL Query", value="SELECT * FROM sales LIMIT 5", height=150, key=KeyManager.get_key("pilot", "sql_input"))
        
        btn_c1, btn_c2 = st.columns([3, 1])
        with btn_c1:
            exec_btn = st.button("▶ Execute", type="primary", use_container_width=True)
        with btn_c2:
            save_btn = st.button("💾 Save", use_container_width=True)
            
        if save_btn and query.strip():
            if query not in st.session_state.saved_queries:
                st.session_state.saved_queries.append(query)
                try:
                    with open(SAVED_QUERIES_FILE, "w", encoding="utf-8") as f:
                        json.dump(st.session_state.saved_queries, f, ensure_ascii=False)
                    st.toast("Query saved successfully!", icon="✅")
                except Exception:
                    pass
            else:
                st.toast("Query is already saved.", icon="ℹ️")
        
        if exec_btn:
            cmd = query.strip().lower()
            if cmd == "clear":
                st.session_state.pilot_term_logs = []
                try:
                    with open(SYSTEM_LOG_FILE, "w", encoding="utf-8") as f:
                        json.dump([], f)
                except Exception: pass
                st.rerun()
            elif cmd == "help":
                log_to_term("SYSTEM COMMANDS: 'clear', 'help'")
                log_to_term("AVAILABLE TABLES: sales, returns, stock")
                for info in schema_info:
                    log_to_term(info)
                st.rerun()
            else:
                try:
                    log_to_term(f"Executing: {query}")
                    result_df = pd.read_sql_query(query, conn)
                    log_to_term(f"Success: {len(result_df)} rows returned.")
                    st.session_state[KeyManager.get_key("pilot", "sql_result")] = result_df
                except Exception as e:
                    log_to_term(f"ERROR: {str(e)}", is_error=True)
                    st.error(f"SQL Error: {str(e)}")
                    if KeyManager.get_key("pilot", "sql_result") in st.session_state:
                        del st.session_state[KeyManager.get_key("pilot", "sql_result")]
                
        if KeyManager.get_key("pilot", "sql_result") in st.session_state:
            result_df = st.session_state[KeyManager.get_key("pilot", "sql_result")]
            st.success(f"Last query executed successfully! ({len(result_df)} rows)")
            
            tab_data, tab_viz = st.tabs(["📋 Data Table", "📊 Visualizer"])
            
            with tab_data:
                st.dataframe(result_df, use_container_width=True)
                
                btn_c1, btn_c2 = st.columns(2)
                with btn_c1:
                    excel_bytes = export_to_excel(result_df, sheet_name="SQL Extract")
                    st.download_button(
                        label="📥 Export to Excel",
                        data=excel_bytes,
                        file_name=f"sql_extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=KeyManager.get_key("pilot", "export_excel_btn"),
                        use_container_width=True
                    )
                with btn_c2:
                    if st.button("🧠 Explain Result", use_container_width=True, key=KeyManager.get_key("pilot", "explain_btn")):
                        st.session_state[KeyManager.get_key("pilot", "chat_explain_request")] = True
                        st.rerun()
                
            with tab_viz:
                if not result_df.empty and len(result_df.columns) >= 2:
                    viz_c1, viz_c2, viz_c3 = st.columns(3)
                    with viz_c1: x_col = st.selectbox("X-Axis", result_df.columns, key=KeyManager.get_key("pilot", "viz_x"))
                    with viz_c2: y_col = st.selectbox("Y-Axis", result_df.columns, index=min(1, len(result_df.columns)-1), key=KeyManager.get_key("pilot", "viz_y"))
                    with viz_c3: chart_type = st.selectbox("Chart Type", ["Bar Chart", "Line Chart", "Scatter Plot"], key=KeyManager.get_key("pilot", "viz_type"))
                        
                    import plotly.express as px
                    fig_func = px.bar if chart_type == "Bar Chart" else px.line if chart_type == "Line Chart" else px.scatter
                    st.plotly_chart(fig_func(result_df, x=x_col, y=y_col, template="plotly_dark"), use_container_width=True, key=KeyManager.get_key("pilot", "viz_chart"))
                else:
                    st.info("Insufficient data for visualization. Query must return at least 2 columns.")

    with c2:
        st.markdown("#### 🤖 ML / NLP Assistant")
        st.caption("Ask in plain English. Our NLP model will generate the SQL.")
        
        nl_query = st.text_area("Natural Language Request", placeholder="e.g., Show me the top 5 product categories by revenue from the sales table", height=100)
        
        if st.button("✨ Suggest SQL", use_container_width=True):
            with st.spinner("AI is thinking..."):
                try:
                    schema_context = "\n".join(schema_info)
                    prompt = f"Given the following SQLite schemas:\n{schema_context}\n\nWrite a valid SQLite query for: '{nl_query}'. Return ONLY the SQL code, no markdown ticks, no explanation."
                    
                    agent = LLMAgent()
                    sql_suggestion = agent.query(prompt, pd.DataFrame()) 
                    sql_suggestion = sql_suggestion.replace('```sql', '').replace('```', '').strip()
                    
                    log_to_term(f"AI Suggested SQL for: '{nl_query}'")
                    st.session_state[KeyManager.get_key("pilot", "sql_input")] = sql_suggestion
                    st.info("SQL Generated & Copied to editor!")
                    st.rerun()
                except Exception as e:
                    log_to_term(f"AI Error: {str(e)}", is_error=True)
                    st.error(f"Failed to generate SQL: {e}")

    # --- Native Streamlit Chatbot UI ---
    st.markdown("---")
    st.markdown("#### 💬 Chat with Data Pilot AI")
    
    if "pilot_chat_messages" not in st.session_state:
        st.session_state.pilot_chat_messages = [
            {"role": "assistant", "content": "Hello Commander. I am your Data Pilot assistant. I can help explain your SQL results or write new queries based on the database schema!"}
        ]
        
    # Render chat history
    for msg in st.session_state.pilot_chat_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            
    # Chat input
    prompt = None
    if st.session_state.get(KeyManager.get_key("pilot", "chat_explain_request"), False):
        prompt = "Please act as a data analyst. I just ran a SQL query. Explain the key insights and trends visible in the resulting data table you have in context."
        st.session_state[KeyManager.get_key("pilot", "chat_explain_request")] = False
        # Need to spoof the chat input appearance in history
        st.session_state.pilot_chat_messages.append({"role": "user", "content": "🧠 *Requested Data Explanation*"})
        with st.chat_message("user"): st.markdown("🧠 *Requested Data Explanation*")
    elif user_input := st.chat_input("Ask a question about your data or request a SQL query..."):
        prompt = user_input
        st.session_state.pilot_chat_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"): st.markdown(prompt)

    if prompt:
            
        with st.chat_message("assistant"):
            with st.spinner("AI is analyzing..."):
                try:
                    import re
                    agent = LLMAgent()
                    ctx_df = st.session_state.get(KeyManager.get_key("pilot", "sql_result"), pd.DataFrame())
                    recent_logs = "\n".join([re.sub(r'<[^>]+>', '', log) for log in st.session_state.pilot_term_logs[-5:]])
                    schema_context = "\\n".join(schema_info)
                    
                    enhanced_prompt = f"""You are DEEN-BI Data Pilot, an expert e-commerce analyst and Python data scientist.
Database Schema:
{schema_context}

Recent Terminal Activity:
{recent_logs}

User Query: {prompt}

If the user asks for a chart, graph, or visualization, you MUST provide a valid Python code snippet using `plotly.express`.
The dataframe is available as `df`.
Example:
```python
import plotly.express as px
fig = px.bar(df, x='Category', y='Revenue', title='Revenue by Category')
fig.show()
```
Otherwise, provide a concise, professional analysis based on the user's query and the provided context.
"""
                    
                    response = agent.query(enhanced_prompt, ctx_df)
                    st.markdown(response)
                    st.session_state.pilot_chat_messages.append({"role": "assistant", "content": response})
                except Exception as e:
                    err_msg = f"Sorry, I encountered an error connecting to the LLM: {str(e)}"
                    st.error(err_msg)
                    st.session_state.pilot_chat_messages.append({"role": "assistant", "content": err_msg})