import streamlit as st
import pandas as pd
import sqlite3
import json
import uuid
from datetime import datetime
from pathlib import Path
from BackEnd.commerce_ops.persistence import KeyManager
from BackEnd.services.nlp_engine import LLMAgent
from BackEnd.core.paths import SYSTEM_LOG_FILE
from FrontEnd.components.ui import export_to_excel

def render_advanced_sql_terminal(sales_df: pd.DataFrame | None = None, returns_df: pd.DataFrame | None = None, stock_df: pd.DataFrame | None = None):
    st.markdown("### 🚀 Advanced SQL Data Pilot")
    
    # Initialize persistent offline database
    db_path = Path("BackEnd/cache/offline_data.db")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    schema_info = []
    
    if sales_df is not None and not sales_df.empty:
        sales_df.to_sql('sales', conn, index=False, if_exists='replace')
        schema_info.append(f"Table 'sales' columns: {', '.join(sales_df.columns)}")
        
    if returns_df is not None and not returns_df.empty:
        returns_df.to_sql('returns', conn, index=False, if_exists='replace')
        schema_info.append(f"Table 'returns' columns: {', '.join(returns_df.columns)}")
        
    if stock_df is not None and not stock_df.empty:
        stock_df.to_sql('stock', conn, index=False, if_exists='replace')
        schema_info.append(f"Table 'stock' columns: {', '.join(stock_df.columns)}")
        
    # Dynamically load existing views into schema context so AI can query them
    try:
        views_df = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='view'", conn)
        for _, row in views_df.iterrows():
            v_name = row['name']
            v_schema = pd.read_sql_query(f"PRAGMA table_info({v_name})", conn)
            schema_info.append(f"View '{v_name}' columns: {', '.join(v_schema['name'].tolist())}")
    except Exception:
        pass

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
        
        btn_c1, btn_c2, btn_c3 = st.columns([2, 1, 1])
        with btn_c1:
            exec_btn = st.button("▶ Execute", type="primary", use_container_width=True)
        with btn_c2:
            format_btn = st.button("🪄 Format", use_container_width=True)
        with btn_c3:
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
        
        if format_btn and query.strip():
            try:
                import sqlparse
                formatted_query = sqlparse.format(query, reindent=True, keyword_case='upper')
                st.session_state[KeyManager.get_key("pilot", "sql_input")] = formatted_query
                st.rerun()
            except ImportError:
                st.toast("Please install 'sqlparse' (pip install sqlparse) for formatting.", icon="⚠️")
                
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
                    st.session_state[KeyManager.get_key("pilot", "last_executed_query")] = query
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
                st.dataframe(result_df, use_container_width=True, key=KeyManager.get_key("pilot", "sql_result_df"))
                
                btn_c1, btn_c2, btn_c3, btn_c4 = st.columns(4)
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
                with btn_c3:
                    # New button to copy Plotly code
                    if st.button("📈 Get Plotly Code", use_container_width=True, key=KeyManager.get_key("pilot", "plotly_code_btn")):
                        st.session_state[KeyManager.get_key("pilot", "chat_plotly_request")] = True
                        st.rerun()
                with btn_c4:
                    with st.popover("💾 Save as View", use_container_width=True):
                        view_name = st.text_input("View Name", "ai_custom_view", key=KeyManager.get_key("pilot", "view_name"))
                        if st.button("Create View", key=KeyManager.get_key("pilot", "create_view_btn"), use_container_width=True):
                            last_q = st.session_state.get(KeyManager.get_key("pilot", "last_executed_query"), "")
                            if last_q and view_name:
                                try:
                                    conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                                    conn.execute(f"CREATE VIEW {view_name} AS {last_q}")
                                    conn.commit()
                                    log_to_term(f"Created virtual view: {view_name}")
                                    st.toast(f"View '{view_name}' created successfully!", icon="✅")
                                except Exception as e:
                                    st.error(f"Failed to create view: {e}")
                                    log_to_term(f"View Creation Error: {e}", is_error=True)
                
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
                    
                    import os
                    agent_type = "Standard"
                    try:
                        if "OPENROUTER_API_KEY" in st.secrets or os.environ.get("OPENROUTER_API_KEY"):
                            agent_type = "OpenRouter"
                        elif "HUGGINGFACE_API_KEY" in st.secrets or os.environ.get("HUGGINGFACE_API_KEY"):
                            agent_type = "HuggingFace"
                        elif "GROQ_API_KEY" in st.secrets or os.environ.get("GROQ_API_KEY"):
                            agent_type = "Groq"
                        elif "GEMINI_API_KEY" in st.secrets or os.environ.get("GEMINI_API_KEY"):
                            agent_type = "Google Gemini"
                    except Exception:
                        pass
                    agent = LLMAgent(agent_type=agent_type)
                    
                    sql_suggestion = ""
                    last_error = ""
                    current_prompt = prompt
                    
                    for attempt in range(3):
                        sql_suggestion = agent.query(current_prompt, {}) 
                        sql_suggestion = sql_suggestion.replace('```sql', '').replace('```', '').strip()
                        
                        try:
                            import sqlparse
                            sql_suggestion = sqlparse.format(sql_suggestion, reindent=True, keyword_case='upper')
                        except ImportError:
                            pass
                        
                        try:
                            # Evaluate SQL syntax and schema validity without executing it
                            conn.execute(f"EXPLAIN {sql_suggestion}")
                            last_error = ""
                            break # Valid query, break the loop
                        except sqlite3.Error as e:
                            last_error = str(e)
                            log_to_term(f"SQL Validation Failed (Attempt {attempt+1}): {last_error}", is_error=True)
                            # Feed the exact schema error back to the LLM to auto-correct
                            current_prompt = f"{prompt}\n\nYour previous query:\n{sql_suggestion}\n\nFailed with SQLite error:\n{last_error}\n\nPlease correct the SQL query. Return ONLY the valid SQL code."
                            
                    if last_error:
                        st.warning(f"AI struggled to form a perfect query. Last error: {last_error}")
                    
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
    import re
    import streamlit.components.v1 as components
    
    for msg in st.session_state.pilot_chat_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("html_snippets"):
                for snippet in msg["html_snippets"]:
                    components.html(snippet, height=450)
            elif msg["role"] == "assistant" and "```python" in msg["content"] and "plotly" in msg["content"]:
                code_blocks = re.findall(r'```python\n(.*?)\n```', msg["content"], re.DOTALL)
                for code in code_blocks:
                    if "plotly" in code:
                        try:
                            hist_df = st.session_state.get(KeyManager.get_key("pilot", "sql_result"), pd.DataFrame())
                            local_vars = {"df": hist_df.copy()}
                            safe_code = code.replace("fig.show()", "")
                            exec(safe_code, globals(), local_vars)
                            if "fig" in local_vars:
                                st.plotly_chart(
                                    local_vars["fig"], 
                                    use_container_width=True, 
                                    config={'displayModeBar': True, 'toImageButtonOptions': {'format': 'png', 'filename': 'ai_chart_export'}}
                                )
                        except Exception as e:
                            st.error(f"Could not render chart from history: {e}")
            
    # Chat input
    prompt_user_input = st.chat_input("Ask a question about your data or request a SQL query...")
    
    chat_prompt = None
    
    def sanitize_input(user_input: str) -> str:
        """Prevent prompt injection and limit length."""
        if not user_input: return ""
        return user_input[:1000].strip()
    
    # Handle explain request
    if st.session_state.get(KeyManager.get_key("pilot", "chat_explain_request"), False):
        st.session_state[KeyManager.get_key("pilot", "chat_explain_request")] = False
        chat_prompt = "Please act as a data analyst. I just ran a SQL query. Explain the key insights and trends visible in the resulting data table you have in context."
        # Need to spoof the chat input appearance in history
        st.session_state.pilot_chat_messages.append({"role": "user", "content": "🧠 *Requested Data Explanation*"})
        with st.chat_message("user"): st.markdown("🧠 *Requested Data Explanation*")
    elif st.session_state.get(KeyManager.get_key("pilot", "chat_plotly_request"), False):
        st.session_state[KeyManager.get_key("pilot", "chat_plotly_request")] = False
        chat_prompt = "Please generate a Python code snippet using `plotly.express` to visualize the primary relationship in the current SQL result. The dataframe is available as `df`."
        st.session_state.pilot_chat_messages.append({"role": "user", "content": "📈 *Requested Plotly Code*"})
        with st.chat_message("user"): st.markdown("📈 *Requested Plotly Code*")
    elif prompt_user_input:
        chat_prompt = sanitize_input(prompt_user_input)
        st.session_state.pilot_chat_messages.append({"role": "user", "content": chat_prompt})
        with st.chat_message("user"): st.markdown(chat_prompt)

    if chat_prompt:
        
        # Check for Learn/Remember intercept
        if chat_prompt.strip().lower().startswith("learn:") or chat_prompt.strip().lower().startswith("remember:"):
            from pathlib import Path
            new_knowledge = chat_prompt.split(":", 1)[1].strip()
            knowledge_file = Path("BackEnd/data/pilot_knowledge.txt")
            knowledge_file.parent.mkdir(parents=True, exist_ok=True)
            with open(knowledge_file, "a", encoding="utf-8") as f:
                f.write(f"- {new_knowledge}\n")
            if "llm_response_cache" in st.session_state:
                st.session_state.llm_response_cache.clear()
            msg = f"✅ Got it! I have updated my knowledge base with: '{new_knowledge}'. I'll remember this for future SQL and data queries."
            st.session_state.pilot_chat_messages.append({"role": "assistant", "content": msg})
            st.rerun()
            
        with st.chat_message("assistant"):
            with st.spinner("AI is analyzing..."):
                try:
                    import re
                    import os
                    agent_type = "Standard"
                    try:
                        if "OPENROUTER_API_KEY" in st.secrets or os.environ.get("OPENROUTER_API_KEY"):
                            agent_type = "OpenRouter"
                        elif "HUGGINGFACE_API_KEY" in st.secrets or os.environ.get("HUGGINGFACE_API_KEY"):
                            agent_type = "HuggingFace"
                        elif "GROQ_API_KEY" in st.secrets or os.environ.get("GROQ_API_KEY"):
                            agent_type = "Groq"
                        elif "GEMINI_API_KEY" in st.secrets or os.environ.get("GEMINI_API_KEY"):
                            agent_type = "Google Gemini"
                    except Exception:
                        pass
                    agent = LLMAgent(agent_type=agent_type)
                    ctx_df = st.session_state.get(KeyManager.get_key("pilot", "sql_result"), pd.DataFrame())
                    recent_logs = "\n".join([re.sub(r'<[^>]+>', '', log) for log in st.session_state.pilot_term_logs[-5:]]) # Clean HTML from logs
                    schema_context = "\n".join(schema_info) # Corrected escaping
                    
                    from pathlib import Path
                    custom_instructions = ""
                    knowledge_file = Path("BackEnd/data/pilot_knowledge.txt")
                    if knowledge_file.exists():
                        try:
                            with open(knowledge_file, "r", encoding="utf-8") as f:
                                custom_instructions = f.read().strip()
                        except Exception: pass

                    enhanced_prompt = f"""You are DEEN-BI Data Pilot, an autonomous AI SQL Agent and Python data scientist.
Database Schema:
{schema_context}

CRITICAL RULES:
1. Order Logic: An `order_id` represents a single unique order. An order may contain multiple item lines. You must NEVER count item rows as a single order. When asked for 'total orders' or 'number of orders', you must perform a COUNT(DISTINCT order_id).
2. Continuous Learning Protocol: Treat all user corrections as updates to your permanent knowledge base for this specific dataset. Do not repeat the corrected mistake in subsequent queries.
3. Auto-Memorization: If the user corrects a mistake or provides a new persistent rule, you MUST output the exact string `[KNOWLEDGE_UPDATE: <the new rule>]` on a new line.

USER KNOWLEDGE BASE / CUSTOM INSTRUCTIONS:
{custom_instructions}

Recent Terminal Activity:
{recent_logs}

User Query: {chat_prompt}

If the user asks for a chart, graph, or visualization, you MUST provide a valid Python code snippet using `plotly.express`.
The dataframe is available as `df`.
Example:
'''python
import plotly.express as px
fig = px.bar(df, x='category_column', y='value_column', title='Chart Title')
fig.show()
'''
Otherwise, provide a concise, professional analysis based on the user's query and the provided context.
"""
                    
                    response = agent.query(enhanced_prompt, ctx_df)
                    
                    def stream_data(text):
                        import time
                        for word in text.split(" "):
                            yield word + " "
                            time.sleep(0.015)
                    st.write_stream(stream_data(response))
                    
                    code_blocks = re.findall(r'```python\n(.*?)\n```', response, re.DOTALL)
                    html_snippets = []
                    for code in code_blocks:
                        if "plotly" in code:
                            # Auto-write the generated code back to the SQL text area for visibility
                            st.session_state[KeyManager.get_key("pilot", "sql_input")] = code
                            
                        if "plotly" in code:
                            try:
                                local_vars = {"df": ctx_df.copy()}
                                safe_code = code.replace("fig.show()", "")
                                exec(safe_code, globals(), local_vars)
                                if "fig" in local_vars:
                                    st.plotly_chart(
                                        local_vars["fig"], 
                                        use_container_width=True, 
                                        config={'displayModeBar': True, 'toImageButtonOptions': {'format': 'png', 'filename': 'ai_chart_export'}}
                                    )
                                    html_snippets.append(local_vars["fig"].to_html(full_html=False, include_plotlyjs='cdn'))
                                    
                                    # Excel Export for the generated chart data
                                    if "df" in local_vars and isinstance(local_vars["df"], pd.DataFrame):
                                        excel_bytes = export_to_excel(local_vars["df"], "Chart Data")
                                        st.download_button(
                                            label="📥 Download Chart Data (Excel)",
                                            data=excel_bytes,
                                            file_name=f"ai_chart_data_{uuid.uuid4().hex[:6]}.xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            key=f"dl_chart_{uuid.uuid4().hex[:6]}"
                                        )
                            except Exception as e:
                                st.error(f"Could not render generated chart: {e}")
                                
                    st.session_state.pilot_chat_messages.append({
                        "role": "assistant", 
                        "content": response,
                        "html_snippets": html_snippets
                    })
                except Exception as e:
                    err_msg = f"Sorry, I encountered an error connecting to the LLM: {str(e)}"
                    st.error(err_msg)
                    st.session_state.pilot_chat_messages.append({"role": "assistant", "content": err_msg})