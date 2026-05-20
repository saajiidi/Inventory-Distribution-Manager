import pandas as pd
from datetime import datetime, timedelta
import requests
import json

class DataNLPInterpreter:
    """Interprets natural language queries into Pandas operations for DEEN-BI."""
    
    def __init__(self, sales_df: pd.DataFrame):
    def __init__(self, sales_df: pd.DataFrame, returns_df: pd.DataFrame | None = None):
        self.df = sales_df
        self.returns_df = returns_df if returns_df is not None else pd.DataFrame()
        self.today = datetime.now()

    def process_query(self, query: str) -> str:
        query = query.lower()
        
        # 1. Date Range Detection
        if 'order_date' in self.df.columns:
            date_mask = self.df['order_date'] >= (self.today - timedelta(days=365))
        else:
            date_mask = pd.Series(True, index=self.df.index)
        time_label = "over the last year"
        
        if "yesterday" in query:
            start = (self.today - timedelta(days=1)).replace(hour=0, minute=0, second=0)
            end = self.today.replace(hour=0, minute=0, second=0)
            date_mask = (self.df['order_date'] >= start) & (self.df['order_date'] < end)
            time_label = "yesterday"
        elif "today" in query:
            start = self.today.replace(hour=0, minute=0, second=0)
            date_mask = self.df['order_date'] >= start
            time_label = "today"
        elif "last week" in query:
            start = self.today - timedelta(days=7)
            date_mask = self.df['order_date'] >= start
            time_label = "last 7 days"
        elif "this month" in query:
            start = self.today.replace(day=1)
            date_mask = self.df['order_date'] >= start
            time_label = "this month"

        filtered_df = self.df[date_mask] if not self.df.empty else self.df
        
        # 2. Metric Detection
        if "revenue" in query or "sale" in query or "earn" in query:
        # 2. Metric Detection (Prioritize returns questions)
        if "return" in query:
            if not self.returns_df.empty:
                # Apply same time filter to returns if 'date' column exists
                filtered_returns_df = self.returns_df
                if 'date' in self.returns_df.columns:
                    # Assuming returns_df['date'] is datetime
                    if "yesterday" in query:
                        start = (self.today - timedelta(days=1)).replace(hour=0, minute=0, second=0)
                        end = self.today.replace(hour=0, minute=0, second=0)
                        ret_mask = (self.returns_df['date'] >= start) & (self.returns_df['date'] < end)
                        filtered_returns_df = self.returns_df[ret_mask]

                if "how many" in query:
                    return f"There are **{len(filtered_returns_df)}** issues logged as returns **{time_label}**."
                
                if "value" in query or "loss" in query:
                    total_loss = 0
                    if 'partial_amount' in filtered_returns_df.columns:
                        total_loss += filtered_returns_df[filtered_returns_df['issue_type'] == 'Partial']['partial_amount'].sum()
                    
                    if 'returned_items' in filtered_returns_df.columns:
                        def calculate_return_loss(items):
                            if isinstance(items, list):
                                return sum(item.get('revenue_impact', 0) for item in items if isinstance(item, dict))
                            return 0
                        
                        return_mask = filtered_returns_df['issue_type'].isin(['Paid Return', 'Non Paid Return'])
                        total_loss += filtered_returns_df[return_mask]['returned_items'].apply(calculate_return_loss).sum()

                    return f"The total financial loss from returns and partials **{time_label}** is **৳{total_loss:,.2f}**."
                
                if "top reason" in query:
                    if 'return_reason' in filtered_returns_df.columns and not filtered_returns_df.empty:
                        reason_counts = filtered_returns_df['return_reason'].value_counts()
                        if not reason_counts.empty:
                            top_reason = reason_counts.idxmax()
                            return f"The top reason for returns **{time_label}** is **'{top_reason}'**."
                    return "I can't determine the top return reason from the available data."

            else:
                return "I don't have any returns data loaded to answer that question."

        # --- Sales-specific logic ---
        elif "revenue" in query or "sale" in query or "earn" in query:
            val = filtered_df['item_revenue'].sum() if 'item_revenue' in filtered_df.columns else 0
            return f"💰 Your total revenue **{time_label}** is **৳{val:,.2f}**."
            
        if "order" in query or "count" in query:
            val = filtered_df['order_id'].nunique() if 'order_id' in filtered_df.columns else 0
            return f"🛒 You had **{val:,}** unique orders **{time_label}**."

        if "top category" in query or "best category" in query:
             if 'Category' in filtered_df.columns and not filtered_df.empty:
                 cat_sums = filtered_df.groupby('Category')['item_revenue'].sum()
                 if not cat_sums.empty:
                     top = cat_sums.idxmax()
                     val = cat_sums.max()
                     return f"🏆 Your top performing category **{time_label}** is **{top}** with ৳{val:,.2f} in revenue."

        if "best selling" in query or "top product" in query:
             if 'item_name' in filtered_df.columns and not filtered_df.empty:
                 counts = filtered_df['item_name'].value_counts()
                 if not counts.empty:
                     top = counts.idxmax()
                     count = counts.max()
                     return f"📦 Your best selling product **{time_label}** is **'{top}'** with {count} units sold."

        if "lowest" in query or "worst" in query:
             if 'Category' in filtered_df.columns and not filtered_df.empty:
                 cat_sums = filtered_df.groupby('Category')['item_revenue'].sum()
                 if not cat_sums.empty:
                     low = cat_sums.idxmin()
                     return f"⚠️ The lowest performing category **{time_label}** is **{low}**. You might want to review its stock or marketing."
                     
        if "why" in query or "root cause" in query:
            return ("🧠 **Root Cause Analysis Mode:** To understand why metrics changed, I look at Order Volume, Average Order Value (AOV), and Return Rates. If you notice a drop, it is typically driven by a decrease in traffic (fewer orders) or lower basket sizes (AOV). Try asking me for 'revenue yesterday' vs 'revenue last week' to compare!")

        # Fallback for complex queries
        return ("🔍 I've analyzed your data and found that your overall performance is stable. "
                "For deeper specific insights, try asking about 'revenue today', 'top category last week', or 'best selling products'.")

class LLMAgent:
    """Agent that communicates with local or remote LLMs (Ollama, LM Studio, etc.)."""
    
    def __init__(self, model_name: str = "gemma", base_url: str = "http://localhost:11434", agent_type: str = "Local AI Agent"):
        self.model_name = model_name
        self.base_url = base_url.rstrip('/')
        self.agent_type = agent_type

    def query(self, prompt: str, context_df: pd.DataFrame) -> str:
    def query(self, prompt: str, context_dfs: dict[str, pd.DataFrame]) -> str:
        """Query the local model."""
        sales_df = context_dfs.get("sales", pd.DataFrame())
        returns_df = context_dfs.get("returns", pd.DataFrame())
        
        stats = {
            "total_rows": len(context_df),
            "columns": list(context_df.columns),
            "total_revenue": context_df['item_revenue'].sum() if 'item_revenue' in context_df.columns else 0,
            "order_count": context_df['order_id'].nunique() if 'order_id' in context_df.columns else 0,
            "top_categories": context_df['Category'].value_counts().head(5).to_dict() if 'Category' in context_df.columns else {}
            "sales_summary": {
                "total_rows": len(sales_df),
                "columns": list(sales_df.columns),
                "total_revenue": sales_df['item_revenue'].sum() if 'item_revenue' in sales_df.columns else 0,
                "order_count": sales_df['order_id'].nunique() if 'order_id' in sales_df.columns else 0,
                "top_categories": sales_df['Category'].value_counts().head(5).to_dict() if 'Category' in sales_df.columns else {}
            }
        }
        
        if not returns_df.empty:
            stats["returns_summary"] = {
                "total_issues": len(returns_df),
                "columns": list(returns_df.columns),
                "issue_types": returns_df['issue_type'].value_counts().to_dict() if 'issue_type' in returns_df.columns else {},
                "top_reasons": returns_df['return_reason'].value_counts().head(3).to_dict() if 'return_reason' in returns_df.columns else {}
            }
        
        system_prompt = f"""
        You are DEEN-BI Data Pilot, an expert e-commerce analyst. 
        You have access to the following dataset summary:
        You have access to the following dataset summaries:
        {json.dumps(stats, indent=2)}
        
        Answer the user's question accurately based ONLY on this summary. 
        Be professional, concise, and use markdown.
        """
        
        if self.agent_type == "Google Gemini":
            try:
                import google.generativeai as genai
                import streamlit as st
                api_key = st.secrets.get("GEMINI_API_KEY")
                if not api_key:
                    return "❌ **Missing API Key:** Please add `GEMINI_API_KEY` to your `.streamlit/secrets.toml` file to use Google Gemini."
                
                genai.configure(api_key=api_key)
                model = genai.GenerativeModel('gemini-1.5-flash')
                response = model.generate_content(f"{system_prompt}\n\nUser Question: {prompt}")
                return response.text
            except Exception as e:
                return f"❌ **Gemini API Error:** {str(e)}"
        
        # Determine if it's Ollama or OpenAI-compatible (LM Studio)
        is_ollama = "11434" in self.base_url
        
        try:
            if is_ollama:
                url = f"{self.base_url}/api/generate"
                payload = {
                    "model": self.model_name,
                    "prompt": f"{system_prompt}\n\nUser Question: {prompt}",
                    "stream": False
                }
            else:
                # OpenAI Compatible (LM Studio / LocalAI)
                url = f"{self.base_url}/v1/chat/completions" if "/v1" not in self.base_url else f"{self.base_url}/chat/completions"
                payload = {
                    "model": self.model_name,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.2
                }

            response = requests.post(url, json=payload, timeout=30)
            
            if response.status_code == 200:
                res_json = response.json()
                if is_ollama:
                    return res_json.get("response", "No response.")
                else:
                    return res_json.get("choices", [{}])[0].get("message", {}).get("content", "No response.")
            else:
                return f"⚠️ Model API Error: Status {response.status_code}. Check URL: {url}"
        except Exception as e:
            return f"❌ Connection Failed: Ensure your LLM server (LM Studio/Ollama) is running at {self.base_url}"

def get_nlp_response(query: str, sales_df: pd.DataFrame, agent_type: str = "Standard", model_name: str = "gemma", base_url: str = "http://localhost:11434") -> str:
def get_nlp_response(query: str, sales_df: pd.DataFrame, returns_df: pd.DataFrame | None = None, agent_type: str = "Standard", model_name: str = "gemma", base_url: str = "http://localhost:11434") -> str:
    """Main entry point for NLP Pilot servicing."""
    context_dfs = {
        "sales": sales_df,
        "returns": returns_df if returns_df is not None else pd.DataFrame()
    }

    if agent_type in ["Local AI Agent", "Google Gemini"]:
        agent = LLMAgent(model_name=model_name, base_url=base_url, agent_type=agent_type)
        return agent.query(query, sales_df)
        return agent.query(query, context_dfs)
    elif agent_type == "RAG Agent (Deep Data)":
        from BackEnd.services.rag_engine import RAGAgent
        # Using Google Gemini as the default underlying LLM for the RAG demo here, or mapping it to the selected one
        rag_backend = "Google Gemini" if st.secrets.get("GEMINI_API_KEY") else "Local AI Agent"
        agent = RAGAgent(model_name=model_name, base_url=base_url, agent_type=rag_backend)
        return agent.query(query, sales_df)
        return agent.query(query, sales_df) # RAG agent would need updating to handle multiple DFs
    else:
        interpreter = DataNLPInterpreter(sales_df)
        interpreter = DataNLPInterpreter(sales_df, returns_df)
        return interpreter.process_query(query)
