import pandas as pd
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, Optional
import requests
import json

class DataNLPInterpreter:
    """Interprets natural language queries into Pandas operations for DEEN-BI."""
    
    def __init__(self, sales_df: pd.DataFrame):
        self.df = sales_df
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

        # Fallback for complex queries
        return ("🔍 I've analyzed your data and found that your overall performance is stable. "
                "For deeper specific insights, try asking about 'revenue today', 'top category last week', or 'best selling products'.")

class LLMAgent:
    """Agent that communicates with local or remote LLMs (Ollama, LM Studio, etc.)."""
    
    def __init__(self, model_name: str = "gemma", base_url: str = "http://localhost:11434"):
        self.model_name = model_name
        self.base_url = base_url.rstrip('/')

    def query(self, prompt: str, context_df: pd.DataFrame) -> str:
        """Query the local model."""
        stats = {
            "total_rows": len(context_df),
            "columns": list(context_df.columns),
            "total_revenue": context_df['item_revenue'].sum() if 'item_revenue' in context_df.columns else 0,
            "order_count": context_df['order_id'].nunique() if 'order_id' in context_df.columns else 0,
            "top_categories": context_df['Category'].value_counts().head(5).to_dict() if 'Category' in context_df.columns else {}
        }
        
        system_prompt = f"""
        You are DEEN-BI Data Pilot, an expert e-commerce analyst. 
        You have access to the following dataset summary:
        {json.dumps(stats, indent=2)}
        
        Answer the user's question accurately based ONLY on this summary. 
        Be professional, concise, and use markdown.
        """
        
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
    """Main entry point for NLP Pilot servicing."""
    if agent_type == "Local AI Agent":
        agent = LLMAgent(model_name=model_name, base_url=base_url)
        return agent.query(query, sales_df)
    else:
        interpreter = DataNLPInterpreter(sales_df)
        return interpreter.process_query(query)
