import pandas as pd
from datetime import datetime, timedelta
import requests
import json

class DataNLPInterpreter:
    """Interprets natural language queries into Pandas operations for DEEN-BI."""
    
    def __init__(self, sales_df: pd.DataFrame, returns_df: pd.DataFrame | None = None, stock_df: pd.DataFrame | None = None):
        self.df = sales_df
        self.returns_df = returns_df if returns_df is not None else pd.DataFrame()
        self.stock_df = stock_df if stock_df is not None else pd.DataFrame()
        self.today = datetime.now()

    def process_query(self, query: str) -> str:
        query = query.lower()
        
        # Check for SKU queries
        mentioned_skus = []
        all_skus = set()
        if not self.stock_df.empty and 'SKU' in self.stock_df.columns:
            all_skus.update(self.stock_df['SKU'].dropna().astype(str).unique())
        if not self.df.empty and 'sku' in self.df.columns:
            all_skus.update(self.df['sku'].dropna().astype(str).unique())
            
        for s in all_skus:
            if len(s) > 2 and s.lower() in query:
                mentioned_skus.append(s)
                
        if mentioned_skus:
            response_parts = []
            for sku in mentioned_skus:
                part = f"### 📦 SKU: {sku}\n"
                if not self.stock_df.empty and 'SKU' in self.stock_df.columns:
                    stock_matches = self.stock_df[self.stock_df['SKU'].astype(str).str.lower().str.contains(sku.lower(), regex=False)]
                    if not stock_matches.empty:
                        exact = stock_matches[stock_matches['SKU'].astype(str).str.lower() == sku.lower()]
                        target = exact if not exact.empty else stock_matches
                        price = target['Regular Price'].max() if 'Regular Price' in target.columns else target.get('Price', 0).max()
                        sale_price = target['Sale Price'].max() if 'Sale Price' in target.columns else price
                        total_stock = int(target['Stock Quantity'].sum())
                        status = "✅ In Stock" if total_stock > 0 else "❌ Out of Stock"
                        part += f"- **Price**: ৳{price:,.0f} (Sale: ৳{sale_price:,.0f})\n"
                        part += f"- **Status**: {status} ({total_stock} total units)\n"
                        if 'Name' in target.columns:
                            sizes = [f"{str(r.get('Name', '')).split('-')[-1].strip()}: {int(r.get('Stock Quantity', 0))}" for _, r in target.iterrows()]
                            if sizes:
                                part += f"- **By Size**: {', '.join(sizes)}\n"
                if not self.df.empty and 'sku' in self.df.columns:
                    sales_matches = self.df[self.df['sku'].astype(str).str.lower().str.contains(sku.lower(), regex=False)]
                    lifetime_sold = int(sales_matches['qty'].sum()) if 'qty' in sales_matches.columns else len(sales_matches)
                    last_month_sold = "N/A"
                    if 'order_date' in sales_matches.columns:
                        sm = sales_matches.copy()
                        sm['order_date'] = pd.to_datetime(sm['order_date'], errors='coerce')
                        recent = sm[sm['order_date'] >= self.today - timedelta(days=30)]
                        last_month_sold = int(recent['qty'].sum()) if 'qty' in recent.columns else len(recent)
                    part += f"- **Sales (Last 30 Days)**: {last_month_sold} units sold\n"
                    if "lifetime" in query or "all time" in query or "whole time" in query:
                        part += f"- **Sales (Lifetime)**: {lifetime_sold} units sold\n"
                response_parts.append(part)
            return "\n".join(response_parts)
            
        # Explicit Comparison Detection
        if "compare" in query and "yesterday" in query and "today" in query:
            if 'order_date' in self.df.columns:
                start_yest = (self.today - timedelta(days=1)).replace(hour=0, minute=0, second=0)
                end_yest = self.today.replace(hour=0, minute=0, second=0)
                yest_mask = (pd.to_datetime(self.df['order_date'], errors='coerce') >= start_yest) & (pd.to_datetime(self.df['order_date'], errors='coerce') < end_yest)
                
                start_today = self.today.replace(hour=0, minute=0, second=0)
                today_mask = pd.to_datetime(self.df['order_date'], errors='coerce') >= start_today
                
                yest_rev = self.df[yest_mask]['item_revenue'].sum() if 'item_revenue' in self.df.columns else 0
                today_rev = self.df[today_mask]['item_revenue'].sum() if 'item_revenue' in self.df.columns else 0
                
                yest_ord = self.df[yest_mask]['order_id'].nunique() if 'order_id' in self.df.columns else 0
                today_ord = self.df[today_mask]['order_id'].nunique() if 'order_id' in self.df.columns else 0
                
                diff = today_rev - yest_rev
                trend = "up" if diff >= 0 else "down"
                
                return (f"📊 **Comparison (Yesterday vs Today):**\n"
                        f"- **Yesterday:** ৳{yest_rev:,.0f} ({yest_ord} orders)\n"
                        f"- **Today:** ৳{today_rev:,.0f} ({today_ord} orders)\n"
                        f"Today's revenue is **{trend}** by ৳{abs(diff):,.0f} compared to yesterday.")

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
                        
                if any(kw in query for kw in ["summary", "analyze", "table", "breakdown"]):
                    if 'issue_type' in filtered_returns_df.columns:
                        summary_df = filtered_returns_df['issue_type'].value_counts().reset_index()
                        md_table = "| Issue Type | Count |\n|---|---|\n"
                        for _, r in summary_df.iterrows(): md_table += f"| {r.iloc[0]} | {r.iloc[1]} |\n"
                        return f"Here is the returns data summary **{time_label}**:\n\n{md_table}"

                if any(kw in query for kw in ["how many", "count", "total"]):
                    return f"There are **{len(filtered_returns_df)}** issues/returns logged **{time_label}**."
                
                if any(kw in query for kw in ["value", "loss", "cost", "money", "amount"]):
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
                
                if any(kw in query for kw in ["compare", "same"]) or ("highest" in query and "best" in query):
                    top_ret = None
                    if not filtered_returns_df.empty and 'returned_items' in filtered_returns_df.columns:
                        items_list = [i.get('name') for items in filtered_returns_df['returned_items'] if isinstance(items, list) for i in items if isinstance(i, dict) and 'name' in i]
                        if items_list:
                            top_ret = pd.Series(items_list).value_counts().idxmax()
                    
                    top_sell = None
                    if 'item_name' in filtered_df.columns and not filtered_df.empty:
                        top_sell = filtered_df['item_name'].value_counts().idxmax()
                        
                    if top_ret and top_sell:
                        if top_ret == top_sell:
                            return f"⚠️ **Yes!** Your highest returned item (**{top_ret}**) is ALSO your best-selling item **{time_label}**. This indicates a high-volume quality control issue."
                        else:
                            return f"✅ **No.** Your best-selling item is **{top_sell}**, but your most returned item is **{top_ret}** **{time_label}**."
                    return "I don't have enough data to compare top sellers vs top returns."

                if any(kw in query for kw in ["reason", "why"]):
                    if 'return_reason' in filtered_returns_df.columns and not filtered_returns_df.empty:
                        reason_counts = filtered_returns_df['return_reason'].value_counts()
                        if not reason_counts.empty:
                            total = reason_counts.sum()
                            res = f"Here is the breakdown of return reasons **{time_label}**:\n\n"
                            res += "| Reason | Percentage | Visual |\n|---|---|---|\n"
                            for reason, count in reason_counts.head(5).items():
                                pct = (count / total) * 100
                                blocks = "█" * int(pct / 5)
                                res += f"| {reason} | {pct:.1f}% | `{blocks}` |\n"
                            return res
                    return "I can't determine the return reasons from the available data."

                if any(kw in query for kw in ["top", "highest", "most", "which", "what"]) and any(kw in query for kw in ["product", "item", "return"]):
                    if not filtered_returns_df.empty:
                        top_prod = None
                        if 'returned_items' in filtered_returns_df.columns:
                            items_list = [i.get('name') for items in filtered_returns_df['returned_items'] if isinstance(items, list) for i in items if isinstance(i, dict) and 'name' in i]
                            if items_list:
                                top_prod = pd.Series(items_list).value_counts().idxmax()
                                top_val = pd.Series(items_list).value_counts().max()
                        if not top_prod and 'product_details' in filtered_returns_df.columns:
                            top_prod = filtered_returns_df['product_details'].value_counts().idxmax()
                            top_val = filtered_returns_df['product_details'].value_counts().max()
                        if top_prod:
                            return f"The product with the highest returns **{time_label}** is **'{top_prod}'** with {top_val} occurrences."
                    return "I don't have enough product detail data to determine the most returned product."
                
                return f"You had **{len(filtered_returns_df)}** returns/issues **{time_label}**. Ask 'what are the top return reasons?' or 'which product is returned the most?' for more details."

            else:
                return "I don't have any returns data loaded to answer that question."

        # --- Sales-specific logic ---
        if any(kw in query for kw in ["revenue", "sale", "earn", "income", "money"]):
            val = filtered_df['item_revenue'].sum() if 'item_revenue' in filtered_df.columns else 0
            return f"💰 Your total revenue **{time_label}** is **৳{val:,.2f}**."
            
        if any(kw in query for kw in ["order", "count", "how many"]) and not any(kw in query for kw in ["customer", "buyer", "people", "user"]):
            val = filtered_df['order_id'].nunique() if 'order_id' in filtered_df.columns else 0
            return f"🛒 You had **{val:,}** unique orders **{time_label}**."

        if any(kw in query for kw in ["customer", "buyer", "people", "user"]):
            val = filtered_df['customer_key'].nunique() if 'customer_key' in filtered_df.columns else (filtered_df['phone'].nunique() if 'phone' in filtered_df.columns else 0)
            return f"👥 You had **{val:,}** unique customers **{time_label}**."

        if any(kw in query for kw in ["average order value", "aov", "average order", "basket size"]):
            val = filtered_df['item_revenue'].sum() if 'item_revenue' in filtered_df.columns else 0
            orders = filtered_df['order_id'].nunique() if 'order_id' in filtered_df.columns else 0
            aov = val / orders if orders > 0 else 0
            return f"💳 Your Average Order Value (AOV) **{time_label}** is **৳{aov:,.0f}**."

        if any(kw in query for kw in ["trend", "chart", "graph", "plot"]):
            if 'order_date' in filtered_df.columns and not filtered_df.empty:
                temp = filtered_df.copy()
                temp['date'] = pd.to_datetime(temp['order_date'], errors='coerce').dt.date
                daily = temp.groupby('date').agg(
                    revenue=('item_revenue', 'sum') if 'item_revenue' in temp.columns else ('qty', 'sum'),
                    orders=('order_id', 'nunique') if 'order_id' in temp.columns else ('qty', 'count')
                ).tail(10)
                
                if not daily.empty:
                    max_rev = daily['revenue'].max()
                    res = f"📈 **Trend Chart ({time_label.title()}):**\n\n"
                    res += "| Date | Revenue | Orders | Visual |\n|---|---|---|---|\n"
                    for date_idx, row in daily.iterrows():
                        rev = row['revenue']
                        ord_cnt = int(row['orders'])
                        blocks = "█" * int((rev / max_rev) * 10) if max_rev > 0 else ""
                        if not blocks: blocks = "▏"
                        res += f"| {date_idx} | ৳{rev:,.0f} | {ord_cnt} | `{blocks}` |\n"
                    return res
            return "I don't have enough time-series data to draw a trend chart."

        if "category" in query and any(kw in query for kw in ["top", "best", "highest", "most"]):
             if 'Category' in filtered_df.columns and not filtered_df.empty:
                 cat_sums = filtered_df.groupby('Category')['item_revenue'].sum()
                 if not cat_sums.empty:
                     top = cat_sums.idxmax()
                     val = cat_sums.max()
                     return f"🏆 Your top performing category **{time_label}** is **{top}** with ৳{val:,.2f} in revenue."

        if any(kw in query for kw in ["top", "best", "highest", "most"]) and any(kw in query for kw in ["product", "item", "sell"]):
             if 'item_name' in filtered_df.columns and not filtered_df.empty:
                 counts = filtered_df['item_name'].value_counts()
                 if not counts.empty:
                     top = counts.idxmax()
                     count = counts.max()
                     return f"📦 Your best selling product **{time_label}** is **'{top}'** with {count} units sold."

        if any(kw in query for kw in ["lowest", "worst", "bad"]):
             if 'Category' in filtered_df.columns and not filtered_df.empty:
                 cat_sums = filtered_df.groupby('Category')['item_revenue'].sum()
                 if not cat_sums.empty:
                     low = cat_sums.idxmin()
                     return f"⚠️ The lowest performing category **{time_label}** is **{low}**. You might want to review its stock or marketing."
                     
        if "why" in query or "root cause" in query:
            return ("🧠 **Root Cause Analysis Mode:** To understand why metrics changed, I look at Order Volume, Average Order Value (AOV), and Return Rates. If you notice a drop, it is typically driven by a decrease in traffic (fewer orders) or lower basket sizes (AOV). Try asking me for 'revenue yesterday' vs 'revenue last week' to compare!")

        # Fallback for complex queries
        val = filtered_df['item_revenue'].sum() if 'item_revenue' in filtered_df.columns else 0
        orders = filtered_df['order_id'].nunique() if 'order_id' in filtered_df.columns else 0
        items_sold = int(filtered_df['qty'].sum()) if 'qty' in filtered_df.columns else 0
        aov = val / orders if orders > 0 else 0
        
        summary_parts = [
            f"📊 **Data Summary ({time_label.title()}):**",
            f"- **Orders:** {orders:,}",
            f"- **Revenue:** ৳{val:,.0f}",
            f"- **Items Sold:** {items_sold:,}",
            f"- **Average Order Value:** ৳{aov:,.0f}"
        ]
        
        if 'Category' in filtered_df.columns and not filtered_df.empty:
            cat_sums = filtered_df.groupby('Category')['item_revenue'].sum()
            if not cat_sums.empty:
                top_cat = cat_sums.idxmax()
                summary_parts.append(f"- **Top Category:** {top_cat}")
                
        if 'item_name' in filtered_df.columns and not filtered_df.empty:
            counts = filtered_df['item_name'].value_counts()
            if not counts.empty:
                top_item = counts.idxmax()
                summary_parts.append(f"- **Top Product:** {top_item}")
                
        if not self.returns_df.empty:
            summary_parts.append(f"- **Recorded Returns/Issues:** {len(self.returns_df):,}")

        summary_parts.append("\n*I am running in 'Standard' (rule-based) mode. For conversational answers, switch the brain to **Google Gemini / Groq / Local AI**, or try asking me specifically about 'top products', 'return reasons', or 'sales revenue'.*")
        
        return "\n".join(summary_parts)

class LLMAgent:
    """Agent that communicates with local or remote LLMs (Ollama, LM Studio, etc.)."""
    
    def __init__(self, model_name: str = "gemma", base_url: str = "http://localhost:11434", agent_type: str = "Local AI Agent"):
        self.model_name = model_name
        self.base_url = base_url.rstrip('/')
        self.agent_type = agent_type

    def query(self, prompt: str, context_dfs: dict[str, pd.DataFrame]) -> str:
        """Query the local model."""
        sales_df = context_dfs.get("sales", pd.DataFrame())
        returns_df = context_dfs.get("returns", pd.DataFrame())
        stock_df = context_dfs.get("stock", pd.DataFrame())
        
        daily_sales = {}
        if not sales_df.empty and 'order_date' in sales_df.columns:
            try:
                temp_sales = sales_df.copy()
                temp_sales['order_date'] = pd.to_datetime(temp_sales['order_date'], errors='coerce')
                recent_mask = temp_sales['order_date'] >= (datetime.now() - timedelta(days=7))
                recent_sales = temp_sales[recent_mask]
                if not recent_sales.empty:
                    daily_grouped = recent_sales.groupby(recent_sales['order_date'].apply(lambda x: x.date() if pd.notna(x) else None)).agg(
                        revenue=('item_revenue', 'sum'),
                        orders=('order_id', 'nunique')
                    ).to_dict('index')
                    daily_sales = {str(k): {"revenue": round(v['revenue'], 2), "orders": v['orders']} for k, v in daily_grouped.items()}
            except Exception:
                pass

        stats = {
            "recent_daily_performance": daily_sales,
            "sales_summary": {
                "total_rows": len(sales_df),
                "columns": list(sales_df.columns),
                "total_revenue": sales_df['item_revenue'].sum() if 'item_revenue' in sales_df.columns else 0,
                "order_count": sales_df['order_id'].nunique() if 'order_id' in sales_df.columns else 0,
                "top_categories": sales_df['Category'].value_counts().head(5).to_dict() if 'Category' in sales_df.columns else {},
                "top_selling_items": sales_df['item_name'].value_counts().head(5).to_dict() if 'item_name' in sales_df.columns else {}
            }
        }
        
        if not returns_df.empty:
            stats["returns_summary"] = {
                "total_issues": len(returns_df),
                "columns": list(returns_df.columns),
                "issue_types": returns_df['issue_type'].value_counts().to_dict() if 'issue_type' in returns_df.columns else {},
                "top_reasons": returns_df['return_reason'].value_counts().head(3).to_dict() if 'return_reason' in returns_df.columns else {}
            }
        
        # Handle "Learn:" or "Remember:" commands
        from pathlib import Path
        knowledge_file = Path("BackEnd/data/pilot_knowledge.txt")
        if prompt.strip().lower().startswith("learn:") or prompt.strip().lower().startswith("remember:"):
            new_knowledge = prompt.split(":", 1)[1].strip()
            knowledge_file.parent.mkdir(parents=True, exist_ok=True)
            with open(knowledge_file, "a", encoding="utf-8") as f:
                f.write(f"- {new_knowledge}\n")
            
            # Clear cache when knowledge base is updated
            if "llm_response_cache" in st.session_state:
                st.session_state.llm_response_cache.clear()
                
            return f"✅ Got it! I have updated my knowledge base with: '{new_knowledge}'. I'll remember this for future queries."

        custom_instructions = ""
        if knowledge_file.exists():
            try:
                with open(knowledge_file, "r", encoding="utf-8") as f:
                    custom_instructions = f.read().strip()
            except Exception:
                pass

        # LLM Response Cache Check
        import hashlib
        import streamlit as st
        if "llm_response_cache" not in st.session_state:
            st.session_state.llm_response_cache = {}
            
        state_hash = hashlib.md5(json.dumps(stats, sort_keys=True).encode('utf-8')).hexdigest()
        prompt_hash = hashlib.md5(prompt.encode('utf-8')).hexdigest()
        # Include custom_instructions in cache key so changes invalidate cache
        cache_key = f"nlp_{self.agent_type}_{self.model_name}_{prompt_hash}_{state_hash}_{hashlib.md5(custom_instructions.encode('utf-8')).hexdigest()}"
        
        if cache_key in st.session_state.llm_response_cache:
            return st.session_state.llm_response_cache[cache_key]

        system_prompt = f"""
        You are DEEN-BI Data Pilot, an autonomous expert e-commerce AI agent. 
        You have access to the following real-time dataset summaries and metrics:
        {json.dumps(stats, indent=2)}
        
        CRITICAL RULES:
        1. Order Logic: An `order_id` represents a single unique order. An order may contain multiple item lines. You must NEVER count item rows as a single order. When asked for 'total orders' or 'number of orders', you must use `order_count` (unique order IDs), NOT `total_rows`.
        2. Continuous Learning Protocol: Treat all user corrections as updates to your permanent knowledge base for this specific dataset. Do not repeat the corrected mistake in subsequent queries.
        3. Auto-Memorization: If the user corrects a mistake or provides a new persistent rule, you MUST output the exact string `[KNOWLEDGE_UPDATE: <the new rule>]` on a new line.
        
        USER KNOWLEDGE BASE / CUSTOM INSTRUCTIONS:
        {custom_instructions}
        
        Answer the user's question accurately based ONLY on this summary and the rules above. Act proactively to detect anomalies or trends.
        Be professional, concise, and use markdown.
        """
        
        def try_gemini(sys_prompt=system_prompt, user_query=prompt):
            import google.generativeai as genai
            import streamlit as st
            import os
            api_key = st.secrets.get("GEMINI_API_KEY") or os.environ.get("GEMINI_API_KEY")
            if not api_key:
                return "MISSING_KEY"
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-1.5-flash')
            response = model.generate_content(f"{sys_prompt}\n\nUser Question: {user_query}")
            return response.text

        def try_groq(sys_prompt=system_prompt, user_query=prompt):
            from groq import Groq
            import streamlit as st
            import os
            api_key = st.secrets.get("GROQ_API_KEY") or os.environ.get("GROQ_API_KEY")
            if not api_key:
                return "MISSING_KEY"
            client = Groq(api_key=api_key)
            completion = client.chat.completions.create(
                model="llama3-70b-8192",
                messages=[
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": user_query}
                ],
                temperature=0.2,
            )
            return completion.choices[0].message.content
            
        def try_local(sys_prompt=system_prompt, user_query=prompt):
            is_ollama = "11434" in self.base_url
            url = f"{self.base_url}/api/generate" if is_ollama else (f"{self.base_url}/v1/chat/completions" if "/v1" not in self.base_url else f"{self.base_url}/chat/completions")
            payload = {
                    "model": self.model_name,
                    "prompt": f"{sys_prompt}\n\nUser Question: {user_query}",
                    "stream": False
            } if is_ollama else {
                    "model": self.model_name,
                    "messages": [
                        {"role": "system", "content": sys_prompt},
                        {"role": "user", "content": user_query}
                    ],
                    "temperature": 0.2
            }
            try:
                response = requests.post(url, json=payload, timeout=30)
                if response.status_code == 200:
                    res_json = response.json()
                    if is_ollama:
                        return res_json.get("response", "No response.")
                    else:
                        return res_json.get("choices", [{}])[0].get("message", {}).get("content", "No response.")
                else:
                    return "LOCAL_ERROR"
            except Exception:
                return "LOCAL_ERROR"

        def try_openrouter(sys_prompt=system_prompt, user_query=prompt):
            import streamlit as st
            import os
            import requests
            api_key = st.secrets.get("OPENROUTER_API_KEY") or os.environ.get("OPENROUTER_API_KEY")
            if not api_key:
                return "MISSING_KEY"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "HTTP-Referer": "http://localhost:8501",
                "X-Title": "DEEN-BI"
            }
            payload = {
                "model": "meta-llama/llama-3-8b-instruct:free",
                "messages": [
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": user_query}
                ]
            }
            resp = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload, timeout=30)
            if resp.status_code == 200:
                return resp.json().get("choices", [{}])[0].get("message", {}).get("content", "No response.")
            return "LOCAL_ERROR"

        def try_huggingface(sys_prompt=system_prompt, user_query=prompt):
            import streamlit as st
            import os
            import requests
            api_key = st.secrets.get("HUGGINGFACE_API_KEY") or os.environ.get("HUGGINGFACE_API_KEY")
            if not api_key:
                return "MISSING_KEY"
            headers = {"Authorization": f"Bearer {api_key}"}
            payload = {
                "inputs": f"<|system|>\n{sys_prompt}</s>\n<|user|>\n{user_query}</s>\n<|assistant|>",
                "parameters": {"max_new_tokens": 512, "temperature": 0.2, "return_full_text": False}
            }
            resp = requests.post("https://api-inference.huggingface.co/models/HuggingFaceH4/zephyr-7b-beta", headers=headers, json=payload, timeout=30)
            if resp.status_code == 200:
                res = resp.json()
                if isinstance(res, list) and len(res) > 0:
                    return res[0].get("generated_text", "No response.")
            return "LOCAL_ERROR"

        fallback_order = []
        all_funcs = [("Groq", try_groq), ("Google Gemini", try_gemini), ("OpenRouter", try_openrouter), ("HuggingFace", try_huggingface), ("Local AI", try_local)]
        
        if self.agent_type == "Groq":
            fallback_order = [("Groq", try_groq)] + [f for f in all_funcs if f[0] != "Groq"]
        elif self.agent_type == "Google Gemini":
            fallback_order = [("Google Gemini", try_gemini)] + [f for f in all_funcs if f[0] != "Google Gemini"]
        elif self.agent_type == "OpenRouter":
            fallback_order = [("OpenRouter", try_openrouter)] + [f for f in all_funcs if f[0] != "OpenRouter"]
        elif self.agent_type == "HuggingFace":
            fallback_order = [("HuggingFace", try_huggingface)] + [f for f in all_funcs if f[0] != "HuggingFace"]
        else:
            fallback_order = [("Local AI", try_local)] + [f for f in all_funcs if f[0] != "Local AI"]
            
        last_error = "❌ **AI Generation Failed:** No valid models available."
        
        eval_system_prompt = "You are an objective AI judge. Evaluate the response against the rules. Reply ONLY with 'YES' if it violates the rules, or 'NO' if it complies."
        
        for name, func in fallback_order:
            try:
                for attempt in range(2):
                    res = func(system_prompt, prompt)
                    if res in ["MISSING_KEY", "LOCAL_ERROR"]:
                        break # Move to next provider
                        
                    # LLM-as-a-Judge Validation
                    eval_user_prompt = f"RULES:\n1. 'Total Orders' ALWAYS refers to a distinct count of unique `order_id` values.\n2. Do NOT use row counts when asked for order counts.\n\nRESPONSE TO EVALUATE:\n{res}\n\nDoes the response violate these rules? (YES/NO)"
                    eval_res = func(eval_system_prompt, eval_user_prompt)
                    
                    if "YES" in str(eval_res).upper() and attempt == 0:
                        import logging
                        logging.warning(f"[{name}] NLP Validation failed on attempt {attempt+1}. Regenerating...")
                        continue
                        
                    import re
                    from pathlib import Path
                    updates = re.findall(r'\[KNOWLEDGE_UPDATE:\s*(.*?)\]', res)
                    if updates:
                        knowledge_file = Path("BackEnd/data/pilot_knowledge.txt")
                        knowledge_file.parent.mkdir(parents=True, exist_ok=True)
                        with open(knowledge_file, "a", encoding="utf-8") as f:
                            for update in updates:
                                f.write(f"- {update.strip()}\n")
                        res = re.sub(r'\[KNOWLEDGE_UPDATE:\s*.*?\]', '', res).strip()
                        import streamlit as st
                        st.toast("🤖 Auto-learned a new rule from your correction.", icon="🧠")

                    st.session_state.llm_response_cache[cache_key] = res
                    return res
            except Exception as e:
                last_error = f"❌ **{name} Error:** {str(e)}"
                continue
                
        return last_error

def get_nlp_response(query: str, sales_df: pd.DataFrame, returns_df: pd.DataFrame | None = None, stock_df: pd.DataFrame | None = None, agent_type: str = "Standard", model_name: str = "gemma", base_url: str = "http://localhost:11434") -> str:
    """Main entry point for NLP Pilot servicing."""
    
    # Auto-enrich knowledge base if missing from UI state
    if returns_df is None or returns_df.empty:
        try:
            from BackEnd.services.returns_tracker import load_returns_data
            returns_df = load_returns_data(sales_df=sales_df)
        except Exception:
            returns_df = pd.DataFrame()
            
    if stock_df is None or stock_df.empty:
        try:
            from BackEnd.services.hybrid_data_loader import load_cached_woocommerce_stock_data
            stock_df = load_cached_woocommerce_stock_data()
        except Exception:
            stock_df = pd.DataFrame()
            
    context_dfs = {
        "sales": sales_df,
        "returns": returns_df,
        "stock": stock_df
    }

    if agent_type in ["Local AI Agent", "Google Gemini", "Groq", "OpenRouter", "HuggingFace"]:
        agent = LLMAgent(model_name=model_name, base_url=base_url, agent_type=agent_type)
        return agent.query(query, context_dfs)
    elif agent_type == "RAG Agent (Deep Data)":
        from BackEnd.services.rag_engine import RAGAgent
        # Using Google Gemini as the default underlying LLM for the RAG demo here, or mapping it to the selected one
        rag_backend = "Google Gemini" if st.secrets.get("GEMINI_API_KEY") else "Local AI Agent"
        agent = RAGAgent(model_name=model_name, base_url=base_url, agent_type=rag_backend)
        return agent.query(query, context_dfs)
    else:
        interpreter = DataNLPInterpreter(sales_df, returns_df, stock_df)
        return interpreter.process_query(query)
