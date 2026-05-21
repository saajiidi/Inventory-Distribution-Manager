import pandas as pd
import numpy as np
import requests
import json
import hashlib
import streamlit as st
from typing import List, Dict, Any
from pathlib import Path
from BackEnd.core.logging_config import get_logger

logger = get_logger("rag_engine")

class SimpleVectorStore:
    """In-memory numpy-based vector store for lightweight RAG."""
    def __init__(self):
        self.cache_dir = Path("BackEnd/cache/vector_store")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.vec_file = self.cache_dir / "vectors.npy"
        self.norm_file = self.cache_dir / "vectors_norm.npy"
        self.doc_file = self.cache_dir / "documents.json"
        
        self.documents: List[Dict[str, Any]] = []
        self.vectors: np.ndarray = np.array([])
        self.vectors_norm: np.ndarray = np.array([])
        self.load()

    def load(self):
        if self.vec_file.exists() and self.norm_file.exists() and self.doc_file.exists():
            try:
                self.vectors = np.load(self.vec_file)
                self.vectors_norm = np.load(self.norm_file)
                with open(self.doc_file, 'r', encoding='utf-8') as f:
                    self.documents = json.load(f)
            except Exception as e:
                logger.error(f"Failed to load vector store: {e}")
                
    def save(self):
        np.save(self.vec_file, self.vectors)
        np.save(self.norm_file, self.vectors_norm)
        with open(self.doc_file, 'w', encoding='utf-8') as f:
            json.dump(self.documents, f, ensure_ascii=False)

    def add_documents(self, documents: List[Dict[str, Any]], embeddings: np.ndarray):
        if not documents:
            return
        self.documents.extend(documents)
        if self.vectors.size == 0:
            self.vectors = embeddings
        else:
            self.vectors = np.vstack([self.vectors, embeddings])
            
        # Pre-compute normalized vectors for O(1) distance scaling during search
        norms = np.linalg.norm(self.vectors, axis=1, keepdims=True)
        norms[norms == 0] = 1  # Avoid division by zero
        self.vectors_norm = self.vectors / norms
        self.save()

    def search(self, query_embedding: np.ndarray, top_k: int = 5) -> List[Dict[str, Any]]:
        if self.vectors.size == 0:
            return []
        
        # Cosine similarity: dot product of normalized vectors
        q_norm = np.linalg.norm(query_embedding)
        query_norm = query_embedding / (q_norm if q_norm != 0 else 1)
        similarities = np.dot(self.vectors_norm, query_norm)
        
        # Fast top-K extraction using argpartition instead of full argsort
        if len(similarities) > top_k:
            top_indices = np.argpartition(similarities, -top_k)[-top_k:]
            top_indices = top_indices[np.argsort(similarities[top_indices])][::-1]
        else:
            top_indices = np.argsort(similarities)[::-1]
        
        results = []
        for idx in top_indices:
            doc = self.documents[idx].copy()
            doc["score"] = similarities[idx]
            results.append(doc)
            
        return results

class RAGAgent:
    """Retrieval-Augmented Generation Agent for Data Pilot."""
    
    def __init__(self, model_name: str = "gemma", base_url: str = "http://localhost:11434", agent_type: str = "Local AI Agent"):
        self.model_name = model_name
        self.base_url = base_url.rstrip('/')
        self.agent_type = agent_type
        self.vector_store = SimpleVectorStore()
        import os
        self._api_key = (st.secrets.get("GEMINI_API_KEY") or os.environ.get("GEMINI_API_KEY")) if agent_type == "Google Gemini" else None

    def _load_embedding_cache(self):
        if "embedding_cache" not in st.session_state:
            st.session_state.embedding_cache = {}
            cache_dir = Path("BackEnd/cache")
            cache_file = cache_dir / "embedding_cache.parquet"
            if cache_file.exists():
                try:
                    df = pd.read_parquet(cache_file)
                    cache = dict(zip(df['cache_key'], df['embedding']))
                    st.session_state.embedding_cache = cache
                except Exception as e:
                    logger.error(f"Failed to load embedding cache: {e}")

    def _save_embedding_cache(self):
        if "embedding_cache" in st.session_state and st.session_state.embedding_cache:
            cache_dir = Path("BackEnd/cache")
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file = cache_dir / "embedding_cache.parquet"
            try:
                embeddings_list = [
                    emb.tolist() if isinstance(emb, np.ndarray) else emb
                    for emb in st.session_state.embedding_cache.values()
                ]
                df = pd.DataFrame({
                    'cache_key': list(st.session_state.embedding_cache.keys()),
                    'embedding': embeddings_list
                })
                df.to_parquet(cache_file, index=False)
            except Exception as e:
                logger.error(f"Failed to save embedding cache: {e}")

    def _get_embeddings(self, texts: List[str]) -> np.ndarray:
        """Generate embeddings using the configured provider with local session caching."""
        if not texts:
            return np.array([])
            
        self._load_embedding_cache()
            
        embeddings = []
        texts_to_fetch = []
        indices_to_fetch = []
        
        for i, text in enumerate(texts):
            text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
            cache_key = f"{self.agent_type}_{self.model_name}_{text_hash}"
            if cache_key in st.session_state.embedding_cache:
                embeddings.append(st.session_state.embedding_cache[cache_key])
            else:
                embeddings.append(None)
                texts_to_fetch.append(text)
                indices_to_fetch.append(i)
                
        if texts_to_fetch:
            if self.agent_type == "Google Gemini":
                try:
                    import google.generativeai as genai
                    genai.configure(api_key=self._api_key)
                    # Using standard Gemini embedding model
                    result = genai.embed_content(
                        model="models/text-embedding-004",
                        content=texts_to_fetch,
                        task_type="retrieval_document"
                    )
                    fetched_embs = result['embedding']
                except Exception as e:
                    logger.error(f"Gemini Embedding Error: {e}")
                    fetched_embs = [np.zeros(768).tolist() for _ in texts_to_fetch]
            else:
                # Local Ollama Embeddings (e.g., nomic-embed-text)
                fetched_embs = []
                url = f"{self.base_url}/api/embeddings"
                for text in texts_to_fetch:
                    try:
                        payload = {"model": "nomic-embed-text", "prompt": text}
                        res = requests.post(url, json=payload, timeout=10)
                        if res.status_code == 200:
                            fetched_embs.append(res.json().get("embedding", []))
                        else:
                            fetched_embs.append(np.zeros(768).tolist())
                    except Exception as e:
                        logger.error(f"Ollama Embedding Error: {e}")
                        fetched_embs.append(np.zeros(768).tolist())
                        
            for i, text, emb in zip(indices_to_fetch, texts_to_fetch, fetched_embs):
                text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
                cache_key = f"{self.agent_type}_{self.model_name}_{text_hash}"
                st.session_state.embedding_cache[cache_key] = emb
                embeddings[i] = emb
                
        if texts_to_fetch:
            self._save_embedding_cache()
                
        return np.array(embeddings)

    def _ingest_dataframe(self, df: pd.DataFrame, max_rows: int = 500):
        """Convert DataFrame rows into searchable text documents."""
        if df.empty:
            return
            
        # Take recent rows to avoid massive API overhead for a quick response
        sample_df = df.tail(max_rows).copy()
        
        docs = []
        texts = []
        existing_contents = {doc["content"] for doc in self.vector_store.documents}
        
        for _, row in sample_df.iterrows():
            # Format the row into a readable chunk
            row_dict = row.dropna().to_dict()
            text_chunk = ", ".join([f"{k}: {v}" for k, v in row_dict.items()])
            if text_chunk not in existing_contents:
                texts.append(text_chunk)
                docs.append({"content": text_chunk, "metadata": {"index": int(_) if str(_).isdigit() else str(_)}})
            
        if texts:
            embeddings = self._get_embeddings(texts)
            if embeddings.size > 0:
                self.vector_store.add_documents(docs, embeddings)

    def query(self, prompt: str, context_dfs: dict[str, pd.DataFrame]) -> str:
        """Full RAG Pipeline: Ingest -> Embed Query -> Retrieve -> Generate."""
        # 0. Vector store is now persistent cross-session, deduplication handled in ingest
        
        # Extract individual DFs
        sales_df = context_dfs.get("sales", pd.DataFrame())
        returns_df = context_dfs.get("returns", pd.DataFrame())
        stock_df = context_dfs.get("stock", pd.DataFrame())
        
        # 1. Determine active page context to prioritize
        active_section = st.session_state.get("active_section", "💎 Sales Overview")
        dashboard_data = st.session_state.get("dashboard_data", {})
        
        active_df = None
        if active_section == "📦 Stock Insight":
            active_df = stock_df
        elif active_section == "👥 Customer Insight":
            active_df = dashboard_data.get("customers", pd.DataFrame())
        elif active_section == "🔄 Returns Insights":
            active_df = returns_df
        elif active_section == "💎 Sales Overview" or active_section == "🛡️ Strategic Command":
            active_df = sales_df
            
        ingested_datasets = set()
        
        # 2. Ingest Active Page Data (High Priority)
        if active_df is not None and not active_df.empty:
            active_context = active_df.copy()
            active_context["_Data_Context"] = f"Active Page: {active_section}"
            self._ingest_dataframe(active_context, max_rows=300)
            
            # Keep track of what we ingested
            if active_df.equals(sales_df): ingested_datasets.add("sales")
            if active_df.equals(returns_df): ingested_datasets.add("returns")
            if active_df.equals(stock_df): ingested_datasets.add("stock")

        # 3. Ingest Relevant Cross-Domain Data based on Prompt Context
        if "return" in prompt.lower() and "returns" not in ingested_datasets and not returns_df.empty:
            ret_context = returns_df.copy()
            ret_context["_Data_Context"] = "Global Site Data (Returns)"
            self._ingest_dataframe(ret_context, max_rows=200)
            ingested_datasets.add("returns")
            
        if ("stock" in prompt.lower() or "inventory" in prompt.lower()) and "stock" not in ingested_datasets and not stock_df.empty:
            stk_context = stock_df.copy()
            stk_context["_Data_Context"] = "Global Site Data (Stock)"
            self._ingest_dataframe(stk_context, max_rows=200)
            ingested_datasets.add("stock")

        # Ingest Sales as general fallback if not already ingested
        if "sales" not in ingested_datasets and not sales_df.empty:
            site_context = sales_df.copy()
            site_context["_Data_Context"] = "Global Site Data (Sales/Orders)"
            self._ingest_dataframe(site_context, max_rows=200)
            ingested_datasets.add("sales")
            
        # Pre-compute an ASCII visual trend chart to supply to the LLM context
        recent_trend_str = "Not available"
        if not sales_df.empty and 'order_date' in sales_df.columns:
            temp = sales_df.copy()
            temp['date'] = pd.to_datetime(temp['order_date'], errors='coerce').dt.date
            daily = temp.groupby('date').agg(
                revenue=('item_revenue', 'sum') if 'item_revenue' in temp.columns else ('qty', 'sum'),
                orders=('order_id', 'nunique') if 'order_id' in temp.columns else ('qty', 'count')
            ).tail(10)
            if not daily.empty:
                max_rev = daily['revenue'].max()
                res = "| Date | Revenue | Orders | Visual |\n|---|---|---|---|\n"
                for date_idx, row in daily.iterrows():
                    rev = row['revenue']
                    ord_cnt = int(row['orders'])
                    blocks = "█" * int((rev / max_rev) * 10) if max_rev > 0 else "▏"
                    res += f"| {date_idx} | ৳{rev:,.0f} | {ord_cnt} | `{blocks}` |\n"
                recent_trend_str = res

        # Extract global aggregates to provide wide intelligence across all active dataframes
        global_stats = {
            "sales_summary": {
                "total_revenue": sales_df['item_revenue'].sum() if not sales_df.empty and 'item_revenue' in sales_df.columns else 0,
                "total_orders": sales_df['order_id'].nunique() if not sales_df.empty and 'order_id' in sales_df.columns else 0,
                "top_selling_items": sales_df['item_name'].value_counts().head(5).to_dict() if not sales_df.empty and 'item_name' in sales_df.columns else {},
                "recent_trend_chart_markdown": recent_trend_str
            }
        }
        if not returns_df.empty:
            global_stats["returns_summary"] = {
                "total_returns": len(returns_df),
                "top_reasons": returns_df['return_reason'].value_counts().head(5).to_dict() if 'return_reason' in returns_df.columns else {}
            }
            if 'returned_items' in returns_df.columns:
                items_list = [i.get('name') for items in returns_df['returned_items'] if isinstance(items, list) for i in items if isinstance(i, dict) and 'name' in i]
                if items_list:
                    global_stats["returns_summary"]["top_returned_items"] = pd.Series(items_list).value_counts().head(5).to_dict()
        if not stock_df.empty:
            global_stats["stock_summary"] = {
                "out_of_stock_count": len(stock_df[stock_df['Stock Status'] == 'outofstock']) if 'Stock Status' in stock_df.columns else 0,
                "total_inventory_value": (pd.to_numeric(stock_df['Stock Quantity'], errors='coerce').fillna(0) * pd.to_numeric(stock_df['Price'], errors='coerce').fillna(0)).sum() if 'Stock Quantity' in stock_df.columns and 'Price' in stock_df.columns else 0
            }
        
        # LLM Response Cache Check
        if "llm_response_cache" not in st.session_state:
            st.session_state.llm_response_cache = {}
            
        state_hash = hashlib.md5(json.dumps(global_stats, sort_keys=True).encode('utf-8')).hexdigest()
        prompt_hash = hashlib.md5(prompt.encode('utf-8')).hexdigest()
        cache_key = f"rag_{self.agent_type}_{self.model_name}_{prompt_hash}_{state_hash}"
        
        if cache_key in st.session_state.llm_response_cache:
            return st.session_state.llm_response_cache[cache_key]

        # 4. Embed the User Query
        query_emb = self._get_embeddings([prompt])
        if query_emb.size == 0 or not query_emb.any():
            return "⚠️ Vector Search Unavailable: Could not generate embeddings. Ensure your embedding model is active."
            
        # 5. Retrieve Top K relevant records
        retrieved_docs = self.vector_store.search(query_emb[0], top_k=7)
        
        context_block = "\n\n".join([f"Record: {doc['content']}" for doc in retrieved_docs])
        
        # 6. Augmented Generation
        system_prompt = f"""
        You are DEEN-BI Data Pilot, an expert e-commerce analyst.
        
        GLOBAL AGGREGATES (Existing System Analysis across all domains):
        {json.dumps(global_stats, indent=2)}
        
        SPECIFIC RECORDS (Semantic search on recent rows):
        
        {context_block}
        
        The provided records prioritize the user's currently active page ("{active_section}"), followed by general site data.
        Answer the user's question accurately based on the global aggregates and specific records above. Be concise, professional, and use markdown.
        If the user asks to compare datasets (e.g., "Is the highest returned item also my best-selling item?"), proactively cross-reference the top_returned_items and top_selling_items.
        When asked for return reasons or similar distributions, present them using visual markdown charts (e.g., `Reason | ██████ 60%`).
        When queried about top-performing items, sales rankings, or categories, present the data in a clean Markdown table.
        If the user asks for a trend, ASCII chart, or textual graph, utilize the pre-calculated `recent_trend_chart_markdown` from the global aggregates.
        If the user explicitly asks for an interactive chart or visualization, output valid Python Plotly code (using plotly.express or plotly.graph_objects) inside a ```python block, defining the data inline within the code based on the records.
        """
        
        def try_gemini():
            import google.generativeai as genai
            import os
            import streamlit as st
            api_key = st.secrets.get("GEMINI_API_KEY") or os.environ.get("GEMINI_API_KEY")
            if not api_key: return "MISSING_KEY"
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-1.5-flash')
            response = model.generate_content(f"{system_prompt}\n\nUser Question: {prompt}")
            return response.text
            
        def try_groq():
            from groq import Groq
            import os
            import streamlit as st
            api_key = st.secrets.get("GROQ_API_KEY") or os.environ.get("GROQ_API_KEY")
            if not api_key: return "MISSING_KEY"
            client = Groq(api_key=api_key)
            completion = client.chat.completions.create(
                model="llama3-70b-8192",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
            )
            return completion.choices[0].message.content
            
        def try_local():
            is_ollama = "11434" in self.base_url
            url = f"{self.base_url}/api/generate" if is_ollama else (f"{self.base_url}/v1/chat/completions" if "/v1" not in self.base_url else f"{self.base_url}/chat/completions")
            payload = {
                "model": self.model_name,
                "prompt": f"{system_prompt}\n\nUser Question: {prompt}",
                "stream": False
            } if is_ollama else {
                "model": self.model_name,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.2
            }
            try:
                response = requests.post(url, json=payload, timeout=30)
                if response.status_code == 200:
                    res_json = response.json()
                    return res_json.get("response", "No response.") if is_ollama else res_json.get("choices", [{}])[0].get("message", {}).get("content", "No response.")
                return "LOCAL_ERROR"
            except Exception:
                return "LOCAL_ERROR"

        fallback_order = []
        if self.agent_type == "Groq":
            fallback_order = [("Groq", try_groq), ("Google Gemini", try_gemini), ("Local AI", try_local)]
        elif self.agent_type == "Google Gemini":
            fallback_order = [("Google Gemini", try_gemini), ("Groq", try_groq), ("Local AI", try_local)]
        else:
            fallback_order = [("Local AI", try_local), ("Groq", try_groq), ("Google Gemini", try_gemini)]
            
        last_error = "❌ **AI Generation Failed:** No valid models available."
        
        for name, func in fallback_order:
            try:
                res = func()
                if res in ["MISSING_KEY", "LOCAL_ERROR"]:
                    continue
                st.session_state.llm_response_cache[cache_key] = res
                return res
            except Exception as e:
                last_error = f"❌ **{name} Error:** {str(e)}"
                continue
                
        return last_error