import pandas as pd
import numpy as np
import requests
import json
import hashlib
import streamlit as st
from typing import List, Dict, Any
from BackEnd.core.logging_config import get_logger

logger = get_logger("rag_engine")

class SimpleVectorStore:
    """In-memory numpy-based vector store for lightweight RAG."""
    def __init__(self):
        self.documents: List[Dict[str, Any]] = []
        self.vectors: np.ndarray = np.array([])

    def add_documents(self, documents: List[Dict[str, Any]], embeddings: np.ndarray):
        self.documents.extend(documents)
        if self.vectors.size == 0:
            self.vectors = embeddings
        else:
            self.vectors = np.vstack([self.vectors, embeddings])

    def search(self, query_embedding: np.ndarray, top_k: int = 5) -> List[Dict[str, Any]]:
        if self.vectors.size == 0:
            return []
        
        # Cosine similarity: dot product of normalized vectors
        query_norm = query_embedding / np.linalg.norm(query_embedding)
        vectors_norm = self.vectors / np.linalg.norm(self.vectors, axis=1)[:, np.newaxis]
        similarities = np.dot(vectors_norm, query_norm)
        
        top_indices = np.argsort(similarities)[-top_k:][::-1]
        
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
        self._api_key = st.secrets.get("GEMINI_API_KEY") if agent_type == "Google Gemini" else None

    def _get_embeddings(self, texts: List[str]) -> np.ndarray:
        """Generate embeddings using the configured provider with local session caching."""
        if not texts:
            return np.array([])
            
        if "embedding_cache" not in st.session_state:
            st.session_state.embedding_cache = {}
            
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
                
        return np.array(embeddings)

    def _ingest_dataframe(self, df: pd.DataFrame, max_rows: int = 500):
        """Convert DataFrame rows into searchable text documents."""
        if df.empty:
            return
            
        # Take recent rows to avoid massive API overhead for a quick response
        sample_df = df.tail(max_rows).copy()
        
        docs = []
        texts = []
        
        for _, row in sample_df.iterrows():
            # Format the row into a readable chunk
            row_dict = row.dropna().to_dict()
            text_chunk = ", ".join([f"{k}: {v}" for k, v in row_dict.items()])
            texts.append(text_chunk)
            docs.append({"content": text_chunk, "metadata": {"index": _}})
            
        embeddings = self._get_embeddings(texts)
        if embeddings.size > 0:
            self.vector_store.add_documents(docs, embeddings)

    def query(self, prompt: str, context_df: pd.DataFrame) -> str:
        """Full RAG Pipeline: Ingest -> Embed Query -> Retrieve -> Generate."""
        # 0. Clear previous vector store to prevent unbounded growth per session
        self.vector_store = SimpleVectorStore()
        
        # 1. Determine active page context to prioritize
        active_section = st.session_state.get("active_section", "💎 Sales Overview")
        dashboard_data = st.session_state.get("dashboard_data", {})
        
        active_df = None
        if active_section == "📦 Stock Insight":
            active_df = dashboard_data.get("stock", pd.DataFrame())
        elif active_section == "👥 Customer Insight":
            active_df = dashboard_data.get("customers", pd.DataFrame())
        elif active_section == "🔄 Returns Insights":
            active_df = st.session_state.get("returns_data", pd.DataFrame())
        elif active_section == "💎 Sales Overview" or active_section == "🛡️ Strategic Command":
            active_df = dashboard_data.get("sales_active", pd.DataFrame())
            
        ingested_site_data = False
        
        # 2. Ingest Active Page Data (High Priority)
        if active_df is not None and not active_df.empty:
            active_context = active_df.copy()
            active_context["_Data_Context"] = f"Active Page: {active_section}"
            self._ingest_dataframe(active_context, max_rows=300)
            
            # Check if active_df is identical to context_df to avoid double ingestion
            if context_df is not None and not context_df.empty and active_df.equals(context_df):
                ingested_site_data = True

        # 3. Ingest Whole Site Data (Fallback / General Context)
        if not ingested_site_data and context_df is not None and not context_df.empty:
            site_context = context_df.copy()
            site_context["_Data_Context"] = "Global Site Data (Sales/Orders)"
            self._ingest_dataframe(site_context, max_rows=200)
        
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
        You have performed a semantic search on the database. Here are the most relevant row records for the user's query:
        
        {context_block}
        
        The provided records prioritize the user's currently active page ("{active_section}"), followed by general site data.
        Answer the user's question accurately based ONLY on these specific records. Be concise, professional, and use markdown.
        When queried about top-performing items, sales rankings, or categories, present the data in a clean Markdown table.
        """
        
        if self.agent_type == "Google Gemini":
            import google.generativeai as genai
            model = genai.GenerativeModel('gemini-1.5-flash')
            response = model.generate_content(f"{system_prompt}\n\nUser Question: {prompt}")
            return response.text
        else:
            is_ollama = "11434" in self.base_url
            url = f"{self.base_url}/api/generate" if is_ollama else f"{self.base_url}/v1/chat/completions"
            
            payload = {
                "model": self.model_name,
                "prompt": f"{system_prompt}\n\nUser Question: {prompt}",
                "stream": False
            }
            
            response = requests.post(url, json=payload, timeout=30)
            res_json = response.json()
            return res_json.get("response", "No response.") if is_ollama else res_json.get("choices", [{}])[0].get("message", {}).get("content", "No response.")