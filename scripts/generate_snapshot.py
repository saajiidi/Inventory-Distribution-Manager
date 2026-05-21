import os
import sys
import sqlite3
from pathlib import Path

# Ensure project root is in python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from BackEnd.services.hybrid_data_loader import load_hybrid_data, refresh_woocommerce_stock_cache
from BackEnd.services.returns_tracker import load_returns_data
from BackEnd.services.customer_manager import build_customer_mapping, load_raw_customer_data
from BackEnd.services.rag_engine import RAGAgent

def generate_snapshot():
    print("🚀 Starting Offline Data Crunching...")
    
    # 1. Fetch & Cache Sales Data
    print("📦 Fetching Sales Data...")
    sales_df = load_hybrid_data(force=True, woocommerce_mode="direct")
    
    # 2. Fetch & Cache Stock Data
    print("📦 Fetching Stock Data...")
    stock_df = refresh_woocommerce_stock_cache()
    
    # 3. Fetch & Cache Returns Data
    print("📦 Fetching Returns Data...")
    returns_df = load_returns_data(sales_df=sales_df)
    
    # 4. Fetch & Cache Customer Data
    print("👥 Consolidating Customer Data...")
    raw_cust_df = load_raw_customer_data()
    build_customer_mapping(sales_df, raw_cust_df)
    
    # 5. Build Offline SQLite Database
    print("🗄️ Building Offline SQLite DB...")
    db_path = Path("BackEnd/cache/offline_data.db")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    if not sales_df.empty: sales_df.to_sql('sales', conn, index=False, if_exists='replace')
    if not stock_df.empty: stock_df.to_sql('stock', conn, index=False, if_exists='replace')
    if not returns_df.empty: returns_df.to_sql('returns', conn, index=False, if_exists='replace')
    conn.close()
    
    # 6. Pre-compute RAG Embeddings (Vector Store)
    print("🧠 Generating Vector Embeddings...")
    agent = RAGAgent(agent_type="Google Gemini")
    if not sales_df.empty: agent._ingest_dataframe(sales_df, max_rows=500)
    
    print("✅ Offline Snapshot Generation Complete!")

if __name__ == "__main__":
    generate_snapshot()