import pandas as pd
import streamlit as st

def query_app_data(prompt: str, df: pd.DataFrame, context: str = "") -> tuple[str, pd.DataFrame | None]:
    """Processes natural language queries against the provided DataFrame."""
    if df.empty:
        return "I don't have any data to analyze right now. Please sync your WooCommerce store.", None
    
    prompt_lower = prompt.lower()
    
    qty_col = "quantity" if "quantity" in df.columns else "qty" if "qty" in df.columns else None
    
    # Simple rule-based intelligence for common BI questions
    if "top product" in prompt_lower or "best selling" in prompt_lower:
        if "item_name" in df.columns and qty_col:
            numeric_qty = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
            top = df.assign(**{qty_col: numeric_qty}).groupby("item_name")[qty_col].sum().sort_values(ascending=False).head(5).reset_index()
            return "Here are your top 5 products by units sold:", top
    
    if "total sales" in prompt_lower or "revenue" in prompt_lower:
        if "order_total" in df.columns and "order_id" in df.columns:
            # Note: we need to handle multi-line orders by counting each order_total once per order_id
            numeric_totals = pd.to_numeric(df["order_total"], errors='coerce').fillna(0)
            order_totals = numeric_totals.groupby(df["order_id"]).max()
            total = order_totals.sum()
            return f"Your total revenue for the current filter window is TK {total:,.0f}.", None

    if "how many orders" in prompt_lower:
        if "order_id" in df.columns:
            count = df["order_id"].nunique()
            return f"You have {count:,} orders in the current view.", None
        return "I couldn't find order information in the current view.", None

    if "multi-item" in prompt_lower or "bulk" in prompt_lower or "bundled" in prompt_lower:
        if qty_col and "order_id" in df.columns:
            numeric_qty = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
            multi = df[numeric_qty > 1]
            count = multi["order_id"].nunique()
            return f"There are {count:,} orders containing multi-item units or bulk quantities.", multi.head(10)

    return generic_chat(prompt, context), None

def generic_chat(prompt: str, context: str = "", history: list = None) -> str:
    """Fallback generic chat logic."""
    # This would normally call an LLM. For now, it provides a smart context-aware placeholder.
    return (
        "I'm your ShopAI assistant. I can help you analyze your sales, track inventory, "
        "and find customer trends. Try asking: 'What are my top products?' or 'What is my total revenue?'"
    )
