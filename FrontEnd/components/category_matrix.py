"""Category and Sub-category Sales Matrix View.

Renders a responsive CSS-grid dashboard card for each main category,
showing sub-categories/products, order counts, and revenue trends.
"""

import pandas as pd
import streamlit as st
from datetime import timedelta
from typing import Optional

from BackEnd.utils.woocommerce_helpers import format_currency
from BackEnd.core.logging_config import get_logger

logger = get_logger("category_matrix")

def render_category_matrix(
    sales_df: pd.DataFrame, 
    returns_df: Optional[pd.DataFrame] = None,
    top_n: int = 5,
    cat_col: str = "Category",
    subcat_col: str = "_clean_name", 
    val_col: str = "total",
    order_col: str = "order_id",
    date_col: str = "date_created",
    qty_col: str = "qty"
) -> None:
    """Render the HTML matrix view for category and sub-category sales data."""
    if sales_df is None or sales_df.empty:
        st.info("No sales data available for matrix view.")
        return

    df = sales_df.copy()
    
    # 1. Column normalization & Fallbacks
    if cat_col not in df.columns:
        df[cat_col] = "Uncategorized"
    if subcat_col not in df.columns:
        df[subcat_col] = df.get("item_name", "Unknown Product")
    if val_col not in df.columns:
        val_col = "order_total" if "order_total" in df.columns else "line_total"
        if val_col not in df.columns:
            df[val_col] = 0.0
            
    if order_col not in df.columns:
        df[order_col] = df.index  # fallback for counting
        
    if qty_col not in df.columns:
        df[qty_col] = 1

    # Derive Main Category and Sub-Category from the hierarchy (e.g. Jeans -> Regular Fit)
    df['_main_cat'] = df[cat_col].apply(lambda x: str(x).split(' - ')[0].strip() if pd.notna(x) else 'Unknown')
    df['_sub_cat'] = df[cat_col].apply(lambda x: str(x).split(' - ')[1].strip() if pd.notna(x) and ' - ' in str(x) else str(x).strip())

    # Integrate Returns Data for High Return Rate Warnings
    if returns_df is None and "returns_data" in st.session_state:
        returns_df = st.session_state.returns_data
        
    df['is_returned'] = False
    if returns_df is not None and not returns_df.empty and 'issue_type' in returns_df.columns:
        ret_orders = returns_df[returns_df['issue_type'].isin(['Paid Return', 'Non Paid Return', 'Partial'])]['order_id'].astype(str).unique()
        df['is_returned'] = df[order_col].astype(str).isin(ret_orders)
        
    df['ret_qty'] = df[qty_col].where(df['is_returned'], 0)

    # 2. Date grouping mapping for Contextual Comparison based on active Time Window
    df['period'] = 'current'
    prev_df_mapped = pd.DataFrame()
    if "dashboard_data" in st.session_state and "sales_prev" in st.session_state.dashboard_data:
        prev_sales_df = st.session_state.dashboard_data["sales_prev"]
        if not prev_sales_df.empty:
            prev_df_mapped = prev_sales_df.copy()
            if cat_col not in prev_df_mapped.columns: prev_df_mapped[cat_col] = "Uncategorized"
            if subcat_col not in prev_df_mapped.columns: prev_df_mapped[subcat_col] = prev_df_mapped.get("item_name", "Unknown Product")
            
            v_col = val_col if val_col in prev_df_mapped.columns else ("order_total" if "order_total" in prev_df_mapped.columns else "line_total")
            prev_df_mapped[val_col] = prev_df_mapped[v_col] if v_col in prev_df_mapped.columns else 0.0
            
            if qty_col not in prev_df_mapped.columns: prev_df_mapped[qty_col] = 1
            
            prev_df_mapped['_main_cat'] = prev_df_mapped[cat_col].apply(lambda x: str(x).split(' - ')[0].strip() if pd.notna(x) else 'Unknown')
            prev_df_mapped['_sub_cat'] = prev_df_mapped[cat_col].apply(lambda x: str(x).split(' - ')[1].strip() if pd.notna(x) and ' - ' in str(x) else str(x).strip())
            prev_df_mapped['period'] = 'previous'

    # 3. CSS Injection (Scoping generic classes slightly to avoid Streamlit conflicts)
    css = """
    <style>
        .matrix-dashboard-grid {
            display: grid;
            gap: 1.5rem;
            align-items: start;
            width: 100%;
            margin: 0 auto;
        }
        .matrix-card {
            background: var(--surface, white);
            border-radius: 1rem;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
            transition: all 0.2s;
            border: 1px solid var(--outline, #e2e8f0);
            container-type: inline-size;
        }
        .matrix-card-header {
            padding: 1rem 1.25rem;
            border-bottom: 1px solid var(--outline, #e2e8f0);
            display: flex;
            align-items: center;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 0.5rem;
            border-radius: 1rem 1rem 0 0;
        }
        .matrix-title-section {
            display: flex;
            align-items: center;
            gap: 0.6rem;
        }
        .matrix-title-section h3 {
            font-size: clamp(0.9rem, 5cqi, 1.1rem);
            font-weight: 600;
            color: var(--on-surface, #0f172a);
            margin: 0;
        }
        .matrix-icon {
            font-size: 1.4rem;
            background: rgba(16, 185, 129, 0.1);
            color: #10b981;
            padding: 0.3rem;
            border-radius: 12px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 2.2rem;
            height: 2.2rem;
        }
        .matrix-comparison-badge {
            padding: 0.25rem 0.75rem;
            border-radius: 2rem;
            font-size: 0.8rem;
            font-weight: 500;
        }
        .matrix-positive { color: var(--green); background: rgba(16, 185, 129, 0.15); }
        .matrix-negative { color: var(--red); background: rgba(239, 68, 68, 0.15); }
        .matrix-neutral { color: var(--on-surface-variant, #64748b); background: rgba(100, 116, 139, 0.15); }
        .matrix-scroll {
            overflow-x: auto;
            overflow-y: hidden;
            width: 100%;
            scroll-behavior: smooth;
        }
        .matrix-table {
            min-width: 100%;
            width: 100%;
            border-collapse: collapse;
            font-size: 0.85rem;
        }
        .matrix-table th, .matrix-table td {
            padding: 0.9rem 1rem;
            text-align: left;
            border-bottom: 1px solid var(--outline, #eef2f6);
            white-space: nowrap;
            color: var(--on-surface-variant, #334155);
        }
        .matrix-table td:first-child {
            max-width: 180px;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .matrix-table th {
            background-color: var(--surface, #fafcff);
            font-weight: 600;
            color: var(--on-surface, #1e293b);
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.3px;
        }
        .matrix-table tbody tr {
            transition: background-color 0.2s ease;
        }
        .matrix-table tbody tr:hover {
            background-color: rgba(128, 128, 128, 0.05);
        }
        .matrix-revenue { font-weight: 700; color: var(--on-surface, #0f172a); }
        .matrix-trend {
            display: inline-flex;
            align-items: center;
            gap: 0.2rem;
            font-weight: 500;
            font-size: 0.8rem;
            background: var(--surface, #f8fafc);
            padding: 0.2rem 0.5rem;
            border-radius: 1rem;
        }
        .matrix-trend-up { color: var(--green); }
        .matrix-trend-down { color: var(--red); }
        .matrix-view-icon { cursor: pointer; font-size: 1.2rem; opacity: 0.7; transition: 0.1s; }
        .matrix-view-icon:hover { opacity: 1; }
        .matrix-card-footer {
            padding: 0.75rem 1.25rem;
            font-size: 0.75rem;
            color: var(--on-surface-variant, #64748b);
            text-align: right;
            background: var(--surface, #fafcff);
            border-radius: 0 0 1rem 1rem;
            border-top: 1px solid var(--outline, #eef2f6);
        }
        .matrix-scroll::-webkit-scrollbar { height: 6px; }
        .matrix-scroll::-webkit-scrollbar-track { background: var(--surface, #e2e8f0); border-radius: 4px; }
        .matrix-scroll::-webkit-scrollbar-thumb { background: var(--outline, #94a3b8); border-radius: 4px; }
    </style>
    """

    def get_cat_icon(cat_name):
        cat_lower = str(cat_name).lower()
        if any(w in cat_lower for w in ['electronic', 'tech', 'phone']): return '📱'
        if any(w in cat_lower for w in ['fashion', 'cloth', 'apparel', 'wear']): return '👗'
        if any(w in cat_lower for w in ['home', 'furn', 'living']): return '🛒'
        if any(w in cat_lower for w in ['beauty', 'health', 'cosmetic']): return '💄'
        return '📦'

    cards_html = ""
    
    # 4. Generate Cards by Main Category (Sorted by Top Revenue)
    cat_metrics = df.groupby('_main_cat')[val_col].sum().sort_values(ascending=False)
    
    for cat in cat_metrics.index:
        cat_df = df[df['_main_cat'] == cat]
        
        curr_df = cat_df[cat_df['period'] == 'current']
        if not prev_df_mapped.empty:
            prev_df = prev_df_mapped[prev_df_mapped['_main_cat'] == cat]
        else:
            prev_df = pd.DataFrame()
        
        curr_cat_rev = curr_df[val_col].sum()
        prev_cat_rev = prev_df[val_col].sum()
        
        cat_growth = ((curr_cat_rev - prev_cat_rev) / prev_cat_rev * 100) if prev_cat_rev > 0 else 0
        badge_class = "matrix-positive" if cat_growth >= 0 else "matrix-negative"
        badge_text = f"+{cat_growth:.1f}% vs prev period" if cat_growth >= 0 else f"{cat_growth:.1f}% vs prev period"
        
        if prev_cat_rev == 0 and curr_cat_rev > 0:
            badge_text, badge_class = "New Sales", "matrix-positive"
        elif prev_cat_rev == 0 and curr_cat_rev == 0:
            badge_text, badge_class = "No change", "matrix-neutral"

        # 5. Generate Rows by Sub-Category with "Others" aggregation
        sub_metrics = cat_df.groupby('_sub_cat').agg(
            rev_total=(val_col, 'sum'),
            items_total=(qty_col, 'sum'),
            ret_total=('ret_qty', 'sum')
        ).sort_values('rev_total', ascending=False)
        
        top_subs = sub_metrics.head(top_n)
        other_subs = sub_metrics.iloc[top_n:]
        
        curr_subs = curr_df.groupby('_sub_cat')[val_col].sum()
        prev_subs = prev_df.groupby('_sub_cat')[val_col].sum()

        max_sub_rev = top_subs['rev_total'].max() if not top_subs.empty else 1

        rows_data = []
        for subcat, row in top_subs.iterrows():
            rows_data.append({
                'name': subcat,
                'items': row['items_total'],
                'rev': row['rev_total'],
                'ret': row['ret_total'],
                'curr': curr_subs.get(subcat, 0),
                'prev': prev_subs.get(subcat, 0),
                'is_other': False
            })
            
        if not other_subs.empty:
            rows_data.append({
                'name': 'Others',
                'items': other_subs['items_total'].sum(),
                'rev': other_subs['rev_total'].sum(),
                'ret': other_subs['ret_total'].sum(),
                'curr': curr_subs[other_subs.index].sum() if not other_subs.empty else 0,
                'prev': prev_subs[other_subs.index].sum() if not other_subs.empty else 0,
                'is_other': True,
                'other_count': len(other_subs)
            })

        rows_html = ""
        for r in rows_data:
            sub_curr = r['curr']
            sub_prev = r['prev']
        
            sub_growth = ((sub_curr - sub_prev) / sub_prev * 100) if sub_prev > 0 else 0
            if sub_prev == 0 and sub_curr > 0:
                trend_html = '<span class="matrix-trend matrix-trend-up">▲ New</span>'
            elif sub_growth > 0:
                trend_html = f'<span class="matrix-trend matrix-trend-up">▲ +{sub_growth:.1f}%</span>'
            elif sub_growth < 0:
                trend_html = f'<span class="matrix-trend matrix-trend-down">▼ {sub_growth:.1f}%</span>'
            else:
                trend_html = '<span class="matrix-trend">➖ 0%</span>'
            
            bar_pct = min((r['rev'] / max_sub_rev * 100), 100) if max_sub_rev > 0 else 0
            
            if r['is_other']:
                bg_style = f"background: linear-gradient(90deg, rgba(100, 116, 139, 0.1) {bar_pct}%, transparent {bar_pct}%);"
                tooltip = f"Includes {r['other_count']} aggregated sub-categories"
            else:
                bg_style = f"background: linear-gradient(90deg, rgba(16, 185, 129, 0.12) {bar_pct}%, transparent {bar_pct}%);"
                avg_price = r['rev'] / r['items'] if r['items'] > 0 else 0
                tooltip = f"Avg Price: {format_currency(avg_price)}"
                
            ret_rate = (r['ret'] / r['items'] * 100) if r['items'] > 0 else 0
            warning_html = f" <span title='High Return Rate: {ret_rate:.1f}%' style='color: #ef4444; font-size: 0.85rem; cursor: help;'>⚠️</span>" if ret_rate >= 10 else ""
            
            subcat_display = f"{r['name']}{warning_html}"

            # Formatted without leading indentation to prevent Streamlit Markdown Code Block parsing
            rows_html += f"<tr><td>{subcat_display}</td><td>{int(r['items']):,}</td><td class='matrix-revenue' style='{bg_style}'>{format_currency(r['rev'])}</td><td>{trend_html}</td><td class='matrix-view-icon' title='{tooltip}'>ℹ️</td></tr>"

        cards_html += (
            f"<div class='matrix-card'>"
            f"<div class='matrix-card-header'><div class='matrix-title-section'>"
            f"<span class='matrix-icon'>{get_cat_icon(cat)}</span>"
            f"<h3>{cat} · Revenue Matrix</h3></div>"
            f"<div class='matrix-comparison-badge {badge_class}'>{badge_text}</div></div>"
            f"<div class='matrix-scroll'><table class='matrix-table'>"
            f"<thead><tr><th>Sub-Category</th><th>Items Sold</th><th>Revenue</th><th>vs Prev week</th><th>View</th></tr></thead>"
            f"<tbody>{rows_html}</tbody>"
            f"</table></div>"
            f"<div class='matrix-card-footer'>Period-over-Period comparison · Top Sub-Categories</div>"
            f"</div>"
        )

    st.markdown(f"{css}<div class='matrix-dashboard-grid'>{cards_html}</div>", unsafe_allow_html=True)