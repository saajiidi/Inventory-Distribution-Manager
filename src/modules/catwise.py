import streamlit as st
import pandas as pd
import plotly.express as px
from io import BytesIO
from datetime import datetime

# --- Configuration & Mappings (Ported from Catwise-Analytics) ---

SALES_CATEGORY_MAPPING = {
    'Boxer': ['boxer'],
    'Tank Top': ['tank top', 'tanktop', 'tank', 'top'],
    'Jeans': ['jeans'],
    'Denim Shirt': ['denim'],
    'Flannel Shirt': ['flannel'],
    'Polo Shirt': ['polo'],
    'Panjabi': ['panjabi', 'punjabi'],
    'Trousers': ['trousers', 'pant', 'cargo', 'trouser', 'joggers', 'track pant', 'jogger'],
    'Twill Chino': ['twill chino'],
    'Mask': ['mask'],
    'Water Bottle': ['water bottle'],
    'Contrast Shirt': ['contrast'],
    'Turtleneck': ['turtleneck', 'mock neck'],
    'Drop Shoulder': ['drop', 'shoulder'],
    'Wallet': ['wallet'],
    'Kaftan Shirt': ['kaftan'],
    'Active Wear': ['active wear'],
    'Jersy': ['jersy'],
    'Sweatshirt': ['sweatshirt', 'hoodie', 'pullover'],
    'Jacket': ['jacket', 'outerwear', 'coat'],
    'Belt': ['belt'],
    'Sweater': ['sweater', 'cardigan', 'knitwear'],
    'Passport Holder': ['passport holder'],
    'Cap': ['cap'],
    'Leather Bag': ['bag', 'backpack'],
}

STOCK_CATEGORY_MAPPING = {
    "Jeans Slim Fit": lambda n: "jeans" in n and "slim fit" in n,
    "Jeans Regular Fit": lambda n: "jeans" in n and "regular fit" in n,
    "Jeans Straight Fit": lambda n: "jeans" in n and "straight fit" in n,
    "Panjabi": lambda n: "panjabi" in n,
    "Active Wear": lambda n: "active wear" in n,
    "T-shirt Basic Full": lambda n: "t-shirt" in n and "full sleeve" in n,
    "T-shirt Drop-Shoulder": lambda n: "t-shirt" in n and ("drop-shoulder" in n or "drop shoulder" in n),
    "T-shirt Basic Half": lambda n: "t-shirt" in n and not ("full sleeve" in n or "drop-shoulder" in n or "drop shoulder" in n),
    "Sweatshirt": lambda n: "sweatshirt" in n,
    "Turtle-Neck": lambda n: "turtle-neck" in n or "turtleneck" in n,
    "Tank-Top": lambda n: "tank-top" in n or "tank top" in n,
    "Trousers Terry Fabric": lambda n: ("trouser" in n or "jogger" in n or "pant" in n) and "terry" in n,
    "Trousers Cotton Fabric": lambda n: ("trouser" in n or "jogger" in n or "pant" in n) and ("twill" in n or "chino" in n or "cotton" in n),
    "Polo": lambda n: "polo" in n,
    "Kaftan Shirt": lambda n: "kaftan" in n,
    "Contrast Stich": lambda n: "contrast stitch" in n or "contrast stich" in n,
    "Denim Shirt": lambda n: "denim" in n and "shirt" in n,
    "Flannel Shirt": lambda n: "flannel" in n and "shirt" in n,
    "Casual Shirt Full": lambda n: "shirt" in n and "full sleeve" in n and not any(k in n for k in ["denim", "flannel", "kaftan", "contrast", "stitch", "stich", "polo", "sweatshirt"]),
    "Casual Shirt Half": lambda n: "shirt" in n and not any(k in n for k in ["full sleeve", "denim", "flannel", "kaftan", "contrast", "stitch", "stich", "polo", "t-shirt", "sweatshirt"]),
    "Belt": lambda n: "belt" in n,
    "Wallet Bifold": lambda n: "wallet" in n and "bifold" in n,
    "Wallet Trifold": lambda n: "wallet" in n and "trifold" in n,
    "Wallet Long": lambda n: "wallet" in n and "long" in n,
    "Passport Holder": lambda n: "passport holder" in n,
    "Mask": lambda n: "mask" in n,
    "Card Holder": lambda n: "card holder" in n,
    "Water Bottle": lambda n: "water bottle" in n,
    "Boxer": lambda n: "boxer" in n,
    "Bag": lambda n: "bag" in n,
}

COLUMN_ALIAS_MAPPING = {
    'name': ['item name', 'product name', 'product', 'item', 'title', 'description', 'name'],
    'cost': ['item cost', 'price', 'unit price', 'cost', 'rate', 'mrp', 'selling price', 'regular price'],
    'qty': ['quantity', 'qty', 'units', 'sold', 'count', 'total quantity', 'stock', 'inventory', 'stock quantity', 'quantity sold'],
    'date': ['date', 'order date', 'month', 'time', 'created at'],
    'order_id': ['order id', 'order #', 'invoice number', 'invoice #', 'order number', 'transaction id', 'id'],
    'phone': ['phone', 'contact', 'mobile', 'cell', 'phone number', 'customer phone']
}

# --- Core Logic Functions ---

def get_product_category(name, mode="Sales Performance"):
    """Categorizes product based on keywords or lambdas from mapping."""
    name_str = str(name).lower()
    mapping = STOCK_CATEGORY_MAPPING if mode == "Stock Count" else SALES_CATEGORY_MAPPING
    
    for cat, check in mapping.items():
        if callable(check):
            if check(name_str): return cat
        elif any(kw.lower() in name_str for kw in check):
            return cat
    return 'Others'

def find_columns(df):
    """Auto-detects columns from dataframe based on aliases."""
    found = {}
    actual_cols = list(df.columns)
    lower_cols = [c.strip().lower() for c in actual_cols]
    
    for key, aliases in COLUMN_ALIAS_MAPPING.items():
        for alias in aliases:
            if alias in lower_cols:
                found[key] = actual_cols[lower_cols.index(alias)]
                break
        if key not in found:
            for i, col in enumerate(lower_cols):
                if any(alias in col for alias in aliases):
                    found[key] = actual_cols[i]
                    break
    return found

def process_analytics(df, mapping, mode="Sales Performance"):
    """Core data processing and metric calculation."""
    df = df.copy()
    
    df['Clean_Name'] = df[mapping['name']].fillna('Unknown').astype(str)
    df = df[~df['Clean_Name'].str.contains('Choose Any', case=False, na=False)]
    
    cost_col = mapping.get('cost')
    qty_col = mapping.get('qty')
    
    df['Clean_Cost'] = pd.to_numeric(df[cost_col], errors='coerce').fillna(0) if cost_col else 0
    df['Clean_Qty'] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0) if qty_col else 0
    df.loc[df['Clean_Qty'] < 0, 'Clean_Qty'] = 0
    
    df['Total Amount'] = df['Clean_Cost'] * df['Clean_Qty']
    df['Category'] = df['Clean_Name'].apply(lambda n: get_product_category(n, mode=mode))
    
    timeframe = ""
    if mapping.get('date') and mapping['date'] in df.columns:
        try:
            dates = pd.to_datetime(df[mapping['date']], errors='coerce').dropna()
            if not dates.empty:
                if dates.dt.to_period('M').nunique() == 1:
                    timeframe = dates.iloc[0].strftime("%B_%Y")
                else:
                    timeframe = f"{dates.min().strftime('%d%b')}_to_{dates.max().strftime('%d%b_%y')}"
        except: timeframe = "Report"

    summary = df.groupby('Category').agg({'Clean_Qty': 'sum', 'Total Amount': 'sum'}).reset_index()
    summary.columns = ['Category', 'Total Qty', 'Total Amount']
    
    t_rev = summary['Total Amount'].sum()
    t_qty = summary['Total Qty'].sum()
    if t_rev > 0: summary['Revenue Share (%)'] = (summary['Total Amount'] / t_rev * 100).round(2)
    if t_qty > 0: summary['Quantity Share (%)'] = (summary['Total Qty'] / t_qty * 100).round(2)
    
    drilldown = df.groupby(['Category', 'Clean_Cost']).agg({'Clean_Qty': 'sum', 'Total Amount': 'sum'}).reset_index()
    drilldown.columns = ['Category', 'Price (TK)', 'Total Qty', 'Total Amount']
    
    top_items = df.groupby('Clean_Name').agg({'Clean_Qty': 'sum', 'Total Amount': 'sum', 'Category': 'first'}).reset_index()
    top_items.columns = ['Product Name', 'Total Qty', 'Total Amount', 'Category']
    top_items = top_items.sort_values('Total Amount', ascending=False)
    
    avg_basket_value = 0
    order_groups_count = 0
    group_cols = [c for c in [mapping.get('order_id'), mapping.get('phone')] if c and c in df.columns]
    
    if group_cols:
        order_groups = df.groupby(group_cols).agg({'Total Amount': 'sum'})
        avg_basket_value = order_groups['Total Amount'].mean()
        order_groups_count = len(order_groups)
        
    return {
        'drilldown': drilldown,
        'summary': summary,
        'top_items': top_items,
        'timeframe': timeframe,
        'avg_basket_value': avg_basket_value,
        'total_qty': t_qty,
        'total_rev': t_rev,
        'total_orders': order_groups_count
    }

# --- UI Tab Functions ---

def render_catwise_analytics_tab():
    """Renders the Catwise Analytics feature in the main dashboard."""
    st.header("📦 Catwise Smart Analytics")
    st.info("Upload sales or stock data to see automated categorical performance insights.")
    
    mode = st.radio("Select Analysis Mode", ["Sales Performance", "Stock Count"], horizontal=True)
    uploaded_file = st.file_uploader(f"Upload {mode} Data (Excel or CSV)", type=['xlsx', 'csv'], key="catwise_uploader")
    
    if uploaded_file:
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
                
            if mode == "Stock Count" and 'Type' in df.columns:
                df = df[df['Type'].str.lower().isin(['variation', 'simple'])]
            
            st.success(f"Attached: {uploaded_file.name}")
            
            auto_cols = find_columns(df)
            all_cols = list(df.columns)
            
            with st.expander("🛠️ Column Mapping & Preview", expanded=True):
                mc1, mc2, mc3 = st.columns(3)
                mc4, mc5, mc6 = st.columns(3)
                
                def get_idx(key):
                    return all_cols.index(auto_cols[key]) if key in auto_cols else 0

                m_name = mc1.selectbox("Product Name", all_cols, index=get_idx('name'), key="mw_name")
                m_cost = mc2.selectbox("Price/Cost", all_cols, index=get_idx('cost'), key="mw_cost")
                m_qty = mc3.selectbox("Quantity/Stock", all_cols, index=get_idx('qty'), key="mw_qty")
                m_date = mc4.selectbox("Date (Opt)", ["None"] + all_cols, index=get_idx('date')+1 if 'date' in auto_cols else 0, key="mw_date")
                m_order = mc5.selectbox("Order ID (Opt)", ["None"] + all_cols, index=get_idx('order_id')+1 if 'order_id' in auto_cols else 0, key="mw_order")
                m_phone = mc6.selectbox("Phone (Opt)", ["None"] + all_cols, index=get_idx('phone')+1 if 'phone' in auto_cols else 0, key="mw_phone")
                
                mapping = {
                    'name': m_name, 'cost': m_cost, 'qty': m_qty,
                    'date': m_date if m_date != "None" else None,
                    'order_id': m_order if m_order != "None" else None,
                    'phone': m_phone if m_phone != "None" else None
                }
                
                st.dataframe(df.head(5), use_container_width=True)

            if st.button("🔥 Generate Engine Insights", use_container_width=True):
                results = process_analytics(df, mapping, mode=mode)
                
                # Metrics Row
                m1, m2, m3, m4 = st.columns(4)
                if mode == "Sales Performance":
                    m1.metric("Orders", f"{results['total_orders']:,.0f}" if results['total_orders'] > 0 else "N/A")
                    m2.metric("Units Sold", f"{results['total_qty']:,.0f}")
                    m3.metric("Total Revenue", f"TK {results['total_rev']:,.2f}")
                    m4.metric("Avg Basket (TK)", f"TK {results['avg_basket_value']:,.2f}" if results['avg_basket_value'] > 0 else "N/A")
                else:
                    m1.metric("Total SKU Count", f"{len(df):,.0f}")
                    m2.metric("Total Stock Qty", f"{results['total_qty']:,.0f}")
                    m3.metric("Total Stock Value", f"TK {results['total_rev']:,.2f}")
                    m4.metric("Avg Qty/SKU", f"{results['total_qty']/len(df):,.1f}" if len(df) > 0 else "N/A")

                st.divider()
                
                # Visuals
                v1, v2 = st.columns(2)
                summ_sorted = results['summary'].sort_values('Total Amount', ascending=False)
                color_seq = px.colors.qualitative.Pastel
                
                label_prefix = "Revenue" if mode == "Sales Performance" else "Value"
                
                v1.plotly_chart(px.pie(summ_sorted, values='Total Amount', names='Category', hole=0.5, 
                                       title=f'{label_prefix} Share by Category', color_discrete_sequence=color_seq), use_container_width=True)
                
                v2.plotly_chart(px.bar(summ_sorted, x='Category', y='Total Qty', color='Category', 
                                       title='Volume Breakdown by Category', color_discrete_sequence=color_seq), use_container_width=True)
                
                # Data Tables
                t1, t2, t3 = st.tabs(["📑 Aggregated Breakdown", "🏆 Top Performing Items", "🔍 Full Price Drilldown"])
                with t1: st.dataframe(results['summary'].sort_values('Total Amount', ascending=False), use_container_width=True)
                with t2: st.dataframe(results['top_items'].head(25), use_container_width=True)
                with t3: st.dataframe(results['drilldown'], use_container_width=True)
                
                # Export functionality
                buf = BytesIO()
                with pd.ExcelWriter(buf, engine='xlsxwriter') as wr:
                    results['summary'].to_excel(wr, sheet_name='Category Summary', index=False)
                    results['top_items'].to_excel(wr, sheet_name='Product Rankings', index=False)
                    results['drilldown'].to_excel(wr, sheet_name='Price Points', index=False)
                
                fname = f"Catwise_{mode.replace(' ', '_')}_{results['timeframe']}.xlsx"
                st.download_button("📥 Download Analysis Report", data=buf.getvalue(), file_name=fname, key="catwise_download")
                
        except Exception as e:
            st.error(f"Engine Error: {e}")
