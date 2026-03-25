import streamlit as st
import pandas as pd
import plotly.express as px
import os
import json
import re
import streamlit.components.v1 as components
from datetime import date, datetime, timedelta, timezone
from src.core.categories import get_category_for_sales
from src.core.paths import prepare_data_dirs, SYSTEM_LOG_FILE
from src.ui.components import section_card
from src.utils.data import find_columns, parse_dates
from src.core.sync import (
    load_shared_gsheet,
    normalize_gsheet_url_to_csv,
    load_published_sheet_tabs,
    clear_sync_cache,
    DEFAULT_GSHEET_URL,
)

# CONFIGURATION
TOTAL_SALES_EXCLUDED_TABS = {"lastdaysales"}
prepare_data_dirs()

# --- SYSTEM HELPERS ---

def log_system_event(event_type, details):
    log_file = SYSTEM_LOG_FILE
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = {"timestamp": timestamp, "type": event_type, "details": details}
    try:
        logs = []
        if os.path.exists(log_file):
            with open(log_file, "r", encoding="utf-8") as f:
                logs = json.load(f)
        logs.append(log_entry)
        with open(log_file, "w", encoding="utf-8") as f:
            json.dump(logs, f, indent=4)
    except Exception:
        pass

def get_setting(key, default=None):
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, default)

def get_custom_report_tab_label():
    return "📂 Total Sales Report"

# --- ANALYTICS ENGINE ---

@st.cache_data(show_spinner=False)
def process_data(df, selected_cols):
    try:
        df = df.copy()
        df['Internal_Name'] = df[selected_cols['name']].fillna('Unknown Product').astype(str)
        df['Internal_Cost'] = pd.to_numeric(df[selected_cols['cost']], errors='coerce').fillna(0)
        df['Internal_Qty'] = pd.to_numeric(df[selected_cols['qty']], errors='coerce').fillna(0)
        
        tf = ""
        if 'date' in selected_cols:
            ds = pd.to_datetime(df[selected_cols['date']], errors='coerce').dropna()
            if not ds.empty: tf = f"{ds.min().strftime('%d%b')}_to_{ds.max().strftime('%d%b_%y')}"

        df['Category'] = df['Internal_Name'].apply(get_category_for_sales)
        df['Total Amount'] = df['Internal_Cost'] * df['Internal_Qty']
        
        summ = df.groupby('Category').agg({'Internal_Qty': 'sum', 'Total Amount': 'sum'}).reset_index()
        summ.columns = ['Category', 'Total Qty', 'Total Amount']
        
        drill = df.groupby(['Category', 'Internal_Cost']).agg({'Internal_Qty': 'sum', 'Total Amount': 'sum'}).reset_index()
        drill.columns = ['Category', 'Price (TK)', 'Total Qty', 'Total Amount']
        
        top = df.groupby('Internal_Name').agg({'Internal_Qty': 'sum', 'Total Amount': 'sum', 'Category': 'first'}).reset_index()
        top.columns = ['Product Name', 'Total Qty', 'Total Amount', 'Category']
        top = top.sort_values('Total Amount', ascending=False)
        
        bk = {"avg_basket_qty": 0, "avg_basket_value": 0, "total_orders": 0}
        gc = [selected_cols[k] for k in ('order_id', 'phone', 'email') if k in selected_cols and selected_cols[k] in df.columns]
        if gc:
            og = df.groupby(gc).agg({'Internal_Qty': 'sum', 'Total Amount': 'sum'})
            bk = {"avg_basket_qty": og['Internal_Qty'].mean(), "avg_basket_value": og['Total Amount'].mean(), "total_orders": len(og)}
            
        return drill, summ, top, tf, bk
    except Exception as e:
        log_system_event("CALC_ERROR", str(e))
        return None, None, None, "", {}

def render_story_summary(summ, tp, timeframe, bk):
    """Conversational data storytelling component."""
    if summ is None or summ.empty: return
    
    total_rev = summ['Total Amount'].sum()
    top_cat = summ.sort_values('Total Amount', ascending=False).iloc[0]['Category']
    top_prod = tp.iloc[0]['Product Name']
    orders = bk.get('total_orders', 0)
    
    story = f"""
    <div style="background: rgba(59, 130, 246, 0.05); border-left: 4px solid var(--neon-blue); padding: 1.5rem; border-radius: 0 16px 16px 0; margin-bottom: 2rem; font-family: 'Outfit';">
        <div style="color: var(--neon-blue); font-weight: 700; text-transform: uppercase; font-size: 0.8rem; letter-spacing: 0.1em; margin-bottom: 0.5rem;">📊 EXECUTIVE NARRATIVE</div>
        <div style="font-size: 1.1rem; color: var(--text-primary); line-height: 1.5;">
            In the period <b>{timeframe or 'Overview'}</b>, the operations processed <b>{orders:,} orders</b> driving a total revenue of <b>TK {total_rev:,.0f}</b>. 
            The performance was primarily led by the <b>{top_cat}</b> category, with <b>{top_prod}</b> emerging as the high-velocity item. 
            Customer engagement shows an average basket value of <b>TK {bk.get('avg_basket_value', 0):,.0f}</b> per transaction.
        </div>
    </div>
    """
    st.markdown(story, unsafe_allow_html=True)

# --- UI RENDERING ---

def render_dashboard_output(drill, summ, top, timeframe, basket, source, updated):
    render_story_summary(summ, top, timeframe, basket)
    st.markdown(f"### ⚡ Statement: {timeframe or 'All Records'}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Items", f"{summ['Total Qty'].sum():,.0f}")
    c2.metric("Orders", f"{basket['total_orders']:,}")
    c3.metric("Revenue", f"TK {summ['Total Amount'].sum():,.0f}")
    c4.metric("Avg Basket", f"TK {basket['avg_basket_value']:,.0f}")
    
    col1, col2 = st.columns(2)
    with col1:
        # Sort for color consistency
        sorted_summ = summ.sort_values('Total Amount', ascending=False)
        st.plotly_chart(
            px.pie(
                sorted_summ, 
                values='Total Amount', 
                names='Category', 
                hole=0.5, 
                title="Revenue Share",
                color_discrete_sequence=px.colors.sequential.Blues_r
            ), 
            use_container_width=True,
            key=f"sales_pie_{source or 'default'}"
        )
    with col2:
        st.plotly_chart(
            px.bar(
                sorted_summ, 
                x='Total Amount', 
                y='Category', 
                orientation='h', 
                title="Category Performance",
                color='Total Amount',
                color_continuous_scale='Blues'
            ), 
            use_container_width=True,
            key=f"sales_bar_{source or 'default'}"
        )

    with st.expander("Detailed Product Breakdown"):
        st.dataframe(top, use_container_width=True, hide_index=True)

# --- TABS ---

def render_live_tab():
    section_card("📡 Live Stream", "Real-time performance synchronized with LastDaySales.")
    if st.button("🔄 Sync Now", use_container_width=True, key="live_sync_btn"): clear_sync_cache(); st.rerun()
    try:
        df, src, upd = load_shared_gsheet("LastDaySales")
        mc = find_columns(df)
        dr, sm, tp, tf, bk = process_data(df, mc)
        render_dashboard_output(dr, sm, tp, tf, bk, src, upd)
    except Exception as e:
        st.error(f"Live sync error: {e}")

@st.cache_data(ttl=3600, show_spinner=False)
def get_all_statements_master(full_history: bool = False):
    """Fetches statement tabs. If full_history is False, only fetches recent statements."""
    url = get_setting("GSHEET_URL", DEFAULT_GSHEET_URL)
    try:
        tabs = load_published_sheet_tabs(url)
    except Exception:
        return None, "Failed to load tabs"
    
    # Priority sorting: Current year/statements first
    relevant_tabs = []
    for tab in tabs:
        tname = tab["name"].lower()
        if tname in TOTAL_SALES_EXCLUDED_TABS or "sample" in tname or "template" in tname: continue
        relevant_tabs.append(tab)
        
    # If not full history, only take top 3 latest tabs for speed
    if not full_history and len(relevant_tabs) > 3:
        # We assume recent are at the beginning or end? 
        # Usually they are at the beginning of the list returned by published GSheets.
        relevant_tabs = relevant_tabs[:3]
    
    all_dfs = []
    for tab in relevant_tabs:
        try:
            from src.core.sync import normalize_gsheet_url_to_csv
            from src.utils.io import read_remote_csv
            csv_url = normalize_gsheet_url_to_csv(url, tab["gid"])
            df, _ = read_remote_csv(csv_url)
            if not df.empty:
                m = find_columns(df)
                df = df.copy()
                df['_src_tab'] = tab["name"]
                if 'name' in m: df['_p_name'] = df[m['name']].astype(str)
                if 'cost' in m: df['_p_cost'] = pd.to_numeric(df[m['cost']], errors='coerce').fillna(0)
                if 'qty' in m: df['_p_qty'] = pd.to_numeric(df[m['qty']], errors='coerce').fillna(0)
                if 'date' in m: df['_p_date'] = parse_dates(df[m['date']])
                if 'order_id' in m: df['_p_order'] = df[m['order_id']].astype(str)
                if 'phone' in m: df['_p_phone'] = df[m['phone']].astype(str)
                if 'email' in m: df['_p_email'] = df[m['email']].astype(str)
                all_dfs.append(df)
        except Exception:
            pass
    
    if not all_dfs: return None, "No valid data found"
    master = pd.concat(all_dfs, ignore_index=True)
    return master, f"Loaded {'Full' if full_history else 'Recent'} Database"

def render_custom_period_tab():
    section_card("📂 Historical Data Explorer", "Automated filtering with incremental loading.")
    
    # Check if we need full history
    full_requested = st.toggle("Enable Full Deep-History (Fetches all years)", value=False, key="full_hist_toggle")
    
    master, msg = get_all_statements_master(full_history=full_requested)
    if master is None:
        st.error(msg)
        return
    
    if '_p_date' in master.columns:
        valid_dates = master[master['_p_date'].notna()]
        if valid_dates.empty: st.warning("No dated records found."); return
        
        min_d, max_d = valid_dates['_p_date'].min().date(), valid_dates['_p_date'].max().date()
        
        # ALLOW SELECTION FROM 2022
        abs_min = date(2022, 1, 1)
        f1, f2 = st.columns(2)
        default_start = max(min_d, date.today() - timedelta(days=30))
        start = f1.date_input("Filter From", default_start, min_value=abs_min, max_value=date.today(), key="cust_start")
        end = f2.date_input("Filter To", date.today(), min_value=abs_min, max_value=date.today(), key="cust_end")
        
        # If user picked a date earlier than current master min, hint at full history
        if start < min_d and not full_requested:
            st.info("💡 Selecting dates further back? Toggle 'Enable Full Deep-History' above.")
        
        filtered = master[(master['_p_date'].dt.date >= start) & (master['_p_date'].dt.date <= end)].copy()
        
        if filtered.empty:
            st.warning("No records found for this period in current view.")
            return
            
        mc = {'name': '_p_name', 'cost': '_p_cost', 'qty': '_p_qty', 'date': '_p_date', 'order_id': '_p_order', 'phone': '_p_phone', 'email': '_p_email'}
        dr, sm, tp, tf, bk = process_data(filtered, mc)
        render_dashboard_output(dr, sm, tp, tf, bk, "MasterDB", "Incremental Load")
    else:
        st.error("Date column not found in database to support period filtering.")

def render_customer_pulse_tab():
    section_card("👥 Customer Pulse", "Cross-statement unique customer insights and loyalty metrics.")
    if st.button("🔄 Refresh Pulse Data", use_container_width=True, key="refresh_pulse_btn"): get_all_statements_master.clear(); st.rerun()
    
    # Check if we need full history for Pulse too
    full_hist = st.toggle("Enable Full History Analytics", value=False, key="pulse_hist_toggle")
    
    master, msg = get_all_statements_master(full_history=full_hist)
    if master is None: st.info("Pulse data not yet ready."); return
    
    # Default to last 30 days for Pulse overview
    if '_p_date' in master.columns:
        valid_dates = master[master['_p_date'].notna()]
        if not valid_dates.empty:
            min_d = valid_dates['_p_date'].min().date()
            abs_min = date(2022, 1, 1)
            f1, f2 = st.columns(2)
            default_pulse_start = max(min_d, date.today() - timedelta(days=180))
            p_start = f1.date_input("Pulse From", default_pulse_start, min_value=abs_min, max_value=date.today(), key="pulse_start")
            p_end = f2.date_input("Pulse To", date.today(), min_value=abs_min, max_value=date.today(), key="pulse_end")
            db = master[(master['_p_date'].dt.date >= p_start) & (master['_p_date'].dt.date <= p_end)].copy()
        else:
            db = master.copy()
    else:
        db = master.copy()

    if db.empty: st.warning("No data found for selected pulse range."); return
    
    # Identify unique customers across the ENTIRE current master (Global View)
    master['UID'] = master.get('_p_phone', pd.Series(dtype=str)).fillna(master.get('_p_email', pd.Series(dtype=str))).astype(str).str.strip().str.lower()
    global_unique = master[(master['UID'] != "") & (master['UID'] != "nan") & (master['UID'].notna())]['UID'].nunique()
    
    # Process Filtered View for New/Growth analysis
    db['UID'] = db.get('_p_phone', pd.Series(dtype=str)).fillna(db.get('_p_email', pd.Series(dtype=str))).astype(str).str.strip().str.lower()
    db = db[(db['UID'] != "") & (db['UID'] != "nan") & (db['UID'].notna())]
    
    cust = db.sort_values('_p_date').groupby('UID')['_p_date'].min().reset_index()
    cust.columns = ['UID', 'AcqDate']
    
    # Calculate Acquisition Metrics
    today = date.today()
    this_m = date(today.year, today.month, 1)
    last_m_end = this_m - timedelta(days=1)
    last_m_start = date(last_m_end.year, last_m_end.month, 1)
    
    new_lm = len(cust[(cust['AcqDate'].dt.date >= last_m_start) & (cust['AcqDate'].dt.date <= last_m_end)])
    new_tm = len(cust[cust['AcqDate'].dt.date >= this_m])
    
    m0, m1, m2, m3 = st.columns(4)
    m0.metric("Total Customers (Current Data)", f"{global_unique:,}")
    m1.metric("Range Unique Customers", f"{len(cust):,}")
    m2.metric("New (Last Month)", f"{new_lm:,}")
    m3.metric("New (This Month)", f"{new_tm:,}")
    
    # Visuals
    trend = cust.dropna(subset=['AcqDate']).copy()
    trend['Month'] = trend['AcqDate'].dt.strftime('%Y-%m')
    trend_grp = trend.groupby('Month').size().reset_index(name='New')
    trend_grp['Total (Cumulative)'] = trend_grp['New'].cumsum()
    
    col1, col2 = st.columns(2)
    with col1:
        # Dual-series graph: Line for monthly new and total growth
        fig_trend = px.line(
            trend_grp, 
            x='Month', 
            y=['Total (Cumulative)', 'New'], 
            title="Customer Growth & Acquisition", 
            template="plotly_dark", 
            color_discrete_sequence=['#3b82f6', '#10b981']
        ).update_layout(hovermode="x unified")
        # Add markers for better clarity on data points
        fig_trend.update_traces(mode='lines+markers')
        st.plotly_chart(fig_trend, use_container_width=True)
    
    with col2:
        freq = db.groupby('UID').agg({'_p_order': 'nunique', '_p_cost': 'sum'}).reset_index()
        freq.columns = ['UID', 'Orders', 'Revenue']
        returning = len(freq[freq['Orders'] > 1])
        one_time = len(freq[freq['Orders'] == 1])
        retention_df = pd.DataFrame({'Type': ['Returning', 'One-time'], 'Count': [returning, one_time]})
        st.plotly_chart(px.pie(retention_df, values='Count', names='Type', title="Retention Snapshot", hole=0.5, color_discrete_sequence=['#10b981', '#334155']), use_container_width=True)

    # NEW FEATURES: VIP Leaderboard
    st.markdown("### 🏆 VIP Leaderboard (Top Spenders)")
    vip = freq.sort_values('Revenue', ascending=False).head(10).copy()
    # Mask UID for privacy if needed, but here we show it for internal use
    st.table(vip.style.format({'Revenue': 'TK {:,.0f}'}))
    
    with st.expander("🔍 Customer Geography & Channel Hint"):
        if '_p_phone' in db.columns:
            total_with_phone = len(db[db['_p_phone'].str.startswith('01', na=False)])
            st.info(f"Verified mobile contacts in database: {total_with_phone:,}")
        
        # Acquisition source if multiple tabs
        if '_src_tab' in db.columns:
            source_grp = db.groupby('_src_tab').size().reset_index(name='Records')
            st.plotly_chart(px.bar(source_grp, x='Records', y='_src_tab', orientation='h', title="Records by Statement Source"), use_container_width=True)
