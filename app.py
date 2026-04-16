import os
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
import numpy as np
from streamlit_autorefresh import st_autorefresh

from FrontEnd.utils.config import APP_TITLE, APP_DATA_START_DATE
from FrontEnd.utils.error_handler import ERROR_LOG_FILE, get_logs, log_error
from FrontEnd.utils.state import init_state, save_state
from FrontEnd.components import ui


_original_dataframe = st.dataframe


def _numbered_dataframe(data, *args, **kwargs):
    try:
        import pandas as pd

        if isinstance(data, pd.DataFrame) or isinstance(data, pd.Series):
            copied = data.copy()
            if len(copied) > 0:
                copied.index = range(1, len(copied) + 1)
            return _original_dataframe(copied, *args, **kwargs)
    except Exception:
        pass
    return _original_dataframe(data, *args, **kwargs)


st.dataframe = _numbered_dataframe

st.set_page_config(
    page_title=APP_TITLE,
    page_icon="AP",
    layout="wide",
    initial_sidebar_state="expanded",
)


def _clear_error_logs():
    if os.path.exists(ERROR_LOG_FILE):
        os.remove(ERROR_LOG_FILE)


def _render_workspace_sidebar():
    with st.sidebar:
        ui.sidebar_branding()
        
        if "time_window" not in st.session_state:
            st.session_state.time_window = "Last Month"

        st.markdown('<div class="sidebar-group-label">⏱️ Operational Range</div>', unsafe_allow_html=True)
        st.select_slider(
            "Time Window",
            options=[
                "Last Day", "Last 3 Days", "Last 7 Days", "Last 15 Days", "Last Month",
                "Last 3 Months", "Last Quarter", "Last Half Year", "Last 9 Months", "Last Year", "Custom Date Range"
            ],
            key="time_window", label_visibility="collapsed"
        )
        

        # Smart Shift Logic
        now = datetime.now()
        shift_cutoff = now.replace(hour=17, minute=30, second=0, microsecond=0)
        is_after_cutoff = now >= shift_cutoff
        
        shift_label = "Night Shift (Post-Cutoff)" if is_after_cutoff else "Day Shift (Processing)"
        st.markdown(f'<div style="font-size:0.75rem; color:var(--on-surface-variant); opacity:0.8; margin-top:-10px; margin-bottom:15px; padding-left:4px;">🔄 Current: <b>{shift_label}</b></div>', unsafe_allow_html=True)
        
        cycle_start = shift_cutoff if is_after_cutoff else (shift_cutoff - timedelta(days=1))
        
        if st.session_state.get("time_window") == "Custom Date Range":
            col1, col2 = st.columns(2)
            with col1:
                st.date_input("Start Date", value=datetime.now().date() - timedelta(days=30), min_value=APP_DATA_START_DATE, max_value=datetime.now().date(), key="wc_sync_start_date")
                
            with col2:
                st.date_input("End Date", value=datetime.now().date(), min_value=APP_DATA_START_DATE, max_value=datetime.now().date(), key="wc_sync_end_date")
                
        st.divider()



        # 1. Fetch live metrics for alerts
        stats = {"proc": 0, "low": 0}
        if "dashboard_data" in st.session_state:
            data = st.session_state.dashboard_data
            df_raw = data.get("sales", pd.DataFrame())
            stock = data.get("stock", pd.DataFrame())
            if not df_raw.empty:
                stats["proc"] = df_raw[df_raw["order_status"].str.lower() == "processing"]["order_id"].nunique()
            if not stock.empty:
                stats["low"] = len(stock[stock["Stock Quantity"] <= 5])

        # 2. Unified Navigation (Single Stack for Smooth Performance)
        st.markdown('<div class="sidebar-group-label">⚡ NAVIGATION HUB</div>', unsafe_allow_html=True)
        
        ins_label = f"📦 Stock Insight [{stats['low']}!] " if stats['low'] > 0 else "📦 Stock Insight"
        
        nav_map = {
            "💎 Sales Overview": "💎 Sales Overview",
            "📥 Sales Data Ingestion": "📥 Sales Data Ingestion",
            "📊 Traffic & Acquisition": "📊 Traffic & Acquisition",
            "👥 Customer Insight": "👥 Customer Insight",
            "🔄 Returns & Net Sales": "🔄 Returns & Net Sales",
            ins_label: "📦 Stock Insight",
            "🚀 Data Pilot": "🚀 Data Pilot",
            "🛡️ Data Trust": "🛡️ Data Trust"
        }

        # Initialize section if not set
        if "active_section" not in st.session_state:
            st.session_state.active_section = "💎 Sales Overview"

        # Find the index of the active section in the labels list
        # Map values back to display labels to find the index
        reverse_map = {v: k for k, v in nav_map.items()}
        current_label = reverse_map.get(st.session_state.active_section, "💎 Sales Overview")
        
        labels = list(nav_map.keys())
        try:
            current_index = labels.index(current_label)
        except ValueError:
            current_index = 0

        selection = st.radio(
            "Navigation",
            labels,
            index=current_index,
            key="main_nav",
            label_visibility="collapsed"
        )
        
        # Update formal state
        st.session_state.active_section = nav_map[selection]

        st.markdown('<div class="sidebar-group-label">⚙️ Workspace Status</div>', unsafe_allow_html=True)
        st.markdown(
            """
            <div class="heartbeat-card">
                <div class="pulse-text">
                    <span class="heartbeat-dot"></span>
                    Operational Cell: Active
                </div>
                <div style="margin-top: 10px;">
                    <a href="https://deen-ops.streamlit.app/" target="_blank" style="text-decoration: none; color: var(--primary); font-size: 0.8rem; font-weight: 600; display: flex; align-items: center; gap: 6px;">
                        🔗 <span>DEEN OPS Terminal</span>
                    </a>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

        st.divider()

        # 3. Global Sync Trigger
        if st.button("🔄 Sync Operations", type="primary", use_container_width=True):
            st.session_state["global_sync_request"] = True
            st.rerun()
            
        # 4. Global Export & Utilities
        with st.expander("🛠️ Advanced Controls"):
            auto_refresh = st.toggle("Auto-Refresh (15m)", value=False)
            if auto_refresh:
                st_autorefresh(interval=15 * 60 * 1000, key="global_refresh")
                
            if "dashboard_data" in st.session_state:
                st.markdown("**🔌 Power BI Connector**", help="Generates an optimized Star Schema (Facts & Dimensions) for DAX modeling.")
                
                if "pbi_export_bytes" not in st.session_state:
                    if st.button("Generate Power BI Matrix", use_container_width=True):
                        with st.spinner("Extracting Facts & Dimensions..."):
                            from BackEnd.services.powerbi_export import build_star_schema
                            returns_df = st.session_state.get("returns_data", None)
                            excel_bytes, _ = build_star_schema(st.session_state.dashboard_data, returns_df=returns_df)
                            st.session_state.pbi_export_bytes = excel_bytes
                            st.rerun()
                else:
                    st.download_button(
                        label="📥 Download Star Schema (.xlsx)",
                        data=st.session_state.pbi_export_bytes,
                        file_name=f"deen_powerbi_schema_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        type="primary"
                    )
                    if st.button("🔄 Clear Schema Cache", use_container_width=True):
                        del st.session_state.pbi_export_bytes
                        st.rerun()

                st.divider()
                csv = st.session_state.dashboard_data["sales"].to_csv(index=False)
                st.download_button("📥 Raw Dashboard Export (CSV)", csv, "deen_analysis_export.csv", "text/csv", use_container_width=True)

        # 5. Anomaly Detection (Toasts)
        if "dashboard_data" in st.session_state:
            df_curr = st.session_state.dashboard_data["sales"]
            refund_count = len(df_curr[df_curr["order_status"].str.lower() == "refunded"])
            if refund_count > 5:
                st.toast("🚨 Unusual refund activity detected in the current window!", icon="⚠️")

        # 4. System Heartbeat Widget (Suggestion 3)
        sync_time = "Just now"
        if st.session_state.get("live_sync_time"):
            diff = datetime.now() - st.session_state.live_sync_time
            mins = int(diff.total_seconds() / 60)
            sync_time = f"{mins}m ago" if mins > 0 else "Just now"

        st.markdown(f"""
            <div class="heartbeat-card">
                <div class="pulse-text">
                    <div class="heartbeat-dot"></div>
                    SYSTEM HEARTBEAT
                </div>
                <div style="font-size:0.8rem; color:var(--on-surface-variant); margin-top:8px;">
                    <b>Sync Fidelity:</b> {sync_time}<br>
                    <span style="opacity:0.7;">Sajid | Executive Data Stream</span>
                </div>
            </div>
        """, unsafe_allow_html=True)

        # 4. Compact Utils
        with st.expander("🛠️ System Utils", expanded=False):
            if st.button("Full System Reset", use_container_width=True):
                from FrontEnd.utils.state import STATE_FILE
                if os.path.exists(STATE_FILE): os.remove(STATE_FILE)
                st.session_state.clear()
                st.rerun()
            
            _render_system_logs()


def _render_system_logs():
    with st.sidebar.expander("System Logs", expanded=False):
        logs = get_logs()
        if not logs:
            st.info("No system events logged.")
            return

        for log in reversed(logs[-8:]):
            st.caption(f"**{log.get('timestamp')}** | {log.get('context')}")
            st.text(log.get("error"))
            st.divider()

        if st.button("Clear logs", use_container_width=True):
            _clear_error_logs()
            st.rerun()


def _render_primary_navigation():
    from FrontEnd.pages.dashboard import render_intelligence_hub_page
    render_intelligence_hub_page()


def run_app():
    init_state()
    ui.setup_theme()
    _render_workspace_sidebar()
    ui.page_header()
    _render_primary_navigation()
    ui.page_footer()


try:
    run_app()
except Exception as exc:
    log_error(exc, context="App Bootstrap")
    st.error("Application failed to render. Check 'More Tools -> System Logs' for details.")
    st.code(str(exc))
