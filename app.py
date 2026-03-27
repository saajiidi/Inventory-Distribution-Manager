import streamlit as st

st.set_page_config(
    page_title="Automation Pivot",
    page_icon="AH",
    layout="wide",
    initial_sidebar_state="expanded",
)


def run_app():
    # Lazy imports keep bootstrap resilient on cloud when a module has runtime incompatibilities.
    from src.modules.ai import render_ai_chat_tab
    from src.modules.inventory import render_distribution_tab
    from src.core.errors import get_logs
    from src.modules.parser import render_fuzzy_parser_tab
    from src.modules.tools import (
        render_daily_summary_export_tab,
        render_data_quality_monitor_tab,
    )
    from src.modules.logistics import render_pathao_tab
    from src.core.persistence import init_state, save_state
    from src.modules.sales import (
        render_custom_period_tab,
        render_live_tab,
        render_customer_pulse_tab,
        render_cache_health_panel,
        render_data_completeness_report
    )
    from src.modules.woo_report import render_wp_api_orders_tab
    from src.ui.components import inject_base_styles, render_header
    from src.modules.whatsapp import render_whatsapp_api_tab
    from src.modules.ecommerce import render_wp_tab

    init_state()
    inject_base_styles()
    
    with st.sidebar:
        st.image("https://cdn.brandfetch.io/deencommerce.com", width=40)
        st.markdown(f"### OPS COMMAND\nAutomation Pivot")
        
        if "main_nav" not in st.session_state:
            st.session_state.main_nav = "📡 Live Sync"
            
        nav_options = [
            "📡 Live Sync",
            "📂 Sales Hub",
            "👥 Customers",
            "🚛 Operations",
            "🛠️ System"
        ]
        
        st.markdown("#### Navigation Hub")
        st.session_state.main_nav = st.radio(
            "Navigation",
            nav_options,
            index=nav_options.index(st.session_state.main_nav) if st.session_state.main_nav in nav_options else 0,
            key="main_nav_radio",
            label_visibility="collapsed"
        )
        
        st.divider()
        st.markdown("#### Operational State")
        
        if st.button("💾 Persist State", use_container_width=True):
            save_state()
            st.toast("✅ State Secured")
            
        st.divider()
        st.markdown("#### Saved Views")
        if st.button("📅 Today's Live", use_container_width=True):
             st.session_state.main_nav = "📡 Live Sync"
             st.rerun()
        if st.button("📈 This Month", use_container_width=True):
             st.session_state.main_nav = "📂 Sales Hub"
             st.session_state.cust_start = date.today().replace(day=1)
             st.session_state.cust_end = date.today()
             st.rerun()
        if st.button("💎 VIP Pulse", use_container_width=True):
             st.session_state.main_nav = "👥 Customers"
             st.rerun()
        if st.button("🚨 Failed Syncs", use_container_width=True):
             st.session_state.main_nav = "🛠️ System"
             # Assuming s_sub is controlled by state in next version or we just go to System
             st.rerun()
        
        st.divider()
        st.markdown("### 🔄 Global Recovery")
        if st.button("Clear Cache & Rerun", use_container_width=True):
            st.cache_data.clear()
            st.session_state.clear()
            st.rerun()

    render_header()

    # SECTION ROUTING
    if st.session_state.main_nav == "📡 Live Sync":
        render_live_tab()
        
    elif st.session_state.main_nav == "📂 Sales Hub":
        render_custom_period_tab()
        
    elif st.session_state.main_nav == "👥 Customers":
        render_customer_pulse_tab()
        
    elif st.session_state.main_nav == "🚛 Operations":
        sub_nav = ["Pathao", "Parser", "Inventory", "WhatsApp", "WooCommerce"]
        sub = st.segmented_control("Operations Hub", sub_nav, selection_mode="single", default="Pathao")
        
        if sub == "Pathao": render_pathao_tab(guided=False)
        elif sub == "Parser": render_fuzzy_parser_tab(guided=False)
        elif sub == "Inventory": render_distribution_tab(search_q=st.session_state.get("inv_matrix_search", ""), guided=False)
        elif sub == "WhatsApp": render_wp_tab(guided=False)
        elif sub == "WooCommerce": render_wp_api_orders_tab()
        
    elif st.session_state.main_nav == "🛠️ System":
        s_nav = ["Health", "Exports", "AI Analyst", "Logs"]
        s_sub = st.segmented_control("System Tools", s_nav, selection_mode="single", default="Health")
        
        if s_sub == "Health":
            render_cache_health_panel()
            st.divider()
            render_data_completeness_report()
            st.divider()
            render_data_quality_monitor_tab()
        elif s_sub == "Exports":
            render_daily_summary_export_tab()
        elif s_sub == "AI Analyst":
            render_ai_chat_tab()
        elif s_sub == "Logs":
            logs = get_logs()
            if logs:
                for entry in reversed(logs):
                    st.error(f"[{entry['timestamp']}] {entry['context']}: {entry['error']}")
            else:
                st.success("System core stable. 0 anomalies detected.")

    # Footer
    st.markdown("---")
    st.caption("© 2026 DEEN COMMERCE • Powered by Antigravity AI Engine")


if __name__ == "__main__":
    try:
        run_app()
    except Exception as exc:
        from src.core.errors import log_error
        log_error(exc, context="Main App Bootstrap")
        st.error("Critical: Application failed to render.")
        st.code(str(exc))
