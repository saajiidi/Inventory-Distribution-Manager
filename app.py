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
    from src.ui.animations import render_bike_animation
    from src.modules.inventory import render_distribution_tab
    from src.core.errors import get_logs, log_error
    from src.modules.parser import render_fuzzy_parser_tab
    from src.modules.tools import (
        render_daily_summary_export_tab,
        render_data_quality_monitor_tab,
    )
    from src.modules.logistics import render_pathao_tab
    from src.core.persistence import init_state, save_state
    from src.modules.sales import (
        get_custom_report_tab_label,
        render_custom_period_tab,
        render_live_tab,
    )
    from src.modules.woo_report import (
        get_wp_api_orders_tab_label,
        render_wp_api_orders_tab,
    )
    from src.ui.components import (
        inject_base_styles,
        render_header,
        sample_file_download,
        section_card,
    )
    from src.ui.config import PRIMARY_NAV
    from src.modules.whatsapp import render_whatsapp_api_tab
    from src.modules.ecommerce import render_wp_tab

    init_state()
    inject_base_styles()
    st.session_state._dashboard_dialog_opened_this_run = False

    with st.sidebar:
        st.markdown("### 🎛️ SYSTEM COCKPIT")
        st.session_state.show_animation = st.toggle(
            "Motion Effects",
            value=st.session_state.get("show_animation", False),
        )

        if st.button("💾 Persist Session State", use_container_width=True):
            save_state()
            st.toast("✅ State Secured", icon="💾")

    if st.session_state.get("show_animation"):
        render_bike_animation()

    render_header()

    # Optimized Command Center Navigation
    primary_nav = [
        "📡 Live Stream",
        get_custom_report_tab_label(),
        "👥 Customer Pulse",
        "🚛 Orders & Logistics",
        "🏠 InventoryHub",
        "💬 WhatsApp",
        get_wp_api_orders_tab_label()
    ]
    tabs = st.tabs(primary_nav)

    # 0. 📡 LIVE STREAM DASHBOARD
    with tabs[0]:
        render_live_tab()

    # 1. 📂 TOTAL SALES (HISTORICAL)
    with tabs[1]:
        render_custom_period_tab()

    # 2. 👥 CUSTOMER PULSE
    with tabs[2]:
        from src.modules.sales import render_customer_pulse_tab
        render_customer_pulse_tab()

    # 3. 🚛 LOGISTICS & ORDERS
    with tabs[3]:
        o_p, o_f = st.tabs(["🚚 Pathao Processor", "🔍 Delivery Text Parser"])
        with o_p:
            render_pathao_tab(guided=False)
        with o_f:
            render_fuzzy_parser_tab(guided=False)

    # 4. 📦 INVENTORY HUB
    with tabs[4]:
        render_distribution_tab(
            search_q=st.session_state.get("inv_matrix_search", ""),
            guided=False,
        )

    # 5. ☎️ WHATSAPP CHANNEL
    with tabs[5]:
        render_wp_tab(guided=False)

    # 6. 🌐 WOOCOMMERCE SYNC
    with tabs[6]:
        render_wp_api_orders_tab()

    # ➕ UTILITY DRAWER
    with st.expander("🛠️ ADVANCED UTILITIES", expanded=False):
        u1, u2, u3, u4 = st.tabs(["📜 Logs", "🧪 Data Health", "📅 Daily Summary", "🚀 Experimental"])
        with u1:
            logs = get_logs()
            if logs:
                for entry in reversed(logs):
                    st.error(f"[{entry['timestamp']}] {entry['context']}: {entry['error']}")
            else:
                st.success("System clear. No anomalies detected.")
        with u2: render_data_quality_monitor_tab()
        with u3: render_daily_summary_export_tab()
        with u4:
            x1, x2 = st.tabs(["🧠 AI Analyst", "📲 WhatsApp Broadcast"])
            with x1: render_ai_chat_tab()
            with x2: render_whatsapp_api_tab()


try:
    run_app()
except Exception as exc:
    # Failsafe to prevent full redacted crash pages on Streamlit Cloud.
    from src.core.errors import log_error

    log_error(exc, context="App Bootstrap")
    st.error("Application failed to render. Check 'More Tools -> System Logs' for details.")
    st.code(str(exc))
