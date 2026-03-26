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
    )
    from src.modules.woo_report import (
        render_wp_api_orders_tab,
    )
    from src.ui.components import (
        inject_base_styles,
        render_header,
    )
    from src.modules.whatsapp import render_whatsapp_api_tab
    from src.modules.ecommerce import render_wp_tab

    init_state()
    inject_base_styles()
    st.session_state._dashboard_dialog_opened_this_run = False

    with st.sidebar:
        st.markdown("### 🎛️ SYSTEM COCKPIT")

        # Theme Control
        st.radio(
            "Visual Protocol",
            ["Dark Mode", "Light Mode"],
            key="app_theme",
            horizontal=True,
        )

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

    # Consolidated Command Center Navigation with Static Identifiers
    primary_nav = [
        "📡 Live",
        "📂 Sales Hub",
        "👥 Pulse",
        "🚛 Operations",
        "🛠️ System Tools",
    ]
    tabs = st.tabs(primary_nav)

    # 1. 📡 LIVE STREAM
    with tabs[0]:
        render_live_tab()

    # 2. 📂 HISTORICAL SALES
    with tabs[1]:
        render_custom_period_tab()

    # 3. 👥 CUSTOMER PULSE
    # 3. CUSTOMER PULSE
    with tabs[2]:
        from src.modules.sales import render_customer_pulse_tab

        render_customer_pulse_tab()

    # 4. OPERATIONS HUB (NESTED)
    with tabs[3]:
        sub_nav = [
            "Pathao",
            "Parser",
            "Inventory",
            "WhatsApp Hub",
            "WP Orders",
        ]
        sub_tabs = st.tabs(sub_nav)

        with sub_tabs[0]:
            render_pathao_tab(guided=False)

        with sub_tabs[1]:
            render_fuzzy_parser_tab(guided=False)

        with sub_tabs[2]:
            render_distribution_tab(
                search_q=st.session_state.get("inv_matrix_search", ""),
                guided=False,
            )

        with sub_tabs[3]:
            render_wp_tab(guided=False)

        with sub_tabs[4]:
            render_wp_api_orders_tab()

    # 5. 🛠️ SYSTEM TOOLS (MOVED TO MAIN TABS)
    with tabs[4]:
        utils_nav = ["📜 Logs", "🧪 Health", "📅 Exports", "🚀 Experiments"]
        utils_tabs = st.tabs(utils_nav)

        with utils_tabs[0]:
            logs = get_logs()
            if logs:
                for entry in reversed(logs):
                    st.error(
                        f"[{entry['timestamp']}] {entry['context']}: {entry['error']}"
                    )
            else:
                st.success("System core stable. 0 anomalies detected.")

        with utils_tabs[1]:
            render_data_quality_monitor_tab()

        with utils_tabs[2]:
            render_daily_summary_export_tab()

        with utils_tabs[3]:
            exp_tabs = st.tabs(["🧠 AI Analyst", "📲 Broadcast"])
            with exp_tabs[0]:
                render_ai_chat_tab()
            with exp_tabs[1]:
                render_whatsapp_api_tab()

    # Sidebar Recovery
    with st.sidebar:
        st.divider()
        st.markdown("### 🔄 Global Recovery")
        if st.button("Clear Cache & Rerun", use_container_width=True):
            st.cache_data.clear()
            st.session_state.clear()
            st.rerun()

    # Footer
    st.markdown("---")
    c1, c2 = st.columns([2, 1])
    with c1:
        st.caption("© 2026 DEEN COMMERCE • Automation Pivot")
    with c2:
        st.caption("Powered by Antigravity AI Engine")


if __name__ == "__main__":
    try:
        run_app()
    except Exception as exc:
        from src.core.errors import log_error

        log_error(exc, context="Main App Bootstrap")
        st.error("Critical: Application failed to render.")
        st.code(str(exc))
