import os

import streamlit as st

from FrontEnd.utils.config import APP_TITLE, PRIMARY_NAV
from FrontEnd.utils.error_handler import ERROR_LOG_FILE, get_logs, log_error
from FrontEnd.utils.state import init_state, save_state


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
    from FrontEnd.components import render_sidebar_branding

    with st.sidebar:
        render_sidebar_branding()
        
        # Polish: Clean spacing and user-friendly UX for sidebar
        st.markdown("<br>", unsafe_allow_html=True)
        st.info("Explore the primary navigation tabs above to switch between different operational, customer, and product views.")
        
        st.markdown("<br><br>", unsafe_allow_html=True)
        # Hide developer/debug interactions in an expander for a much cleaner production look
        with st.expander("🛠️ Developer & Admin Tools", expanded=False):
            if st.button("Save Session State", use_container_width=True):
                save_state()
                st.success("Session state saved.")

            st.divider()
            registered = st.session_state.get("registered_resets", {})
            if registered:
                tool_to_wipe = st.selectbox("Select tool to reset", list(registered.keys()))
                if st.button("Reset Tool Now", use_container_width=True, type="secondary"):
                    registered[tool_to_wipe]["fn"]()
                    st.session_state.confirm_tool_reset = False
                    st.success("Selected tool state was reset.")
                    st.rerun()

            st.divider()
            if st.button("Full System Reset", use_container_width=True, type="secondary"):
                st.session_state.confirm_app_reset = True

            if st.session_state.get("confirm_app_reset"):
                st.warning("This clears saved session state and all active tool data for this app session.")
                c1, c2 = st.columns(2)
                if c1.button("Yes", type="primary", use_container_width=True):
                    from FrontEnd.utils.state import STATE_FILE

                    if os.path.exists(STATE_FILE):
                        os.remove(STATE_FILE)
                    st.session_state.clear()
                    st.rerun()
                if c2.button("No", use_container_width=True):
                    st.session_state.confirm_app_reset = False
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
    from FrontEnd.pages import get_primary_pages

    pages = get_primary_pages()
    nav_tabs = st.tabs(PRIMARY_NAV)
    for tab, page in zip(nav_tabs, pages):
        with tab:
            page.render()


def run_app():
    from FrontEnd.components import inject_base_styles, render_footer, render_header

    init_state()
    inject_base_styles()
    _render_workspace_sidebar()
    render_header()
    _render_primary_navigation()
    render_footer()


try:
    run_app()
except Exception as exc:
    log_error(exc, context="App Bootstrap")
    st.error("Application failed to render. Check 'More Tools -> System Logs' for details.")
    st.code(str(exc))
