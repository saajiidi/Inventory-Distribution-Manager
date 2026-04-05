import os

import pandas as pd
import streamlit as st

from FrontEnd.utils.error_handler import ERROR_LOG_FILE, LATEST_PROMPT_FILE, get_logs, log_error



def clear_error_logs():
    """Clears all logged errors."""
    if os.path.exists(ERROR_LOG_FILE):
        os.remove(ERROR_LOG_FILE)
    if os.path.exists(LATEST_PROMPT_FILE):
        os.remove(LATEST_PROMPT_FILE)
    st.success("Error logs cleared.")
    st.rerun()



def render_system_health_tab():
    """Renders the System Health and Error Resolution hub."""
    st.header("System Health & Error Resolver")
    st.info(
        "This module captures runtime exceptions, preserves structured diagnostics, and stores AI-ready prompts for future fixes."
    )

    logs = get_logs()

    if not logs:
        st.success("No errors detected. System is running smoothly.")
        if st.button("Simulate Test Error"):
            try:
                1 / 0
            except Exception as e:
                log_error(e, context="Test Simulation", details={"trigger": "manual button"})
                st.toast("Test error logged.")
                st.rerun()
        return

    df_errors = pd.DataFrame(logs).sort_values("timestamp", ascending=False)
    st.subheader(f"Reported Issues ({len(df_errors)})")

    action_col, latest_col = st.columns([1, 3])
    with action_col:
        if st.button("Clear All Logs", use_container_width=True):
            clear_error_logs()
    with latest_col:
        if os.path.exists(LATEST_PROMPT_FILE):
            st.caption(f"Latest AI-ready prompt file: `{LATEST_PROMPT_FILE}`")

    for idx, entry in enumerate(reversed(logs)):
        title = f"{entry.get('timestamp', '')} | {entry.get('context', 'General')} | {entry.get('error', '')[:80]}"
        with st.expander(title):
            st.write(f"**Error Type:** {entry.get('error_type', 'Unknown')}")
            st.code(entry.get("error", ""), language="text")

            details = entry.get("details") or {}
            if details:
                st.caption("Details")
                st.json(details)

            environment = entry.get("environment") or {}
            if environment:
                st.caption("Environment")
                st.json(environment)

            st.caption("Traceback")
            st.code(entry.get("traceback", ""), language="python")

            prompt_payload = entry.get("fix_prompt") or ""
            st.markdown("---")
            st.subheader("AI Resolver Prompt")
            st.code(prompt_payload, language="markdown")

            prompt_file = entry.get("prompt_file")
            if prompt_file:
                st.caption(f"Saved prompt file: `{prompt_file}`")

            if st.button(f"Mark as Reviewed #{idx}", key=f"review_{idx}"):
                st.toast("Error reviewed. Prompt is ready for reuse.")

    st.divider()
    st.subheader("All System Logs")
    visible_cols = [col for col in ["timestamp", "context", "error_type", "error"] if col in df_errors.columns]
    st.dataframe(df_errors[visible_cols], use_container_width=True, hide_index=True)
