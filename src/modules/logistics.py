import streamlit as st
from src.utils.io import read_uploaded_file
from src.core.persistence import clear_state_keys, save_state
from src.engine.processor import process_orders_dataframe
from src.ui.components import (
    render_action_bar,
    render_file_summary,
    render_reset_confirm,
    render_steps,
    section_card,
    to_excel_bytes,
)
from src.core.sync import load_shared_gsheet, clear_sync_cache
from src.utils.data import find_columns

REQUIRED_COLUMNS = ["Phone (Billing)"]


def _reset_pathao_state():
    clear_state_keys(["pathao_res_df", "pathao_preview_df", "pathao_uploaded_name"])


def render_pathao_tab(guided: bool = True):
    section_card(
        "Pathao Order Processor",
        "Upload order file, validate required columns, generate repaired export.",
    )

    if guided:
        step = 0
        if st.session_state.get("pathao_preview_df") is not None:
            step = 1
        if st.session_state.get("pathao_res_df") is not None:
            step = 2
        render_steps(["Upload", "Validate", "Preview", "Export"], min(step + 1, 3))

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("📡 Sync from Live Stream", use_container_width=True):
            try:
                clear_sync_cache()
                df_sync, source_name, _ = load_shared_gsheet("LastDaySales")
                st.session_state.pathao_preview_df = df_sync
                st.session_state.pathao_uploaded_name = source_name
                st.rerun()
            except Exception as e:
                st.error(f"Sync failed: {e}")
    with c2:
        up_pathao = st.file_uploader(
            "Manual Upload",
            type=["xlsx", "csv"],
            key="pathao_up",
            label_visibility="collapsed",
        )

    preview_df = st.session_state.get("pathao_preview_df")
    valid_file = False

    if up_pathao:
        try:
            preview_df = read_uploaded_file(up_pathao)
            st.session_state.pathao_preview_df = preview_df
            st.session_state.pathao_uploaded_name = up_pathao.name
        except Exception as exc:
            st.error(f"Failed to read file: {exc}")

    if preview_df is not None:
        # Check if we can auto-map required Phone column if it's not exact match
        cols = find_columns(preview_df)
        if "phone" in cols:
            preview_df = preview_df.rename(columns={cols["phone"]: "Phone (Billing)"})

        from collections import namedtuple

        FileMock = namedtuple("FileMock", ["name"])
        mock = FileMock(
            name=st.session_state.get("pathao_uploaded_name", "Data_Source")
        )
        valid_file = render_file_summary(mock, preview_df, REQUIRED_COLUMNS)

    run_clicked, clear_clicked = render_action_bar(
        "Process orders", "pathao_run", "Clear", "pathao_clr"
    )

    if clear_clicked:
        _reset_pathao_state()
        st.rerun()

    if run_clicked and valid_file:
        try:
            with st.status("Processing orders...", expanded=True) as status:
                result_df = process_orders_dataframe(preview_df)
                st.session_state.pathao_res_df = result_df
                save_state()
                status.update(label="Complete", state="complete")
            st.success(f"Processed {len(result_df)} orders.")
        except Exception as exc:
            st.error(f"Processing failed: {exc}")

    res_df = st.session_state.get("pathao_res_df")
    if res_df is not None:
        st.dataframe(res_df, use_container_width=True, hide_index=True)
        st.download_button(
            "Download repaired Pathao file",
            to_excel_bytes(res_df),
            "Pathao_Final.xlsx",
            type="primary",
        )

    render_reset_confirm("pathao", _reset_pathao_state)
