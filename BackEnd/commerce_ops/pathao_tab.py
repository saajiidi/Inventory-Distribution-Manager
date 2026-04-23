import pandas as pd
import streamlit as st

from BackEnd.core.logging_config import log_error
from BackEnd.commerce_ops.persistence import clear_state_keys, save_state
from BackEnd.services.processor import process_orders_dataframe
from BackEnd.commerce_ops.ui_components import (
    render_action_bar,
    render_file_summary,
    render_reset_confirm,
    section_card,
    to_excel_bytes,
)
from BackEnd.utils.io import read_uploaded_file

REQUIRED_COLUMNS = ["Phone (Billing)"]


def _reset_pathao_state():
    clear_state_keys(["pathao_res_df", "pathao_preview_df", "pathao_uploaded_name"])


def render_pathao_tab():
    render_reset_confirm("Pathao Processor", "pathao", _reset_pathao_state)
    # section_card("Pathao Order Processor", "")

    up_pathao = st.file_uploader("", type=["xlsx", "csv"], key="pathao_up")

    fetch_live_clicked = st.button(
        "Pull from Live Dash Data & Auto-Process",
        type="secondary",
        use_container_width=True,
        key="pathao_live",
    )

    preview_df = None
    valid_file = False

    if fetch_live_clicked:
        try:
            from BackEnd.services.hybrid_data_loader import load_hybrid_data

            with st.spinner("Pulling WooCommerce data..."):
                df_live = load_hybrid_data(woocommerce_mode="cache_only")
                source_name = "WooCommerce_Live"

            preview_df = df_live
            st.session_state.pathao_preview_df = preview_df
            st.session_state.pathao_uploaded_name = source_name
            st.session_state.pathao_auto_process = True

            missing = [c for c in REQUIRED_COLUMNS if c not in preview_df.columns]
            valid_file = len(missing) == 0
            st.success("Fetched from WooCommerce perfectly. Processing...")

        except Exception as exc:
            log_error(exc, context="Pathao WooCommerce Pull")
            st.error(f"Failed to fetch WooCommerce data: {exc}")
    elif up_pathao:
        try:
            preview_df = read_uploaded_file(up_pathao)
            st.session_state.pathao_preview_df = preview_df
            st.session_state.pathao_uploaded_name = up_pathao.name
            valid_file = render_file_summary(up_pathao, preview_df, REQUIRED_COLUMNS)
        except Exception as exc:
            log_error(exc, context="Pathao Upload")
            st.error("Failed to read uploaded file.")
    elif st.session_state.get("pathao_preview_df") is not None:
        preview_df = st.session_state.pathao_preview_df
        missing = [c for c in REQUIRED_COLUMNS if c not in preview_df.columns]
        valid_file = len(missing) == 0

    run_clicked, clear_clicked = render_action_bar(
        primary_label="Process orders",
        primary_key="pathao_process_btn",
        secondary_label="Clear upload",
        secondary_key="pathao_clear_btn",
    )

    if st.session_state.get("pathao_auto_process"):
        run_clicked = True
        st.session_state.pathao_auto_process = False

    if clear_clicked:
        _reset_pathao_state()
        st.rerun()

    if run_clicked:
        if preview_df is None or not valid_file:
            st.warning(
                "Upload a valid file or pull from live source before processing."
            )
        else:
            try:
                with st.status("Processing orders...", expanded=True) as status:
                    st.write("Applying standard cleanup and address formatting...")
                    result_df = process_orders_dataframe(preview_df)
                    st.session_state.pathao_res_df = result_df
                    save_state()
                    status.update(
                        label="Processing complete", state="complete", expanded=False
                    )
                st.success(f"Processed {len(result_df)} grouped orders.")
            except Exception as exc:
                log_error(exc, context="Pathao Processor")
                st.error("Pathao processing failed. Check System Logs for details.")

    result_df = st.session_state.get("pathao_res_df")
    if result_df is not None:
        with st.expander("Preview output", expanded=True):
            st.dataframe(result_df, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                "Download repaired file",
                to_excel_bytes(result_df, sheet_name="Pathao"),
                "Pathao_Final.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True,
            )

        with c2:
            if st.button(
                "Generate Verification Links",
                type="secondary",
                use_container_width=True,
            ):
                st.session_state.show_vlink_gen = True

        if st.session_state.get("show_vlink_gen"):
            with st.status("Generating links...", expanded=True):
                import random

                df_v = result_df.copy()
                domain = "https://deencommerce.com/v"
                # Use Phone or Order ID
                links = []
                for _, row in df_v.iterrows():
                    token = f"{random.getrandbits(32):08x}"
                    order_id = str(row.get("Order ID", "VERIFY"))
                    links.append(f"{domain}/verify?id={order_id}&token={token}")
                df_v["Verification Link"] = links
                st.session_state.pathao_vlink_df = df_v
                st.success("Verification links generated!")

            vlink_df = st.session_state.get("pathao_vlink_df")
            if vlink_df is not None:
                st.download_button(
                    "Download Verification Report",
                    to_excel_bytes(vlink_df, sheet_name="Verification"),
                    "Deliveries_Verification.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
