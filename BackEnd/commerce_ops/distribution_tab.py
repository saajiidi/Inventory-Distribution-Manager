import io

import pandas as pd
import plotly.express as px
import streamlit as st

from BackEnd.core.logging_config import log_error
from BackEnd.commerce_ops.persistence import clear_state_keys, save_state
from BackEnd.commerce_ops.ui_components import (
    render_action_bar,
    render_reset_confirm,
    section_card,
)
from BackEnd.commerce_ops.ui_config import INVENTORY_LOCATIONS
from BackEnd.commerce_ops import core as inv_core
from BackEnd.utils.io import read_uploaded_file, to_excel_bytes


def _reset_inventory_state():
    clear_state_keys(
        ["inv_res_data", "inv_active_l", "inv_t_col", "inv_master_df_live"]
    )


def _render_upload_summary(master_df, title_col):
    c1, c2 = st.columns(2)
    c1.metric("Master rows", 0 if master_df is None else len(master_df))
    c2.metric("Title column", title_col if title_col else "Not detected")


def render_distribution_tab(search_q):
    render_reset_confirm("Inventory Distribution", "inventory", _reset_inventory_state)
    master_file = st.file_uploader("", type=["xlsx", "csv"], key="inv_up")

    fetch_live_clicked = st.button(
        "Pull from Live Dash Data & Auto-Analyze",
        type="secondary",
        use_container_width=True,
        key="dist_live",
    )

    loc_files = {}
    loc_cols = st.columns(len(INVENTORY_LOCATIONS))
    for i, loc in enumerate(INVENTORY_LOCATIONS):
        with loc_cols[i]:
            uploaded = st.file_uploader(
                f"{loc}", key=f"inv_l_{loc}", type=["xlsx", "csv"]
            )
            if uploaded:
                loc_files[loc] = uploaded

    master_df = None
    title_col = None
    sku_col = None

    if fetch_live_clicked:
        try:
            from BackEnd.services.hybrid_data_loader import load_hybrid_data

            with st.spinner("Pulling WooCommerce data..."):
                df_live = load_hybrid_data(
                    start_date=(pd.Timestamp.now() - pd.Timedelta(days=7)).strftime("%Y-%m-%d"),
                    woocommerce_mode="cache_only"
                )

            master_df = df_live
            st.session_state.inv_master_df_live = master_df
            st.session_state.inv_auto_analyze = True

            _, _, title_col, sku_col = inv_core.identify_columns(master_df)

            if not title_col:
                st.error(
                    "Could not detect an item title/name column in the WooCommerce data."
                )
            else:
                st.success("Fetched from WooCommerce perfectly. Analyzing...")
        except Exception as exc:
            log_error(exc, context="Inventory WooCommerce Pull")
            st.error(f"Failed to fetch WooCommerce data: {exc}")
    elif master_file:
        try:
            master_df = read_uploaded_file(master_file)
            st.session_state.inv_master_df_live = master_df
            _, _, title_col, sku_col = inv_core.identify_columns(master_df)
            _render_upload_summary(master_df, title_col)
            if not title_col:
                st.error(
                    "Could not detect an item title/name column in the master list."
                )
            else:
                st.success("Validation passed. Ready to run analysis.")
        except Exception as exc:
            log_error(exc, context="Inventory Upload")
            st.error("Failed to read master stock list.")
    elif st.session_state.get("inv_master_df_live") is not None:
        master_df = st.session_state.inv_master_df_live
        _, _, title_col, sku_col = inv_core.identify_columns(master_df)

    analyze_clicked, clear_clicked = render_action_bar(
        primary_label="Analyze distribution",
        primary_key="inv_analyze_btn",
        secondary_label="Clear inventory data",
        secondary_key="inv_clear_btn",
    )

    if st.session_state.get("inv_auto_analyze"):
        analyze_clicked = True
        st.session_state.inv_auto_analyze = False

    if clear_clicked:
        _reset_inventory_state()
        st.rerun()

    if analyze_clicked:
        if master_df is None or not title_col:
            st.warning(
                "Upload a valid master stock list or pull from live source before analysis."
            )
        else:
            try:
                inventory_map, warnings, _, sku_map = (
                    inv_core.load_inventory_from_uploads(loc_files)
                )
                if warnings:
                    for warning in warnings:
                        st.warning(warning)

                result_df, _ = inv_core.add_stock_columns_from_inventory(
                    master_df,
                    title_col,
                    inventory_map,
                    INVENTORY_LOCATIONS,
                    sku_col,
                    sku_map,
                )

                st.session_state.inv_res_data = result_df
                st.session_state.inv_active_l = INVENTORY_LOCATIONS
                st.session_state.inv_t_col = title_col
                save_state()
                st.success("Distribution analysis complete.")
            except Exception as exc:
                log_error(exc, context="Inventory Analyze")
                st.error("Distribution analysis failed.")

    if st.session_state.get("inv_res_data") is not None:
        df = st.session_state.inv_res_data.copy()
        title_key = st.session_state.inv_t_col
        active_locations = st.session_state.inv_active_l

        if search_q:
            df = df[
                df[title_key]
                .astype(str)
                .str.lower()
                .str.contains(search_q.lower(), na=False)
            ]

        st.dataframe(df, use_container_width=True)

        st.download_button(
            "Download distribution report",
            to_excel_bytes(df, sheet_name="Distribution"),
            "Stock_Distribution.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary",
        )
