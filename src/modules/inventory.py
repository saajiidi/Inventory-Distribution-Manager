import pandas as pd
import plotly.express as px
import streamlit as st

from src.utils.io import read_uploaded_file
from src.core.persistence import clear_state_keys, save_state
from src.ui.components import (
    render_action_bar,
    render_reset_confirm,
    render_steps,
    section_card,
    render_mini_uploader,
)
from src.core.sync import load_shared_gsheet, clear_sync_cache
from src.ui.config import INVENTORY_LOCATIONS
from src.utils.data import find_columns
from src.engine.inventory import core as inv_core


def _reset_inventory_state():
    clear_state_keys(
        [
            "inv_res_data",
            "inv_active_l",
            "inv_t_col",
            "inv_master_df",
            "inv_master_name",
        ]
    )


def render_distribution_tab(search_q, guided: bool = True):
    section_card(
        "Distribution Hub",
        "Analyze stock mapping, monitor low-stock risk, and export pick manifests.",
    )

    if guided:
        step = 0
        if st.session_state.get("inv_res_data") is not None:
            step = 2
        render_steps(["Upload", "Validate", "Analyze", "Export"], step)

    m_tab, i_tab, p_tab = st.tabs(["Matrix Analyzer", "Insights", "Pick List"])

    with m_tab:
        c_sync1, c_sync2 = st.columns([1, 1])
        with c_sync1:
            if st.button(
                "📡 Sync Master Stock", use_container_width=True, key="inv_sync_btn"
            ):
                try:
                    clear_sync_cache()
                    df_sync, source_name, _ = load_shared_gsheet("LastDaySales")
                    st.session_state.inv_master_df = df_sync
                    st.session_state.inv_master_name = source_name
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")
        with c_sync2:
            master_file = st.file_uploader(
                "Master Stock List",
                type=["xlsx", "csv"],
                key="inv_up",
                label_visibility="collapsed",
            )

        # Outlet Overrides (Mini Icons)
        st.markdown(
            "<div style='margin-top:30px; text-align:center; font-family:JetBrains Mono; color:var(--text-secondary); letter-spacing:0.2em; font-size:0.8rem;'>▼ OUTLET STOCK COMMAND</div>",
            unsafe_allow_html=True,
        )
        loc_files = {}
        cols = st.columns(len(INVENTORY_LOCATIONS))
        for i, loc in enumerate(INVENTORY_LOCATIONS):
            with cols[i]:
                up = render_mini_uploader(loc, key=f"inv_l_{loc}")
                if up:
                    loc_files[loc] = up

        master_df = st.session_state.get("inv_master_df")
        title_col, sku_col = None, None

        if master_file:
            try:
                master_df = read_uploaded_file(master_file)
                st.session_state.inv_master_df = master_df
                st.session_state.inv_master_name = master_file.name
            except Exception:
                st.error("Failed to read master file.")

        if master_df is not None:
            # Auto-detect columns using shared utils
            mc = find_columns(master_df)
            title_col = mc.get("name")
            sku_col = mc.get(
                "order_id"
            )  # Fallback or specific SKU detection? inv_core has its own.
            # We use inv_core for specialized inventory logic.
            _, _, tc, sc = inv_core.identify_columns(master_df)
            title_col, sku_col = tc or title_col, sc or sku_col

            from src.ui.components import render_metric_hud

            c1, c2 = st.columns(2)
            with c1:
                render_metric_hud("Master Rows", f"{len(master_df):,}", "📦")
            with c2:
                render_metric_hud("Detected Title", title_col or "N/A", "🏷️")

        run_btn, clr_btn = render_action_bar(
            "Analyze distribution", "inv_run", "Clear", "inv_clr"
        )

        if clr_btn:
            _reset_inventory_state()
            st.rerun()

        if run_btn:
            if master_df is None or not title_col:
                st.warning("Upload or Sync a master stock list first.")
            else:
                try:
                    inv_map, warns, _, smap = inv_core.load_inventory_from_uploads(
                        loc_files
                    )
                    for w in warns:
                        st.warning(w)
                    res, _ = inv_core.add_stock_columns_from_inventory(
                        master_df,
                        title_col,
                        inv_map,
                        INVENTORY_LOCATIONS,
                        sku_col,
                        smap,
                    )
                    (
                        st.session_state.inv_res_data,
                        st.session_state.inv_active_l,
                        st.session_state.inv_t_col,
                    ) = (res, INVENTORY_LOCATIONS, title_col)
                    save_state()
                    st.success("Analysis complete.")
                except Exception as e:
                    st.error(f"Analysis failed: {e}")

        res_df = st.session_state.get("inv_res_data")
        if res_df is not None:
            if search_q:
                tc = st.session_state.inv_t_col
                res_df = res_df[
                    res_df[tc]
                    .astype(str)
                    .str.lower()
                    .str.contains(search_q.lower(), na=False)
                ]
            st.dataframe(res_df, use_container_width=True, hide_index=True)
            from src.ui.components import to_excel_bytes

            st.download_button(
                "Download distribution report",
                to_excel_bytes(res_df),
                "Stock_Distribution.xlsx",
                type="primary",
                key="inv_ex_btn",
            )

    with i_tab:
        if st.session_state.get("inv_res_data") is not None:
            df, locs, tc = (
                st.session_state.inv_res_data,
                st.session_state.inv_active_l,
                st.session_state.inv_t_col,
            )
            c1, c2 = st.columns(2)
            with c1:
                melt = df.melt(
                    id_vars=[tc], value_vars=locs, var_name="Loc", value_name="Stock"
                )
                st.plotly_chart(
                    px.density_heatmap(
                        melt,
                        x="Loc",
                        y=tc,
                        z="Stock",
                        color_continuous_scale="RdYlGn",
                        title="Stock Heatmap",
                    ),
                    use_container_width=True,
                )
            with c2:
                tots = df[locs].apply(pd.to_numeric, errors="coerce").fillna(0).sum()
                st.plotly_chart(
                    px.pie(
                        values=tots.values,
                        names=tots.index,
                        title="Location Distribution",
                        hole=0.4,
                    ),
                    use_container_width=True,
                )
        else:
            st.info("Run analysis to see insights.")

    with p_tab:
        if st.session_state.get("inv_res_data") is not None:
            df, locs, tc = (
                st.session_state.inv_res_data,
                st.session_state.inv_active_l,
                st.session_state.inv_t_col,
            )
            if "Dispatch Suggestion" in df.columns:
                for loc in locs:
                    loc_df = df[df["Dispatch Suggestion"] == loc]
                    if not loc_df.empty:
                        st.markdown(f"**{loc}**")
                        st.dataframe(
                            loc_df.groupby(tc).size().reset_index(name="Units"),
                            use_container_width=True,
                            hide_index=True,
                        )
        else:
            st.info("Run analysis to see pick list.")

    render_reset_confirm("inventory", _reset_inventory_state)
