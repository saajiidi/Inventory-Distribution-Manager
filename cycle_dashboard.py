"""Standalone Streamlit entry point for the Business Cycles page."""

from __future__ import annotations

import streamlit as st

from FrontEnd.components.ui_components import inject_base_styles
from FrontEnd.pages.cycle_analytics import render_cycle_analytics_tab


st.set_page_config(
    page_title="DEEN Business Cycles",
    page_icon="AP",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_base_styles()
render_cycle_analytics_tab()
