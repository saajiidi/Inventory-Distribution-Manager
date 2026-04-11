import pandas as pd
import streamlit as st
from .data_display import _safe_datetime_series















def badge(note: str):
    if not note:
        return
    st.markdown(f'<div class="bi-kpi-note">{note}</div>', unsafe_allow_html=True)




def icon_metric(label: str, value: str, icon: str = "📊", delta: str = "", delta_val: float = 0):
    delta_class = "delta-up" if delta_val >= 0 else "delta-down"
    delta_icon = "↑" if delta_val >= 0 else "↓"
    delta_html = f'<div class="metric-delta {delta_class}">{delta_icon} {delta}</div>' if delta else ""
    
    st.markdown(
        f"""
        <div class="hub-card metric-icon-card">
          <div class="metric-icon-wrap">{icon}</div>
          <div class="metric-content">
            <div class="metric-highlight-label">{label}</div>
            <div class="metric-highlight-value" style="font-size: 1.8rem;">{value}</div>
            {delta_html}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def metric_highlight(label: str, value: str, delta: str = "", delta_type: str = "up", help_text: str = ""):
    """Premium Enterprise KPI card with glassmorphism and motion transitions."""
    delta_class = "delta-up" if delta_type == "up" else "delta-down"
    delta_icon = "↑" if delta_type == "up" else "↓"
    delta_color = "#10b981" if delta_type == "up" else "#ef4444"
    
    delta_html = f"""
    <div style="display: flex; align-items: center; gap: 4px; color: {delta_color}; font-size: 0.85rem; font-weight: 700; margin-top: 4px;">
        <span>{delta_icon} {delta}</span>
    </div>
    """ if delta else ""
    
    help_block = f'<div style="color:var(--text-muted); font-size:0.75rem; margin-top:8px; font-weight:500;">{help_text}</div>' if help_text else ""
    
    st.markdown(
        f"""
        <div class="hub-card metric-highlight">
            <div class="metric-highlight-label">{label}</div>
            <div class="metric-highlight-value">{value}</div>
            {delta_html}
            {help_block}
        </div>
        """,
        unsafe_allow_html=True,
    )












def date_context(
    requested_start=None,
    requested_end=None,
    loaded_start=None,
    loaded_end=None,
    label: str = "Loaded data",
):
    requested_parts = []
    if requested_start is not None:
        requested_parts.append(f"from {pd.to_datetime(requested_start).strftime('%Y-%m-%d')}")
    if requested_end is not None:
        requested_parts.append(f"to {pd.to_datetime(requested_end).strftime('%Y-%m-%d')}")
    requested_text = " ".join(requested_parts).strip()
    prefix = f"Requested range: {requested_text}" if requested_text else "Requested range: not specified"

    loaded_start_series = _safe_datetime_series(loaded_start)
    loaded_end_series = _safe_datetime_series(loaded_end)
    loaded_start_ts = loaded_start_series.min() if not loaded_start_series.empty and loaded_start_series.notna().any() else pd.NaT
    loaded_end_ts = loaded_end_series.max() if not loaded_end_series.empty and loaded_end_series.notna().any() else pd.NaT
    if pd.notna(loaded_start_ts) and pd.notna(loaded_end_ts):
        st.caption(
            f"{prefix} | {label}: {loaded_start_ts.strftime('%Y-%m-%d %H:%M')} to {loaded_end_ts.strftime('%Y-%m-%d %H:%M')}"
        )
    else:
        st.caption(f"{prefix} | {label}: dates are not available in the current result.")











def operational_card(title: str, order_count: int, item_count: int, revenue: float, icon: str = "📦", delta_text: str = "", delta_val: int = 0, item_label: str = "Items", is_alert: bool = False):
    """Premium multi-line operational metric card with optional alert pulse."""
    delta_class = "delta-up" if delta_val >= 0 else "delta-down"
    delta_icon = "↑" if delta_val >= 0 else "↓"
    delta_html = f'<div class="metric-delta {delta_class}" style="margin-top:10px; font-size:0.85rem;">{delta_icon} {delta_text}</div>' if delta_text else ""
    
    pulse_css = "animation: pulse-amber 2s infinite;" if is_alert else ""
    
    st.markdown(
        f"""
        <style>
        @keyframes pulse-amber {{
            0% {{ box-shadow: 0 0 0 0 rgba(245, 158, 11, 0.4); }}
            70% {{ box-shadow: 0 0 0 10px rgba(245, 158, 11, 0); }}
            100% {{ box-shadow: 0 0 0 0 rgba(245, 158, 11, 0); }}
        }}
        </style>
        <div class="metric-card" style="padding: 1.2rem; min-height: 180px; {pulse_css} border: { '2px solid #F59E0B' if is_alert else '1px solid var(--border)' };">
            <div style="display: flex; justify-content: space-between; align-items: start;">
                <div style="font-size: 1.1rem; font-weight: 700; color: var(--on-surface);">{title}</div>
                <div style="font-size: 1.5rem;">{icon}</div>
            </div>
            <div style="margin-top: 15px;">
                <div style="font-size: 0.85rem; color: var(--on-surface-variant); font-weight: 500;">Orders: <b>{order_count:,}</b></div>
                <div style="font-size: 0.85rem; color: var(--on-surface-variant); font-weight: 500;">{item_label}: <b>{item_count:,}</b></div>
                <div style="font-size: 1.4rem; font-weight: 800; color: var(--primary); margin-top: 8px;">৳ {revenue:,.0f}</div>
                {delta_html}
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )
