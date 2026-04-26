import pandas as pd
import streamlit as st
from .data_display import _safe_datetime_series


def skeleton_metric(icon: str = "📊"):
    """Skeleton loading state for metric cards - renders instantly while data loads."""
    st.markdown(
        f"""
        <div class="hub-card metric-icon-card" style="opacity: 0.7;">
          <div class="metric-icon-wrap" style="animation: pulse 1.5s infinite;">{icon}</div>
          <div class="metric-content">
            <div class="metric-highlight-label" style="background: linear-gradient(90deg, rgba(128,128,128,0.1) 25%, rgba(128,128,128,0.2) 50%, rgba(128,128,128,0.1) 75%); background-size: 200% 100%; animation: shimmer 1.5s infinite; height: 14px; width: 80px; border-radius: 4px; margin-bottom: 8px;"></div>
            <div class="metric-highlight-value" style="background: linear-gradient(90deg, rgba(128,128,128,0.1) 25%, rgba(128,128,128,0.2) 50%, rgba(128,128,128,0.1) 75%); background-size: 200% 100%; animation: shimmer 1.5s infinite; height: 28px; width: 100px; border-radius: 4px;"></div>
          </div>
        </div>
        <style>
          @keyframes shimmer {{
            0% {{ background-position: 200% 0; }}
            100% {{ background-position: -200% 0; }}
          }}
          @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.5; }}
          }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def skeleton_row(count: int = 6):
    """Render multiple skeleton metric cards in a row."""
    cols = st.columns(count)
    icons = ["📦", "💰", "🛒", "📅", "👥", "💎"]
    for i, col in enumerate(cols):
        with col:
            skeleton_metric(icon=icons[i % len(icons)])















def badge(note: str):
    if not note:
        return
    st.markdown(f'<div class="bi-kpi-note">{note}</div>', unsafe_allow_html=True)




def icon_metric(label: str, value: str, icon: str = "📊", delta: str = "", delta_val: float = 0, loading: bool = False, delta_color: str = "normal"):
    """Render metric card with optional loading skeleton state and dynamic delta coloring."""
    if loading:
        skeleton_metric(icon=icon)
        return

    delta_icon = "↑" if delta_val >= 0 else "↓"
    
    if delta_color == "inverse":
        color = "var(--red)" if delta_val > 0 else "var(--green)" if delta_val < 0 else "var(--on-surface-variant)"
    elif delta_color == "off":
        color = "var(--on-surface-variant)"
    else:
        color = "var(--green)" if delta_val > 0 else "var(--red)" if delta_val < 0 else "var(--on-surface-variant)"
        
    value_color = color if delta else "var(--on-surface)"
    delta_html = f'<div class="metric-delta" style="color: {color}; font-size: clamp(0.6rem, 6cqi, 0.7rem); font-weight: 700; overflow-wrap: break-word; line-height: 1.2;">{delta_icon} {delta}</div>' if delta else ""

    st.markdown(
        f"""
        <style>
        .metric-icon-card {{
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            text-align: center;
            height: 100%;
            min-height: 110px;
            padding: 12px;
            overflow: hidden;
            container-type: inline-size;
        }}
        @media (max-width: 1250px) {{
            .metric-icon-card {{ padding: 10px !important; min-height: 90px !important; }}
            .metric-icon-card .metric-icon-wrap {{ display: none !important; }}
        }}
        </style>
        <div class="hub-card metric-icon-card">
          <div style="display: flex; justify-content: center; align-items: center; gap: 6px; width: 100%; margin-bottom: auto;">
              <div class="metric-highlight-label" style="font-size: clamp(0.55rem, 6cqi, 0.7rem); margin: 0; overflow-wrap: break-word; line-height: 1.2;">{label}</div>
              <div class="metric-icon-wrap" style="font-size: clamp(0.7rem, 8cqi, 0.9rem); opacity: 0.7; width: auto; height: auto; background: transparent; flex-shrink: 0;">{icon}</div>
          </div>
          <div class="metric-highlight-value" style="font-size: clamp(1.1rem, 15cqi, 2.2rem); margin: 0; color: {value_color} !important; overflow-wrap: break-word; line-height: 1.1;">{value}</div>
          <div style="margin-top: auto; min-height: 16px; width: 100%; display: flex; justify-content: center; align-items: center;">
              {delta_html}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def metric_highlight(label: str, value: str, delta: str = "", delta_type: str = "up", help_text: str = "", icon: str = None, delta_color_mode: str = "normal"):
    """Premium Enterprise KPI card with glassmorphism, motion transitions, and optional icon."""
    delta_icon = "↑" if delta_type == "up" else "↓"
    
    if delta_color_mode == "inverse":
        delta_color = "var(--red)" if delta_type == "up" else "var(--green)"
    elif delta_color_mode == "off":
        delta_color = "var(--on-surface-variant)"
    else:
        delta_color = "var(--green)" if delta_type == "up" else "var(--red)"
        
    value_color = delta_color if delta else "var(--on-surface)"
    delta_html = f'<span class="metric-highlight-delta" style="color: {delta_color}; font-weight: 700;">{delta_icon} {delta}</span>' if delta else ""
    help_html = f'<span class="metric-highlight-help" style="color: #64748b; font-weight: 500;">{help_text}</span>' if help_text else ""
    
    footer_content = f"{delta_html} {help_html}".strip()
    footer_block = f'<div style="font-size: clamp(0.55rem, 6cqi, 0.65rem); margin-top: auto; min-height: 16px; overflow-wrap: break-word; line-height: 1.2; width: 100%; display: flex; justify-content: center; align-items: center; gap: 6px; text-align: center;">{footer_content}</div>'
    icon_html = f'<div class="metric-highlight-icon" style="font-size: clamp(0.7rem, 8cqi, 0.85rem); opacity: 0.7; flex-shrink: 0;">{icon}</div>' if icon else ""
    
    html_content = f"""
    <div class="hub-card metric-highlight" style="container-type: inline-size;">
        <div style="display: flex; justify-content: center; align-items: center; gap: 6px; width: 100%; margin-bottom: auto;">
            <div class="metric-highlight-label" style="font-size: clamp(0.55rem, 6cqi, 0.7rem); margin: 0; overflow-wrap: break-word; line-height: 1.2;">{label}</div>
            {icon_html}
        </div>
        <div class="metric-highlight-value" style="font-size: clamp(1.1rem, 15cqi, 2.2rem); color: {value_color} !important; margin: 0; overflow-wrap: break-word; line-height: 1.1;">{value}</div>
        {footer_block}
    </div>
    """
    st.markdown(html_content, unsafe_allow_html=True)












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











def operational_card(title: str, order_count: int, item_count: int, revenue: float, icon: str = "📦", delta_text: str = "", delta_val: int = 0, item_label: str = "Items", is_alert: bool = False, delta_color: str = "normal"):
    """Premium multi-line operational metric card with optional alert pulse."""
    delta_icon = "↑" if delta_val >= 0 else "↓"
    
    if delta_color == "inverse":
        color = "var(--red)" if delta_val > 0 else "var(--green)" if delta_val < 0 else "var(--on-surface-variant)"
    elif delta_color == "off":
        color = "var(--on-surface-variant)"
    else:
        color = "var(--green)" if delta_val > 0 else "var(--red)" if delta_val < 0 else "var(--on-surface-variant)"
        
    value_color = color if delta_text else "var(--primary)"
    delta_html = f'<div class="op-card-delta" style="color: {color}; font-size: clamp(0.6rem, 6cqi, 0.7rem); font-weight:700; overflow-wrap: break-word; line-height: 1.2;">{delta_icon} {delta_text}</div>' if delta_text else ""

    pulse_css = "animation: pulse-amber 2s infinite;" if is_alert else ""
    border_style = "2px solid var(--warning)" if is_alert else "1px solid var(--outline)"

    st.markdown(
        f"""
        <style>
        @keyframes pulse-amber {{
            0% {{ box-shadow: 0 0 0 0 rgba(245, 158, 11, 0.4); }}
            70% {{ box-shadow: 0 0 0 10px rgba(245, 158, 11, 0); }}
            100% {{ box-shadow: 0 0 0 0 rgba(245, 158, 11, 0); }}
        }}
        .op-card {{
            padding: 12px;
            height: 100%;
            min-height: 140px;
            background: var(--surface);
            border-radius: 16px;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            text-align: center;
            overflow: hidden;
            box-sizing: border-box;
            container-type: inline-size;
        }}
        .op-card:hover {{
            transform: translateY(-4px);
            box-shadow: 0 12px 20px -5px rgba(0,0,0,0.1);
            border-color: var(--primary);
        }}
        @media (max-width: 1250px) {{
            .op-card {{ padding: 10px !important; min-height: 110px !important; }}
            .op-card .op-card-icon {{ display: none !important; }}
        }}
        </style>
        <div class="op-card" style="{pulse_css} border: {border_style};">
            <div style="display: flex; justify-content: center; align-items: center; gap: 6px; width: 100%; margin-bottom: auto;">
                <div style="font-size: clamp(0.5rem, 6cqi, 0.75rem); font-weight: 800; color: var(--on-surface-variant); text-transform: uppercase; overflow-wrap: break-word; line-height: 1.2;">{title}</div>
                <div class="op-card-icon" style="font-size: clamp(0.7rem, 8cqi, 0.9rem); opacity: 0.7; flex-shrink: 0;">{icon}</div>
            </div>
            <div style="font-size: clamp(1.1rem, 15cqi, 2.2rem); font-weight: 800; color: {value_color}; letter-spacing: -0.03em; overflow-wrap: break-word; line-height: 1.1; margin: 0;">TK {revenue:,.0f}</div>
            <div style="margin-top: auto; width: 100%; display: flex; flex-direction: column; justify-content: center; align-items: center; gap: 4px; min-height: 24px;">
                <div style="display: flex; justify-content: center; gap: 12px; width: 100%; flex-wrap: wrap;">
                    <div style="font-size: clamp(0.55rem, 5cqi, 0.65rem); color: var(--on-surface-variant); font-weight: 600; overflow-wrap: break-word; line-height: 1.2;">Orders: <span style="color: var(--on-surface);">{order_count:,}</span></div>
                    <div style="font-size: clamp(0.55rem, 5cqi, 0.65rem); color: var(--on-surface-variant); font-weight: 600; overflow-wrap: break-word; line-height: 1.2;">{item_label}: <span style="color: var(--on-surface);">{item_count:,}</span></div>
                </div>
                {delta_html}
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )
