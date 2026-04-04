import os
from datetime import date, datetime, timedelta
from io import BytesIO

import pandas as pd
import streamlit as st

from src.ui.config import APP_TITLE, APP_VERSION


def inject_base_styles():
    styles = """
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;700&family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
    :root {
        --app-bg-1: #f7f9fc;
        --app-bg-2: #f3f6fb;
        --app-bg-3: #eef2f7;
        --panel-bg: rgba(255, 255, 255, 0.94);
        --panel-bg-strong: rgba(255, 255, 255, 0.99);
        --panel-muted: rgba(248, 250, 252, 0.96);
        --panel-border: #d7e1eb;
        --panel-border-soft: #e5ebf2;
        --surface-bg: rgba(255, 255, 255, 0.98);
        --surface-border: #dbe4ed;
        --text-strong: #1f2937;
        --text-soft: #4b5563;
        --text-dim: #64748b;
        --text-color: #475569;
        --text-primary: #1f2937;
        --text-secondary: #64748b;
        --accent: #2563eb;
        --accent-primary: #2563eb;
        --accent-strong: #1d4ed8;
        --accent-soft: #e8f0ff;
        --success: #15803d;
        --warning: #b45309;
        --danger: #b91c1c;
        --shadow-soft: 0 8px 24px rgba(15, 23, 42, 0.04);
        --shadow-panel: 0 12px 32px rgba(15, 23, 42, 0.05);
        --radius-lg: 24px;
        --radius-md: 18px;
        --radius-sm: 14px;
    }

    @media (prefers-color-scheme: dark) {
        :root {
            --app-bg-1: #0f172a;
            --app-bg-2: #020617;
            --app-bg-3: #000000;
            --panel-bg: rgba(30, 41, 59, 0.7);
            --panel-bg-strong: rgba(15, 23, 42, 0.95);
            --panel-muted: rgba(15, 23, 42, 0.6);
            --panel-border: #334155;
            --panel-border-soft: #1e293b;
            --surface-bg: #1e293b;
            --surface-border: #334155;
            --text-strong: #f8fafc;
            --text-soft: #cbd5e1;
            --text-dim: #94a3b8;
            --text-color: #cbd5e1;
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --accent: #3b82f6;
            --accent-primary: #3b82f6;
            --accent-strong: #60a5fa;
            --accent-soft: rgba(37, 99, 235, 0.15);
            --shadow-soft: 0 8px 24px rgba(0, 0, 0, 0.4);
            --shadow-panel: 0 12px 32px rgba(0, 0, 0, 0.5);
        }
    }

    html,
    body,
    [data-testid="stAppViewContainer"],
    .main,
    section[data-testid="stMain"] {
        background: linear-gradient(180deg, var(--app-bg-1) 0%, var(--app-bg-2) 52%, var(--app-bg-3) 100%) !important;
        color: var(--text-strong) !important;
    }

    .stApp {
        background: transparent !important;
        color: var(--text-strong) !important;
    }

    .block-container {
        max-width: 1480px !important;
        padding-top: 1.1rem !important;
        padding-bottom: 100px !important;
    }

    [data-testid="stHeader"] {
        background: var(--panel-bg-strong) !important;
        border-bottom: 1px solid var(--panel-border-soft) !important;
    }

    [data-testid="stToolbar"],
    [data-testid="stDecoration"],
    [data-testid="stStatusWidget"],
    [data-testid="stSidebarHeader"] {
        background: transparent !important;
        color: var(--text-soft) !important;
    }

    [data-testid="stDecoration"] {
        display: none !important;
    }

    [data-testid="collapsedControl"] button {
        background: var(--panel-bg-strong) !important;
        border: 1px solid var(--panel-border) !important;
        border-radius: 12px !important;
        color: var(--text-strong) !important;
        box-shadow: none !important;
    }

    hr {
        border-color: var(--panel-border-soft) !important;
    }

    .stApp,
    .stApp p,
    .stCaption,
    .stText,
    label,
    [data-testid="stMarkdownContainer"] p,
    [data-testid="stMarkdownContainer"] li,
    [data-testid="stMarkdownContainer"] span {
        color: var(--text-soft) !important;
        font-family: 'Manrope', sans-serif !important;
    }

    h1, h2, h3, h4, h5, h6, strong, b, th, td {
        color: var(--text-strong) !important;
        font-family: 'Manrope', sans-serif !important;
    }

    code, pre, .mono, [data-testid="stMetricValue"] {
        font-family: 'IBM Plex Mono', monospace !important;
    }

    a {
        color: var(--accent-strong) !important;
    }

    [data-testid="stSidebar"] {
        background: var(--app-bg-1) !important;
        border-right: 1px solid var(--panel-border-soft);
    }

    [data-testid="stSidebar"] * {
        color: var(--text-soft) !important;
    }

    .sidebar-shell {
        background: var(--panel-bg-strong);
        border: 1px solid var(--panel-border);
        border-radius: var(--radius-lg);
        padding: 1rem 1.05rem;
        margin-bottom: 1.15rem;
        box-shadow: var(--shadow-soft);
    }

    .sidebar-brand-row {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        margin-bottom: 0.55rem;
    }

    .sidebar-brand-logo {
        width: 2.2rem;
        height: 2.2rem;
        border-radius: 12px;
        object-fit: cover;
        border: 1px solid var(--panel-border-soft);
        background: var(--panel-bg-strong);
    }

    .sidebar-brand-text {
        display: flex;
        flex-direction: column;
        gap: 0.15rem;
    }

    .app-shell-header {
        display: flex;
        justify-content: space-between;
        gap: 1rem;
        align-items: center;
        background: var(--panel-bg-strong);
        border: 1px solid var(--panel-border);
        border-radius: 28px;
        padding: 1.05rem 1.2rem;
        margin-bottom: 1.15rem;
        box-shadow: var(--shadow-panel);
    }

    .app-shell-brand {
        display: flex;
        flex-direction: column;
        gap: 0.35rem;
    }

    .app-shell-label {
        font-size: 0.73rem;
        text-transform: uppercase;
        letter-spacing: 0.22em;
        color: var(--accent-strong);
        font-weight: 800;
    }

    .app-shell-title {
        font-size: 1.4rem;
        font-weight: 800;
        color: var(--text-strong);
        letter-spacing: -0.04em;
    }

    .app-shell-subtitle {
        font-size: 0.9rem;
        color: var(--text-soft);
        max-width: 50rem;
        line-height: 1.5;
    }

    .app-shell-chip-row {
        display: flex;
        gap: 0.55rem;
        flex-wrap: wrap;
        align-items: flex-start;
        justify-content: flex-end;
    }

    .app-shell-chip {
        background: var(--panel-bg-strong);
        color: var(--text-strong);
        border: 1px solid var(--panel-border);
        border-radius: 999px;
        padding: 0.48rem 0.9rem;
        font-size: 0.74rem;
        font-family: 'IBM Plex Mono', monospace;
        box-shadow: none;
    }

    .hub-card,
    .glass-panel,
    [data-testid="stMetric"],
    [data-testid="stExpander"] {
        background: var(--panel-bg-strong) !important;
        border: 1px solid var(--panel-border) !important;
        box-shadow: var(--shadow-soft);
    }

    .hub-card {
        border-radius: var(--radius-lg);
        padding: 1.25rem 1.35rem;
        margin-bottom: 1rem;
    }

    .glass-panel {
        border-radius: 20px;
        padding: 1.05rem 1.1rem;
    }

    .period-selector {
        background: var(--panel-bg-strong) !important;
        padding: 1.35rem !important;
        border-radius: 22px !important;
        border: 1px solid var(--panel-border) !important;
        margin-bottom: 1.35rem !important;
        box-shadow: var(--shadow-soft);
    }

    .period-status {
        font-size: 0.82rem !important;
        background: var(--accent-soft) !important;
        color: var(--accent-strong) !important;
        padding: 0.45rem 0.95rem !important;
        border-radius: 999px !important;
        display: inline-block !important;
        margin-top: 1rem !important;
        font-weight: 700 !important;
        letter-spacing: 0.04em !important;
        border: 1px solid #bfdbfe;
    }

    .status-chip {
        background: rgba(255,255,255,0.96);
        color: var(--text-strong);
        padding: 0.45rem 0.8rem;
        border-radius: 999px;
        font-size: 0.74rem;
        font-family: 'IBM Plex Mono', monospace;
        border: 1px solid var(--panel-border);
    }

    [data-testid="stMetric"] {
        border-radius: 18px !important;
        padding: 0.95rem 1rem !important;
    }

    [data-testid="stMetricLabel"] {
        color: var(--text-dim) !important;
    }

    [data-testid="stMetricValue"] {
        color: var(--text-strong) !important;
        font-weight: 700 !important;
    }

    [data-testid="stMetricDelta"] {
        color: var(--text-soft) !important;
    }

    .ops-hero {
        background: var(--panel-bg-strong);
        border: 1px solid var(--panel-border);
        border-radius: 28px;
        padding: 1.3rem;
        margin-bottom: 1rem;
        box-shadow: var(--shadow-panel);
    }

    .ops-kpi-card {
        background: var(--panel-bg-strong);
        border: 1px solid var(--panel-border-soft);
        border-radius: 18px;
        padding: 1rem 1.05rem;
        height: 100%;
        box-shadow: var(--shadow-soft);
    }

    .ops-kpi-label {
        font-size: 0.74rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: var(--text-dim);
        margin-bottom: 0.42rem;
        font-weight: 700;
    }

    .ops-kpi-value {
        font-size: 1.4rem;
        font-family: 'IBM Plex Mono', monospace;
        color: var(--text-strong);
        font-weight: 700;
    }

    .ops-kpi-note {
        margin-top: 0.45rem;
        font-size: 0.82rem;
        color: var(--text-soft);
        line-height: 1.45;
    }

    .ops-mini-list {
        display: grid;
        gap: 0.72rem;
    }

    .ops-mini-item {
        display: flex;
        justify-content: space-between;
        gap: 1rem;
        padding-bottom: 0.68rem;
        border-bottom: 1px solid var(--panel-border-soft);
        color: var(--text-soft);
        font-size: 0.9rem;
    }

    .ops-mini-item:last-child {
        border-bottom: 0;
        padding-bottom: 0;
    }

    .ops-mini-item strong {
        color: var(--text-strong);
    }

    .stButton > button,
    [data-testid="baseButton-secondary"],
    [data-testid="baseButton-primary"] {
        border-radius: 14px !important;
        border: 1px solid var(--panel-border) !important;
        background: var(--panel-bg-strong) !important;
        color: var(--text-strong) !important;
        box-shadow: none !important;
        font-weight: 600 !important;
    }

    .stButton > button:hover {
        border-color: var(--accent-strong) !important;
        background: var(--panel-bg-strong) !important;
        color: var(--accent-strong) !important;
    }

    [data-testid="baseButton-primary"] {
        background: var(--accent) !important;
        color: #ffffff !important;
        border-color: var(--accent-strong) !important;
    }

    [data-testid="baseButton-primary"]:hover {
        background: var(--accent-strong) !important;
        color: #ffffff !important;
    }

    .stTextInput input,
    .stDateInput input,
    .stTextArea textarea,
    .stNumberInput input,
    div[data-baseweb="select"] > div,
    .stMultiSelect [data-baseweb="select"] > div {
        background: var(--panel-bg-strong) !important;
        color: var(--text-strong) !important;
        border: 1px solid var(--panel-border) !important;
        border-radius: 14px !important;
    }

    .stTextInput input::placeholder,
    .stTextArea textarea::placeholder {
        color: var(--text-dim) !important;
    }

    .stRadio [role="radiogroup"],
    .stSegmentedControl {
        background: transparent !important;
    }

    .stRadio [role="radiogroup"] label {
        background: var(--panel-bg-strong) !important;
        border: 1px solid var(--panel-border) !important;
        border-radius: 14px !important;
        padding: 0.5rem 0.75rem !important;
        color: var(--text-strong) !important;
        margin-bottom: 0.45rem !important;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 0.45rem;
        background: transparent;
    }

    .stTabs [data-baseweb="tab"] {
        background: var(--panel-bg) !important;
        border: 1px solid var(--panel-border) !important;
        color: var(--text-soft) !important;
        border-radius: 999px !important;
        padding: 0.45rem 0.9rem !important;
        font-weight: 600 !important;
    }

    .stTabs [aria-selected="true"] {
        background: var(--accent-soft) !important;
        color: var(--accent-strong) !important;
        border-color: var(--accent-strong) !important;
    }

    [data-baseweb="button-group"] {
        background: transparent !important;
        gap: 0.4rem !important;
    }

    [data-baseweb="button-group"] button {
        background: var(--panel-bg-strong) !important;
        border: 1px solid var(--panel-border) !important;
        border-radius: 999px !important;
        color: var(--text-soft) !important;
        box-shadow: none !important;
    }

    [data-baseweb="button-group"] button[aria-pressed="true"] {
        background: var(--accent-soft) !important;
        color: var(--accent-strong) !important;
        border-color: #bfdbfe !important;
    }

    [data-testid="stDataFrame"],
    [data-testid="stTable"] {
        background: var(--panel-bg-strong) !important;
        border: 1px solid var(--panel-border) !important;
        border-radius: 18px !important;
        padding: 0.25rem !important;
        box-shadow: none !important;
    }

    [data-testid="stDataFrame"] *,
    [data-testid="stTable"] * {
        color: var(--text-strong) !important;
    }

    [data-testid="stDataFrame"] [role="columnheader"],
    [data-testid="stDataFrame"] thead th,
    [data-testid="stTable"] thead th {
        background: var(--panel-muted) !important;
        color: var(--text-strong) !important;
    }

    [data-testid="stDataFrame"] [role="gridcell"],
    [data-testid="stTable"] tbody td {
        background: transparent !important;
    }

    [data-testid="stExpander"] {
        border-radius: 18px !important;
    }

    [data-testid="stExpanderSummary"] {
        padding: 0.85rem 1rem !important;
    }

    [data-testid="stExpanderSummary"] > div {
        color: var(--text-strong) !important;
        font-weight: 600 !important;
    }

    .stAlert {
        border-radius: 16px !important;
        border: 1px solid var(--panel-border-soft) !important;
    }

    .stAlert[data-baseweb="notification"] {
        background: var(--panel-bg-strong) !important;
    }

    div[data-testid="stInfo"] {
        background: var(--accent-soft) !important;
    }

    div[data-testid="stSuccess"] {
        background: rgba(21, 128, 61, 0.1) !important;
    }

    div[data-testid="stWarning"] {
        background: rgba(180, 83, 9, 0.1) !important;
    }

    div[data-testid="stError"] {
        background: rgba(185, 28, 28, 0.1) !important;
    }

    .inline-action-row {
        position: sticky;
        top: 0;
        z-index: 100;
        padding: 0.75rem 1rem;
        background: var(--panel-bg-strong);
        border: 1px solid var(--panel-border);
        border-radius: 18px;
        margin-top: 1rem;
        display: flex;
        gap: 12px;
        box-shadow: var(--shadow-soft);
    }

    div[data-baseweb="popover"],
    ul[role="listbox"] {
        background: var(--panel-bg-strong) !important;
        border: 1px solid var(--panel-border) !important;
    }

    div[data-baseweb="popover"] *,
    ul[role="listbox"] * {
        color: var(--text-strong) !important;
    }

    .fade-in {
        animation: simpleFade 0.24s ease forwards;
    }

    @keyframes simpleFade {
        from { opacity: 0; transform: translateY(4px); }
        to { opacity: 1; transform: translateY(0); }
    }

    /* Mobile Responsive Styles */
    @media (max-width: 768px) {
        .block-container {
            max-width: 100% !important;
            padding: 0.5rem !important;
        }
        .ops-kpi-card {
            margin-bottom: 0.5rem;
            padding: 0.75rem;
        }
        .app-shell-header {
            flex-direction: column;
            align-items: flex-start;
            gap: 0.5rem;
        }
        .app-shell-title {
            font-size: 1.1rem;
        }
        .app-shell-chip-row {
            justify-content: flex-start;
            width: 100%;
        }
    }

    /* Accessibility: Focus Styles */
    *:focus-visible {
        outline: 2px solid var(--accent) !important;
        outline-offset: 2px !important;
    }

    /* STICKY FOOTER SYSTEM */
    .app-footer {
        position: fixed;
        bottom: 0;
        left: 0;
        width: 100%;
        background: var(--panel-bg-strong);
        color: var(--text-strong);
        text-align: center;
        padding: 0.75rem 0;
        font-size: 0.8rem;
        border-top: 1px solid var(--panel-border);
        z-index: 999999;
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 6px;
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
    }

    /* Hide Default Streamlit Branding */
    footer {visibility: hidden !important;}
    #MainMenu {visibility: hidden !important;}
</style>"""
    st.markdown(styles, unsafe_allow_html=True)


def render_sidebar_shell():
    st.markdown(
        """
        <div class="sidebar-shell">
            <div class="sidebar-brand-row">
                <img
                    class="sidebar-brand-logo"
                    src="https://cdn.brandfetch.io/deencommerce.com"
                    alt="DEEN Commerce logo"
                />
                <div class="sidebar-brand-text">
                    <div class="app-shell-label" style="margin:0;">DEEN COMMERCE</div>
                </div>
            </div>
            <div class="app-shell-title" style="font-size:1.08rem;">Operations Dashboard</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_header():
    from src.core.sync import load_manifest

    manifest = load_manifest()
    last_sync = "Never"
    if manifest:
        sync_times = [
            datetime.fromisoformat(v["fetched_at"])
            for v in manifest.values()
            if isinstance(v, dict) and "fetched_at" in v
        ]
        if sync_times:
            last_sync = max(sync_times).strftime("%H:%M")

    st.markdown(
        f"""
        <div class="app-shell-header fade-in">
            <div class="app-shell-brand">
                <div class="app-shell-label">Commerce Operations</div>
                <div class="app-shell-title">{APP_TITLE} <span style="font-size:0.8rem;color:var(--text-dim);font-weight:600;">{APP_VERSION}</span></div>
            </div>
            <div class="app-shell-chip-row">
                <div class="app-shell-chip">LAST SYNC {last_sync}</div>
                <div class="app-shell-chip">CACHE {len(manifest)} TABS</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_card(title: str, help_text: str = "", primary_action=None):
    st.markdown(
        f"""
        <div class="hub-card fade-in">
            <div style="font-size:1.08rem; font-weight:700; color:var(--text-strong); margin-bottom:0.28rem;">{title}</div>
            <div style="color:var(--text-soft); font-size:0.94rem; line-height:1.6;">{help_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_status_strip(source="Local", rows=0, last_refresh="N/A", status="Ready"):
    cols = st.columns([1, 1, 1, 1])
    with cols[0]:
        st.caption(f"SOURCE: **{source}**")
    with cols[1]:
        st.caption(f"DATASET: **{rows:,} rows**")
    with cols[2]:
        st.caption(f"REFRESH: **{last_refresh}**")
    with cols[3]:
        color = "#15803d" if ("Ready" in status or "Active" in status) else "#b45309"
        st.markdown(
            f"<div style='font-size:0.76rem; text-align:right; color:{color}; font-weight:700;'>{status.upper()}</div>",
            unsafe_allow_html=True,
        )


def render_metric_hud(label: str, value: str, icon: str = ""):
    st.markdown(
        f"""
        <div style="background: var(--panel-bg-strong); border: 1px solid var(--panel-border-soft); padding: 1rem; border-radius: 18px; height: 100%; box-shadow: var(--shadow-soft);">
            <div style="font-size:0.74rem; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.12em; margin-bottom:0.48rem; display:flex; align-items:center; gap:6px;">
                <span>{icon}</span> {label}
            </div>
            <div style="font-family:'IBM Plex Mono', monospace; color:var(--text-strong); font-size:1.34rem; font-weight:700; white-space:nowrap;">
                {value}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_action_bar(
    primary_label: str,
    primary_key: str,
    secondary_label: str | None = None,
    secondary_key: str | None = None,
    sticky: bool = False,
):
    if sticky:
        st.markdown('<div class="inline-action-row">', unsafe_allow_html=True)

    col1, col2 = st.columns([1, 1])
    with col1:
        p_click = st.button(
            primary_label, type="primary", use_container_width=True, key=primary_key
        )
    with col2:
        s_click = False
        if secondary_label and secondary_key:
            s_click = st.button(
                secondary_label, use_container_width=True, key=secondary_key
            )

    if sticky:
        st.markdown("</div>", unsafe_allow_html=True)
    return p_click, s_click


def render_ops_hero(title: str, subtitle: str, chips: list[str]):
    chips_html = "".join(f"<div class='app-shell-chip'>{chip}</div>" for chip in chips)
    st.markdown(
        f"""
        <div class="ops-hero fade-in">
            <div style="display:flex;justify-content:space-between;gap:1rem;align-items:flex-start;flex-wrap:wrap;">
                <div>
                    <div class="app-shell-label">Operational View</div>
                    <div class="app-shell-title" style="font-size:1.28rem;">{title}</div>
                    <div class="app-shell-subtitle">{subtitle}</div>
                </div>
                <div class="app-shell-chip-row">{chips_html}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_ops_kpi(label: str, value: str, note: str = "", delta: float = None, invert_delta: bool = False):
    """
    Render a KPI card with an optional delta percentage.
    delta: Float indicating percentage change (e.g. 5.2 or -10.1)
    invert_delta: If True, positive delta is red and negative is green (e.g. for churn/errors)
    """
    delta_html = ""
    if delta is not None:
        is_pos = delta >= 0
        # Determine color based on delta and inversion
        if invert_delta:
            color = "var(--danger)" if is_pos else "var(--success)"
        else:
            color = "var(--success)" if is_pos else "var(--danger)"
        
        icon = "&uarr;" if is_pos else "&darr;"
        prefix = "+" if is_pos else ""
        delta_html = f"<span style='color:{color}; font-weight:700; font-size:0.85rem; margin-left:8px;'>{icon} {prefix}{delta:.1f}%</span>"

    st.markdown(
        f"""
        <div class="ops-kpi-card">
            <div class="ops-kpi-label">{label}</div>
            <div class="ops-kpi-value">{value}{delta_html}</div>
            <div class="ops-kpi-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_ops_list(items: list[tuple[str, str]]):
    rows = "".join(
        f"<div class='ops-mini-item'><span>{label}</span><strong>{value}</strong></div>"
        for label, value in items
    )
    st.markdown(
        f"<div class='glass-panel'><div class='ops-mini-list'>{rows}</div></div>",
        unsafe_allow_html=True,
    )


def render_steps(steps: list[str], current_step: int):
    cols = st.columns(len(steps))
    for idx, step in enumerate(steps):
        is_active = idx == current_step
        color = "#2563eb" if is_active else "#94a3b8"
        weight = "700" if is_active else "500"
        opacity = "1" if is_active else "0.72"
        cols[idx].markdown(
            f"""
            <div style="text-align:center; padding-bottom:4px; border-bottom: 2px solid {color if is_active else 'transparent'}; opacity:{opacity};">
                <div style="font-size:0.7rem; text-transform:uppercase; color:var(--text-dim);">Step {idx+1}</div>
                <div style="font-size:0.86rem; font-weight:{weight}; color:var(--text-strong);">{step}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_mini_uploader(label: str, key: str):
    return st.file_uploader(label, key=key, type=["xlsx", "csv"])


def render_file_summary(uploaded_file, df: pd.DataFrame | None, required_columns: list[str]):
    if not uploaded_file or df is None:
        return False

    st.markdown(
        f"""
        <div class="glass-panel" style="margin:1rem 0;">
            <div style="font-size:0.8rem; font-weight:700;">FILE {uploaded_file.name.upper()}</div>
            <div style="display:flex; gap:20px; margin-top:8px;">
                <div style="font-size:0.75rem;">ROWS: <b class="mono">{len(df):,}</b></div>
                <div style="font-size:0.75rem;">COLS: <b class="mono">{len(df.columns)}</b></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        st.error(f"Missing Columns: {', '.join(missing)}")
        return False
    return True


def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.read()


def render_date_range_selector(key_prefix: str):
    """Standardized date range selector with high-visibility feedback."""

    st.markdown("""<div class="period-selector">""", unsafe_allow_html=True)
    st.markdown("#### 📅 Select Analysis Period")

    p1, p2, p3, p4 = st.columns(4)
    abs_min = date(2022, 1, 1)
    start_key = f"{key_prefix}_range_start"
    end_key = f"{key_prefix}_range_end"

    if start_key not in st.session_state:
        st.session_state[start_key] = date.today().replace(day=1)
    if end_key not in st.session_state:
        st.session_state[end_key] = date.today()

    if p1.button("This Month", key=f"{key_prefix}_tm_btn", use_container_width=True):
        st.session_state[start_key] = date.today().replace(day=1)
        st.session_state[end_key] = date.today()
        st.rerun()
    if p2.button("Last 90 Days", key=f"{key_prefix}_90_btn", use_container_width=True):
        st.session_state[start_key] = date.today() - timedelta(days=90)
        st.session_state[end_key] = date.today()
        st.rerun()
    if p3.button("Year to Date", key=f"{key_prefix}_ytd_btn", use_container_width=True):
        st.session_state[start_key] = date(date.today().year, 1, 1)
        st.session_state[end_key] = date.today()
        st.rerun()
    if p4.button("Full History", key=f"{key_prefix}_all_btn", use_container_width=True):
        st.session_state[start_key] = abs_min
        st.session_state[end_key] = date.today()
        st.rerun()

    c1, c2 = st.columns(2)
    start = c1.date_input(
        "Analysis From",
        value=st.session_state[start_key],
        key=start_key,
    )
    end = c2.date_input(
        "Analysis To", value=st.session_state[end_key], key=end_key
    )

    if start > end:
        end = start
        st.session_state[end_key] = end

    st.markdown(
        f"""<div class='period-status'>⚡ DASHBOARD ACTIVE: {start.strftime('%d %b %Y')} to {end.strftime('%d %b %Y')}</div>""",
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    return start, end


def render_reset_confirm(state_key: str, reset_fn):
    if st.button("Reset current workflow", key=f"reset_{state_key}"):
        st.session_state[f"confirm_reset_{state_key}"] = True

    if st.session_state.get(f"confirm_reset_{state_key}"):
        st.warning("Confirm reset: this clears current workflow data.")
        c1, c2 = st.columns(2)
        if c1.button("Confirm reset", key=f"confirm_yes_{state_key}", type="primary"):
            reset_fn()
            st.session_state[f"confirm_reset_{state_key}"] = False
            st.success("Workflow reset complete.")
            st.rerun()
        if c2.button("Cancel", key=f"confirm_no_{state_key}"):
            st.session_state[f"confirm_reset_{state_key}"] = False


def sample_file_download(label: str, data: list[dict], file_name: str):
    df = pd.DataFrame(data)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=label,
        data=csv_bytes,
        file_name=file_name,
        mime="text/csv",
        width="stretch",
    )


def render_plotly_chart(fig, key=None):
    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(255,255,255,0)",
        plot_bgcolor="rgba(255,255,255,0)",
        font_color="#1f2937",
        colorway=["#2563eb", "#0f766e", "#ea580c", "#7c3aed", "#0891b2", "#be185d"],
        hoverlabel=dict(
            bgcolor="#ffffff",
            font_color="#1f2937",
            bordercolor="#d8e1e8",
            font_family="Manrope",
        ),
        legend=dict(
            bgcolor="rgba(255,255,255,0.85)",
            bordercolor="#d8e1e8",
            borderwidth=1,
        ),
        margin=dict(l=28, r=18, t=54, b=28),
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="#e5ebf0",
        zerolinecolor="#dce4ea",
        linecolor="#dce4ea",
        tickfont=dict(color="#475569"),
        title_font=dict(color="#475569"),
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="#e5ebf0",
        zerolinecolor="#dce4ea",
        linecolor="#dce4ea",
        tickfont=dict(color="#475569"),
        title_font=dict(color="#475569"),
    )
    st.plotly_chart(fig, use_container_width=True, key=key)


def show_last_updated(path: str):
    if not os.path.exists(path):
        return
    updated = datetime.fromtimestamp(os.path.getmtime(path)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    st.caption(f"Last updated: {updated}")


def render_footer():
    """Universal premium footer."""
    footer_html = f"""
    <div class="app-footer">
        <span>&copy; 2026 DEEN COMMERCE</span>
        <span style="opacity: 0.4;">&bull;</span>
        <span>v{APP_VERSION}</span>
    </div>
    """
    st.markdown(footer_html, unsafe_allow_html=True)


def render_reset_confirm(label: str, state_key: str, reset_fn):
    """
    Registers a tool's reset function for the unified sidebar.
    """
    if "registered_resets" not in st.session_state:
        st.session_state.registered_resets = {}

    st.session_state.registered_resets[label] = {"fn": reset_fn, "key": state_key}


def render_sidebar_workspace_control():
    """Renders the unified workspace control hub in the sidebar."""
    st.divider()
    st.subheader("Workspace Control")
    with st.expander("Reset Active Tool Data", expanded=True):
        registered = st.session_state.get("registered_resets", {})
        if not registered:
            st.info("No active tool data found.")
        else:
            tool_to_wipe = st.selectbox(
                "Select tool to clear", list(registered.keys()), key="sidebar_reset_tool"
            )
            if st.button("Reset Tool Now", use_container_width=True, type="primary"):
                registered[tool_to_wipe]["fn"]()
                st.success(f"Wiped {tool_to_wipe} data.")
                st.rerun()

    if st.button("Full System Reset", use_container_width=True, type="secondary"):
        st.session_state.confirm_app_reset = True

    if st.session_state.get("confirm_app_reset"):
        st.warning("⚠️ Wipe EVERYTHING?")
        c1, c2 = st.columns(2)
        if c1.button("Yes", type="primary", use_container_width=True):
            from src.core.persistence import STATE_FILE

            if os.path.exists(STATE_FILE):
                os.remove(STATE_FILE)
            st.session_state.clear()
            st.rerun()
        if c2.button("No", use_container_width=True):
            st.session_state.confirm_app_reset = False
            st.rerun()
