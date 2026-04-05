import os
from datetime import datetime
from io import BytesIO

import pandas as pd
import streamlit as st

from FrontEnd.utils.config import APP_TITLE, APP_VERSION


def inject_base_styles():
    st.markdown(
        """
        <style>
        :root {
            --primary: #0f4c81;
            --primary-strong: #083358;
            --accent: #14b8a6;
            --surface: #f4f7fb;
            --surface-raised: #ffffff;
            --surface-soft: #eef4f8;
            --text-strong: #102132;
            --text-muted: #5f7183;
            --border-soft: rgba(148, 163, 184, 0.22);
            --action-surface: rgba(255, 255, 255, 0.88);
            --card-shadow: 0 18px 44px rgba(15, 35, 58, 0.10);
            --card-shadow-soft: 0 10px 28px rgba(15, 35, 58, 0.06);
        }
        html, body, [class*="css"] {
            font-family: "Segoe UI", "IBM Plex Sans", "Helvetica Neue", sans-serif;
        }
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(20, 184, 166, 0.08), transparent 28%),
                radial-gradient(circle at top right, rgba(15, 76, 129, 0.10), transparent 24%),
                linear-gradient(180deg, #f6fafc 0%, #eef4f8 100%);
        }
        .hub-footer {
            position: fixed;
            bottom: 0;
            left: 0;
            width: 100%;
            background: var(--action-surface);
            backdrop-filter: blur(8px);
            padding: 12px 24px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            color: var(--text-muted);
            font-size: 0.8rem;
            border-top: 1px solid var(--border-soft);
            z-index: 999;
        }
        .hub-footer a {
            color: var(--primary);
            text-decoration: none;
            font-weight: 500;
        }
        /* Extra padding for main content so it doesn't get hidden by fixed footer */
        .main .block-container {
            padding-bottom: 80px !important;
        }
        .deen-logo-small {
            vertical-align: middle;
            margin-right: 6px;
            border-radius: 4px;
        }
        .hub-title-row {
            display: flex;
            align-items: center;
            justify-content: center;
            background: linear-gradient(90deg, rgba(15, 76, 129, 0.08) 0%, rgba(20, 184, 166, 0.02) 100%);
            border: 1px solid var(--border-soft);
            padding: 0.75rem 1rem;
            margin-bottom: 0.75rem;
            border-radius: 18px;
            text-align: center;
            box-shadow: var(--card-shadow-soft);
        }
        /* Remove the top gap without touching the sidebar toggle */
        .main .block-container {
            padding-top: 0.5rem !important;
            margin-top: 0 !important;
            padding-bottom: 80px !important;
        }
        .hub-title {
            margin: 0;
            font-weight: 700;
            color: var(--text-strong);
        }
        .hub-subtitle {
            margin: 0;
            color: var(--text-muted);
            font-size: 0.95rem;
        }
        .hub-card {
            background: linear-gradient(180deg, rgba(255,255,255,0.94) 0%, rgba(248,251,253,0.98) 100%);
            border: 1px solid var(--border-soft);
            border-radius: 18px;
            padding: 16px 18px;
            margin-bottom: 12px;
            box-shadow: var(--card-shadow-soft);
        }
        .bi-hero {
            background: linear-gradient(135deg, rgba(8, 51, 88, 0.96) 0%, rgba(15, 76, 129, 0.94) 58%, rgba(20, 184, 166, 0.88) 100%);
            color: #f8fbff;
            border-radius: 24px;
            padding: 1.35rem 1.5rem;
            margin-bottom: 1rem;
            box-shadow: var(--card-shadow);
            position: relative;
            overflow: hidden;
        }
        .bi-hero::after {
            content: "";
            position: absolute;
            inset: auto -8% -35% auto;
            width: 220px;
            height: 220px;
            background: radial-gradient(circle, rgba(255,255,255,0.18) 0%, rgba(255,255,255,0.0) 68%);
        }
        .bi-hero-title {
            font-size: 1.45rem;
            font-weight: 700;
            letter-spacing: -0.02em;
            margin-bottom: 0.35rem;
        }
        .bi-hero-subtitle {
            max-width: 760px;
            font-size: 0.92rem;
            line-height: 1.55;
            color: rgba(244, 250, 255, 0.86);
        }
        .bi-chip-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.55rem;
            margin-top: 0.85rem;
        }
        .bi-chip {
            border-radius: 999px;
            padding: 0.36rem 0.72rem;
            background: rgba(255, 255, 255, 0.12);
            border: 1px solid rgba(255, 255, 255, 0.16);
            font-size: 0.75rem;
            color: #f5fbff;
            backdrop-filter: blur(8px);
        }
        .bi-commentary {
            background: linear-gradient(180deg, rgba(255,255,255,0.95) 0%, rgba(242,248,251,0.92) 100%);
            border: 1px solid var(--border-soft);
            border-radius: 18px;
            box-shadow: var(--card-shadow-soft);
            padding: 1rem 1.1rem;
            margin-bottom: 1rem;
        }
        .bi-commentary-label {
            font-size: 0.72rem;
            font-weight: 700;
            color: var(--primary);
            letter-spacing: 0.12em;
            text-transform: uppercase;
            margin-bottom: 0.55rem;
        }
        .bi-commentary ul {
            margin: 0;
            padding-left: 1rem;
            color: var(--text-strong);
        }
        .bi-commentary li {
            margin-bottom: 0.45rem;
            line-height: 1.5;
        }
        [data-testid="stMetricContainer"] {
            background: rgba(255,255,255,0.80);
            border: 1px solid var(--border-soft);
            border-radius: 18px;
            padding: 0.85rem 0.95rem;
            box-shadow: var(--card-shadow-soft);
        }
        /* Target the streamlit container that HAS the hub-action-wrap marker inside it */
        div[data-testid="stVerticalBlock"]:has(> div[data-testid="stMarkdownContainer"] .hub-action-wrap) {
            position: sticky;
            bottom: 60px; /* Offset to stay above fixed footer */
            padding: 16px;
            border: 1px solid var(--border-soft);
            border-radius: 18px;
            background: var(--action-surface);
            backdrop-filter: blur(16px);
            box-shadow: var(--card-shadow);
            z-index: 100;
            margin-top: 20px;
        }
        
        /* Ensure the marker itself doesn't take up space */
        .hub-action-wrap {
            display: none;
        }
        
        /* Premium Tab Styling */
        div[data-testid="stTab"] button {
            font-size: 0.9rem !important;
            font-weight: 600 !important;
            color: #617385 !important;
            transition: all 0.3s ease !important;
            border: none !important;
            background: transparent !important;
            padding: 10px 18px !important;
        }
        div[data-testid="stTab"] button:hover {
            color: var(--primary) !important;
            background: rgba(15, 76, 129, 0.05) !important;
            border-radius: 10px 10px 0 0 !important;
        }
        div[data-testid="stTab"] button[aria-selected="true"] {
            color: var(--primary) !important;
            border-bottom: 2px solid var(--primary) !important;
        }
        
        /* Responsive Design - Mobile First */
        @media (max-width: 768px) {
            .main .block-container {
                padding-left: 0.5rem !important;
                padding-right: 0.5rem !important;
                padding-bottom: 120px !important;
                margin-top: -0.5rem !important;
            }
            .hub-title-row {
                padding: 8px 12px;
                margin-bottom: 8px;
            }
            .hub-title {
                font-size: 1.1rem !important;
                line-height: 1.3;
            }
            .hub-subtitle {
                font-size: 0.75rem !important;
            }
            .hub-card {
                padding: 10px 12px;
                border-radius: 8px;
                margin-bottom: 8px;
            }
            /* Mobile Footer */
            .hub-footer {
                padding: 8px 12px;
                font-size: 0.7rem;
                flex-direction: column;
                text-align: center;
                gap: 4px;
            }
            /* Mobile Metrics */
            div[data-testid="stMetricValue"] {
                font-size: 1.1rem !important;
            }
            div[data-testid="stMetricLabel"] {
                font-size: 0.7rem !important;
            }
            div[data-testid="stMetricDelta"] {
                font-size: 0.65rem !important;
            }
            /* Mobile Tabs */
            div[data-testid="stTab"] button {
                padding: 6px 10px !important;
                font-size: 0.75rem !important;
            }
            /* Mobile Tables */
            .stDataFrame {
                font-size: 0.8rem !important;
            }
            /* Mobile Buttons */
            .stButton > button {
                font-size: 0.85rem !important;
                padding: 6px 12px !important;
            }
            /* Mobile Sidebar */
            [data-testid="stSidebar"] {
                width: 280px !important;
            }
        }
        
        /* Tablet / Small Laptop */
        @media (min-width: 769px) and (max-width: 1024px) {
            .main .block-container {
                padding-left: 1rem !important;
                padding-right: 1rem !important;
                padding-bottom: 100px !important;
            }
            .hub-title {
                font-size: 1.3rem !important;
            }
            .hub-card {
                padding: 12px 14px;
            }
            div[data-testid="stTab"] button {
                padding: 8px 16px !important;
                font-size: 0.85rem !important;
            }
        }
        
        /* Large Screens */
        @media (min-width: 1400px) {
            .main .block-container {
                max-width: 1400px !important;
                padding-left: 2rem !important;
                padding-right: 2rem !important;
            }
            .hub-card {
                padding: 18px 20px;
            }
        }
        
        /* Ensure proper column stacking on mobile */
        @media (max-width: 640px) {
            [data-testid="stHorizontalBlock"] > [data-testid="column"] {
                min-width: 100% !important;
                flex: 1 1 100% !important;
            }
        }
        
        /* Metric container alignment */
        [data-testid="stMetricContainer"] {
            min-height: 100px;
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
        }
        
        /* Consistent metric value alignment */
        [data-testid="stMetricValue"] {
            font-size: 1.38rem !important;
            font-weight: 600 !important;
            line-height: 1.2 !important;
            color: var(--text-strong) !important;
        }
        
        /* Metric label alignment */
        [data-testid="stMetricLabel"] {
            font-size: 0.8rem !important;
            font-weight: 500 !important;
            color: var(--text-muted) !important;
        }
        
        /* Ensure proper column stacking on mobile */
        @media (max-width: 640px) {
            [data-testid="stHorizontalBlock"] > [data-testid="column"] {
                min-width: 100% !important;
                flex: 1 1 100% !important;
            }
            /* Mobile metric adjustments */
            [data-testid="stMetricValue"] {
                font-size: 1.1rem !important;
            }
            [data-testid="stMetricContainer"] {
                min-height: auto !important;
            }
        }
        
        /* Ensure dialogs are scrollable and properly sized */
        div[role="dialog"] {
            max-width: 95vw !important;
            max-height: 90vh !important;
            overflow-y: auto !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_branding():
    """Elegant sidebar branding to save main screen space."""
    logo_src = "https://logo.clearbit.com/deencommerce.com"
    try:
        import base64
        import os

        logo_jpg = os.path.join("assets", "deen_logo.jpg")
        if os.path.exists(logo_jpg):
            with open(logo_jpg, "rb") as f:
                b64 = base64.b64encode(f.read()).decode()
            logo_src = f"data:image/jpeg;base64,{b64}"
    except:
        pass

    # Add Last Synced info if available
    sync_html = ""
    if st.session_state.get("live_sync_time"):
        diff = datetime.now() - st.session_state.live_sync_time
        mins = int(diff.total_seconds() / 60)
        sync_label = "Just now" if mins < 1 else f"{mins}m ago"
        sync_html = f'<div style="font-size:0.75rem; color:#64748b; margin-top:10px;">🔄 Last Synced: {sync_label}</div>'

    # Render exactly as previous vertical stack
    st.markdown(
        f"""<div style="padding:10px 16px; border-bottom:1px solid rgba(128,128,128,0.1); margin-bottom:15px;">
            <div style="font-weight:700; font-size:1.1rem; line-height:1.2;">
                Automation Pivot<br>
                <span style="font-size:0.85rem; font-weight:400; color:#64748b;">v2.5.0</span>
            </div>
        </div>""",
        unsafe_allow_html=True,
    )


def render_header():
    """Minimal header for the main page content area."""
    st.markdown(
        f"""
        <div class="hub-title-row">
            <h1 class="hub-title">{APP_TITLE} <span style="color:var(--primary);">{APP_VERSION}</span></h1>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_card(title: str, help_text: str = ""):
    """Render a section card with title and optional help text."""
    st.markdown(
        f"""
        <div class="hub-card">
          <div style="font-weight:600;">{title}</div>
          <div style="color:var(--text-muted); margin-top:4px;">{help_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_bi_hero(title: str, subtitle: str, chips: list[str] | None = None):
    chips_html = ""
    if chips:
        chips_html = '<div class="bi-chip-row">' + "".join(
            f'<span class="bi-chip">{chip}</span>' for chip in chips if chip
        ) + "</div>"
    st.markdown(
        f"""
        <div class="bi-hero">
          <div class="bi-hero-title">{title}</div>
          <div class="bi-hero-subtitle">{subtitle}</div>
          {chips_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_commentary_panel(title: str, bullet_points: list[str]):
    if not bullet_points:
        return
    items = "".join(f"<li>{point}</li>" for point in bullet_points if point)
    st.markdown(
        f"""
        <div class="bi-commentary">
          <div class="bi-commentary-label">{title}</div>
          <ul>{items}</ul>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_footer():
    """Renders a robust and persistent branding footer."""
    logo_src = "https://logo.clearbit.com/deencommerce.com"
    try:
        import base64

        logo_jpg = os.path.join("assets", "deen_logo.jpg")
        if os.path.exists(logo_jpg):
            with open(logo_jpg, "rb") as f:
                b64 = base64.b64encode(f.read()).decode()
            logo_src = f"data:image/jpeg;base64,{b64}"
    except:
        pass

    st.markdown(
        f"""
        <div class="hub-footer">
            <div style="width:100%; text-align:center;">
                <span style="color:var(--text-muted); margin-right:12px;">© 2026 <a href="https://github.com/saajiidi" target="_blank" style="color:var(--primary);">Sajid Islam</a>. All rights reserved.</span>
                <span style="color:var(--text-muted); margin:0 12px; opacity:0.5;">|</span>
                <a href="https://deencommerce.com/" target="_blank" style="color:var(--primary); text-decoration:none;">
                    <img src="{logo_src}" width="20" class="deen-logo-small" onerror="this.style.display='none'">
                    Powered by <b>DEEN Commerce</b>
                </a>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_file_summary(uploaded_file, df: pd.DataFrame | None, required_columns: list[str]):
    if not uploaded_file:
        st.info("No file uploaded yet.")
        return False

    st.caption(f"File: {uploaded_file.name}")
    if df is None:
        st.warning("Could not read this file.")
        return False

    c1, c2, c3 = st.columns(3)
    c1.metric("Rows", len(df))
    c2.metric("Columns", len(df.columns))
    c3.metric("Required", len(required_columns))

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        st.error(f"Missing required columns: {', '.join(missing)}")
        return False
    st.success("Required columns check passed.")
    return True


def render_action_bar(
    primary_label: str,
    primary_key: str,
    secondary_label: str | None = None,
    secondary_key: str | None = None,
):
    with st.container():
        # This marker allows the CSS :has() selector to style this entire container
        st.markdown('<div class="hub-action-wrap"></div>', unsafe_allow_html=True)
        if secondary_label and secondary_key:
            c1, c2 = st.columns([2, 1])
            primary_clicked = c1.button(
                primary_label, type="primary", use_container_width=True, key=primary_key
            )
            secondary_clicked = c2.button(
                secondary_label, use_container_width=True, key=secondary_key
            )
        else:
            primary_clicked = st.button(
                primary_label, type="primary", use_container_width=True, key=primary_key
            )
            secondary_clicked = False
    return primary_clicked, secondary_clicked


def render_reset_confirm(label: str, state_key: str, reset_fn):
    """
    Registers a tool's reset function for the unified sidebar.
    Doesn't render anything in the sidebar immediately to avoid duplicates.
    """
    if "registered_resets" not in st.session_state:
        st.session_state.registered_resets = {}

    st.session_state.registered_resets[label] = {"fn": reset_fn, "key": state_key}


def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.read()


def show_last_updated(path: str):
    if not os.path.exists(path):
        return
    updated = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M:%S")
    st.caption(f"Last updated: {updated}")
