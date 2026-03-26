import os
from datetime import datetime
from io import BytesIO

import pandas as pd
import streamlit as st

from src.ui.config import APP_TITLE, APP_VERSION


def inject_base_styles():
    theme = st.session_state.get("app_theme", "Dark Mode")
    if theme == "Dark Mode":
        root_vars = """
        --glass-bg: rgba(15, 23, 42, 0.75);
        --glass-border: rgba(255, 255, 255, 0.08);
        --accent-glow: rgba(59, 130, 246, 0.5);
        --neon-blue: #3b82f6;
        --text-primary: #f8fafc;
        --text-secondary: #94a3b8;
        --bg-grad: radial-gradient(circle at top right, #1e293b, #0f172a);
        """
    else:
        root_vars = """
        --glass-bg: rgba(255, 255, 255, 0.9);
        --glass-border: rgba(0, 0, 0, 0.12);
        --accent-glow: rgba(30, 64, 175, 0.15);
        --neon-blue: #1d4ed8;
        --text-primary: #0f172a;
        --text-secondary: #4b5563;
        --bg-grad: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
        """

    styles = """
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
    :root {
        __ROOT_VARS__
        --neon-green: #10b981;
    }

    /* THEME SYNC ENGINE */
    .stApp {
        background: var(--bg-grad);
        color: var(--text-primary);
        font-family: 'Outfit', sans-serif;
        background-color: var(--background-color); /* Native Sync */
    }

    /* GLASS CARD EFFECT */
    .hub-card {
        background: var(--glass-bg);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        border: 1px solid var(--glass-border);
        border-radius: 24px;
        padding: 2rem;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    }
    .hub-card:hover {
        border-color: var(--neon-blue);
        box-shadow: 0 0 20px rgba(59, 130, 246, 0.2);
        transform: translateY(-2px);
    }

    .hub-title {
        font-size: 3.8rem !important;
        font-weight: 800 !important;
        letter-spacing: -0.05em !important;
        background: linear-gradient(to bottom right, var(--text-primary) 30%, var(--neon-blue));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem !important;
        filter: drop-shadow(0 0 8px rgba(59, 130, 246, 0.3));
        line-height: 1.1 !important;
    }

    /* STEPS HUD */
    .hub-step {
        background: var(--glass-bg);
        border: 1px solid var(--glass-border);
        border-radius: 16px;
        padding: 1.25rem;
        text-align: center;
        transition: all 0.3s ease;
        font-size: 0.9rem;
    }
    .hub-step.active {
        background: rgba(59, 130, 246, 0.1);
        border-color: var(--neon-blue);
        box-shadow: 0 0 15px rgba(59, 130, 246, 0.3);
    }

    /* ACTION BAR STICKY GLASS */
    .hub-action-wrap {
        position: fixed;
        bottom: 2rem;
        left: 50%;
        transform: translateX(-50%);
        width: 90%;
        max-width: 800px;
        background: var(--glass-bg);
        backdrop-filter: blur(20px);
        border: 1px solid var(--glass-border);
        border-radius: 30px;
        padding: 1rem 2rem;
        box-shadow: 0 -10px 40px rgba(0, 0, 0, 0.4);
        z-index: 9999;
    }

    /* METRICS UPGRADE */
    [data-testid="stMetric"] {
        background: var(--glass-bg);
        padding: 1.2rem;
        border-radius: 20px;
        border: 1px solid var(--glass-border);
    }
    [data-testid="stMetricValue"] {
        font-family: 'JetBrains Mono', monospace !important;
        color: var(--neon-blue) !important;
        font-size: 2.0rem !important;
        white-space: nowrap !important;
    }

    /* TABS STYLING */
    .stTabs [data-baseweb="tab-list"] {
        gap: 12px;
        background-color: transparent;
    }
    .stTabs [data-baseweb="tab"] {
        height: 48px;
        background-color: var(--glass-bg);
        border-radius: 8px 8px 0 0;
        color: var(--text-secondary);
        border: 1px solid var(--glass-border);
        border-bottom: none;
        padding: 0 16px;
        transition: all 0.2s ease;
        margin-top: 4px;
    }
    .stTabs [aria-selected="true"] {
        background-color: rgba(59, 130, 246, 0.15) !important;
        color: var(--neon-blue) !important;
        border-color: var(--neon-blue) !important;
    }

    /* BUTTONS */
    .stButton>button {
        border-radius: 14px !important;
        border: 1px solid var(--glass-border) !important;
        background: var(--glass-bg) !important;
        color: var(--text-primary) !important;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        font-weight: 700 !important;
    }
    .stButton>button[kind="primary"] {
        background: linear-gradient(135deg, var(--neon-blue) 0%, #1d4ed8 100%) !important;
        border: none !important;
        box-shadow: 0 4px 15px rgba(59, 130, 246, 0.4) !important;
    }

    /* GLOBAL FILE UPLOADER THEME */
    [data-testid="stFileUploader"] {
        border: 2px dashed rgba(59, 130, 246, 0.2) !important;
        border-radius: 20px !important;
        background: var(--glass-bg) !important;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1) !important;
    }
    [data-testid="stFileUploader"]:hover {
        border-color: var(--neon-blue) !important;
        background: rgba(59, 130, 246, 0.08) !important;
    }
    [data-testid="stFileUploader"] label, 
    [data-testid="stFileUploader"] div[data-testid="stMarkdownContainer"],
    [data-testid="stFileUploader"] small,
    [data-testid="stFileUploader"] div[data-testid="stFileUploaderDropzoneInstructions"] > div {
        display: none !important;
    }

    /* MINI UPLOADER SCOPING */
    .mini-uploader [data-testid="stFileUploader"] {
        max-width: 80px;
        margin: 0 auto;
    }
    .mini-uploader [data-testid="stFileUploader"] section {
        padding: 0px !important;
        border: 1px solid var(--glass-border) !important;
    }
    .mini-uploader [data-testid="stFileUploader"] section::after {
        display: none !important;
    }
    .mini-uploader [data-testid="stFileUploader"] [data-testid="stBaseButton-secondary"] {
        height: 50px !important;
        width: 100% !important;
        background: transparent !important;
        border: none !important;
    }
    .mini-uploader [data-testid="stFileUploader"] div,
    .mini-uploader [data-testid="stFileUploader"] span,
    .mini-uploader [data-testid="stFileUploader"] button::after {
        font-size: 0px !important;
        color: transparent !important;
        display: none !important;
    }
    .mini-uploader [data-testid="stFileUploader"] [data-testid="stBaseButton-secondary"]::before {
        content: "↑";
        font-size: 20px;
        color: var(--neon-blue);
        display: block;
        width: 100%;
        text-align: center;
        line-height: 50px;
        filter: drop-shadow(0 0 5px var(--neon-blue));
    }
    [data-testid="stFileUploader"] section {
        padding: 2rem !important;
        display: flex;
        justify-content: center;
        align-items: center;
    }

    [data-testid="stFileUploader"] section::after {
        content: "📤";
        font-size: 2.5rem;
        opacity: 0.6;
        transition: all 0.3s ease;
        filter: grayscale(1);
    }

    [data-testid="stFileUploader"]:hover section::after {
        transform: scale(1.1) translateY(-5px);
        opacity: 1;
        filter: grayscale(0) drop-shadow(0 0 10px var(--neon-blue));
    }

    /* SCROLLBAR & TABLES */
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: #334155; border-radius: 10px; }
    ::-webkit-scrollbar-thumb:hover { background: #475569; }

    /* MOBILE PROTOCOL OVERRIDES */
    @media (max-width: 768px) {
        .hub-title { font-size: 2.3rem !important; margin-bottom: 0.2rem !important; }
        .hub-card { padding: 1.25rem; border-radius: 18px; margin-bottom: 1rem; }
        .hub-action-wrap { width: 95%; bottom: 1rem; padding: 0.75rem 1rem; border-radius: 20px; }
        .stMetricValue { font-size: 2rem !important; }
        [data-testid="stSidebar"] { width: 100vw !important; }
        .stTabs [data-baseweb="tab"] { padding: 0 12px; font-size: 0.8rem; }
    }

</style>""".replace("__ROOT_VARS__", root_vars)
    st.markdown(styles, unsafe_allow_html=True)


def render_header():
    header_html = f"""
    <div style="text-align: center; padding: 3rem 0 1rem; margin-bottom: 2rem; display: flex; flex-direction: column; align-items: center; justify-content: center; width: 100%;">
        <div style="display: inline-block; position: relative; padding: 0 1rem;">
            <h1 class="hub-title" style="margin: 0 !important; padding: 0 !important; line-height: 1 !important; text-transform: none; letter-spacing: -0.04em !important; font-size: 3.8rem !important;">{APP_TITLE}</h1>
            <div style="display: flex; justify-content: center; align-items: center; gap: 10px; margin-top: -6px; opacity: 1; padding: 0;">
                 <div style="height: 1.5px; background: linear-gradient(to right, transparent, var(--neon-blue)); flex-grow: 1; min-width: 20px;"></div>
                 <img src="https://cdn.brandfetch.io/deencommerce.com" style="height: 16px; border-radius: 4px; filter: drop-shadow(0 0 8px var(--neon-blue));">
                 <a href="https://deencommerce.com/" target="_blank" style="color: var(--text-primary); text-decoration: none; font-size: 0.85rem; letter-spacing: 0.2em; font-weight: 800; text-transform: uppercase; line-height: 1;">DEEN COMMERCE</a>
                 <div style="height: 1.5px; background: linear-gradient(to left, transparent, var(--neon-blue)); flex-grow: 1; min-width: 20px;"></div>
            </div>
        </div>
        <p style="color: var(--text-secondary); font-size: 0.8rem; letter-spacing: 0.15em; text-transform: uppercase; margin-top: 20px; opacity: 0.4; font-weight: 500;">
            🛰️ NEXT-GEN OPS COMMAND • {APP_VERSION}
        </p>
    </div>"""
    st.markdown(header_html, unsafe_allow_html=True)


def section_card(title: str, help_text: str = ""):
    card_html = f"""<div class="hub-card"><div style="font-size:1.4rem; font-weight:700; color:var(--text-primary); margin-bottom:0.5rem;">{title}</div><div style="color:var(--text-secondary); font-size:1rem; line-height:1.6;">{help_text}</div></div>"""
    st.markdown(card_html, unsafe_allow_html=True)


def render_metric_hud(label: str, value: str, icon: str = "📈"):
    """
    Renders a premium, non-truncating metric card using HTML.
    Prevents the Streamlit '...' truncation for large revenue figures.
    """
    html = f"""
    <div style="background: var(--glass-bg); border: 1px solid var(--glass-border); padding: 1rem; border-radius: 16px; margin: 0.5rem 0;">
        <div style="font-size: 0.75rem; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 0.4rem; display: flex; align-items: center; gap: 8px;">
            <span>{icon}</span> {label}
        </div>
        <div style="font-family: 'JetBrains Mono', monospace; color: var(--neon-blue); font-size: 1.5rem; font-weight: 700; white-space: nowrap;" title="{value}">
            {value}
        </div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def render_steps(steps: list[str], current_step: int):
    cols = st.columns(len(steps))
    for idx, step in enumerate(steps):
        is_active = idx == current_step
        cls = "hub-step active" if is_active else "hub-step"
        step_html = f"""<div class="{cls}"><span style="opacity:0.5; font-size:0.75rem; display:block; margin-bottom:2px;">Step {idx + 1}</span>{step}</div>"""
        cols[idx].markdown(step_html, unsafe_allow_html=True)


def render_file_summary(
    uploaded_file, df: pd.DataFrame | None, required_columns: list[str]
):
    if not uploaded_file:
        st.info("No file uploaded yet.")
        return False

    if df is None:
        st.error(f"❌ Failed to read {uploaded_file.name}")
        return False

    with st.container(border=True):
        st.caption(f"📄 FILENAME: {uploaded_file.name}")
        c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
        c1.metric("Rows", f"{len(df):,}")
        c2.metric("Cols", len(df.columns))
        c3.metric("Steps", len(required_columns))

        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            c4.error(f"Missing: {', '.join(missing)}")
            return False
        else:
            c4.success("✅ All columns verified")
            return True


def render_action_bar(
    primary_label: str,
    primary_key: str,
    secondary_label: str | None = None,
    secondary_key: str | None = None,
):
    st.markdown('<div class="hub-action-wrap">', unsafe_allow_html=True)
    if secondary_label and secondary_key:
        c1, c2 = st.columns([2, 1])
        primary_clicked = c1.button(
            primary_label, type="primary", width="stretch", key=primary_key
        )
        secondary_clicked = c2.button(
            secondary_label, width="stretch", key=secondary_key
        )
    else:
        primary_clicked = st.button(
            primary_label, type="primary", width="stretch", key=primary_key
        )
        secondary_clicked = False
    st.markdown("</div>", unsafe_allow_html=True)
    return primary_clicked, secondary_clicked


def render_mini_uploader(label: str, key: str):
    """Compact file uploader with a bold label and minimal icon interface."""
    st.markdown(
        f"<div style='text-align:center;'><b>{label}</b></div>", unsafe_allow_html=True
    )
    st.markdown("<div class='mini-uploader'>", unsafe_allow_html=True)
    f = st.file_uploader(
        label, key=key, type=["xlsx", "csv"], label_visibility="collapsed"
    )
    st.markdown("</div>", unsafe_allow_html=True)
    return f


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


def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.read()


def show_last_updated(path: str):
    if not os.path.exists(path):
        return
    updated = datetime.fromtimestamp(os.path.getmtime(path)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    st.caption(f"Last updated: {updated}")
