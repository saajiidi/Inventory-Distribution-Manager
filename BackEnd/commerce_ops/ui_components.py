import os
from datetime import datetime
from io import BytesIO

import pandas as pd
import streamlit as st

from BackEnd.commerce_ops.ui_config import APP_TITLE, APP_VERSION
from BackEnd.utils.io import to_excel_bytes


def inject_base_styles():
    st.markdown(
        """
        <style>
        :root {
            --primary: var(--primary-color, #1d4ed8);
            --surface: var(--background-color, #f8fafc);
            --border: var(--secondary-background-color, #e2e8f0);
            --text-muted: var(--text-color, #64748b);
            --step-surface: var(--background-color, #ffffff);
            --step-text: var(--text-color, #0f172a);
            --step-active-bg: var(--secondary-background-color, #eff6ff);
            --action-surface: var(--background-color, rgba(255, 255, 255, 0.96));
            --card-shadow: rgba(0, 0, 0, 0.15);
        }
        .hub-footer {
            position: fixed;
            bottom: 0;
            left: 0;
            width: 100%;
            background: var(--background-color);
            color: var(--text-color);
            padding: 12px 24px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 0.8rem;
            border-top: 1px solid rgba(128, 128, 128, 0.2);
            z-index: 999;
        }
        .hub-footer a {
            color: inherit;
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
            background: linear-gradient(90deg, rgba(29, 78, 216, 0.03) 0%, rgba(29, 78, 216, 0) 100%);
            border-left: 4px solid #1d4ed8;
            border-bottom: 1px solid var(--border);
            padding: 2px 16px;
            margin-bottom: 0px;
            border-radius: 0 4px 4px 0;
            text-align: center;
        }
        /* Remove the top gap without touching the sidebar toggle */
        .main .block-container {
            padding-top: 0 !important;
            margin-top: -1.75rem !important;
            padding-bottom: 80px !important;
        }
        .hub-title {
            margin: 0;
            font-weight: 700;
        }
        .hub-subtitle {
            margin: 0;
            color: var(--text-muted);
            font-size: 0.95rem;
        }
        .hub-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 14px 16px;
            margin-bottom: 4px;
            box-shadow: 0 4px 12px var(--card-shadow);
            transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
            display: flex;
            flex-direction: column;
            height: 100%;
        }
        .hub-welcome-banner {
            background-color: transparent;
            border: 1px solid rgba(128, 128, 128, 0.2);
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 15px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
        }
        .hub-welcome-banner a, .hub-welcome-banner b {
            color: inherit !important;
        }
        .hub-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 12px 32px var(--card-shadow);
            border-color: var(--primary);
        }
        
        /* Premium Export / Download Button Styling */
        div[data-testid="stDownloadButton"] > button {
            background: linear-gradient(135deg, #10b981 0%, #059669 100%) !important;
            color: white !important;
            border: none !important;
            border-radius: 100px !important;
            font-weight: 600 !important;
            letter-spacing: 0.5px !important;
            box-shadow: 0 4px 10px rgba(16, 185, 129, 0.3) !important;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
        }
        div[data-testid="stDownloadButton"] > button:hover {
            transform: translateY(-2px) !important;
            box-shadow: 0 6px 14px rgba(16, 185, 129, 0.4) !important;
            background: linear-gradient(135deg, #059669 0%, #047857 100%) !important;
        }
        
        /* Global button hover scaling */
        button[kind="secondary"]:hover, button[kind="primary"]:hover {
            transform: scale(1.01);
            transition: all 0.2s ease !important;
        }
        
        /* Premium Tab Styling */
        div[data-testid="stTab"] button {
            font-size: 0.9rem !important;
            font-weight: 600 !important;
            color: #64748b !important;
            transition: all 0.3s ease !important;
            border: none !important;
            background: transparent !important;
            padding: 10px 20px !important;
        }
        div[data-testid="stTab"] button:hover {
            color: #1d4ed8 !important;
            background: rgba(29, 78, 216, 0.04) !important;
            border-radius: 8px 8px 0 0 !important;
        }
        div[data-testid="stTab"] button[aria-selected="true"] {
            color: #1d4ed8 !important;
            border-bottom: 2px solid #1d4ed8 !important;
        }
        
        /* Premium Dataframe & Table Wrapper Styling */
        div[data-testid="stDataFrame"] > div, div[data-testid="stTable"] > div {
            border: 1px solid rgba(128, 128, 128, 0.15) !important;
            border-radius: 8px !important;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.04) !important;
            transition: box-shadow 0.3s ease !important;
        }
        div[data-testid="stDataFrame"] > div:hover, div[data-testid="stTable"] > div:hover {
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.08) !important;
        }
        
        /* Smooth Page Load Animation */
        .block-container {
            animation: fadeIn 0.6s ease-out forwards;
        }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(15px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        /* Global Premium Metric Cards */
        div[data-testid="stMetric"] {
            background: var(--surface) !important;
            border: 1px solid rgba(128, 128, 128, 0.12) !important;
            padding: 16px 20px !important;
            border-radius: 12px !important;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.03) !important;
            transition: all 0.2s ease-in-out !important;
            display: flex !important;
            flex-direction: column !important;
            justify-content: center !important;
            align-items: center !important;
            text-align: center !important;
            height: 100% !important;
            min-height: 100px !important;
            overflow: hidden !important;
        }
        div[data-testid="stMetric"]:hover {
            box-shadow: 0 8px 16px rgba(0, 0, 0, 0.06) !important;
            border-color: rgba(29, 78, 216, 0.4) !important;
            transform: translateY(-2px) !important;
        }
        div[data-testid="stMetricLabel"] {
            color: #64748b !important;
            font-weight: 700 !important;
            font-size: clamp(0.55rem, 1.2vw, 0.7rem) !important;
            text-transform: uppercase !important;
            letter-spacing: 0.5px !important;
            margin-bottom: auto !important;
            width: 100% !important;
            display: block !important;
            white-space: normal !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
        div[data-testid="stMetricLabel"] > div {
            justify-content: center !important;
        }
        div[data-testid="stMetricValue"] {
            font-size: clamp(1.4rem, 4vw, 2.2rem) !important;
            line-height: 1.1 !important;
            margin: 4px 0 !important;
            width: 100% !important;
            white-space: normal !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
        div[data-testid="stMetricDelta"] {
            margin-top: auto !important;
            width: 100% !important;
            display: flex !important;
            justify-content: center !important;
        }
        div[data-testid="stMetricDelta"] > div {
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
        
        /* Premium Expander Styling */
        div[data-testid="stExpander"] {
            border: 1px solid rgba(128, 128, 128, 0.15) !important;
            border-radius: 8px !important;
            background: var(--surface) !important;
            transition: border-color 0.3s ease !important;
        }
        div[data-testid="stExpander"]:hover {
            border-color: rgba(29, 78, 216, 0.3) !important;
        }
        div[data-testid="stExpander"] > details > summary {
            padding: 10px 15px !important;
            font-weight: 600 !important;
        }
        
        @media (max-width: 900px) {
            .block-container {
                padding-left: 0.5rem !important;
                padding-right: 0.5rem !important;
                margin-top: -2.5rem !important;
            }
            .hub-title {
                font-size: 1.2rem !important;
                line-height: 1.2;
            }
            .hub-subtitle {
                font-size: 0.8rem !important;
            }
            .hub-card {
                padding: 10px;
                border-radius: 8px;
            }
        .hub-footer {
            flex-direction: column;
            text-align: center;
            justify-content: center;
            gap: 8px;
            position: relative;
        }
            /* Metric Font Scaling for Small Screens */
            div[data-testid="stMetricValue"] {
                font-size: 1.8rem !important;
                word-break: break-word !important;
            }
            div[data-testid="stMetricLabel"] {
                font-size: 0.6rem !important;
                word-break: break-word !important;
            }
            div[data-testid="stMetricDelta"] {
                display: none !important;
            }
            div[data-testid="stMetricContainer"] {
                min-height: auto !important;
                height: auto !important;
                padding: 12px !important;
            }
            /* Compact Tabs on Mobile */
            div[data-testid="stTab"] button {
                padding: 8px 12px !important;
                font-size: 0.8rem !important;
            }
        }
        
        /* Hide Plotly legends on mobile and small screens */
        @media (max-width: 900px) {
            .js-plotly-plot .legend, .js-plotly-plot .legendtoggle, .js-plotly-plot .legend-bg, .js-plotly-plot .legend-layer {
                display: none !important;
            }
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
                DEEN BI OPS<br>
                <span style="font-size:0.85rem; font-weight:400; color:#64748b;">v9.0</span>
            </div>
        </div>""",
        unsafe_allow_html=True,
    )


def render_header():
    """Minimal header for the main page content area."""
    st.markdown(
        f"""
        <div class="hub-title-row">
            <h1 class="hub-title">{APP_TITLE} <span style="color:#1d4ed8;">{APP_VERSION}</span></h1>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_card(title: str, help_text: str = ""):
    st.markdown(
        f"""
        <div class="hub-card">
          <div style="font-weight:600;">{title}</div>
          <div style="color:var(--text-muted); margin-top:4px;">{help_text}</div>
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
                <div style="display:flex; align-items:center; justify-content:center; flex-wrap:wrap; gap:8px;">
                    <span>© 2026 <a href="https://github.com/saajiidi" target="_blank">Sajid Islam</a>. All rights reserved.</span>
                    <span style="opacity:0.5; margin:0 4px;">|</span>
                    <a href="https://deencommerce.com/" target="_blank" style="text-decoration:none; display:flex; align-items:center; justify-content:center; flex-wrap:wrap; gap:6px;">
                        <span>Powered by</span>
                        <img src="{logo_src}" width="18" class="deen-logo-small" style="margin:0;" onerror="this.style.display='none'">
                        <b>DEEN Commerce Ltd.</b>
                    </a>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def premium_metric_card(label: str, value: str, icon: str, help_text: str = "", border_color: str = "var(--primary)"):
    """Global premium metric card with top border highlight and hover effects."""
    import re
    safe_class = re.sub(r'[^a-z0-9]', '-', label.lower())
    unique_class = f"premium-card-{safe_class}"
    
    st.markdown(f"""
        <style>
            .{unique_class} {{
                transition: all 0.2s ease-in-out;
            }}
            .{unique_class}:hover {{
                transform: translateY(-2px);
                box-shadow: 0 8px 16px rgba(0,0,0,0.08) !important;
                border-color: {border_color} !important;
            }}
            @media (max-width: 1250px) {{
                .{unique_class} {{ padding: 10px !important; min-height: 90px !important; }}
                .{unique_class} .pmc-icon {{ display: none !important; }}
                .{unique_class} .pmc-value {{ font-size: clamp(1.2rem, 3vw, 1.8rem) !important; }}
                .{unique_class} .pmc-help {{ font-size: 0.55rem !important; }}
            }}
            @media (max-width: 640px) {{
                .{unique_class} .pmc-help {{ display: none !important; }}
            }}
        </style>
        <div class="{unique_class}" style="
            background: var(--surface, var(--secondary-background-color, #ffffff));
            border: 1px solid rgba(128, 128, 128, 0.1);
            border-top: 4px solid {border_color};
            border-radius: 10px;
            padding: 12px;
            margin-bottom: 16px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            text-align: center;
            height: 100%;
            min-height: 120px;
            overflow: hidden;
        ">
            <div style="display: flex; justify-content: center; align-items: center; gap: 6px; width: 100%; margin-bottom: auto;">
                <div class="pmc-label" style="font-size: clamp(0.55rem, 1.2vw, 0.7rem); font-weight: 700; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">
                    {label}
                </div>
                <div class="pmc-icon" style="font-size: 0.9rem; flex-shrink: 0; opacity: 0.7;">
                    {icon}
                </div>
            </div>
            <div class="pmc-value" style="font-size: clamp(1.4rem, 3.5vw, 2.2rem); font-weight: 800; color: var(--text-color); line-height: 1.1; margin: 4px 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; width: 100%;">
                {value}
            </div>
            <div class="pmc-help" style="font-size: clamp(0.55rem, 1vw, 0.65rem); color: #94a3b8; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; width: 100%; margin-top: auto;">
                {help_text}
            </div>
        </div>
    """, unsafe_allow_html=True)


def small_metric_card(label: str, value: str, icon: str):
    """Global small metric card for compact layouts."""
    import re
    safe_class = re.sub(r'[^a-z0-9]', '-', label.lower())
    unique_class = f"small-card-{safe_class}"
    
    st.markdown(f"""
        <style>
            .{unique_class} {{
                transition: all 0.2s ease-in-out;
            }}
            .{unique_class}:hover {{
                transform: translateY(-2px);
                box-shadow: 0 6px 12px rgba(0,0,0,0.08) !important;
            }}
            @media (max-width: 1250px) {{
                .{unique_class} {{ padding: 10px !important; min-height: 80px !important; }}
                .{unique_class} .smc-icon {{ display: none !important; }}
                .{unique_class} .smc-value {{ font-size: clamp(1.1rem, 2.5vw, 1.5rem) !important; }}
            }}
        </style>
        <div class="{unique_class}" style="
            background: var(--surface, var(--secondary-background-color, #ffffff));
            border: 1px solid rgba(128, 128, 128, 0.1);
            border-radius: 12px;
            padding: 12px;
            text-align: center;
            margin-bottom: 16px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            text-align: center;
            height: 100%;
            min-height: 100px;
            overflow: hidden;
        ">
            <div style="display: flex; justify-content: center; align-items: center; gap: 4px; width: 100%; margin-bottom: auto;">
                <div class="smc-label" style="font-size: clamp(0.55rem, 1.2vw, 0.7rem); color: #64748b; text-transform: uppercase; font-weight: 700; letter-spacing: 0.5px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">{label}</div>
                <div class="smc-icon" style="font-size: 0.9rem; opacity: 0.7;">{icon}</div>
            </div>
            <div class="smc-value" style="font-size: clamp(1.3rem, 3vw, 1.8rem); font-weight: 800; color: var(--text-color); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; line-height: 1.1; margin: 4px 0;">{value}</div>
        </div>
    """, unsafe_allow_html=True)


def anomaly_alert_card(title: str, description: str, category: str, level: str, action: str):
    """Global anomaly alert card."""
    color = "#ef4444" if level.upper() == "CRITICAL" else "#f59e0b"
    st.markdown(f"""
        <style>
        .anomaly-card:hover {{ transform: translateY(-2px); box-shadow: 0 6px 12px rgba(0,0,0,0.1); }}
        .anomaly-card {{ transition: all 0.2s ease-in-out; }}
        @media (max-width: 1250px) {{
            .anomaly-card {{ padding: 12px !important; }}
            .anomaly-card .ac-title {{ font-size: 0.95rem !important; }}
            .anomaly-card .ac-desc {{ font-size: 0.75rem !important; }}
        }}
        </style>
        <div class="anomaly-card" style="
            background:var(--surface, var(--secondary-background-color, #ffffff)); 
            border: 1px solid rgba(128,128,128,0.1); 
            border-left: 5px solid {color}; 
            padding: 15px; 
            border-radius: 8px; 
            margin-bottom: 12px; 
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            display: flex;
            flex-direction: column;
            height: 100%;
        ">
            <div style="display:flex; justify-content:space-between; flex-wrap: wrap; gap: 8px; margin-bottom: 8px;">
                <span style="font-weight:800; color:{color}; text-transform:uppercase; font-size:0.75rem; word-break: break-word;">{category} | {level}</span>
                <span style="background:{color}20; color:{color}; padding: 2px 8px; border-radius: 12px; font-size:0.7rem; font-weight:700; white-space: nowrap;">Action Required</span>
            </div>
            <div class="ac-title" style="font-size:1.1rem; font-weight:700; margin: 5px 0; word-break: break-word; line-height: 1.3;">{title}</div>
            <div class="ac-desc" style="font-size:0.85rem; opacity:0.8; word-break: break-word; line-height: 1.4; margin-bottom: auto;">{description}</div>
            <div style="margin-top:10px; font-size:0.8rem; font-weight:600; color:{color}; word-break: break-word; line-height: 1.3;">⚡ Suggested Action: {action}</div>
        </div>
    """, unsafe_allow_html=True)


def render_file_summary(
    uploaded_file, df: pd.DataFrame | None, required_columns: list[str]
):
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


from BackEnd.utils.io import to_excel_bytes


def show_last_updated(path: str):
    if not os.path.exists(path):
        return
    updated = datetime.fromtimestamp(os.path.getmtime(path)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    st.caption(f"Last updated: {updated}")
