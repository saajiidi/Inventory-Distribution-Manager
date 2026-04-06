import os
from datetime import datetime

import streamlit as st

from FrontEnd.utils.config import APP_TITLE, APP_VERSION


def setup_theme():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');
        
        :root {
            --primary: #4f46e5;      /* Indigo */
            --primary-strong: #3730a3;
            --accent: #0ea5e9;       /* Light Blue */
            --gradient-1: linear-gradient(135deg, #4f46e5 0%, #3b82f6 100%);
            --surface: #f8fafc;
            --surface-raised: rgba(255, 255, 255, 0.75);
            --surface-soft: rgba(255, 255, 255, 0.45);
            --text-strong: #0f172a;
            --text-muted: #64748b;
            --border-soft: rgba(226, 232, 240, 0.6);
            --action-surface: rgba(255, 255, 255, 0.85);
            --card-shadow: 0 20px 40px rgba(15, 23, 42, 0.08);
            --card-shadow-hover: 0 30px 60px rgba(15, 23, 42, 0.12);
            --card-shadow-soft: 0 10px 25px rgba(15, 23, 42, 0.04);
            --glass-blur: blur(16px);
        }
        html, body, [class*="css"] {
            font-family: 'Outfit', sans-serif !important;
        }
        .stApp {
            background: #f1f5f9;
            background-image: 
                radial-gradient(at 0% 0%, rgba(79, 70, 229, 0.08) 0px, transparent 40%),
                radial-gradient(at 100% 0%, rgba(14, 165, 233, 0.08) 0px, transparent 40%);
            background-attachment: fixed;
        }
        [data-testid="stSidebar"] {
            background: rgba(255, 255, 255, 0.70) !important;
            backdrop-filter: var(--glass-blur);
            -webkit-backdrop-filter: var(--glass-blur);
            border-right: 1px solid var(--border-soft);
        }
        .stButton > button {
            border-radius: 12px !important;
            border: 1px solid rgba(79, 70, 229, 0.2) !important;
            box-shadow: 0 4px 14px rgba(79, 70, 229, 0.1) !important;
            background: white !important;
            color: var(--primary) !important;
            font-weight: 600 !important;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
        }
        .stButton > button:hover {
            transform: translateY(-2px) !important;
            box-shadow: 0 8px 25px rgba(79, 70, 229, 0.2) !important;
            border-color: var(--primary) !important;
        }
        .hub-page_footer {
            position: fixed;
            bottom: 0;
            left: 0;
            width: 100%;
            background: var(--action-surface);
            backdrop-filter: var(--glass-blur);
            padding: 12px 24px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            color: var(--text-muted);
            font-size: 0.85rem;
            border-top: 1px solid var(--border-soft);
            z-index: 999;
        }
        .hub-page_footer a {
            color: var(--primary);
            text-decoration: none;
            font-weight: 500;
            transition: color 0.2s ease;
        }
        .hub-page_footer a:hover {
            color: var(--accent);
        }
        .main .block-container {
            padding-bottom: 90px !important;
            padding-top: 1rem !important;
        }
        .deen-logo-small {
            vertical-align: middle;
            margin-right: 8px;
            border-radius: 6px;
        }
        
        /* Glassmorphic Components */
        .hub-title-row, .hub-sidebar-brand, .hub-card, .bi-commentary, .bi-audit-card, [data-testid="stMetricContainer"] {
            background: var(--surface-raised) !important;
            backdrop-filter: var(--glass-blur) !important;
            -webkit-backdrop-filter: var(--glass-blur) !important;
            border: 1px solid rgba(255, 255, 255, 0.6) !important;
            box-shadow: var(--card-shadow-soft) !important;
            transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275) !important;
        }
        
        .hub-card:hover, .bi-commentary:hover, [data-testid="stMetricContainer"]:hover {
            transform: translateY(-4px) !important;
            box-shadow: var(--card-shadow-hover) !important;
            border-color: rgba(255, 255, 255, 0.9) !important;
        }
        
        .hub-title-row {
            padding: 1.2rem 2rem;
            margin-bottom: 1.5rem;
            border-radius: 20px;
            text-align: left;
            display: flex;
            background: linear-gradient(135deg, rgba(255,255,255,0.9), rgba(255,255,255,0.6)) !important;
        }
        .hub-sidebar-brand {
            padding: 1.2rem;
            border-radius: 20px;
            margin-bottom: 1.2rem;
        }
        .hub-sidebar-kicker {
            font-size: 0.75rem;
            letter-spacing: 0.15em;
            text-transform: uppercase;
            color: var(--primary);
            font-weight: 700;
            margin-bottom: 0.5rem;
        }
        .hub-title {
            margin: 0;
            font-weight: 800;
            font-size: 2.2rem;
            color: var(--text-strong);
            letter-spacing: -0.03em;
        }
        .hub-title span {
            background: var(--gradient-1);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .hub-subtitle {
            margin: 0;
            color: var(--text-muted);
            font-size: 1.05rem;
            font-weight: 400;
        }
        .hub-card {
            border-radius: 20px;
            padding: 20px 24px;
            margin-bottom: 16px;
        }
        .bi-hero {
            background: var(--gradient-1);
            color: #ffffff;
            border-radius: 24px;
            padding: 2rem;
            margin-bottom: 1.5rem;
            box-shadow: var(--card-shadow);
            position: relative;
            overflow: hidden;
            border: 1px solid rgba(255,255,255,0.2);
            transition: transform 0.4s ease;
        }
        .bi-hero:hover {
            transform: scale(1.01);
        }
        .bi-hero::after {
            content: "";
            position: absolute;
            inset: auto -10% -40% auto;
            width: 300px;
            height: 300px;
            background: radial-gradient(circle, rgba(255,255,255,0.25) 0%, transparent 68%);
            animation: pulse-glow 8s ease-in-out infinite alternate;
        }
        @keyframes pulse-glow {
            0% { transform: scale(1); opacity: 0.8; }
            100% { transform: scale(1.1); opacity: 1; }
        }
        .bi-hero-title {
            font-size: 1.8rem;
            font-weight: 800;
            letter-spacing: -0.02em;
            margin-bottom: 0.5rem;
        }
        .bi-hero-subtitle {
            max-width: 800px;
            font-size: 1.05rem;
            line-height: 1.6;
            color: rgba(255, 255, 255, 0.9);
            font-weight: 400;
        }
        .bi-chip-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.6rem;
            margin-top: 1.2rem;
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
        .bi-commentary-label, .bi-audit-title {
            font-size: 0.8rem;
            font-weight: 700;
            color: var(--primary);
            letter-spacing: 0.15em;
            text-transform: uppercase;
            margin-bottom: 0.8rem;
            display: inline-block;
            background: rgba(79, 70, 229, 0.1);
            padding: 4px 10px;
            border-radius: 6px;
        }
        .bi-commentary ul {
            margin: 0;
            padding-left: 1.2rem;
            color: var(--text-strong);
            font-size: 1rem;
        }
        .bi-commentary li {
            margin-bottom: 0.6rem;
            line-height: 1.6;
        }
        .bi-kpi-note {
            margin-top: 0.5rem;
            padding: 0.4rem 0.8rem;
            border-radius: 999px;
            display: inline-block;
            font-size: 0.8rem;
            font-weight: 600;
            color: var(--primary);
            background: rgba(79, 70, 229, 0.1);
            border: 1px solid rgba(79, 70, 229, 0.2);
        }
        .bi-audit-body {
            color: var(--text-strong);
            line-height: 1.6;
            font-size: 1rem;
        }
        .bi-highlight-stat {
            background: var(--gradient-1);
            color: #ffffff;
            border-radius: 24px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            box-shadow: var(--card-shadow);
            position: relative;
            overflow: hidden;
            border: 1px solid rgba(255,255,255,0.2);
        }
        .bi-highlight-stat::before {
            content: "";
            position: absolute;
            top: 0; right: 0;
            width: 150px; height: 150px;
            background: radial-gradient(circle, rgba(255,255,255,0.2) 0%, transparent 70%);
            transform: translate(30%, -30%);
        }
        .bi-highlight-label {
            font-size: 0.85rem;
            font-weight: 700;
            letter-spacing: 0.15em;
            text-transform: uppercase;
            color: rgba(255, 255, 255, 0.8);
            margin-bottom: 0.5rem;
        }
        .bi-highlight-value {
            font-size: 2.5rem;
            font-weight: 800;
            line-height: 1;
            letter-spacing: -0.04em;
        }
        .bi-highlight-help {
            margin-top: 0.8rem;
            color: rgba(255, 255, 255, 0.9);
            font-size: 0.95rem;
            line-height: 1.5;
        }
        [data-testid="stMetricContainer"] {
            border-radius: 20px;
            padding: 1.2rem;
            border-left: 4px solid var(--accent) !important;
        }
        /* Target the streamlit container that HAS the hub-action-wrap marker inside it */
        div[data-testid="stVerticalBlock"]:has(> div[data-testid="stMarkdownContainer"] .hub-action-wrap) {
            position: sticky;
            bottom: 60px; /* Offset to stay above fixed page_footer */
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
            font-size: 1.05rem !important;
            font-weight: 600 !important;
            color: var(--text-muted) !important;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
            border: none !important;
            background: transparent !important;
            padding: 14px 24px !important;
            border-radius: 12px !important;
            margin-right: 8px !important;
            border: 1px solid transparent !important;
        }
        div[data-testid="stTab"] button:hover {
            color: var(--primary) !important;
            background: rgba(79, 70, 229, 0.05) !important;
            border-color: rgba(79, 70, 229, 0.1) !important;
        }
        div[data-testid="stTab"] button[aria-selected="true"] {
            color: var(--primary) !important;
            background: white !important;
            border: 1px solid rgba(79, 70, 229, 0.2) !important;
            box-shadow: 0 4px 12px rgba(79, 70, 229, 0.08) !important;
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
            .hub-page_footer {
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
            font-size: 1.8rem !important;
            font-weight: 800 !important;
            line-height: 1.2 !important;
            color: var(--text-strong) !important;
            letter-spacing: -0.02em !important;
        }
        
        /* Metric label alignment */
        [data-testid="stMetricLabel"] {
            font-size: 0.95rem !important;
            font-weight: 600 !important;
            color: var(--text-muted) !important;
            text-transform: uppercase !important;
            letter-spacing: 0.08em !important;
        }
        
        [data-testid="stMetricDelta"] {
            font-weight: 600 !important;
            margin-top: 4px;
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


def sidebar_branding():
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
        f"""<div class="hub-sidebar-brand">
            <div class="hub-sidebar-kicker">Operating System</div>
            <div style="font-weight:700; font-size:1.1rem; line-height:1.2;">
                DEEN Commerce BI
            </div>
            <div style="font-size:0.85rem; color:#64748b; margin-top:0.25rem;">
                Unified commerce intelligence for revenue, customers, cycles, and ShopAI CRM.
            </div>
            <div style="font-size:0.8rem; color:#64748b; margin-top:0.45rem;">
                {APP_VERSION}
            </div>
            {sync_html}
        </div>""",
        unsafe_allow_html=True,
    )


def page_header():
    """Minimal page_header for the main page content area."""
    st.markdown(
        f"""
        <div class="hub-title-row">
            <div>
                <h1 class="hub-title">{APP_TITLE} <span style="color:var(--primary);">{APP_VERSION}</span></h1>
                <p class="hub-subtitle">Commerce BI with operational storytelling, customer intelligence, and CRM Analytics context.</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


























def page_footer():
    """Renders a robust and persistent branding page_footer."""
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
        <div class="hub-page_footer">
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









