
import streamlit as st









def card(title: str, help_text: str = ""):
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


def hero(title: str, subtitle: str, chips: list[str] | None = None):
    chips_html = ""
    if chips:
        chips_html = '<div style="display:flex; flex-wrap:wrap; gap:8px; margin-top:16px;">' + "".join(
            f'<span style="background:rgba(var(--primary-rgb), 0.1); border:1px solid var(--primary); border-radius:100px; padding:4px 12px; font-size:12px; color:var(--primary); font-weight:600;">{chip}</span>' for chip in chips if chip
        ) + "</div>"
    st.markdown(
        f"""
        <div class="bi-hero hub-card" style="padding:2.5rem; border-radius:24px; position:relative; overflow:hidden;">
          <div style="position:relative; z-index:2;">
            <div style="font-size:2.2rem; font-weight:800; color:var(--on-surface); letter-spacing:-0.03em; line-height:1.1;">{title}</div>
            <div style="color:var(--on-surface-variant); font-size:1.1rem; margin-top:8px; font-weight:500; max-width:600px;">{subtitle}</div>
            {chips_html}
          </div>
          <div style="position:absolute; top:-50px; right:-50px; width:200px; height:200px; background:radial-gradient(circle, rgba(var(--primary-rgb), 0.15) 0%, transparent 70%); border-radius:50%; z-index:1;"></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def commentary(title: str, bullet_points: list[str]):
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




def info_box(title: str, body: str):
    if not title or not body:
        return
    st.markdown(
        f"""
        <div class="bi-audit-card">
          <div class="bi-audit-title">{title}</div>
          <div class="bi-audit-body">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

























