import pandas as pd
import streamlit as st

from src.core.errors import log_error
from src.utils.io import read_uploaded_file
from src.core.persistence import clear_state_keys
from src.ui.components import (
    render_action_bar,
    render_file_summary,
    render_reset_confirm,
    render_steps,
    section_card,
    to_excel_bytes,
)
from src.engine.wp_processor import WhatsAppOrderProcessor

FUZZY_REQUIRED_FIELDS = {
    "phone": ["phone", "mobile", "contact", "billing phone"],
    "name": ["full name", "billing name", "name", "first name", "customer"],
    "product": ["product name", "item name", "product", "item"],
}

def _reset_wp_state():
    clear_state_keys(["wp_links_df", "wp_preview_df", "wp_upload_name"])

def _has_fuzzy_column(columns: list[str], aliases: list[str]) -> bool:
    cols_lower = [str(col).strip().lower() for col in columns]
    for alias in aliases:
        if alias.lower() in cols_lower:
            return True
    for alias in aliases:
        if any(alias.lower() in col for col in cols_lower):
            return True
    return False

def _validate_wp_columns(df: pd.DataFrame):
    missing = [field for field, aliases in FUZZY_REQUIRED_FIELDS.items() if not _has_fuzzy_column(list(df.columns), aliases)]
    return len(missing) == 0, missing

def render_wp_tab(guided: bool = True):
    section_card("WhatsApp Verification", "Generate personalized verification links and export for bulk operations.")

    if guided:
        step = 0
        if st.session_state.get("wp_preview_df") is not None: step = 1
        if st.session_state.get("wp_links_df") is not None: step = 2
        render_steps(["Upload", "Validate", "Preview", "Export"], min(step + 1, 3))

    templates = {
        "Custom": {"intro": "", "footer": ""},
        "Order Confirmation": {
            "intro": "Assalamu Alaikum, {salutation}!\n\nDear {name},\n\nWe would like to confirm your recent order {order_id}. Please verify your details below:",
            "footer": "Please confirm if these details are correct so we can proceed with shipping."
        },
        "Return Request": {
            "intro": "Assalamu Alaikum, {salutation}!\n\nDear {name},\n\nRegarding your return request for order {order_id}, we have received the following details:",
            "footer": "Our team will contact you shortly to process the return."
        },
        "Shipping Update": {
            "intro": "Assalamu Alaikum, {salutation}!\n\nDear {name},\n\nGood news! Your order {order_id} has been packed and is ready for shipping. Here is a summary:",
            "footer": "You will receive a tracking number once it's handed over to the courier."
        }
    }

    with st.expander("Message template customization", expanded=False):
        template_choice = st.selectbox("Select a template:", list(templates.keys()))
        selected_template = templates[template_choice]
        st.caption("Variables: {name}, {salutation}, {order_id}")
        custom_intro = st.text_area("Custom intro", value=selected_template["intro"], height=100)
        custom_footer = st.text_area("Custom footer", value=selected_template["footer"], height=80)
        shorten_urls = st.toggle("Shorten WhatsApp links", value=False)

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("📡 Sync from Live Stream", use_container_width=True, key="ec_sync_btn"):
            from src.modules.sales import load_shared_gsheet
            df_sync, _, _ = load_shared_gsheet()
            st.session_state.wp_preview_df = df_sync
            st.session_state.wp_upload_name = "LiveSync_LastDaySales"
            st.rerun()
    with c2:
        wp_file = st.file_uploader("Upload List", type=["xlsx", "csv"], label_visibility="collapsed")

    preview_df = st.session_state.get("wp_preview_df")
    valid_file = False
    
    if wp_file:
        try:
            preview_df = read_uploaded_file(wp_file)
            st.session_state.wp_preview_df = preview_df
            st.session_state.wp_upload_name = wp_file.name
        except Exception as exc:
            st.error("Failed to read file.")

    if preview_df is not None:
        valid_file, missing = _validate_wp_columns(preview_df)
        if not valid_file:
            st.error(f"Missing columns: {', '.join(missing)}")
        else:
            st.success(f"Verified: {st.session_state.get('wp_upload_name', 'Sync Data')}")

    generate_clicked, clear_clicked = render_action_bar("Generate links", "wp_gen", "Clear", "wp_clr")

    if clear_clicked:
        _reset_wp_state()
        st.rerun()

    if generate_clicked and valid_file:
        try:
            processor = WhatsAppOrderProcessor()
            processed = processor.process_orders(preview_df)
            links_df = processor.create_whatsapp_links(
                processed,
                custom_intro=custom_intro if custom_intro.strip() else None,
                custom_footer=custom_footer if custom_footer.strip() else None,
                shorten_urls=shorten_urls
            )
            st.session_state.wp_links_df = links_df
            st.success(f"Generated {len(links_df)} links.")
        except Exception as exc:
            st.error(f"Generation failed: {exc}")

    links_df = st.session_state.get("wp_links_df")
    if links_df is not None:
        st.subheader("📋 Results")
        st.dataframe(links_df.head(50), use_container_width=True, hide_index=True)
        c1, c2 = st.columns(2)
        bulk_text = "\n\n".join([f"TO: {r['UID']}\n{r['whatsapp_link']}" for _, r in links_df.iterrows()])
        c1.download_button("💾 Export Text", bulk_text, "Bulk_Messages.txt", key="ec_ex_text")
        c2.download_button("📉 Download Excel", to_excel_bytes(links_df), "WhatsApp_Links.xlsx", key="ec_ex_xl")

    render_reset_confirm("wp", _reset_wp_state)
