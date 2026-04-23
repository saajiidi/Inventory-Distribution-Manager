import re
from io import BytesIO
from datetime import datetime

import pandas as pd
import streamlit as st

from BackEnd.utils.io import to_excel_bytes

HEADER_TOKENS = {
    "Cons. ID",
    "Order ID",
    "Store",
    "Recipient Info",
    "Delivery Status",
    "Amount",
    "Payment",
    "Action",
}


def clean_lines(raw: str):
    lines = []
    for line in raw.splitlines():
        value = line.strip()
        if not value:
            continue
        if value in HEADER_TOKENS:
            continue
        lines.append(value)
    return lines


def is_consignment_id(value: str) -> bool:
    return bool(re.match(r"^[A-Z]{2}\d{6}[A-Z0-9]+$", value))


def parse_amount(line: str):
    m = re.search(r"([\d,]+(?:\.\d+)?)", line)
    if not m:
        return 0.0
    return float(m.group(1).replace(",", ""))


def parse_date(line: str):
    if line.lower().startswith("updated on"):
        return line.split("Updated on", 1)[1].strip()
    return line.strip()


def parse_records(raw: str):
    lines = clean_lines(raw)
    records = []
    i = 0

    while i < len(lines):
        if not is_consignment_id(lines[i]):
            i += 1
            continue

        rec = {
            "Consignment ID": lines[i],
            "Type": "",
            "Order ID": "",
            "Store": "",
            "Recipient Name": "",
            "Address": "",
            "Phone": "",
            "Delivery Status": "",
            "Status Updated On": "",
            "COD Amount": 0.0,
            "Charge": 0.0,
            "Discount": 0.0,
            "Payment Status": "",
            "Action": "",
        }
        i += 1

        if i < len(lines) and lines[i].startswith("Type"):
            i += 1
        if i < len(lines):
            rec["Type"] = lines[i]
            i += 1

        if i < len(lines):
            rec["Order ID"] = lines[i]
            i += 1
        if i < len(lines):
            rec["Store"] = lines[i]
            i += 1
        if i < len(lines):
            rec["Recipient Name"] = lines[i]
            i += 1
        if i < len(lines):
            rec["Address"] = lines[i]
            i += 1
        if i < len(lines):
            rec["Phone"] = lines[i]
            i += 1

        status_lines = []
        while i < len(lines) and not lines[i].startswith("Updated on"):
            status_lines.append(lines[i])
            i += 1
        rec["Delivery Status"] = "; ".join(status_lines)

        if i < len(lines) and lines[i].startswith("Updated on"):
            rec["Status Updated On"] = parse_date(lines[i])
            i += 1

        if i < len(lines):
            rec["COD Amount"] = parse_amount(lines[i])
            i += 1
        if i < len(lines):
            rec["Charge"] = parse_amount(lines[i])
            i += 1
        if i < len(lines):
            rec["Discount"] = parse_amount(lines[i])
            i += 1

        if i < len(lines):
            rec["Payment Status"] = lines[i]
            i += 1

        action_lines = []
        while i < len(lines) and not is_consignment_id(lines[i]):
            if not lines[i].startswith("Type"):
                action_lines.append(lines[i])
            i += 1
        rec["Action"] = ", ".join(action_lines)

        records.append(rec)

    return pd.DataFrame(records)


def extract_fields_fuzzy(cons_id, text_block):
    order_id_match = re.search(r"\b(\d{6})\b", text_block)
    order_id = order_id_match.group(1) if order_id_match else ""

    store_match = re.search(
        r"(Deen Commerce|w DEEN WARI OUTLET|c DEEN CUMILLA OUTLET)", text_block
    )
    store = store_match.group(1) if store_match else ""

    phone_match = re.search(r"(01\d{9})", text_block)
    phone = phone_match.group(1) if phone_match else ""

    cod_match = re.search(r"COD\s*à§³?\s*([\d,]+)", text_block)
    cod = cod_match.group(1).replace(",", "") if cod_match else "0"

    charge_match = re.search(r"Charge\s*à§³?\s*([\d,.]+)", text_block)
    charge = charge_match.group(1).replace(",", "") if charge_match else "0"

    discount_match = re.search(r"Discount\s*à§³?\s*([\d,]+)", text_block)
    discount = discount_match.group(1).replace(",", "") if discount_match else "0"

    status = "Unpaid"
    if "Paid" in text_block and "Unpaid" not in text_block:
        status = "Paid"

    delivery_match = re.search(
        r"(At Delivery Hub|Paid Return|Urgent Delivery Requested|Returned)", text_block
    )
    delivery_status = delivery_match.group(1) if delivery_match else ""

    updated_match = re.search(r"Updated on\s*([\d/]+)", text_block)
    status_updated_on = updated_match.group(1) if updated_match else ""

    lines = text_block.split("\n")
    info_lines = []
    ignore_keywords = [
        "Type:",
        "Parcel",
        "COD",
        "Charge",
        "Discount",
        "Paid",
        "Unpaid",
        "View POD",
        "Action",
        "Updated on",
        "Paid At:",
        store,
        order_id,
        cons_id,
    ]

    for line in lines:
        clean_line = line.strip()
        if not clean_line:
            continue
        if re.match(r"01\d{9}", clean_line):
            continue

        should_ignore = False
        for kw in ignore_keywords:
            if kw and kw.lower() in clean_line.lower():
                should_ignore = True
                break

        if not should_ignore:
            info_lines.append(clean_line)

    filtered_info = []
    for line in info_lines:
        if line.isdigit():
            continue
        filtered_info.append(line)

    name = ""
    address = ""

    if len(filtered_info) > 0:
        name = filtered_info[0]
    if len(filtered_info) > 1:
        address = ", ".join(filtered_info[1:])

    return {
        "Consignment ID": cons_id,
        "Type": "",
        "Order ID": order_id,
        "Store": store,
        "Recipient Name": name,
        "Address": address,
        "Phone": phone,
        "Delivery Status": delivery_status,
        "Status Updated On": status_updated_on,
        "COD Amount": float(cod) if cod else 0.0,
        "Charge": float(charge) if charge else 0.0,
        "Discount": float(discount) if discount else 0.0,
        "Payment Status": status,
        "Action": "",
    }


def parse_data_fuzzy(raw_text):
    record_start_pattern = re.compile(r"(DD\d{6}[A-Z0-9]+)")
    parts = record_start_pattern.split(raw_text)
    records = []
    for i in range(1, len(parts), 2):
        if i + 1 < len(parts):
            cons_id = parts[i].strip()
            body = parts[i + 1].strip()
            record = extract_fields_fuzzy(cons_id, body)
            records.append(record)
    return pd.DataFrame(records)




from BackEnd.commerce_ops.persistence import clear_state_keys
from BackEnd.commerce_ops.ui_components import (
    render_action_bar,
    render_reset_confirm,
    section_card,
)


def _reset_parser_state():
    clear_state_keys(["standard_parsed_df", "fuzzy_parsed_df"])


def render_fuzzy_parser_tab():
    render_reset_confirm("Delivery Data Parser", "parser", _reset_parser_state)
    # section_card("Delivery Text Parser", "")

    sample = """Cons. ID
DD040326KR9NUU
Type:
Parcel
193252
Deen Commerce
Raafin
House 10, Road 15, Sector 11, Uttara West, Dhaka
01745166722
At Delivery Hub
Updated on 05/03/2026
COD 0
Charge 50
Discount 10
Unpaid
View
POD"""

    tab1, tab2 = st.tabs(["Standard Parser", "Fuzzy Parser"])

    with tab1:
        raw_text = st.text_area(
            "",
            value="",
            height=150,
            placeholder="Paste copied courier detail blocks...",
            key="standard_raw_text",
        )
        parse_clicked, _ = render_action_bar(
            "Parse with standard rules",
            "standard_btn",
        )

        if parse_clicked:
            parsed_df = parse_records(raw_text)
            if parsed_df.empty:
                st.error("No records were found from standard parser input.")
            else:
                st.session_state.standard_parsed_df = parsed_df
                st.success(f"Parsed {len(parsed_df)} records.")

        if st.session_state.get("standard_parsed_df") is not None:
            st.dataframe(st.session_state.standard_parsed_df, use_container_width=True)
            st.download_button(
                "Download standard parser output",
                to_excel_bytes(st.session_state.standard_parsed_df, sheet_name="Deliveries"),
                f"deliveries_{datetime.now().strftime('%d-%m-%Y')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary",
            )

    with tab2:
        fuzzy_raw_text = st.text_area(
            "",
            value="",
            height=150,
            placeholder="Paste loosely structured text here...",
            key="fuzzy_raw_text",
        )
        fuzzy_parse_clicked, _ = render_action_bar(
            "Parse with fuzzy fallback",
            "fuzzy_btn",
        )

        if fuzzy_parse_clicked:
            if not fuzzy_raw_text.strip():
                st.warning("Paste some text before parsing.")
            else:
                with st.spinner("Processing text..."):
                    try:
                        parsed_df = parse_records(fuzzy_raw_text)
                    except Exception:
                        parsed_df = pd.DataFrame()
                    if parsed_df.empty:
                        try:
                            parsed_df = parse_data_fuzzy(fuzzy_raw_text)
                        except Exception:
                            parsed_df = pd.DataFrame()

                if parsed_df.empty:
                    st.error("No valid records found from fuzzy parser input.")
                else:
                    st.session_state.fuzzy_parsed_df = parsed_df
                    st.success(f"Parsed {len(parsed_df)} records using fuzzy fallback.")

        if st.session_state.get("fuzzy_parsed_df") is not None:
            st.dataframe(st.session_state.fuzzy_parsed_df, use_container_width=True)
            st.download_button(
                "Download fuzzy parser output",
                to_excel_bytes(st.session_state.fuzzy_parsed_df, sheet_name="Deliveries"),
                f"fuzzy_deliveries_{datetime.now().strftime('%d-%m-%Y')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary",
            )
