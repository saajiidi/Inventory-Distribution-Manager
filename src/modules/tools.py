import re
from datetime import datetime
from io import BytesIO

import pandas as pd
import streamlit as st

from src.core.categories import get_category_for_sales
from src.modules.sales import find_columns
from src.utils.io import read_uploaded_file
from src.ui.components import render_action_bar, section_card


def _digits_only(v):
    return re.sub(r"\D", "", str(v or ""))


def _is_valid_phone(v):
    d = _digits_only(v)
    return (len(d) == 11 and d.startswith("01")) or (
        len(d) == 13 and d.startswith("8801")
    )


def _evaluate_data_quality(df: pd.DataFrame):
    auto_cols = find_columns(df)
    required_keys = ["name", "cost", "qty"]
    missing_required = [k for k in required_keys if k not in auto_cols]

    issues = []

    # Missing critical values
    if not missing_required:
        missing_name = df[auto_cols["name"]].isna().sum()
        missing_cost = (
            pd.to_numeric(df[auto_cols["cost"]], errors="coerce").isna().sum()
        )
        missing_qty = pd.to_numeric(df[auto_cols["qty"]], errors="coerce").isna().sum()
        issues.append(("Missing Product Name", int(missing_name)))
        issues.append(("Invalid/Missing Cost", int(missing_cost)))
        issues.append(("Invalid/Missing Quantity", int(missing_qty)))

    # Duplicate orders
    dup_orders = 0
    if "order_id" in auto_cols:
        dup_orders = int(df.duplicated(subset=[auto_cols["order_id"]]).sum())
    issues.append(("Duplicate Order IDs", dup_orders))

    # Suspicious values
    suspicious_price = 0
    suspicious_qty = 0
    invalid_phone = 0
    if "cost" in auto_cols:
        cost = pd.to_numeric(df[auto_cols["cost"]], errors="coerce")
        suspicious_price = int(((cost <= 0) | (cost > 100000)).fillna(False).sum())
    if "qty" in auto_cols:
        qty = pd.to_numeric(df[auto_cols["qty"]], errors="coerce")
        suspicious_qty = int(((qty <= 0) | (qty > 100)).fillna(False).sum())
    if "phone" in auto_cols:
        invalid_phone = int((~df[auto_cols["phone"]].apply(_is_valid_phone)).sum())
    issues.append(("Suspicious Price", suspicious_price))
    issues.append(("Suspicious Quantity", suspicious_qty))
    issues.append(("Invalid Phone Format", invalid_phone))

    issues_df = pd.DataFrame(issues, columns=["Issue", "Count"])
    total_rows = len(df)
    issue_count = int(issues_df["Count"].sum())
    quality_score = max(0, 100 - int((issue_count / max(total_rows, 1)) * 100))

    return auto_cols, missing_required, issues_df, quality_score


def render_data_quality_monitor_tab():
    section_card(
        "Data Quality Monitor",
        "Check missing columns, duplicates, suspicious values, and phone format before processing.",
    )
    uploaded = st.file_uploader(
        "Upload file for quality checks (CSV/XLSX)",
        type=["csv", "xlsx"],
        key="dq_upload",
    )
    if not uploaded:
        return

    try:
        df = read_uploaded_file(uploaded)
    except Exception as exc:
        st.error(f"Failed to read file: {exc}")
        return

    auto_cols, missing_required, issues_df, quality_score = _evaluate_data_quality(df)
    c1, c2, c3 = st.columns(3)
    from src.ui.components import render_metric_hud

    with c1:
        render_metric_hud("Rows", f"{len(df):,}", "📋")
    with c2:
        render_metric_hud("Detected Mappings", f"{len(auto_cols):,}", "🔗")
    with c3:
        render_metric_hud("Quality Score", f"{quality_score}%", "🧪")

    if missing_required:
        st.error(f"Missing required logical columns: {', '.join(missing_required)}")
    else:
        st.success("Required columns are detected.")

    st.dataframe(issues_df, width="stretch", hide_index=True)

    with st.expander("Detected Column Mapping", expanded=False):
        mapping_df = pd.DataFrame(
            [{"Logical Field": k, "Mapped Column": v} for k, v in auto_cols.items()]
        )
        st.dataframe(mapping_df, width="stretch", hide_index=True)

    out = BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        issues_df.to_excel(writer, sheet_name="Quality Issues", index=False)
    st.download_button(
        "Download Quality Report",
        out.getvalue(),
        file_name=f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
        type="primary",
        key="tools_dq_dl",
    )


def render_daily_summary_export_tab():
    section_card(
        "Executive Daily Summary Export",
        "Generate a concise end-of-day summary with KPIs, top changes, and exception highlights.",
    )
    uploaded = st.file_uploader(
        "Upload sales data for summary (CSV/XLSX)",
        type=["csv", "xlsx"],
        key="exec_upload",
    )
    if not uploaded:
        return

    try:
        df = read_uploaded_file(uploaded)
    except Exception as exc:
        st.error(f"Failed to read file: {exc}")
        return

    auto_cols = find_columns(df)
    missing_required = [k for k in ["name", "cost", "qty"] if k not in auto_cols]
    if missing_required:
        st.error(
            f"Cannot generate summary. Missing logical columns: {', '.join(missing_required)}"
        )
        return

    generate_clicked, _ = render_action_bar(
        "Generate Executive Summary", "exec_generate"
    )
    if not generate_clicked:
        return

    cost = pd.to_numeric(df[auto_cols["cost"]], errors="coerce").fillna(0)
    qty = pd.to_numeric(df[auto_cols["qty"]], errors="coerce").fillna(0)
    df_calc = df.copy()
    df_calc["Total Amount"] = cost * qty
    df_calc["Category"] = (
        df_calc[auto_cols["name"]].astype(str).apply(get_category_for_sales)
    )

    total_revenue = float(df_calc["Total Amount"].sum())
    total_items = float(qty.sum())
    total_orders = int(
        df_calc[auto_cols["order_id"]].nunique()
        if "order_id" in auto_cols
        else len(df_calc)
    )
    aov = total_revenue / total_orders if total_orders else 0

    category_summary = (
        df_calc.groupby("Category", as_index=False)["Total Amount"]
        .sum()
        .sort_values("Total Amount", ascending=False)
    )
    top_products = (
        df_calc.groupby(auto_cols["name"], as_index=False)["Total Amount"]
        .sum()
        .sort_values("Total Amount", ascending=False)
        .head(15)
        .rename(columns={auto_cols["name"]: "Product Name"})
    )

    _, _, issues_df, quality_score = _evaluate_data_quality(df_calc)
    exceptions = issues_df[issues_df["Count"] > 0].copy()

    change_note = "Not enough date info for day-over-day comparison."
    if "date" in auto_cols:
        dcol = pd.to_datetime(df_calc[auto_cols["date"]], errors="coerce")
        valid = df_calc[dcol.notna()].copy()
        valid["_d"] = dcol[dcol.notna()].dt.date
        if valid["_d"].nunique() >= 2:
            daily = (
                valid.groupby("_d", as_index=False)["Total Amount"]
                .sum()
                .sort_values("_d")
            )
            prev_val = float(daily.iloc[-2]["Total Amount"])
            curr_val = float(daily.iloc[-1]["Total Amount"])
            # Corrected: Removed misplaced inventory metrics from sales trend loop
            delta = curr_val - prev_val
            pct = (delta / prev_val * 100) if prev_val else 0
            change_note = (
                f"Revenue change vs previous day: {delta:,.2f} TK ({pct:+.2f}%). "
                f"{daily.iloc[-2]['_d']} -> {daily.iloc[-1]['_d']}"
            )

    kpi_df = pd.DataFrame(
        [
            {"Metric": "Total Revenue", "Value": round(total_revenue, 2)},
            {"Metric": "Total Items Sold", "Value": round(total_items, 2)},
            {"Metric": "Total Orders", "Value": total_orders},
            {"Metric": "Average Order Value", "Value": round(aov, 2)},
            {"Metric": "Quality Score", "Value": f"{quality_score}%"},
            {"Metric": "Key Change", "Value": change_note},
        ]
    )

    st.success("Executive summary generated.")
    st.dataframe(kpi_df, width="stretch", hide_index=True)

    out = BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        kpi_df.to_excel(writer, sheet_name="KPI Summary", index=False)
        category_summary.to_excel(writer, sheet_name="Category Summary", index=False)
        top_products.to_excel(writer, sheet_name="Top Products", index=False)
        exceptions.to_excel(writer, sheet_name="Exceptions", index=False)
        issues_df.to_excel(writer, sheet_name="Quality", index=False)

    st.download_button(
        "Download Executive Summary (Excel)",
        out.getvalue(),
        file_name=f"executive_summary_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
        type="primary",
        key="tools_exec_dl",
    )
