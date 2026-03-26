import streamlit as st
import pandas as pd
import plotly.express as px
import os
import json
from datetime import date, datetime, timedelta
from src.core.categories import get_category_for_sales
from src.core.paths import prepare_data_dirs, SYSTEM_LOG_FILE
from src.ui.components import section_card
from src.utils.data import find_columns, parse_dates
from src.core.sync import (
    load_shared_gsheet,
    load_published_sheet_tabs,
    clear_sync_cache,
    DEFAULT_GSHEET_URL,
)

# CONFIGURATION
TOTAL_SALES_EXCLUDED_TABS = {"lastdaysales"}
prepare_data_dirs()

# --- SYSTEM HELPERS ---


def log_system_event(event_type, details):
    log_file = SYSTEM_LOG_FILE
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = {"timestamp": timestamp, "type": event_type, "details": details}
    try:
        logs = []
        if os.path.exists(log_file):
            with open(log_file, "r", encoding="utf-8") as f:
                logs = json.load(f)
        logs.append(log_entry)
        with open(log_file, "w", encoding="utf-8") as f:
            json.dump(logs, f, indent=4)
    except Exception:
        pass


def get_setting(key, default=None):
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, default)


def get_custom_report_tab_label():
    return "📂 Total Sales Report"


# --- ANALYTICS ENGINE ---


@st.cache_data(show_spinner=False)
def process_data(df, selected_cols):
    try:
        df = df.copy()
        df["Internal_Name"] = (
            df[selected_cols["name"]].fillna("Unknown Product").astype(str)
        )
        df["Internal_Cost"] = pd.to_numeric(
            df[selected_cols["cost"]], errors="coerce"
        ).fillna(0)
        df["Internal_Qty"] = pd.to_numeric(
            df[selected_cols["qty"]], errors="coerce"
        ).fillna(0)

        # New: Customer Name support
        c_col = selected_cols.get("customer_name")
        df["Internal_Customer"] = (
            df[c_col].fillna("Unknown Customer").astype(str)
            if c_col and c_col in df.columns
            else "N/A"
        )

        tf = ""
        if "date" in selected_cols:
            ds = pd.to_datetime(df[selected_cols["date"]], errors="coerce").dropna()
            if not ds.empty:
                tf = f"{ds.min().strftime('%d%b')}_to_{ds.max().strftime('%d%b_%y')}"

        df["Category"] = df["Internal_Name"].apply(get_category_for_sales)
        df["Total Amount"] = df["Internal_Cost"] * df["Internal_Qty"]

        summ = (
            df.groupby("Category")
            .agg({"Internal_Qty": "sum", "Total Amount": "sum"})
            .reset_index()
        )
        summ.columns = ["Category", "Total Qty", "Total Amount"]

        drill = (
            df.groupby(["Category", "Internal_Cost"])
            .agg({"Internal_Qty": "sum", "Total Amount": "sum"})
            .reset_index()
        )
        drill.columns = ["Category", "Price (TK)", "Total Qty", "Total Amount"]

        # 👑 Top Spenders (Customers)
        if (
            "Internal_Customer" in df.columns
            and (df["Internal_Customer"] != "N/A").any()
        ):
            top = (
                df.groupby("Internal_Customer")
                .agg({"Total Amount": "sum", "Internal_Qty": "sum"})
                .reset_index()
            )
            top.columns = ["Customer Name", "Total Spent", "Items Purchased"]
        else:
            # Fallback to Top Products if customer names aren't detectable
            top = (
                df.groupby("Internal_Name")
                .agg(
                    {"Internal_Qty": "sum", "Total Amount": "sum", "Category": "first"}
                )
                .reset_index()
            )
            top.columns = ["Product Name", "Total Qty", "Total Amount", "Category"]

        top = top.sort_values(top.columns[1], ascending=False)

        bk = {"avg_basket_qty": 0, "avg_basket_value": 0, "total_orders": 0}
        gc = [
            selected_cols[k]
            for k in ("order_id", "phone", "email")
            if k in selected_cols and selected_cols[k] in df.columns
        ]
        if gc:
            og = df.groupby(gc).agg({"Internal_Qty": "sum", "Total Amount": "sum"})
            bk = {
                "avg_basket_qty": og["Internal_Qty"].mean(),
                "avg_basket_value": og["Total Amount"].mean(),
                "total_orders": len(og),
            }

        return drill, summ, top, tf, bk
    except Exception as e:
        log_system_event("CALC_ERROR", str(e))
        return None, None, None, "", {}


def render_story_summary(summ, tp, timeframe, bk):
    """Conversational data storytelling component."""
    if summ is None or summ.empty:
        return

    total_rev = summ["Total Amount"].sum()
    top_cat = summ.sort_values("Total Amount", ascending=False).iloc[0]["Category"]

    # Adaptive Focus
    is_cust = "Customer Name" in tp.columns
    top_entity = tp.iloc[0][tp.columns[0]]
    entity_label = "top shopper" if is_cust else "high-velocity item"

    orders = bk.get("total_orders", 0)

    story = f"""
    <div style="background: rgba(59, 130, 246, 0.05); border-left: 4px solid var(--neon-blue); padding: 1.5rem; border-radius: 0 16px 16px 0; margin-bottom: 2rem; font-family: 'Outfit';">
        <div style="color: var(--neon-blue); font-weight: 700; text-transform: uppercase; font-size: 0.8rem; letter-spacing: 0.1em; margin-bottom: 0.5rem;">📊 EXECUTIVE NARRATIVE</div>
        <div style="font-size: 1.1rem; color: var(--text-primary); line-height: 1.5;">
            In the period <b>{timeframe or 'Overview'}</b>, the operations processed <b>{orders:,} orders</b> driving a total revenue of <b>TK {total_rev:,.0f}</b>. 
            The performance was primarily led by the <b>{top_cat}</b> category, with <b>{top_entity}</b> emerging as the {entity_label}. 
            Customer engagement shows an average basket value of <b>TK {bk.get('avg_basket_value', 0):,.0f}</b> per transaction.
        </div>
    </div>
    """
    st.markdown(story, unsafe_allow_html=True)


# --- UI RENDERING ---


def render_dashboard_output(drill, summ, top, timeframe, basket, source, updated):
    render_story_summary(summ, top, timeframe, basket)
    st.markdown(f"### ⚡ Statement: {timeframe or 'All Records'}")
    from src.ui.components import render_metric_hud

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_metric_hud("Items Sold", f"{summ['Total Qty'].sum():,.0f}", "📦")
    with c2:
        render_metric_hud("Total Orders", f"{basket['total_orders']:,}", "🛒")
    with c3:
        render_metric_hud(
            "Total Revenue", f"TK {summ['Total Amount'].sum():,.0f}", "💰"
        )
    with c4:
        render_metric_hud("Avg Basket", f"TK {basket['avg_basket_value']:,.0f}", "🛍️")

    is_dark = st.session_state.get("app_theme", "Dark Mode") == "Dark Mode"
    color_scale = "Blues_r" if is_dark else "Plasma"
    chart_font_color = "#f8fafc" if is_dark else "#0f172a"

    col1, col2 = st.columns(2)
    with col1:
        # Sort for color consistency
        sorted_summ = summ.sort_values("Total Amount", ascending=False)
        fig_pie = px.pie(
            sorted_summ,
            values="Total Amount",
            names="Category",
            hole=0.5,
            title="Revenue Share",
            color_discrete_sequence=getattr(px.colors.sequential, color_scale),
        )
        fig_pie.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color=chart_font_color,
        )
        st.plotly_chart(
            fig_pie, use_container_width=True, key=f"sales_pie_{source or 'default'}"
        )

    with col2:
        fig_bar = px.bar(
            sorted_summ,
            x="Total Amount",
            y="Category",
            orientation="h",
            title="Category Performance",
            color="Total Amount",
            color_continuous_scale="Blues" if is_dark else "Viridis",
        )
        fig_bar.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color=chart_font_color,
        )
        st.plotly_chart(
            fig_bar, use_container_width=True, key=f"sales_bar_{source or 'default'}"
        )

    with st.expander("Detailed Product Breakdown"):
        st.dataframe(top, use_container_width=True, hide_index=True)


# --- TABS ---


def render_live_tab():
    section_card(
        "📡 Live Stream", "Real-time performance synchronized with LastDaySales."
    )
    if st.button("🔄 Sync Now", use_container_width=True, key="live_sync_btn"):
        clear_sync_cache()
        st.rerun()
    try:
        df, src, upd = load_shared_gsheet("LastDaySales")
        mc = find_columns(df)
        dr, sm, tp, tf, bk = process_data(df, mc)
        render_dashboard_output(dr, sm, tp, tf, bk, src, upd)
    except Exception as e:
        st.error(f"Live sync error: {e}")


@st.cache_data(ttl=3600, show_spinner=False)
def get_all_statements_master(full_history: bool = False):
    """Fetches statement tabs. If full_history is False, only fetches recent statements."""
    url = get_setting("GSHEET_URL", DEFAULT_GSHEET_URL)
    try:
        tabs = load_published_sheet_tabs(url)
    except Exception:
        return None, "Failed to load tabs"

    # Priority sorting: Current year/statements first
    relevant_tabs = []
    for tab in tabs:
        tname = tab["name"].lower()
        if (
            tname in TOTAL_SALES_EXCLUDED_TABS
            or "sample" in tname
            or "template" in tname
        ):
            continue
        relevant_tabs.append(tab)

    # If not full history, only take top 3 latest tabs for speed
    if not full_history and len(relevant_tabs) > 3:
        # We assume recent are at the beginning or end?
        # Usually they are at the beginning of the list returned by published GSheets.
        relevant_tabs = relevant_tabs[:3]

    all_dfs = []
    for tab in relevant_tabs:
        try:
            from src.core.sync import normalize_gsheet_url_to_csv
            from src.utils.io import read_remote_csv

            csv_url = normalize_gsheet_url_to_csv(url, tab["gid"])
            df, _ = read_remote_csv(csv_url)
            if not df.empty:
                m = find_columns(df)
                df = df.copy()
                df["_src_tab"] = tab["name"]

                # 🛡️ Initialize full internal schema with safe defaults
                df["_p_name"] = "Unknown Product"
                df["_p_cust_name"] = None
                df["_p_cost"] = 0
                df["_p_qty"] = 0
                df["_p_date"] = pd.NaT
                df["_p_order"] = None
                df["_p_phone"] = None
                df["_p_email"] = None

                if "name" in m:
                    df["_p_name"] = df[m["name"]].astype(str)
                if "customer_name" in m:
                    df["_p_cust_name"] = df[m["customer_name"]].astype(str)
                if "cost" in m:
                    df["_p_cost"] = pd.to_numeric(
                        df[m["cost"]], errors="coerce"
                    ).fillna(0)
                if "qty" in m:
                    df["_p_qty"] = pd.to_numeric(df[m["qty"]], errors="coerce").fillna(
                        0
                    )
                if "date" in m:
                    df["_p_date"] = parse_dates(df[m["date"]])
                if "order_id" in m:
                    df["_p_order"] = df[m["order_id"]].astype(str)
                if "phone" in m:
                    df["_p_phone"] = df[m["phone"]].astype(str)
                if "email" in m:
                    df["_p_email"] = df[m["email"]].astype(str)
                all_dfs.append(df)
        except Exception:
            pass

    if not all_dfs:
        return None, "No valid data found"
    master = pd.concat(all_dfs, ignore_index=True)
    return master, f"Loaded {'Full' if full_history else 'Recent'} Database"


def render_custom_period_tab():
    section_card(
        "📂 Historical Data Explorer", "Automated filtering with incremental loading."
    )

    # Check if we need full history
    full_requested = st.toggle(
        "Enable Full Deep-History (Fetches all years)",
        value=False,
        key="full_hist_toggle",
    )

    master, msg = get_all_statements_master(full_history=full_requested)
    if master is None:
        st.error(msg)
        return

    if "_p_date" in master.columns:
        valid_dates = master[master["_p_date"].notna()]
        if valid_dates.empty:
            st.warning("No dated records found.")
            return

        min_d, _max_d = (
            valid_dates["_p_date"].min().date(),
            valid_dates["_p_date"].max().date(),
        )

        # ALLOW SELECTION FROM 2022
        abs_min = date(2022, 1, 1)
        f1, f2 = st.columns(2)
        default_start = max(min_d, date.today() - timedelta(days=90))
        start = f1.date_input(
            "Filter From",
            default_start,
            min_value=abs_min,
            max_value=date.today(),
            key="cust_start",
        )
        end = f2.date_input(
            "Filter To",
            date.today(),
            min_value=abs_min,
            max_value=date.today(),
            key="cust_end",
        )

        # If user picked a date earlier than current master min, hint at full history
        if start < min_d and not full_requested:
            st.info(
                "💡 Selecting dates further back? Toggle 'Enable Full Deep-History' above."
            )

        filtered = master[
            (master["_p_date"].dt.date >= start) & (master["_p_date"].dt.date <= end)
        ].copy()

        if filtered.empty:
            st.warning("No records found for this period in current view.")
            return

        mc = {
            "name": "_p_name",
            "cost": "_p_cost",
            "qty": "_p_qty",
            "date": "_p_date",
            "order_id": "_p_order",
            "phone": "_p_phone",
            "email": "_p_email",
        }
        dr, sm, tp, tf, bk = process_data(filtered, mc)
        render_dashboard_output(dr, sm, tp, tf, bk, "MasterDB", "Incremental Load")
    else:
        st.error("Date column not found in database to support period filtering.")


def render_customer_pulse_tab():
    section_card(
        "👥 Customer Pulse",
        "Cross-statement unique customer insights and loyalty metrics.",
    )
    if st.button(
        "🔄 Refresh Pulse Data", use_container_width=True, key="refresh_pulse_btn"
    ):
        get_all_statements_master.clear()
        st.rerun()

    # Check if we need full history for Pulse too
    full_hist = st.toggle(
        "Enable Full Deep-History Analytics", value=False, key="pulse_hist_toggle"
    )

    master, msg = get_all_statements_master(full_history=full_hist)
    if master is None:
        st.info("Pulse data not yet ready.")
        return

    # Process UIDs across the entire master
    master["UID"] = (
        master.get("_p_phone", pd.Series(dtype=str))
        .fillna(master.get("_p_email", pd.Series(dtype=str)))
        .astype(str)
        .str.strip()
        .str.lower()
    )
    master = master[
        (master["UID"] != "")
        & (master["UID"] != "nan")
        & (master["UID"] != "n/a")
        & (master["UID"] != "none")
        & (master["UID"].notna())
    ]

    try:
        render_customer_pulse_core(master, full_hist)
    except Exception as e:
        from src.core.errors import log_error

        log_error(e, context="Customer Pulse Tab")
        st.error(f"Pulse analysis failed: {e}")
        st.info("💡 Try clicking 'Global Recovery -> Clear Cache' in the sidebar.")


def render_customer_pulse_core(master, full_hist):
    if "_p_date" in master.columns:
        valid_dates = master[master["_p_date"].notna()]
        if not valid_dates.empty:
            min_d = valid_dates["_p_date"].min().date()
            abs_min = date(2022, 1, 1)
            f1, f2 = st.columns(2)
            default_pulse_start = max(min_d, date.today() - timedelta(days=90))
            p_start = f1.date_input(
                "Pulse From",
                default_pulse_start,
                min_value=abs_min,
                max_value=date.today(),
                key="pulse_start",
            )
            p_end = f2.date_input(
                "Pulse To",
                date.today(),
                min_value=abs_min,
                max_value=date.today(),
                key="pulse_end",
            )
            db = master[
                (master["_p_date"].dt.date >= p_start)
                & (master["_p_date"].dt.date <= p_end)
            ].copy()
        else:
            db = master.copy()
    else:
        db = master.copy()

    if db.empty:
        st.warning("No data found for selected pulse range.")
        return

    # Advanced Metrics
    db["Total_Amount"] = db["_p_cost"] * db["_p_qty"]
    total_revenue = db["Total_Amount"].sum()

    unique_customers = db["UID"].nunique()
    # Group by UID and take the last name seen as the most accurate
    freq = (
        db.groupby("UID")
        .agg(
            {
                "_p_cust_name": "last",
                "_p_order": "nunique",
                "Total_Amount": "sum",
                "_p_date": "max",
            }
        )
        .reset_index()
    )
    freq.columns = ["UID", "Name", "Orders", "LifetimeValue", "LastActive"]
    # Fallback for display - handle both None and placeholder strings
    freq["Name"] = (
        freq["Name"].replace(["N/A", "None", None, ""], pd.NA).fillna(freq["UID"])
    )

    returning_count = len(freq[freq["Orders"] > 1])
    retention_rate = (
        (returning_count / unique_customers * 100) if unique_customers > 0 else 0
    )
    avg_clv = total_revenue / unique_customers if unique_customers > 0 else 0

    # STORYTELLING NARRATIVE
    is_dark = st.session_state.get("app_theme", "Dark Mode") == "Dark Mode"
    accent_color = "#3b82f6" if is_dark else "#1d4ed8"
    text_color = "#f8fafc" if is_dark else "#0f172a"

    story = f"""
    <div style="background: rgba(59, 130, 246, 0.08); border-left: 5px solid {accent_color}; padding: 1.5rem; border-radius: 4px 20px 20px 4px; margin-bottom: 2.5rem; font-family: 'Outfit';">
        <div style="color: {accent_color}; font-weight: 800; text-transform: uppercase; font-size: 0.85rem; letter-spacing: 0.15em; margin-bottom: 0.75rem;">🛰️ CUSTOMER BASE INTELLIGENCE</div>
        <div style="font-size: 1.15rem; color: {text_color}; line-height: 1.6; font-weight: 400;">
            Currently tracking <b>{unique_customers:,} unique customers</b> within the selected window. 
            The ecosystem demonstrates a <b>{retention_rate:.1f}% retention rate</b>, with returning loyals driving sustainable growth. 
            On average, each customer represents a lifetime value (CLV) of <b>TK {avg_clv:,.0f}</b>. 
            The high retention suggests a strong product-market fit, while the acquisition trend indicates active scalability.
        </div>
    </div>
    """
    st.markdown(story, unsafe_allow_html=True)

    # Metrics HUD
    from src.ui.components import render_metric_hud

    m1, m2, m3, m4 = st.columns(4)
    with m1:
        render_metric_hud("Unique Pulse", f"{unique_customers:,}", "👥")
    with m2:
        render_metric_hud("Retention Rate", f"{retention_rate:.1f}%", "🔄")
    with m3:
        render_metric_hud("Avg CLV", f"TK {avg_clv:,.0f}", "💎")
    with m4:
        render_metric_hud("Loyalists", f"{returning_count:,}", "🏆")

    # Visual Insights
    theme_template = "plotly_dark" if is_dark else "plotly_white"
    chart_font_color = "#f8fafc" if is_dark else "#0f172a"

    col1, col2 = st.columns(2)
    with col1:
        cust_acq = (
            db.sort_values("_p_date").groupby("UID")["_p_date"].min().reset_index()
        )
        cust_acq.columns = ["UID", "AcqDate"]
        cust_acq["Month"] = cust_acq["AcqDate"].dt.strftime("%Y-%m")
        trend_grp = cust_acq.groupby("Month").size().reset_index(name="New")
        trend_grp["Cumulative"] = trend_grp["New"].cumsum()

        fig_growth = px.line(
            trend_grp,
            x="Month",
            y=["Cumulative", "New"],
            title="Customer Scaling Factor",
            template=theme_template,
            color_discrete_sequence=(
                ["#3b82f6", "#10b981"] if is_dark else ["#1d4ed8", "#059669"]
            ),
        ).update_layout(
            hovermode="x unified",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color=chart_font_color,
        )
        fig_growth.update_traces(mode="lines+markers")
        st.plotly_chart(fig_growth, use_container_width=True, key="pulse_scaling_line")

    with col2:
        retention_df = pd.DataFrame(
            {
                "Segment": ["Returning Loyals", "One-Time Shoppers"],
                "Count": [returning_count, unique_customers - returning_count],
            }
        )
        fig_ret = px.pie(
            retention_df,
            values="Count",
            names="Segment",
            title="Retention Dynamics",
            hole=0.6,
            template=theme_template,
            color_discrete_sequence=(
                ["#10b981", "#334155"] if is_dark else ["#059669", "#cbd5e1"]
            ),
        ).update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color=chart_font_color,
        )
        st.plotly_chart(fig_ret, use_container_width=True, key="pulse_ret_pie")

    # VIP LEADERBOARD
    st.markdown("### 🏆 Platinum Tier: Top 10 Spenders")
    vip = freq.sort_values("LifetimeValue", ascending=False).head(10).copy()
    vip["Engagement Index"] = vip["Orders"].apply(
        lambda x: "🔥 High" if x > 3 else "⚡ Mid"
    )
    st.table(
        vip[
            ["Name", "UID", "Orders", "LifetimeValue", "Engagement Index"]
        ].style.format({"LifetimeValue": "TK {:,.0f}"})
    )

    with st.expander("🔍 Deep Dive: Demographic & Risk Analysis"):
        st.caption("Risk Analysis: Customers not active in 90+ days")
        three_months_ago = datetime.now() - timedelta(days=90)
        risk_count = len(freq[freq["LastActive"] < three_months_ago])
        st.warning(
            f"⚠️ At-Risk Customers (Inactive > 90 days): **{risk_count:,}** ({risk_count/unique_customers*100:.1f}%)"
        )

        if "_src_tab" in db.columns:
            source_grp = db.groupby("_src_tab").size().reset_index(name="Volume")
            st.plotly_chart(
                px.bar(
                    source_grp,
                    x="Volume",
                    y="_src_tab",
                    orientation="h",
                    title="Loyalty by Source Channel",
                ),
                use_container_width=True,
            )
