import streamlit as st
import pandas as pd
import plotly.express as px
import os
import json
from datetime import date, datetime, timedelta, timezone
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


@st.cache_data(show_spinner=False, max_entries=20)
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
        total_rev = summ["Total Amount"].sum()
        total_qty = summ["Total Qty"].sum()
        if total_rev > 0:
            summ["Revenue Share (%)"] = (
                (summ["Total Amount"] / total_rev) * 100
            ).round(2)
        if total_qty > 0:
            summ["Quantity Share (%)"] = (
                (summ["Total Qty"] / total_qty) * 100
            ).round(2)

        drill = (
            df.groupby(["Category", "Internal_Cost"])
            .agg({"Internal_Qty": "sum", "Total Amount": "sum"})
            .reset_index()
        )
        drill.columns = ["Category", "Price (TK)", "Total Qty", "Total Amount"]

        # 🥇 Product Rankings (The requested report style)
        top_products = (
            df.groupby("Internal_Name")
            .agg({"Internal_Qty": "sum", "Total Amount": "sum", "Category": "first"})
            .reset_index()
        )
        top_products.columns = [
            "Product Name",
            "Total Qty",
            "Total Amount",
            "Category",
        ]
        top_products = top_products.sort_values("Total Amount", ascending=False)

        # 👥 Customer Highlights (Optional)
        top_customers = None
        if (
            "Internal_Customer" in df.columns
            and (df["Internal_Customer"] != "N/A").any()
        ):
            top_customers = (
                df.groupby("Internal_Customer")
                .agg({"Total Amount": "sum", "Internal_Qty": "sum"})
                .reset_index()
            )
            top_customers.columns = [
                "Customer Name",
                "Total Spent",
                "Items Purchased",
            ]
            top_customers = top_customers.sort_values("Total Spent", ascending=False)

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

        return drill, summ, top_products, tf, bk, df, top_customers
    except Exception as e:
        log_system_event("CALC_ERROR", str(e))
        return None, None, None, "", {}, None, None


def render_story_summary(summ, tp, timeframe, bk):
    """Compact summary strip."""
    if summ is None or summ.empty:
        return

    total_rev = summ["Total Amount"].sum()
    top_cat = summ.sort_values("Total Amount", ascending=False)
    if top_cat.empty: return
    top_cat_name = top_cat.iloc[0]["Category"]
    orders = bk.get("total_orders", 0)

    st.markdown(f"""
    <div style="background: var(--surface-bg); border: 1px solid var(--surface-border); border-left: 4px solid var(--accent-primary); padding: 1rem; border-radius: 4px; margin-bottom: 1.5rem;">
        <div style="font-size: 0.95rem; color: var(--text-primary); line-height: 1.4;">
            <b>{timeframe or 'Overview'} Analysis:</b> Total revenue of <b>TK {total_rev:,.0f}</b> from <b>{orders:,} orders</b>. 
            Top performance in <b>{top_cat_name}</b>. Average basket: <b>TK {bk.get('avg_basket_value', 0):,.0f}</b>.
        </div>
    </div>
    """, unsafe_allow_html=True)


# --- UI RENDERING ---


def render_dashboard_output(df, dr, sm, top_prod, tf, bk, src, upd, top_cust=None):
    is_dark = st.session_state.get("app_theme", "Dark Mode") == "Dark Mode"
    color_scale = "Blues_r" if is_dark else "Plasma"
    
    render_story_summary(sm, top_prod, tf, bk)
    st.markdown(f"### ⚡ Statement: {tf or 'All Records'}")
    from src.ui.components import render_metric_hud, render_status_strip

    render_status_strip(source=src or "Local", rows=len(df), last_refresh=upd or "N/A", status="Active Dataset ✅")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_metric_hud("Items Sold", f"{sm['Total Qty'].sum():,.0f}", "📦")
    with c2:
        render_metric_hud("Total Orders", f"{bk['total_orders']:,}", "🛒")
    with c3:
        render_metric_hud("Total Revenue", f"TK {sm['Total Amount'].sum():,.0f}", "💰")
    with c4:
        render_metric_hud("Avg Basket", f"TK {bk['avg_basket_value']:,.0f}", "🛍️")

    # 📥 EXCEL EXPORT (Relocated to top to avoid confusion with page footer)
    try:
        from io import BytesIO

        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            sm.to_excel(writer, sheet_name="Summary", index=False)
            top_prod.to_excel(writer, sheet_name="Product Rankings", index=False)
            dr.to_excel(writer, sheet_name="Drilldown", index=False)
            if top_cust is not None:
                top_cust.to_excel(writer, sheet_name="VIP Pulse", index=False)
            df.head(500).to_excel(writer, sheet_name="Sample Raw Data", index=False)

        clean_source = str(src).replace(" ", "_") if src else "Report"
        clean_tf = str(tf).replace("/", "-") if tf else "Overview"
        final_filename = f"Report_{clean_source}_{clean_tf}.xlsx"

        st.download_button(
            label="📥 Export Analysis to Excel",
            data=buf.getvalue(),
            file_name=final_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key=f"dl_{src}_{tf}",
        )
    except Exception as e:
        st.info(f"💡 Export engine standby. ({e})")

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        # Sort for color consistency
        sorted_summ = sm.sort_values("Total Amount", ascending=False)
        fig_pie = px.pie(
            sorted_summ,
            values="Total Amount",
            names="Category",
            hole=0.5,
            title="Revenue Share",
            color_discrete_sequence=getattr(px.colors.sequential, color_scale),
        )
        from src.ui.components import render_plotly_chart
        render_plotly_chart(fig_pie, key=f"sales_pie_{src or 'default'}")

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
        from src.ui.components import render_plotly_chart
        render_plotly_chart(fig_bar, key=f"sales_bar_{src or 'default'}")

    # Analytics Tabs (Replaces "Detailed Product Breakdown" expander)
    analysis_tabs = st.tabs(
        ["📑 Summary", "🏆 Product Rankings", "🔍 Drilldown", "💎 VIP Pulse"]
    )

    with analysis_tabs[0]:
        st.dataframe(
            sm.sort_values("Total Amount", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

    with analysis_tabs[1]:
        st.dataframe(
            top_prod.head(20),
            use_container_width=True,
            hide_index=True,
        )

    with analysis_tabs[2]:
        st.markdown("### 🔍 Category Inspector")
        if not dr.empty:
            categories = ["All Categories"] + sorted(dr["Category"].unique().tolist())
            selected_cat = st.selectbox(
                "Filter Drilldown by Category",
                categories,
                key=f"drill_sel_{src}_{tf}",
            )

            display_drill = dr
            if selected_cat != "All Categories":
                display_drill = dr[dr["Category"] == selected_cat]

            st.dataframe(
                display_drill.sort_values(["Category", "Price (TK)"]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No drilldown data available.")

    with analysis_tabs[3]:
        if top_cust is not None:
            st.dataframe(top_cust.head(20), use_container_width=True, hide_index=True)
        else:
            st.info("Customer-specific data not found in this segment.")

    # 🔎 GLOBAL SEARCH (Matches Catwise Raw Data Search)
    with st.expander("🔎 Deep Search & Raw Data Explorer"):
        search_query = st.text_input(
            "Search for products, orders, or customers...",
            key=f"search_{src}_{tf}",
        ).lower()
        if search_query:
            # Search across all string columns
            mask = (
                df.astype(str)
                .apply(lambda x: x.str.contains(search_query, case=False, na=False))
                .any(axis=1)
            )
            results = df[mask]
            st.success(f"Found {len(results)} matches.")
            st.dataframe(results, use_container_width=True)
        else:
            st.caption("Showing first 20 records. Use the search box above to filter.")
            st.dataframe(df.head(20), use_container_width=True)


# --- TABS ---


def render_live_tab():
    from src.ui.components import render_status_strip, render_action_bar
    section_card(
        "📡 Live Stream", "Real-time performance synchronized with LastDaySales."
    )
    
    p_click, _ = render_action_bar("🔄 Force Manual Sync", "live_sync_btn")
    if p_click:
        from src.core.sync import clear_sync_cache
        clear_sync_cache()
        st.toast("⚡ Syncing Live Records...", icon="🔄")
        st.rerun()
    try:
        from src.core.sync import load_shared_gsheet
        df, src, upd = load_shared_gsheet("LastDaySales", force_refresh=False)
        
        mc = find_columns(df)
        
        # --- NEW: FILTER TO LAST DAY ONLY ---
        if mc.get("date") in df.columns:
            df[mc["date"]] = parse_dates(df[mc["date"]])
            latest_date = df[mc["date"]].max()
            if pd.notna(latest_date):
                target_date = latest_date.date()
                df = df[df[mc["date"]].dt.date == target_date].copy()
                st.info(f"📅 Showing Live Data for: **{target_date.strftime('%d %b %Y')}** (Most Recent Activity)")
        
        # Precomputed KPI Snapshot Check
        from src.core.paths import CACHE_DIR
        kpi_cache_file = CACHE_DIR / "live_kpi_snapshot.json"
        
        dr, sm, tp, tf, bk, df_processed, tc = process_data(df, mc)

        # Save KPI snapshot for even faster cold starts
        if sm is not None:
             try:
                 snapshot = {
                     "upd": upd,
                     "items": int(sm['Total Qty'].sum()),
                     "revenue": float(sm['Total Amount'].sum()),
                     "orders": int(bk['total_orders']),
                     "avg_basket": float(bk['avg_basket_value'])
                 }
                 with open(kpi_cache_file, "w") as f:
                     json.dump(snapshot, f)
             except Exception:
                 pass

        if df_processed is not None:
            render_dashboard_output(df_processed, dr, sm, tp, tf, bk, src, upd, top_cust=tc)
    except Exception as e:
        # Try to show last known KPI if sync fails
        from src.core.paths import CACHE_DIR
        kpi_cache_file = CACHE_DIR / "live_kpi_snapshot.json"
        if os.path.exists(kpi_cache_file):
            with open(kpi_cache_file, "r") as f:
                s = json.load(f)
            st.warning(f"Live sync offline. Showing snapshot from {s.get('upd', 'Unknown')}")
            c1, c2, c3, c4 = st.columns(4)
            from src.ui.components import render_metric_hud
            render_metric_hud("Items (Cached)", f"{s.get('items', 0):,}", "📦")
            render_metric_hud("Orders (Cached)", f"{s.get('orders', 0):,}", "🛒")
            render_metric_hud("Revenue (Cached)", f"TK {s.get('revenue', 0):,.0f}", "💰")
            render_metric_hud("Avg Basket (Cached)", f"TK {s.get('avg_basket', 0):,.0f}", "🛍️")
        
        st.error(f"Live sync error: {e}")


def parse_date_from_tab_name(name):
    """Helper to extract date from tab names for sorting."""
    import dateparser

    try:
        # Try cleaning the name for better parsing (remove 'Statement', 'Sync', etc)
        clean_name = (
            name.lower().replace("statement", "").replace("sync", "").replace("_", " ")
        )
        dt = dateparser.parse(clean_name)
        return dt or datetime(2000, 1, 1)
    except Exception:
        return datetime(2000, 1, 1)


def get_all_statements_master(full_history: bool = False, force_refresh: bool = False):
    """
    Intelligent Incremental Builder for Statement Master.
    Treats historical data (2022-2025) as immutable local blocks.
    Only syncs 2026/Live tabs frequently.
    """
    from src.core.paths import GSHEETS_CACHE_DIR
    from src.core.sync import load_sheet_with_cache, is_volatile, load_published_sheet_tabs
    
    cache_file = GSHEETS_CACHE_DIR / (
        "master_full.parquet" if full_history else "master_recent.parquet"
    )

    # 1. Initialize empty or existing master foundation
    master_df = pd.DataFrame()
    if cache_file.exists():
        try:
            master_df = pd.read_parquet(cache_file)
        except Exception:
            master_df = pd.DataFrame()

    url = get_setting("GSHEET_URL", DEFAULT_GSHEET_URL)
    try:
        tabs = load_published_sheet_tabs(url, force_refresh=force_refresh)
    except Exception as e:
        if not master_df.empty:
            return master_df, f"Offline Mode: Using Local Database ({len(master_df):,} rows)"
        return None, f"Failed to load tabs: {e}"

    # 2. Filter and sort tabs (same logic as before)
    relevant_tabs = []
    for tab in tabs:
        tname = tab["name"].lower()
        if (
            tname in TOTAL_SALES_EXCLUDED_TABS
            or "sample" in tname
            or "template" in tname
        ):
            continue
        tab["parsed_date"] = parse_date_from_tab_name(tab["name"])
        relevant_tabs.append(tab)

    relevant_tabs.sort(key=lambda x: x["parsed_date"], reverse=True)
    if not full_history and len(relevant_tabs) > 3:
        relevant_tabs = relevant_tabs[:3]

    # 3. Incremental Processing Loop
    all_dfs = []
    failures = []
    skipped_count = 0
    synced_count = 0
    
    progress_text = f"Updating {'Full' if full_history else 'Recent'} Database..."
    progress_bar = st.progress(0, text=progress_text)

    for i, tab in enumerate(relevant_tabs):
        tab_name = tab["name"]
        tab_gid = tab["gid"]
        
        # DECISION: Should we sync this tab?
        # A) It's volatile (current year/live)
        # B) It's missing from local master
        # C) User forced a refresh
        is_missing = master_df.empty or "_src_tab" not in master_df.columns or tab_name not in master_df["_src_tab"].unique()
        needs_sync = force_refresh or is_volatile(tab_name) or is_missing

        if not needs_sync:
            # Re-use existing local data for this tab
            local_tab_data = master_df[master_df["_src_tab"] == tab_name]
            if not local_tab_data.empty:
                all_dfs.append(local_tab_data)
                skipped_count += 1
                continue

        # Perform Network Sync
        try:
            progress_bar.progress((i + 1) / len(relevant_tabs), text=f"Syncing {tab_name}...")
            df, _ = load_sheet_with_cache(url, tab_gid, tab_name, force_refresh=force_refresh)
            
            if not df.empty:
                m = find_columns(df)
                df = df.copy()
                df["_src_tab"] = tab_name

                # Schema mapping (same robust logic as before)
                schema_cols = {
                    "_p_name": ("name", "Unknown Product"),
                    "_p_cust_name": ("customer_name", None),
                    "_p_cost": ("cost", 0),
                    "_p_qty": ("qty", 0),
                    "_p_date": ("date", pd.NaT),
                    "_p_order": ("order_id", None),
                    "_p_phone": ("phone", None),
                    "_p_email": ("email", None),
                }

                for internal, (find_key, default) in schema_cols.items():
                    if find_key in m:
                        if internal == "_p_cost" or internal == "_p_qty":
                             df[internal] = pd.to_numeric(df[m[find_key]], errors="coerce").fillna(0)
                        elif internal == "_p_date":
                             df[internal] = parse_dates(df[m[find_key]])
                        else:
                             df[internal] = df[m[find_key]].astype(str)
                    else:
                        df[internal] = default
                
                all_dfs.append(df)
                synced_count += 1
        except Exception as e:
            failures.append(f"{tab_name} ({e})")
            # Fallback to local data if sync failed
            local_tab_data = master_df[master_df["_src_tab"] == tab_name] if not master_df.empty else pd.DataFrame()
            if not local_tab_data.empty:
                all_dfs.append(local_tab_data)

    progress_bar.empty()

    if not all_dfs:
        return None, "Database is empty."

    # 4. Final Reconstruction
    final_master = pd.concat(all_dfs, ignore_index=True)
    
    # Save optimized master
    try:
        final_master.to_parquet(cache_file, index=False)
    except Exception:
        pass

    msg = f"Database Ready: {len(final_master):,} rows ({synced_count} updated, {skipped_count} local)"
    return final_master, msg


def render_custom_period_tab():
    section_card("📂 Sales Hub", "Interactive analysis with incremental multi-year syncing.")
    
    from src.ui.components import render_date_range_selector
    
    # 1. Period Selector (Always Visible)
    cur_start, cur_end = render_date_range_selector("sales_hub")
    
    # 2. Sync Configuration
    full_requested = st.toggle("Enable Deep-History (2022-2025)", value=st.session_state.get("full_hist_toggle", False), key="full_hist_toggle")
    
    if st.button("🔄 Sync Missing Data", use_container_width=True):
        get_all_statements_master(full_history=full_requested, force_refresh=True)
        st.toast("Syncing with cloud...", icon="📡")
        st.rerun()

    st.divider()

    # 3. Load & Filter
    master, msg = get_all_statements_master(full_history=full_requested)
    if master is None:
        st.error(msg)
        return
    
    st.caption(f"🛡️ Local Database: {msg}")

    if "_p_date" in master.columns:
        filtered = master[
            (master["_p_date"].dt.date >= cur_start) & (master["_p_date"].dt.date <= cur_end)
        ].copy()
        
        if filtered.empty:
            st.warning(f"No records locally found for {cur_start} to {cur_end}. Try 'Sync Missing Data' above.")
            return

        mc = {
            "name": "_p_name", "cost": "_p_cost", "qty": "_p_qty",
            "date": "_p_date", "order_id": "_p_order",
            "phone": "_p_phone", "email": "_p_email",
        }
        dr, sm, tp, tf, bk, filtered_df, tc = process_data(filtered, mc)
        if filtered_df is not None:
            render_dashboard_output(filtered_df, dr, sm, tp, tf, bk, "MasterDB", "Delta Sync", top_cust=tc)
    else:
        st.error("Time-series column missing in current dataset.")


def render_customer_pulse_tab():
    section_card("👥 Customer Pulse", "LTV, retention, and scaling trends across history.")
    
    from src.ui.components import render_date_range_selector
    cur_start, cur_end = render_date_range_selector("cust_pulse")
    
    full_requested = st.toggle("Access Deep History (2022-2025)", value=False, key="pulse_full_toggle")
    
    if st.button("🔄 Sync Customer Data", use_container_width=True, key="refresh_pulse_btn"):
        get_all_statements_master(full_history=full_requested, force_refresh=True)
        st.toast("⚡ Updating Pulse Analytics...", icon="🔄")
        st.rerun()

    master, msg = get_all_statements_master(full_history=full_requested)
    if master is None or master.empty:
        st.error(f"Failed to load customer foundation: {msg}")
        return
        
    st.caption(f"🛡️ Database: {msg}")
    
    # Process UIDs across the entire master to find TRUE loyalists
    master["UID"] = (
        master.get("_p_phone", pd.Series(dtype=str))
        .fillna(master.get("_p_email", pd.Series(dtype=str)))
        .astype(str).str.strip().str.lower()
    )
    
    # Filter based on visible date range
    db = master[
        (master["_p_date"].dt.date >= cur_start) & (master["_p_date"].dt.date <= cur_end)
    ].copy()
    
    if db.empty:
        st.info(f"No customer activity found between {cur_start} and {cur_end}. Try expanding the date range.")
        return

    try:
        render_customer_pulse_core(db)
    except Exception as e:
        from src.core.errors import log_error
        log_error(e, context="Customer Pulse Tab")
        st.error(f"Pulse analysis failed: {e}")
        st.info("💡 Try clicking 'Global Recovery -> Clear Cache' in the sidebar.")


def render_customer_pulse_core(db):
    is_dark = st.session_state.get("app_theme", "Dark Mode") == "Dark Mode"

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
            color_discrete_sequence=(
                ["#3b82f6", "#10b981"] if is_dark else ["#1d4ed8", "#059669"]
            ),
        )
        fig_growth.update_traces(mode="lines+markers")
        from src.ui.components import render_plotly_chart
        render_plotly_chart(fig_growth, key="pulse_scaling_line")

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
            color_discrete_sequence=(
                ["#10b981", "#334155"] if is_dark else ["#059669", "#cbd5e1"]
            ),
        )
        from src.ui.components import render_plotly_chart
        render_plotly_chart(fig_ret, key="pulse_ret_pie")

    # COHORT ANALYSIS
    with st.expander("📈 Monthly Acquisition Cohorts"):
        st.markdown("<div style='margin-bottom:1.5rem;'>", unsafe_allow_html=True)
        st.markdown("#### Retention Heatmap")
        try:
            # We need to know first purchase month for each UID
            cohort_data = db.copy()
            cohort_data['Month'] = cohort_data['_p_date'].dt.to_period('M')
            first_purchase = cohort_data.groupby('UID')['_p_date'].min().dt.to_period('M').reset_index()
            first_purchase.columns = ['UID', 'FirstMonth']
            
            cohort_merged = pd.merge(cohort_data, first_purchase, on='UID')
            cohort_pivot = cohort_merged.groupby(['FirstMonth', 'Month']).agg({'UID': 'nunique'}).reset_index()
            cohort_pivot['Period'] = (cohort_pivot['Month'] - cohort_pivot['FirstMonth']).apply(lambda x: x.n)
            
            final_cohort = cohort_pivot.pivot(index='FirstMonth', columns='Period', values='UID')
            # Convert to retention percentage
            cohort_size = final_cohort.iloc[:, 0]
            retention = final_cohort.divide(cohort_size, axis=0) * 100
            
            st.markdown("#### Retention % by Acquisition Month")
            st.dataframe(retention.style.format("{:.1f}%").background_gradient(cmap='Greens', axis=None), use_container_width=True)
            st.caption("Month 0 = Acquisition Month. Month 1+ = Returned in subsequent months.")
        except Exception as e:
            st.info(f"Cohort data requires more history to render correctly. ({e})")

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
        st.markdown("<div style='margin-bottom:1.5rem;'>", unsafe_allow_html=True)
        st.caption("Risk Analysis: Customers not active in 90+ days")
        three_months_ago = datetime.now() - timedelta(days=90)
        risk_count = len(freq[freq["LastActive"] < three_months_ago])
        st.warning(
            f"⚠️ At-Risk Customers (Inactive > 90 days): **{risk_count:,}** ({risk_count/unique_customers*100:.1f}%)"
        )

        if "_src_tab" in db.columns:
            source_grp = db.groupby("_src_tab").size().reset_index(name="Volume")
            fig_src = px.bar(
                source_grp,
                x="Volume",
                y="_src_tab",
                orientation="h",
                title="Loyalty by Source Channel",
            )
            from src.ui.components import render_plotly_chart
            render_plotly_chart(fig_src, key="pulse_source_bar")


def render_cache_health_panel():
    """System tool to inspect the GSheet cache status."""
    from src.core.sync import load_manifest
    from src.core.paths import GSHEETS_CACHE_DIR, GSHEETS_RAW_DIR, GSHEETS_NORM_DIR
    import os

    st.markdown("### 🧪 GSheet Cache Health")
    manifest = load_manifest()

    if not manifest:
        st.info("Cache is empty. Start a sync to populate.")
        return

    # Summary Metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Manifest Entries", len(manifest))
    
    raw_size = sum(f.stat().st_size for f in GSHEETS_RAW_DIR.glob('*.csv')) / (1024 * 1024)
    norm_size = sum(f.stat().st_size for f in GSHEETS_NORM_DIR.glob('*.parquet')) / (1024 * 1024)
    
    c2.metric("Raw Storage", f"{raw_size:.2f} MB")
    c3.metric("Norm Storage", f"{norm_size:.2f} MB")

    st.markdown("#### 📑 Cached Tabs")
    cache_data = []
    for k, v in manifest.items():
        if k.startswith("tabs_"):
            continue
        
        age = "N/A"
        if "fetched_at" in v:
            dt = datetime.fromisoformat(v["fetched_at"])
            diff = datetime.now(timezone.utc) - dt
            age = f"{int(diff.total_seconds() // 60)}m ago"
        
        cache_data.append({
            "Tab": v.get("tab_name", "Unknown"),
            "GID": v.get("gid"),
            "Last Modified": v.get("last_modified", "Unknown"),
            "Rows": v.get("row_count", 0),
            "Age": age,
            "Status": "✅ Fresh" if "m ago" in age and int(age.split('m')[0]) < 60 else "🟠 Stale"
        })
    
    if cache_data:
        st.table(pd.DataFrame(cache_data))
    
    if st.button("🗑️ Wipe All Local Cache", type="secondary"):
        from src.core.sync import clear_sync_cache
        clear_sync_cache()
        st.toast("Cache Purged")
        st.rerun()


def render_data_completeness_report():
    """Detailed report on which months/tabs are loaded vs missing."""
    st.markdown("### 📊 Data Completeness Report")
    url = get_setting("GSHEET_URL", DEFAULT_GSHEET_URL)
    try:
        from src.core.sync import load_published_sheet_tabs, load_manifest
        tabs = load_published_sheet_tabs(url)
        manifest = load_manifest()
        
        report = []
        for t in tabs:
            if t["name"].lower() in TOTAL_SALES_EXCLUDED_TABS:
                continue
            
            cache_key = f"gid_{t['gid']}"
            cached = manifest.get(cache_key)
            status = "❌ Missing"
            details = "Not yet synced"
            
            if cached:
                status = "✅ Synced"
                details = f"{cached.get('row_count', 0)} rows, {cached.get('last_modified', 'No date')}"
            
            report.append({
                "Sheet Tab": t["name"],
                "Status": status,
                "Details": details
            })
        
        st.dataframe(pd.DataFrame(report), use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Failed to generate report: {e}")
