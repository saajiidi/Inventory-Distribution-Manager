import pandas as pd
import streamlit as st
import plotly.express as px
from FrontEnd.components import ui
from BackEnd.core.categories import parse_sku_variants, get_clean_product_name, get_master_category_list, format_category_label, get_subcategory_name, classify_velocity_trend, get_densed_name
from BackEnd.core.geo import get_region_display

def render_deep_dive_tab(df_sales: pd.DataFrame, stock_df: pd.DataFrame, df_prev: pd.DataFrame = None, window_label: str = "period"):

    if "_variant_parsed" not in df_sales.columns:
        parsed_variants = df_sales["item_name"].apply(parse_sku_variants).tolist()
        df_sales["_color"] = [p[0] for p in parsed_variants]
        df_sales["_size"] = [p[1] for p in parsed_variants]
        df_sales["_clean_name"] = df_sales["item_name"].apply(get_clean_product_name)
        
        df_sales["_densed_name"] = df_sales.apply(lambda x: get_densed_name(x["_clean_name"], x["Category"]), axis=1)
        df_sales["_variant_parsed"] = True
        
    if df_prev is not None and not df_prev.empty and "_variant_parsed" not in df_prev.columns:
        parsed_variants_prev = df_prev["item_name"].apply(parse_sku_variants).tolist()
        df_prev["_color"] = [p[0] for p in parsed_variants_prev]
        df_prev["_size"] = [p[1] for p in parsed_variants_prev]
        df_prev["_clean_name"] = df_prev["item_name"].apply(get_clean_product_name)
        df_prev["_densed_name"] = df_prev.apply(lambda x: get_densed_name(x["_clean_name"], x["Category"]), axis=1)
        df_prev["_variant_parsed"] = True

    # --- Trend Type Logic ---
    # Calculate velocity based on current window length
    days_active = (df_sales["order_date"].max() - df_sales["order_date"].min()).days or 1
    v_agg = df_sales.groupby("item_name")["qty"].sum().reset_index()
    v_agg["v_rate"] = v_agg["qty"] / days_active
    
    v_agg["Trend"] = classify_velocity_trend(v_agg["v_rate"])
    if "Trend" not in df_sales.columns:
        df_sales = df_sales.merge(v_agg[["item_name", "Trend"]], on="item_name", how="left")

    # --- Campaign / Coupon logic ---
    if "Coupons" not in df_sales.columns:
        df_sales["Coupons"] = "None"

    # Map precise regions via core geography engine
    df_sales["_region_display"] = df_sales.apply(lambda x: get_region_display(x.get("city", ""), x.get("state", "")), axis=1)

    # MAIN UI LAYOUT
    st.markdown("### 📥 Sales Data Ingestion & Analysis")
    st.caption("Perform high-resolution segment analysis to identify operational opportunities and regional hotspots.")
    
    clean_window = window_label.replace('last ', '').title() if window_label else "Period"

    # CATEGORY PERFORMANCE MATRIX
    st.markdown("#### 📊 Category Performance Matrix")
    st.caption(f"Master and Sub-categories ranked by revenue, comparing current vs previous {clean_window.lower()}.")

    if not df_sales.empty:
        # Safely cross-reference return loss per line item to calculate category Net Yield
        if "Return_Loss" not in df_sales.columns:
            returns_df = st.session_state.get("returns_data", pd.DataFrame())
            order_sku_returns = {}
            if not returns_df.empty and "order_id" in returns_df.columns:
                for _, r_row in returns_df.iterrows():
                    if r_row.get("issue_type") in ["Paid Return", "Non Paid Return", "Partial"]:
                        items = r_row.get("returned_items", [])
                        oid = str(r_row.get("order_id", ""))
                        if isinstance(items, list):
                            for item in items:
                                if isinstance(item, dict):
                                    sku = str(item.get("sku", "N/A"))
                                    impact = item.get("revenue_impact", 0)
                                    key = f"{oid}_{sku}"
                                    order_sku_returns[key] = order_sku_returns.get(key, 0) + impact
            
            keys = df_sales["order_id"].astype(str) + "_" + df_sales.get("sku", "").astype(str)
            df_sales["Return_Loss"] = keys.map(order_sku_returns).fillna(0.0)

        curr_agg = df_sales.groupby("Category").agg(
            Total_Sold=("qty", "sum"),
            Total_Revenue=("item_revenue", "sum"),
            Return_Loss=("Return_Loss", "sum")
        ).reset_index()
        curr_agg["ASP"] = (curr_agg["Total_Revenue"] / curr_agg["Total_Sold"].replace(0, 1)).fillna(0)
        curr_agg["Net_Yield"] = ((curr_agg["Total_Revenue"] - curr_agg["Return_Loss"]) / curr_agg["Total_Revenue"].replace(0, 1) * 100).fillna(100).clip(lower=0, upper=100)
        
        if df_prev is not None and not df_prev.empty:
            prev_agg = df_prev.groupby("Category").agg(
                Prev_Sold=("qty", "sum"),
                Prev_Revenue=("item_revenue", "sum")
            ).reset_index()
            prev_agg["Prev_ASP"] = (prev_agg["Prev_Revenue"] / prev_agg["Prev_Sold"].replace(0, 1)).fillna(0)
            merged = curr_agg.merge(prev_agg, on="Category", how="outer").fillna(0)
        else:
            merged = curr_agg.copy()
            merged["Prev_Sold"] = 0
            merged["Prev_Revenue"] = 0
            merged["Prev_ASP"] = 0

        def format_trend(curr, prev):
            if prev == 0 and curr > 0:
                return "🚀 New"
            elif prev == 0 and curr == 0:
                return "➖"
            diff = curr - prev
            pct = (diff / prev) * 100
            if diff > 0:
                return f"▲ +{pct:.1f}%"
            elif diff < 0:
                return f"▼ {abs(pct):.1f}%"
            else:
                return "➖ 0%"

        merged["Sold Trend"] = merged.apply(lambda x: format_trend(x["Total_Sold"], x["Prev_Sold"]), axis=1)
        merged["Rev Trend"] = merged.apply(lambda x: format_trend(x["Total_Revenue"], x["Prev_Revenue"]), axis=1)
        merged["ASP Trend"] = merged.apply(lambda x: format_trend(x["ASP"], x["Prev_ASP"]), axis=1)
        
        merged["Master Category"] = merged["Category"].apply(lambda x: str(x).split(" - ")[0] if " - " in str(x) else str(x))
        merged["Sub Category"] = merged["Category"].apply(get_subcategory_name)
        
        merged = merged.sort_values(["Total_Revenue", "Master Category"], ascending=[False, True])
        
        display_df = merged[["Master Category", "Sub Category", "Total_Sold", "Sold Trend", "Total_Revenue", "Rev Trend", "ASP", "ASP Trend", "Net_Yield"]].rename(columns={
            "Total_Sold": "Total Sold",
            "Total_Revenue": "Total Revenue",
            "Net_Yield": "Net Yield %"
        })
        
        col_cfg = {
            "Master Category": st.column_config.TextColumn("Master Category", width="small"),
            "Sub Category": st.column_config.TextColumn("Sub Category", width="small"),
            "Total Sold": st.column_config.NumberColumn("Total Sold", format="%d"),
            "Sold Trend": st.column_config.TextColumn(f"Sold vs Prev {clean_window}"),
            "Total Revenue": st.column_config.NumberColumn("Total Revenue", format="৳%d"),
            "Rev Trend": st.column_config.TextColumn(f"Rev vs Prev {clean_window}"),
            "ASP": st.column_config.NumberColumn("ASP", format="৳%d"),
            "ASP Trend": st.column_config.TextColumn(f"ASP vs Prev {clean_window}"),
            "Net Yield %": st.column_config.ProgressColumn("Net Yield %", format="%.1f%%", min_value=0, max_value=100),
        }
        
        def color_trend(val):
            if isinstance(val, str):
                if "▲" in val or "🚀" in val:
                    return "color: #10b981;"  # Emerald Green
                elif "▼" in val:
                    return "color: #ef4444;"  # Red
            return ""

        styler = display_df.style
        styled_df = styler.map(color_trend, subset=["Sold Trend", "Rev Trend", "ASP Trend"]) if hasattr(styler, "map") else styler.applymap(color_trend, subset=["Sold Trend", "Rev Trend", "ASP Trend"])
        
        st.dataframe(styled_df, width="stretch", hide_index=True, column_config=col_cfg)
    else:
        st.info("Insufficient data for category matrix generation.")
    
    # FILTER CONTROL CENTER
    with st.expander("🛠️ Advanced Cluster Filters", expanded=True):
        st.markdown("**📦 Category & Operations**")
        f_c1, f_c2, f_c3, f_c4 = st.columns(4)
        
        with f_c1:
            # 1. Category - use master list for consistent hierarchy display
            cat_list = get_master_category_list()
            sel_cats = st.multiselect("Categories", ["All"] + cat_list, default=["All"], format_func=format_category_label)
            active_cats = [] if "All" in sel_cats or not sel_cats else sel_cats

        with f_c2:
            # Combined Product Name & SKU selection
            if active_cats:
                mask = pd.Series(False, index=df_sales.index)
                for cat in active_cats:
                    mask |= df_sales["Category"].str.startswith(cat, na=False)
                sku_options = df_sales[mask]
            else:
                sku_options = df_sales
            sku_options = sku_options.copy()
            sku_options["_display_name"] = sku_options["_clean_name"] + " [" + sku_options["sku"].astype(str) + "]"
            
            avail_items = sorted([str(s) for s in sku_options["_display_name"].unique() if str(s).strip() and "Unknown" not in str(s)])
            sel_items = st.multiselect("Products (Name + SKU)", ["All"] + avail_items, default=["All"])
            active_items = [] if "All" in sel_items or not sel_items else sel_items

        with f_c3:
            # 3. Size Filter
            if active_items:
                size_options = sku_options[sku_options["_display_name"].isin(active_items)]
            else:
                size_options = sku_options
                
            avail_sizes = sorted([str(s) for s in size_options["_size"].dropna().unique() if str(s).strip()])
            sel_sizes = st.multiselect("Variants (Size)", ["All"] + avail_sizes, default=["All"])
            active_sizes = [] if "All" in sel_sizes or not sel_sizes else sel_sizes

        with f_c4:
            # 4. Trend Filter
            avail_trends = sorted([str(t) for t in df_sales["Trend"].dropna().unique()])
            sel_trends = st.multiselect("Trend Velocity", ["All"] + avail_trends, default=["All"])
            active_trends = [] if "All" in sel_trends or not sel_trends else sel_trends



    # 1. APPLY COMPREHENSIVE FILTERING
    def _apply_cluster_filters(df):
        if df is None or df.empty: return pd.DataFrame()
        temp = df.copy()
        if active_cats:
            mask = pd.Series(False, index=temp.index)
            for cat in active_cats:
                mask |= temp["Category"].str.startswith(cat, na=False)
            temp = temp[mask]
        if active_items:
            temp["_display_name"] = temp["_clean_name"].astype(str) + " [" + temp["sku"].astype(str) + "]"
            temp = temp[temp["_display_name"].isin(active_items)]
        if active_sizes:
            temp = temp[temp["_size"].isin(active_sizes)]
        if active_trends and "Trend" in temp.columns:
            temp = temp[temp["Trend"].isin(active_trends)]
        return temp

    w_df = _apply_cluster_filters(df_sales)
    w_prev = _apply_cluster_filters(df_prev)

    if w_df.empty:
        st.warning("No sales data matches the active filter cluster. Adjust filters to refine your search.")
        return

    # --- Strategic Pulse Calculation ---
    total_items_sold = int(w_df["qty"].sum())
    total_revenue = w_df["item_revenue"].sum()
    avg_item_price = total_revenue / total_items_sold if total_items_sold > 0 else 0
    unique_customers = w_df["customer_key"].nunique()
    
    # Previous stats for delta
    prev_items = int(w_prev["qty"].sum()) if not w_prev.empty else 0
    prev_revenue = w_prev["item_revenue"].sum() if not w_prev.empty else 0
    prev_avg_price = (prev_revenue / prev_items) if prev_items > 0 else 0
    prev_customers = w_prev["customer_key"].nunique() if not w_prev.empty else 0

    def get_delta_data(curr, prev):
        if prev <= 0: return "", 0
        diff = curr - prev
        pct = (diff / prev) * 100
        return f"{abs(pct):.1f}%", diff

    d_items_label, d_items_val = get_delta_data(total_items_sold, prev_items)
    d_rev_label, d_rev_val = get_delta_data(total_revenue, prev_revenue)
    d_price_label, d_price_val = get_delta_data(avg_item_price, prev_avg_price)
    d_cust_label, d_cust_val = get_delta_data(unique_customers, prev_customers)

    # Top Variation logic
    variation_agg = w_df.groupby("_size").agg(Units=("qty", "sum")).reset_index().sort_values("Units", ascending=False)
    top_var_name = variation_agg.iloc[0]["_size"] if not variation_agg.empty else "N/A"
    top_var_units = int(variation_agg.iloc[0]["Units"]) if not variation_agg.empty else 0
    top_var_display = f"{top_var_name} ({top_var_units})"
    
    # VISUALIZATION SUITE
    st.markdown("#### ⚡ Strategic Pulse")
    p_c1, p_c2, p_c3, p_c4, p_c5 = st.columns(5)
    
    with p_c1: ui.icon_metric("Total Items Sold", f"{total_items_sold:,}", icon="📦", delta=d_items_label, delta_val=d_items_val)
    with p_c2: ui.icon_metric("Top Variation", top_var_display, icon="🎯")
    with p_c3: ui.icon_metric("Gross Revenue", f"৳{total_revenue:,.0f}", icon="💰", delta=d_rev_label, delta_val=d_rev_val)
    with p_c4: ui.icon_metric("Avg Item Price", f"৳{avg_item_price:,.0f}", icon="🏷️", delta=d_price_label, delta_val=d_price_val)
    with p_c5: ui.icon_metric("Unique Buyers", f"{unique_customers:,}", icon="👥", delta=d_cust_label, delta_val=d_cust_val)

    st.markdown(f"🚩 **Strategic Intelligence:** This segment is powered by **{unique_customers:,}** unique customers across **{total_items_sold:,}** total units sold. The dominant variation is **{top_var_name}** accounting for **{top_var_units}** units.")

    # --- Cluster Time Series Analysis ---
    st.divider()
    st.markdown("#### 📈 Cluster Performance Over Time")
    
    # Pre-process for Time Series
    ts_df = w_df.copy()
    ts_df['Date'] = pd.to_datetime(ts_df['order_date']).dt.strftime('%Y-%m-%d')
    daily_ts = ts_df.groupby('Date').agg(
        Revenue=("item_revenue", "sum"),
        Units=("qty", "sum")
    ).reset_index().sort_values("Date")
    
    # Dual-axis or simple toggle? Let's go with a beautifully styled revenue line with markers
    fig_ts = px.area(daily_ts, x="Date", y="Revenue", 
                     title="Daily Revenue Trend (Current Cluster)",
                     labels={"Revenue": "Revenue (৳)", "Date": "Timeline"},
                     template="plotly_white")
    
    fig_ts.update_traces(
        line_color="#6366f1", # Indigo
        fillcolor="rgba(99, 102, 241, 0.15)",
        marker=dict(size=6, color="#4338ca"),
        mode='lines+markers'
    )
    
    fig_ts.update_layout(
        hovermode="x unified",
        xaxis=dict(showgrid=False, rangeslider=dict(visible=True, thickness=0.06)),
        yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.05)"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)"
    )
    st.plotly_chart(fig_ts, width="stretch")

    # --- Strategic Visuals & Breakdown ---
    st.divider()
    
    # Pre-calculate bulk propensity for export and visuals
    order_basket = w_df.groupby("order_id")["qty"].sum().reset_index()
    total_orders_in_cluster = len(order_basket)
    p_single, p_bulk, p_other = "N/A", "N/A", "N/A"
    single_piece, bulk_pieces, mid_piece = 0, 0, 0
    if total_orders_in_cluster > 0:
        single_piece = len(order_basket[order_basket["qty"] == 1])
        bulk_pieces = len(order_basket[order_basket["qty"] >= 3])
        mid_piece = total_orders_in_cluster - single_piece - bulk_pieces
        p_single = (single_piece / total_orders_in_cluster) * 100
        p_bulk = (bulk_pieces / total_orders_in_cluster) * 100
        p_other = (mid_piece / total_orders_in_cluster) * 100

    rd1, rd2 = st.columns([3, 1])
    with rd1:
        st.markdown(f"#### 📊 Strategic Analytics Export")
        st.caption(f"Generate a professional multi-sheet intelligence report for the **{len(w_df):,}** items in this selection.")
    with rd2:
        from datetime import datetime
        # 1. Prepare Strategic Summary Sheet
        summary_data = {
            "Metric": ["Gross Revenue", "Total Units", "Unique Buyers", "Avg Item Price", "Single Piece Propensity", "Bulk Propensity (3+)"],
            "Value": [
                f"৳{total_revenue:,.0f}", 
                total_items_sold, 
                unique_customers, 
                f"৳{avg_item_price:,.0f}",
                f"{p_single:.1f}%" if isinstance(p_single, (int, float)) else p_single,
                f"{p_bulk:.1f}%" if isinstance(p_bulk, (int, float)) else p_bulk
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        
        # 2. Prepare Data Sheet
        export_df = w_df.drop(columns=[col for col in w_df.columns if col.startswith("_")], errors="ignore")
        
        # 3. Compile AI Strategic Intelligence
        ai_narrative = [
            f"STRATEGIC OVERVIEW: This filtered cluster generated ৳{total_revenue:,.0f} across {unique_customers:,} unique buyers.",
            f"BASKET DYNAMICS: Bulk buying (3+ units) accounts for {p_bulk:.1f}% of transactions." if isinstance(p_bulk, (int, float)) else "",
            f"BASKET DYNAMICS: Single-item purchases make up {p_single:.1f}% of transactions." if isinstance(p_single, (int, float)) else "",
            f"DOMINANT VARIATION: {top_var_name} is driving the cluster with {top_var_units} units sold."
        ]
        ai_df = pd.DataFrame({"AI Strategic Intelligence": [n for n in ai_narrative if n]})
        
        from FrontEnd.components.data_display import export_to_excel
        report_bytes = ui.export_to_excel(export_df, "Cluster Data", additional_sheets={"Summary": summary_df, "AI Insights": ai_df})
        
        st.download_button(
            label="📊 Export Strategic Analysis",
            data=report_bytes,
            file_name=f"deen_strategic_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="btn_export_strategic"
        )

    cluster_t1, cluster_t2, cluster_t3, cluster_t4 = st.tabs(["📈 Performance Mix", "🔍 Variant Analysis", "🛒 Basket Context", "📋 Cluster Data Ledger"])
    
    with cluster_t1:
        # Leaderboard Strategy Controls
        lc1, lc2 = st.columns([2, 1])
        with lc1:
            leader_mode = st.radio("Spotlight Strategy", ["💰 Top 10", "💰 Top 20", "📉 Underperformers", "⚙️ Custom Window"], 
                                  index=0, horizontal=True, key="leader_mode_sel")
        with lc2:
            granularity = st.radio("Granularity", ["📦 Master Product", "🆔 Variant"], index=0, horizontal=True)
            
        limit = 10
        if leader_mode == "💰 Top 20": limit = 20
        elif leader_mode == "📉 Underperformers": limit = 15
        elif leader_mode == "⚙️ Custom Window":
            limit = st.number_input("Display Limit", 5, 100, 20)
        
        # Prepare Data based on selected granularity
        group_col = "_master_label" if granularity == "📦 Master Product" else "_variant_label"
        w_df["_master_label"] = w_df["_densed_name"] + " [" + w_df["sku"].astype(str) + "]"
        w_df["_variant_label"] = w_df["_densed_name"] + " with Size " + w_df["_size"].astype(str) + " [" + w_df["sku"].astype(str) + "]"
        
        leader_df = w_df.groupby([group_col, "Category"]).agg(
            Units=("qty", "sum"),
            Revenue=("item_revenue", "sum")
        ).reset_index()
        
        if leader_mode == "📉 Underperformers":
            leader_df = leader_df.sort_values("Revenue", ascending=True).head(limit)
            sort_order = 'total descending'
        else:
            leader_df = leader_df.sort_values("Revenue", ascending=False).head(limit)
            sort_order = 'total ascending'

        # Calculate Share % safely
        total_cluster_rev = leader_df["Revenue"].sum()
        leader_df["Revenue Share %"] = (leader_df["Revenue"] / total_cluster_rev * 100).round(1) if total_cluster_rev > 0 else 0
            
        # Strip parent for sub-category hover
        leader_df["Sub-Category"] = leader_df["Category"].apply(get_subcategory_name)
        
        hover_label = "Product Name" if granularity == "📦 Master Product" else "Product (SKU)"
        
        fig = px.bar(leader_df, x="Revenue", y=group_col, 
                     title=f"Products Spotlight ({leader_mode} - {granularity})",
                     orientation='h', color="Units", color_continuous_scale="Viridis",
                     hover_data=["Units", "Revenue", "Sub-Category", "Revenue Share %"],
                     labels={group_col: hover_label, "Revenue": "Gross Revenue (৳)", "Units": "Qty Sold"})
        
        fig.update_layout(yaxis={'categoryorder': sort_order}, height=max(400, limit*25))
        st.plotly_chart(fig, width="stretch")
        
        c1, c2 = st.columns(2)
        with c1:
            # Trend Revenue Pie - Descending Contribution
            t_rev = w_df.groupby("Trend")["item_revenue"].sum().reset_index().sort_values("item_revenue", ascending=False)
            fig = px.pie(t_rev, values="item_revenue", names="Trend", title="Revenue Contribution by Moving Type",
                         hole=0.4, color_discrete_sequence=px.colors.qualitative.Prism)
            st.plotly_chart(fig, width="stretch")
            
        with c2:
            # Source/Platform Bar - Descending
            s_rev = w_df.groupby("source")["item_revenue"].sum().reset_index().sort_values("item_revenue", ascending=False)
            fig = px.bar(s_rev, x="source", y="item_revenue", title="Revenue by Platform Source",
                         color="item_revenue", color_continuous_scale="Tealgrn")
            fig.update_layout(xaxis={'categoryorder': 'total descending'})
            st.plotly_chart(fig, width="stretch")

        # Operational Category Mix (Sub-Category Focus)
        st.divider()
        st.markdown("##### 📦 Operational Sub-Category Performance")
        occ1, occ2 = st.columns(2)
        
        cat_intell = w_df.copy()
        cat_intell["Sub-Cat"] = cat_intell["Category"].apply(get_subcategory_name)
        
        cat_agg = cat_intell.groupby("Sub-Cat").agg(
            Revenue=("item_revenue", "sum"),
            Units=("qty", "sum")
        ).reset_index().sort_values("Revenue", ascending=False)
        
        with occ1:
            fig_cat_pie = px.pie(cat_agg, values="Revenue", names="Sub-Cat", title="Revenue Mix by Sub-Segment",
                                hole=0.4, color_discrete_sequence=px.colors.qualitative.T10)
            st.plotly_chart(fig_cat_pie, width="stretch")
            
        with occ2:
            fig_cat_bar = px.bar(cat_agg.sort_values("Units", ascending=True), x="Units", y="Sub-Cat", 
                                 title="Unit Volume per Sub-Segment",
                                 orientation='h', color="Units", color_continuous_scale="Agsunset")
            st.plotly_chart(fig_cat_bar, width="stretch")

    with cluster_t2:
        v_c1, v_c2 = st.columns(2)
        with v_c1:
            # Size Distribution
            sz_df = w_df.groupby("_size")["qty"].sum().reset_index()
            fig = px.bar(sz_df, x="_size", y="qty", title="Unit Volume by Size Cluster", 
                         color="qty", color_continuous_scale="Portland")
            st.plotly_chart(fig, width="stretch")
        with v_c2:
            # Color Distribution
            clr_df = w_df.groupby("_color")["item_revenue"].sum().reset_index()
            fig = px.pie(clr_df, values="item_revenue", names="_color", title="Revenue by Color Palette",
                         color_discrete_sequence=px.colors.qualitative.Safe)
            st.plotly_chart(fig, width="stretch")

    with cluster_t3:
        # --- ML Analytics: Bulk Purchase Dynamics ---
        st.markdown("##### 🧠 Bulk Purchase Dynamics (ML Analysis)")
        st.caption("Analyzing the propensity for bulk purchasing within this cluster. (Single Piece vs 3+ Units)")
        
        if total_orders_in_cluster > 0:
            # Optimized Columns for readability
            bm1, bm2, bm3 = st.columns(3)
            with bm1: ui.metric_highlight("SINGLE PIECE PROPENSITY", f"{p_single:.1f}%", f"Orders: {single_piece}", icon="🛍️")
            with bm2: ui.metric_highlight("BULK PROPENSITY (3+)", f"{p_bulk:.1f}%", f"Orders: {bulk_pieces}", icon="📦")
            with bm3: ui.metric_highlight("MID-TIER (2 ITEMS)", f"{p_other:.1f}%", f"Balance", icon="✨")
            
            # Distribution Pie
            prop_df = pd.DataFrame({
                "Propensity": ["Single Piece (1)", "Bulk (3+)", "Mid-Tier (2)"],
                "Count": [single_piece, bulk_pieces, mid_piece]
            })
            fig_prop = px.pie(prop_df, values="Count", names="Propensity", hole=0.5, 
                             color_discrete_map={"Single Piece (1)": "#6366f1", "Bulk (3+)": "#10b981", "Mid-Tier (2)": "#f59e0b"},
                             title=f"Basket Size Propensity: {sel_cats[0] if sel_cats and 'All' not in sel_cats else 'Filtered Cluster'}")
            fig_prop.update_layout(height=350, margin=dict(t=30, b=0, l=0, r=0))
            st.plotly_chart(fig_prop, width="stretch")
        
        st.divider()
        b_c1, b_c2 = st.columns(2)
        with b_c1:
            # Quantity Distribution (Basket logic)
            q_dist = w_df.groupby("qty")["order_id"].nunique().reset_index()
            q_dist.columns = ["Items in Line", "Orders"]
            fig = px.bar(q_dist, x="Items in Line", y="Orders", title="Distribution of Units per Line Item",
                         text_auto=True, color_discrete_sequence=["#F59E0B"])
            st.plotly_chart(fig, width="stretch")
        with b_c2:
            # City/Region Mix within this cluster
            city_mix = w_df.groupby("_region_display")["item_revenue"].sum().reset_index().sort_values("item_revenue", ascending=False).head(10)
            fig = px.bar(city_mix, x="item_revenue", y="_region_display", title="Market Hotspots (Regional Density)", 
                         orientation='h', color="item_revenue", color_continuous_scale="Agsunset",
                         labels={"_region_display": "Region/District", "item_revenue": "Revenue (৳)"})
            st.plotly_chart(fig, width="stretch")

    with cluster_t4:
        # Categorization Health Diagnostic
        others_df = w_df[w_df["Category"] == "Others"].copy()
        others_count = len(others_df)
        total_count = len(w_df)
        others_pct = (others_count / total_count * 100) if total_count > 0 else 0
        
        st.markdown("##### 🛠️ Categorization Health Audit")
        ac1, ac2 = st.columns([1.2, 2])
        with ac1:
            ui.metric_highlight(
                "Uncategorized Rate", 
                f"{others_pct:.1f}%", 
                f"{others_count} items", 
                delta_type="down" if others_pct > 0 else "up",
                icon="🛡️"
            )
        with ac2:
            if others_count > 0:
                st.warning("Found items in 'Others'. Review the list below to identify missing keyword rules.")
                top_others = others_df.groupby("item_name").agg(
                    Units=("qty", "sum"),
                    Revenue=("item_revenue", "sum")
                ).reset_index().sort_values("Revenue", ascending=False)
                st.write("**All Uncategorized Items (Others):**")
                st.dataframe(
                    top_others, 
                    width="stretch", 
                    hide_index=True,
                    column_config={"Revenue": st.column_config.NumberColumn("Revenue", format="৳%d")}
                )
            else:
                st.success("✅ Perfection: 100% of items in this cluster are successfully categorized!")

        st.divider()
        # Show clean ledger
        ledger_df = w_df[["order_id", "order_date", "item_name", "sku", "qty", "item_revenue", "Trend", "Coupons", "source", "_region_display"]].copy()
        ledger_df = ledger_df.rename(columns={"_region_display": "Location", "item_revenue": "Revenue", "qty": "Units", "item_name": "Product"})
        st.dataframe(ledger_df, width="stretch", hide_index=True)
