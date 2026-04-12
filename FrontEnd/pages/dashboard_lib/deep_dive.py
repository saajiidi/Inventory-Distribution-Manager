import pandas as pd
import streamlit as st
import plotly.express as px
from FrontEnd.components import ui
from BackEnd.core.categories import parse_sku_variants, get_clean_product_name, sort_categories, format_category_label

def render_deep_dive_tab(df_sales: pd.DataFrame, stock_df: pd.DataFrame):

    if "_variant_parsed" not in df_sales.columns:
        df_sales[["_color", "_size"]] = df_sales["item_name"].apply(lambda x: pd.Series(parse_sku_variants(x)))
        df_sales["_clean_name"] = df_sales["item_name"].apply(get_clean_product_name)
        df_sales["_variant_parsed"] = True

    # --- Trend Type Logic ---
    # Calculate velocity based on current window length
    days_active = (df_sales["order_date"].max() - df_sales["order_date"].min()).days or 1
    v_agg = df_sales.groupby("item_name")["qty"].sum().reset_index()
    v_agg["v_rate"] = v_agg["qty"] / days_active
    
    def classify_trend(rate):
        if rate > 3.0: return "🔥 Fast Moving"
        if rate > 0.8: return "⚖️ Regular"
        if rate > 0: return "🐌 Slow Moving"
        return "❄️ Non-Moving"

    v_agg["Trend"] = v_agg["v_rate"].apply(classify_trend)
    if "Trend" not in df_sales.columns:
        df_sales = df_sales.merge(v_agg[["item_name", "Trend"]], on="item_name", how="left")

    # --- Campaign / Coupon logic ---
    if "Coupons" not in df_sales.columns:
        df_sales["Coupons"] = "None"

    # --- Market Regions Mapping (Official BD Districts) ---
    bd_states = {
        "BD-01": "Bandarban", "BD-02": "Barguna", "BD-03": "Bogura",
        "BD-04": "Brahmanbaria", "BD-05": "Bagerhat", "BD-06": "Barishal",
        "BD-07": "Bhola", "BD-08": "Cumilla", "BD-09": "Chandpur",
        "BD-10": "Chattogram", "BD-11": "Cox's Bazar", "BD-12": "Chuadanga",
        "BD-13": "Dhaka", "BD-14": "Dinajpur", "BD-15": "Faridpur",
        "BD-16": "Feni", "BD-17": "Gopalganj", "BD-18": "Gazipur",
        "BD-19": "Gaibandha", "BD-20": "Habiganj", "BD-21": "Jamalpur",
        "BD-22": "Jashore", "BD-23": "Jhenaidah", "BD-24": "Joypurhat",
        "BD-25": "Jhalokathi", "BD-26": "Kishoreganj", "BD-27": "Khulna",
        "BD-28": "Kurigram", "BD-29": "Khagrachhari", "BD-30": "Kushtia",
        "BD-31": "Lakshmipur", "BD-32": "Lalmonirhat", "BD-33": "Manikganj",
        "BD-34": "Mymensingh", "BD-35": "Munshiganj", "BD-36": "Madaripur",
        "BD-37": "Magura", "BD-38": "Moulvibazar", "BD-39": "Meherpur",
        "BD-40": "Narayanganj", "BD-41": "Netrakona", "BD-42": "Narsingdi",
        "BD-43": "Narail", "BD-44": "Natore", "BD-45": "Chapai Nawabganj",
        "BD-46": "Nilphamari", "BD-47": "Noakhali", "BD-48": "Naogaon",
        "BD-49": "Pabna", "BD-50": "Pirojpur", "BD-51": "Patuakhali",
        "BD-52": "Panchagarh", "BD-53": "Rajbari", "BD-54": "Rajshahi",
        "BD-55": "Rangpur", "BD-56": "Rangamati", "BD-57": "Sherpur",
        "BD-58": "Satkhira", "BD-59": "Sirajganj", "BD-60": "Sylhet",
        "BD-61": "Sunamganj", "BD-62": "Shariatpur", "BD-63": "Tangail",
        "BD-64": "Thakurgaon"
    }
    
    # Pre-map regions for display
    def get_region_name(row):
        code = str(row.get("state", "")).strip()
        if code in bd_states: return bd_states[code]
        city = str(row.get("city", "")).strip()
        # Fallback to city or stay as code
        return bd_states.get(city, city if city else code)

    df_sales["_region_display"] = df_sales.apply(get_region_name, axis=1)

    # MAIN UI LAYOUT
    st.markdown("### 📥 Sales Data Ingestion & Analysis")
    st.caption("Perform high-resolution segment analysis to identify operational opportunities and regional hotspots.")
    
    # FILTER CONTROL CENTER
    with st.expander("🛠️ Advanced Cluster Filters", expanded=True):
        st.markdown("**📦 Category & Operations**")
        f_c1, f_c2, f_c3, f_c4 = st.columns(4)
        
        with f_c1:
            # 1. Category
            raw_cats = list(df_sales["Category"].dropna().unique())
            # Ensure parent categories exist if children do
            for parent in ["Jeans", "T-Shirt"]:
                if any(str(c).startswith(f"{parent} - ") for c in raw_cats) and parent not in raw_cats:
                    raw_cats.append(parent)
                
            cat_list = sort_categories([str(c) for c in raw_cats if str(c).strip()])
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



    # APPLY COMPREHENSIVE FILTERING
    w_df = df_sales.copy()
    if active_cats: 
        # Hierarchical Match: Include children if parent is selected
        mask = pd.Series(False, index=w_df.index)
        for cat in active_cats:
            mask |= w_df["Category"].str.startswith(cat, na=False)
        w_df = w_df[mask]
    if active_items: 
        w_df["_display_name"] = w_df["_clean_name"] + " [" + w_df["sku"].astype(str) + "]"
        w_df = w_df[w_df["_display_name"].isin(active_items)]
    if active_sizes: w_df = w_df[w_df["_size"].isin(active_sizes)]
    if active_trends: w_df = w_df[w_df["Trend"].isin(active_trends)]


    if w_df.empty:
        st.warning("No sales data matches the active filter cluster. Adjust filters to refine your search.")
        return

    # --- Strategic Insights Generation ---
    top_platform = w_df["source"].mode()[0] if not w_df["source"].empty else "N/A"
    top_district = w_df["_region_display"].mode()[0] if not w_df["_region_display"].empty else "N/A"
    velocity_dom = w_df["Trend"].mode()[0] if not w_df["Trend"].empty else "N/A"
    
    # Order Penetration Logic
    total_orders_in_range = df_sales["order_id"].nunique()
    segment_orders = w_df["order_id"].nunique()
    penetration = (segment_orders / total_orders_in_range * 100) if total_orders_in_range > 0 else 0
    
    # VISUALIZATION SUITE
    st.markdown("#### ⚡ Strategic Pulse")
    i_c1, i_c2, i_c3, i_c4 = st.columns(4)
    with i_c1: ui.metric_highlight("Total Sold", f"{int(w_df['qty'].sum()):,}", help_text="Total units in this segment")
    with i_c2: ui.metric_highlight("Segment Value", f"৳{w_df['item_revenue'].sum():,.0f}", help_text="Estimated revenue contribution")
    with i_c3: ui.metric_highlight("Order Penetration", f"{penetration:.1f}%", help_text="% of total orders containing these items")
    with i_c4: ui.metric_highlight("Top Region", top_district, help_text="Dominant market hotspot")

    st.markdown(f"🚩 **Key Insight:** This segment appears in **{penetration:.1f}%** of all orders, primarily driven by **{top_platform}** customers in **{top_district}**.")

    # --- Strategic Visuals & Breakdown ---
    st.divider()
    rd1, rd2 = st.columns([3, 1])
    with rd1:
        st.markdown(f"#### 📊 Cluster Insights Report")
        st.caption(f"Comprehensive analysis export for **{len(w_df):,}** line items in this cluster selection.")
    with rd2:
        from datetime import datetime
        # Prepare filtered dataframe for export (dropping internal helper columns)
        export_df = w_df.drop(columns=[col for col in w_df.columns if col.startswith("_")], errors="ignore")
        report_bytes = ui.export_to_excel(export_df, "Sales Cluster Analysis")
        st.download_button(
            label="📥 Download Data Report",
            data=report_bytes,
            file_name=f"deen_sales_analysis_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    cluster_t1, cluster_t2, cluster_t3, cluster_t4 = st.tabs(["📈 Performance Mix", "🔍 Variant Analysis", "🛒 Basket Context", "📋 Cluster Data Ledger"])
    
    with cluster_t1:
        # Leaderboard Controls
        lc1, lc2 = st.columns([2, 1])
        with lc1:
            leader_mode = st.radio("Focus Mode", ["💰 Top Performers", "📉 Underperformers", "⚙️ Custom Window"], 
                                  index=0, horizontal=True, key="leader_mode_sel")
        
        limit = 20
        if leader_mode == "⚙️ Custom Window":
            limit = st.number_input("Display Limit", 5, 100, 20)
        
        # Prepare Data with SKU
        w_df["_name_sku"] = w_df["_clean_name"] + " (" + w_df["sku"].astype(str) + ")"
        
        leader_df = w_df.groupby("_name_sku").agg(
            Units=("qty", "sum"),
            Revenue=("item_revenue", "sum")
        ).reset_index()
        
        if leader_mode == "📉 Underperformers":
            leader_df = leader_df.sort_values("Revenue", ascending=True).head(limit)
            sort_order = 'total descending' # Smallest at bottom for Bar chart orientation
        else:
            leader_df = leader_df.sort_values("Revenue", ascending=False).head(limit)
            sort_order = 'total ascending' # Largest at bottom for Bar chart orientation (st.plotly_chart horizontal swaps it visually)

        # Calculate Share % safely
        total_cluster_rev = leader_df["Revenue"].sum()
        if total_cluster_rev > 0:
            leader_df["Revenue Share %"] = (leader_df["Revenue"] / total_cluster_rev * 100).round(1)
        else:
            leader_df["Revenue Share %"] = 0
        
        fig = px.bar(leader_df, x="Revenue", y="_name_sku", 
                     title=f"Performance Leaderboard ({leader_mode} - {limit} Samples)",
                     orientation='h', color="Units", color_continuous_scale="Viridis",
                     hover_data=["Units", "Revenue Share %"],
                     labels={"_name_sku": "Product (SKU)", "Revenue": "Gross Revenue (৳)"})
        
        fig.update_layout(yaxis={'categoryorder': sort_order}, height=max(400, limit*20))
        st.plotly_chart(fig, use_container_width=True)
        
        c1, c2 = st.columns(2)
        with c1:
            # Trend Revenue Pie
            t_rev = w_df.groupby("Trend")["item_revenue"].sum().reset_index()
            fig = px.pie(t_rev, values="item_revenue", names="Trend", title="Revenue Contribution by Moving Type",
                         hole=0.4, color_discrete_sequence=px.colors.qualitative.Prism)
            st.plotly_chart(fig, use_container_width=True)
            
        with c2:
            # Source/Platform Bar
            s_rev = w_df.groupby("source")["item_revenue"].sum().reset_index()
            fig = px.bar(s_rev, x="source", y="item_revenue", title="Revenue by Platform Source",
                         color="item_revenue", color_continuous_scale="Tealgrn")
            st.plotly_chart(fig, use_container_width=True)

        # NEW: Operational Category Mix
        st.divider()
        st.markdown("##### 📦 Operational Category Mix")
        occ1, occ2 = st.columns(2)
        
        cat_intell = w_df.groupby("Category").agg(
            Revenue=("item_revenue", "sum"),
            Units=("qty", "sum")
        ).reset_index().sort_values("Revenue", ascending=False)
        
        # Strip parent category for cleaner graph labels as requested
        cat_intell["DisplayCategory"] = cat_intell["Category"].apply(lambda x: x.split(" - ")[1] if " - " in str(x) else x)

        with occ1:
            # Category Revenue Donut
            fig_cat_donut = px.pie(cat_intell, values="Revenue", names="DisplayCategory", title="Revenue Contribution by Category",
                                   hole=0.5, color_discrete_sequence=px.colors.qualitative.Pastel)
            st.plotly_chart(fig_cat_donut, use_container_width=True)
            
        with occ2:
            # Category Volume Bar
            fig_cat_bar = px.bar(cat_intell, x="DisplayCategory", y="Units", title="Unit Velocity by Category",
                                 color="Units", color_continuous_scale="Agsunset")
            st.plotly_chart(fig_cat_bar, use_container_width=True)

    with cluster_t2:
        v_c1, v_c2 = st.columns(2)
        with v_c1:
            # Size Distribution
            sz_df = w_df.groupby("_size")["qty"].sum().reset_index()
            fig = px.bar(sz_df, x="_size", y="qty", title="Unit Volume by Size Cluster", 
                         color="qty", color_continuous_scale="Portland")
            st.plotly_chart(fig, use_container_width=True)
        with v_c2:
            # Color Distribution
            clr_df = w_df.groupby("_color")["item_revenue"].sum().reset_index()
            fig = px.pie(clr_df, values="item_revenue", names="_color", title="Revenue by Color Palette",
                         color_discrete_sequence=px.colors.qualitative.Safe)
            st.plotly_chart(fig, use_container_width=True)

    with cluster_t3:
        b_c1, b_c2 = st.columns(2)
        with b_c1:
            # Quantity Distribution (Basket logic)
            q_dist = w_df.groupby("qty")["order_id"].nunique().reset_index()
            q_dist.columns = ["Items in Line", "Orders"]
            fig = px.bar(q_dist, x="Items in Line", y="Orders", title="Bulk Purchase Propensity",
                         text_auto=True, color_discrete_sequence=["#F59E0B"])
            st.plotly_chart(fig, use_container_width=True)
        with b_c2:
            # City Mix within this cluster
            city_mix = w_df.groupby("city")["item_revenue"].sum().reset_index().sort_values("item_revenue", ascending=False).head(8)
            fig = px.bar(city_mix, x="item_revenue", y="city", title="Market Hotspots", 
                         orientation='h', color="item_revenue", color_continuous_scale="Agsunset")
            st.plotly_chart(fig, use_container_width=True)

    with cluster_t4:
        st.dataframe(
            w_df[["order_id", "order_date", "item_name", "sku", "qty", "item_revenue", "Trend", "Coupons", "source", "city"]],
            use_container_width=True, hide_index=True
        )
