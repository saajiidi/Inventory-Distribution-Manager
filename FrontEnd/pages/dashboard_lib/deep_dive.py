import pandas as pd
import streamlit as st
import plotly.express as px
from FrontEnd.components import ui
from BackEnd.core.categories import parse_sku_variants, get_clean_product_name, sort_categories

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
    st.markdown("### 🔍 Advanced Market Deep-Dive")
    
    # FILTER CONTROL CENTER
    with st.expander("🛠️ Advanced Cluster Filters", expanded=True):
        st.markdown("**📦 Category & Item**")
        f_c1, f_c2 = st.columns(2)
        
        with f_c1:
            # 1. Category
            raw_cats = [str(c) for c in df_sales["Category"].dropna().unique() if str(c).strip()]
            cat_list = sort_categories(raw_cats)
            sel_cats = st.multiselect("Categories", ["All"] + cat_list, default=["All"])
            active_cats = [] if "All" in sel_cats or not sel_cats else sel_cats

        with f_c2:
            # Combined Product Name & SKU selection
            sku_options = df_sales[df_sales["Category"].isin(active_cats)] if active_cats else df_sales
            # Create a display name for the multiselect (Clean Name + SKU)
            sku_options = sku_options.copy()
            sku_options["_display_name"] = sku_options["_clean_name"] + " [" + sku_options["sku"].astype(str) + "]"
            
            avail_items = sorted([str(s) for s in sku_options["_display_name"].unique() if str(s).strip() and "Unknown" not in str(s)])
            sel_items = st.multiselect("Products (Name + SKU)", ["All"] + avail_items, default=["All"])
            active_items = [] if "All" in sel_items or not sel_items else sel_items

    # APPLY COMPREHENSIVE FILTERING
    w_df = df_sales.copy()
    
    # Logic Filters
    if active_cats: w_df = w_df[w_df["Category"].isin(active_cats)]
    if active_items: 
        w_df["_display_name"] = w_df["_clean_name"] + " [" + w_df["sku"].astype(str) + "]"
        w_df = w_df[w_df["_display_name"].isin(active_items)]


    # VISUALIZATION SUITE
    st.markdown(f"**Found {len(w_df)} records matching these constraints**")
    
    cluster_t1, cluster_t2, cluster_t3, cluster_t4 = st.tabs(["📊 Performance Mix", "🔍 Variant Analysis", "🛒 Basket Context", "📋 Cluster Data Ledger"])
    
    with cluster_t1:
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
            fig = px.bar(s_rev, x="source", y="item_revenue", title="revenue by platform source",
                         color="item_revenue", color_continuous_scale="Tealgrn")
            st.plotly_chart(fig, use_container_width=True)

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
