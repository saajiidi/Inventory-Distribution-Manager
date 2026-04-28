import pandas as pd
import streamlit as st
import plotly.express as px
import numpy as np
from datetime import datetime, date, timedelta
from itertools import combinations
from collections import Counter
from FrontEnd.components import ui
from BackEnd.core.categories import get_category_for_sales, parse_sku_variants, get_clean_product_name, get_master_category_list, format_category_label, get_subcategory_name, classify_velocity_trend
from BackEnd.commerce_ops.persistence import KeyManager

def render_inventory_health(stock_df: pd.DataFrame, forecast_df: pd.DataFrame, df_sales: pd.DataFrame = None):
    c1, c2 = st.columns([3, 1])
    with c1:
        st.subheader("📦 Stock Insight")
    with c2:
        if st.button("🔄 Sync Products & Stock", use_container_width=True, key=KeyManager.get_key("inventory", "sync_btn"), help="Fetch latest published products, SKUs, and inventory counts."):
            with st.spinner("Fetching latest product catalog and stock data..."):
                from BackEnd.services.hybrid_data_loader import refresh_woocommerce_stock_cache
                
                # Force clear Streamlit's in-memory caches to ensure fresh data is displayed
                try:
                    from BackEnd.services.woocommerce_client.fetch_products import clear_products_cache
                    clear_products_cache()
                except Exception:
                    pass
                st.cache_data.clear()
                
                refresh_woocommerce_stock_cache()
                st.toast("✅ Product and Stock data synced successfully!")
                st.rerun()
    
    if stock_df is None or stock_df.empty:
        st.info("No live inventory data is available yet. Initializing sync...")
        return
        
    # 1. Consolidated Data Pre-processing
    inventory = stock_df.copy()
    
    # Remove parent variable products to prevent them from hiding variations or showing 0 stock
    if "Product Type" in inventory.columns:
        inventory = inventory[inventory["Product Type"] != "variable"]

    # Initialize missing pillars for pricing
    for col in ["Regular Price", "Sale Price", "Price"]:
        if col not in inventory.columns:
            inventory[col] = 0.0
            
    inventory["Stock Quantity"] = pd.to_numeric(inventory.get("Stock Quantity", 0), errors="coerce").fillna(0)
    inventory["Price"] = pd.to_numeric(inventory.get("Price", 0), errors="coerce").fillna(0)
    inventory["Value"] = inventory["Stock Quantity"] * inventory["Price"]
    
    # Clean Names, Categories & Variants
    inventory["_clean_name"] = inventory["Name"].astype(str).apply(get_clean_product_name)
    parsed_variants = inventory["Name"].astype(str).apply(parse_sku_variants).tolist()
    inventory["_color"] = [p[0] for p in parsed_variants]
    inventory["_size"] = [p[1] for p in parsed_variants]
    
    # Enforce DEEN-BI standard categories over raw WooCommerce tags
    names = inventory["Name"].fillna("").astype(str)
    skus = inventory["SKU"].fillna("").astype(str)
    inventory["Category"] = [get_category_for_sales(n + " " + s) for n, s in zip(names, skus)]
        
    # --- Real Velocity & Trend Calculation ---
    if df_sales is not None and not df_sales.empty and all(c in df_sales.columns for c in ["order_date", "sku", "qty"]):
        # Calculate days in selected window safely
        valid_dates = df_sales["order_date"].dropna()
        if not valid_dates.empty:
            days_active = (valid_dates.max() - valid_dates.min()).days or 1
        else:
            days_active = 1
        
        # Group sales by SKU
        sku_sales = df_sales.groupby("sku")["qty"].sum().reset_index()
        sku_sales["daily_velocity"] = (sku_sales["qty"] / days_active).round(3)
        
        # Merge velocity into inventory
        inventory = inventory.merge(sku_sales[["sku", "daily_velocity"]], left_on="SKU", right_on="sku", how="left").drop(columns=["sku"])
        inventory["daily_velocity"] = inventory["daily_velocity"].fillna(0)
    else:
        inventory["daily_velocity"] = 0.0

    # Vectorized Trend Classification
    inventory["Trend"] = classify_velocity_trend(inventory["daily_velocity"])

    show_exact = st.session_state.get("global_show_exact", False)

    def format_val(num):
        if show_exact: return f"{num:,}"
        if num >= 1_000_000: return f"{num/1_000_000:.1f}M".replace(".0M", "M")
        if num >= 1_000: return f"{num/1_000:.1f}K".replace(".0K", "K")
        return f"{num:,}"
        
    def format_curr(num):
        if show_exact: return f"৳{num:,.0f}"
        if num >= 1_000_000: return f"৳{num/1_000_000:.1f}M".replace(".0M", "M")
        if num >= 1_000: return f"৳{num/1_000:.1f}K".replace(".0K", "K")
        return f"৳{num:,.0f}"

    # Summary Metrics
    low_stock = inventory[inventory["Stock Quantity"] <= 5]
    m1, m2, m3 = st.columns(3)
    with m1: ui.icon_metric("Unique SKU Records", format_val(len(inventory)), icon="🏷️")
    with m2: ui.icon_metric("Low Stock Alerts", format_val(len(low_stock)), icon="⚠️")
    with m3: ui.icon_metric("Inventory Asset Value", format_curr(inventory['Value'].sum()), icon="💰")
    
    st.divider()

    # 2. Inventory Sniper (Structured Search)
    st.markdown("#### 🎯 Inventory Sniper")
    st.caption("Search across categories and velocity trends to isolate specific SKU health.")
    
    # Initialize active filter variables to None for broader scope usage
    active_cat = None
    active_prod = None
    active_size = None
    active_trend = None

    f_c1, f_c2, f_c3, f_c4 = st.columns(4)
    
    with f_c1:
        # Use master category list for consistent hierarchy display
        cat_list = get_master_category_list()
        sel_cat = st.selectbox("Category", ["All"] + cat_list, index=0, key=KeyManager.get_key("inventory", "sniper_cat"), format_func=format_category_label)
        active_cat = None if sel_cat == "All" else sel_cat

    with f_c2:
        # Filter products by category
        prod_options = inventory.copy()
        if active_cat: prod_options = prod_options[prod_options["Category"].str.startswith(active_cat, na=False)]
        
        prod_options["_display_name"] = prod_options["_clean_name"]
        
        avail_prods = sorted([str(p) for p in prod_options["_display_name"].unique() if str(p).strip()])
        sel_prod = st.selectbox("Product Selection", ["All"] + avail_prods, index=0, key=KeyManager.get_key("inventory", "sniper_prod"))
        active_prod = None if sel_prod == "All" else sel_prod

    with f_c3:
        size_options = prod_options.copy()
        if active_prod:
            size_options = size_options[size_options["_display_name"] == active_prod]
            
        avail_sizes = sorted([str(s) for s in size_options["_size"].unique() if str(s).strip() and str(s) != "Unknown"])
        sel_size = st.selectbox("Size Selection", ["All"] + avail_sizes, index=0, key=KeyManager.get_key("inventory", "sniper_size"))
        active_size = None if sel_size == "All" else sel_size
        
    with f_c4:
        trend_options = size_options.copy()
        if active_size:
            trend_options = trend_options[trend_options["_size"] == active_size]
            
        avail_trends = sorted([str(t) for t in trend_options["Trend"].dropna().unique()])
        sel_trend = st.selectbox("Trend Classification", ["All"] + avail_trends, index=0, key=KeyManager.get_key("inventory", "sniper_trend"))
        active_trend = None if sel_trend == "All" else sel_trend

    # Determine Sniper Results
    if active_prod:
        # Match using the display name against the filtered options
        sniper_results = inventory.copy()
        sniper_results["_display_name"] = sniper_results["_clean_name"]
        sniper_results = sniper_results[sniper_results["_display_name"] == active_prod]
        
        if active_size:
            sniper_results = sniper_results[sniper_results["_size"] == active_size]
        if active_trend:
            sniper_results = sniper_results[sniper_results["Trend"] == active_trend]
        
        st.markdown(f"**Detailed Analysis for:** `{active_prod}`")
        
        if not sniper_results.empty:
            # Velocity and Trend display securely cast to scalar types
            item_trend = str(sniper_results["Trend"].iloc[0])
            item_velocity = float(sniper_results["daily_velocity"].iloc[0])
            
            k1, k2, k3 = st.columns(3)
            with k1: ui.icon_metric("Current Stock Balance", f"{int(sniper_results['Stock Quantity'].sum())}", icon="📦")
            with k2: ui.icon_metric("Sales Velocity", f"{item_velocity:.2f} units/day", icon="⚡")
            with k3: ui.icon_metric("Velocity Tier", item_trend, icon="📈")

            # Display product creation and modification timestamps
            k4, k5 = st.columns(2)
            with k4:
                date_created = sniper_results["Date Created"].iloc[0] if "Date Created" in sniper_results.columns and not sniper_results.empty else "N/A"
                ui.icon_metric("First Published", str(date_created), icon="📅")
            with k5:
                date_modified = sniper_results["Date Modified"].iloc[0] if "Date Modified" in sniper_results.columns and not sniper_results.empty else "N/A"
                if int(sniper_results['Stock Quantity'].sum()) <= 0 and date_modified != "N/A":
                    ui.icon_metric("OOS Since (Est.)", str(date_modified), icon="🛑")
                else:
                    ui.icon_metric("Last Updated", str(date_modified), icon="🔄")
            
            k6, k7 = st.columns(2)
            with k6:
                reg_price = float(sniper_results["Regular Price"].iloc[0]) if "Regular Price" in sniper_results.columns and not sniper_results.empty else 0.0
                ui.icon_metric("Regular Price", f"৳{reg_price:,.0f}", icon="💵")
            with k7:
                sale_price = float(sniper_results["Sale Price"].iloc[0]) if "Sale Price" in sniper_results.columns and not sniper_results.empty else 0.0
                ui.icon_metric("Sale Price", f"৳{sale_price:,.0f}", icon="🏷️")

            # Stock-out Countdown
            if item_velocity > 0:
                days_left = float(sniper_results['Stock Quantity'].sum()) / item_velocity
                if days_left < 7:
                    st.error(f"🚨 **Stock-out Risk**: This item is selling {item_velocity:.2f} units/day and will be gone in approximately **{int(days_left)} days**.")
                elif days_left < 15:
                    st.warning(f"⚠️ **Restock Advised**: Approximately **{int(days_left)} days** of stock remaining.")
                else:
                    st.success(f"✅ **Healthy Velocity**: **{int(days_left)} days** of stock available at current sales rate.")

        st.markdown("**Variation Breakdown:**")
        st.dataframe(sniper_results[["Name", "SKU", "_size", "Stock Status", "Stock Quantity", "Price"]].rename(
            columns={"_size": "Dimension"}
        ), width="stretch", hide_index=True)
        st.divider()

    st.markdown("#### Inventory Strategic Analysis")
    if "Category" in inventory.columns:
        # 1. Advanced Value Calculations
        inventory["Regular Price"] = pd.to_numeric(inventory.get("Regular Price", 0), errors="coerce").fillna(0)
        inventory["Sale Price"] = pd.to_numeric(inventory.get("Sale Price", 0), errors="coerce").fillna(0)
        inventory["Regular Value"] = inventory["Stock Quantity"] * inventory["Regular Price"]
        inventory["Sale Value"] = inventory["Stock Quantity"] * inventory["Sale Price"]

        # 2. Controls for dynamic visualization
        ctrl_col1, ctrl_col2 = st.columns([3, 1])
        with ctrl_col1:
            group_basis = st.radio(
                "Aggregation Level",
                ["Main Category", "Sub-Category", "Master Product", "Variant"],
                index=1,
                horizontal=True,
                key=KeyManager.get_key("inventory", "group_basis")
            )
        with ctrl_col2:
            use_sale_price = st.toggle("Graph: Use Sale Price", value=False, key=KeyManager.get_key("inventory", "use_sale_price_graph"))
        
        val_basis = "Sale Value" if use_sale_price else "Regular Value"
        val_col = "Sale Value" if use_sale_price else "Regular Value"

        # 3. Data Preparation
        valid_cats = list(get_master_category_list())
        inventory["Category"] = np.where(inventory["Category"].isin(valid_cats), inventory["Category"], "Others")
        inventory["Main Category"] = inventory["Category"].astype(str).str.split(" - ").str[0]
        inventory["Sub Category"] = inventory["Category"].apply(get_subcategory_name)
        inventory["Master Product"] = inventory["_clean_name"]
        inventory["Variant"] = inventory["Name"]
        
        # Apply active filters to the strategic analysis
        filtered_inv = inventory.copy()
        filtered_inv["_display_name"] = filtered_inv["Master Product"]
        
        if active_cat: 
            filtered_inv = filtered_inv[filtered_inv["Category"].str.startswith(active_cat, na=False)]
        if active_trend: 
            filtered_inv = filtered_inv[filtered_inv["Trend"] == active_trend]
        if active_prod:
            filtered_inv = filtered_inv[filtered_inv["_display_name"] == active_prod]

        # Explicit Aggregation Logic based on Radio Button
        group_col_map = {
            "Main Category": "Main Category",
            "Sub-Category": "Sub Category",
            "Master Product": "Master Product",
            "Variant": "Variant"
        }
        group_col = group_col_map.get(group_basis, "Sub Category")
        group_label = group_basis

        # Include items marked as 'instock' to prevent variable parent products (which often have 0 stock) from vanishing
        in_stock_inv = filtered_inv[(filtered_inv["Stock Quantity"] > 0) | (filtered_inv["Stock Status"].astype(str).str.lower().str.strip() == "instock")]
        cat_agg = in_stock_inv.groupby(group_col).agg(
            Selected_Value=(val_col, "sum"),
            Regular_Value=("Regular Value", "sum"),
            Sale_Value=("Sale Value", "sum"),
            Total_Units=("Stock Quantity", "sum"),
            SKU_Count=("Name", "count")
        ).reset_index()
        
        cat_agg = cat_agg.rename(columns={group_col: group_label})
        
        # Independent limits for each metric so Volume isn't restricted by Value
        # Show all for Categories, limit for Products/Variants to keep charts readable
        display_limit = 150 if group_basis in ["Main Category", "Sub-Category"] else 50
        
        top_val_df = cat_agg.sort_values("Selected_Value", ascending=False).head(display_limit).sort_values("Selected_Value", ascending=True)
        top_unit_df = cat_agg.sort_values("Total_Units", ascending=False).head(display_limit).sort_values("Total_Units", ascending=True)
        top_sku_df = cat_agg.sort_values("SKU_Count", ascending=False).head(display_limit).sort_values("SKU_Count", ascending=True)

        # 4. Interactive Visuals
        t1, t2, t3, t4, t5, t6 = st.tabs(["💰 Value & Volume", "📦 Breadth Analysis", "🛒 Smart Restock", "📉 Dead Stock", "🤝 Bundle Intel", "⏳ Stock Timeline"])
        
        with t1:
            v1, v2 = st.columns(2)
            with v1:
                fig_unit_bar = ui.bar_chart(top_unit_df, x="Total_Units", y=group_label, title=f"Total Unit Volume per {group_label}", color="Total_Units")
                fig_unit_bar.update_traces(texttemplate="%{x:,} Units", textposition="auto") # Moved texttemplate here
                fig_unit_bar.update_layout(height=max(450, len(top_unit_df) * 30), yaxis_title=group_label)
                st.plotly_chart(fig_unit_bar, width="stretch", key=KeyManager.get_key("inventory", "unit_volume_bar"))
            with v2:
                fig_val_bar = ui.bar_chart(top_val_df, x="Selected_Value", y=group_label, title=f"Absolute {val_basis} per {group_label}", color="Selected_Value")
                fig_val_bar.update_traces(texttemplate="৳%{x:,.0f}", textposition="auto") # Moved texttemplate here
                fig_val_bar.update_layout(height=max(450, len(top_val_df) * 30), yaxis_title=group_label)
                st.plotly_chart(fig_val_bar, width="stretch", key=KeyManager.get_key("inventory", "value_volume_bar"))
                
            st.divider()
            st.markdown(f"##### 📋 Detailed {group_label} Ledger")
            display_table = cat_agg.sort_values("Selected_Value", ascending=False).copy()
            st.dataframe(
                display_table,
                use_container_width=True,
                hide_index=True,
                column_config={
                    group_label: st.column_config.TextColumn(group_label),
                    "Selected_Value": None, # Hide dynamic col, explicitly display both below
                    "Regular_Value": st.column_config.NumberColumn("Regular Value", format="৳%.0f"),
                    "Sale_Value": st.column_config.NumberColumn("Sale Value", format="৳%.0f"),
                    "Total_Units": st.column_config.NumberColumn("Total Units", format="%d"),
                    "SKU_Count": st.column_config.NumberColumn("In-Stock SKUs", format="%d")
                }
            )

        with t2:
            breadth_label = "In-Stock Variants" if group_basis == "Variant" else "In-Stock SKUs"
            fig_sku_bar = ui.bar_chart(top_sku_df, x="SKU_Count", y=group_label, title=f"{breadth_label} Breadth per {group_label}", color="SKU_Count")
            fig_sku_bar.update_traces(texttemplate=f"%{{x:,}} {breadth_label}", textposition="auto")
            fig_sku_bar.update_layout(height=max(450, len(top_sku_df) * 30), xaxis_title=breadth_label, yaxis_title=group_label) # Moved texttemplate here
            st.plotly_chart(fig_sku_bar, width="stretch", key=KeyManager.get_key("inventory", "sku_breadth_bar"))

        with t3:
            st.markdown("##### 🚀 Velocity-Based Inventory Planning")
            st.caption("Strategic restock recommendations based on real sales velocity for the selected range.")
            
            # Use real daily_velocity calculated at the top
            filtered_inv["days_remaining"] = (filtered_inv["Stock Quantity"] / filtered_inv["daily_velocity"]).replace([np.inf, -np.inf], 999).fillna(999).astype(int)
            
            # Vectorized Recommendation logic
            filtered_inv["Status"] = np.select(
                [
                    (filtered_inv["days_remaining"] < 3) & (filtered_inv["daily_velocity"] > 0),
                    (filtered_inv["days_remaining"] < 7) & (filtered_inv["daily_velocity"] > 0)
                ],
                ["🚨 CRITICAL: RESTOCK TODAY", "⚠️ WARNING: REORDER NOW"],
                default="✅ HEALTHY"
            )
            
            crit_items = filtered_inv[(filtered_inv["days_remaining"] < 7) & (filtered_inv["daily_velocity"] > 0)].sort_values("days_remaining")
            if not crit_items.empty:
                st.warning(f"Found {len(crit_items)} items that will stock out within 7 days.")
                st.dataframe(crit_items[["Name", "Stock Quantity", "daily_velocity", "days_remaining", "Trend", "Status"]].rename(
                    columns={"daily_velocity": "Daily Velocity", "days_remaining": "Days of Stock"}
                ), width="stretch", hide_index=True)
            else:
                st.success("All stock levels are stable based on current velocity.")

        with t4:
            st.markdown("##### 📉 Dead Stock & Liquidation Hub")
            st.caption("Items with high inventory levels but near-zero transaction velocity in the selected range.")
            
            # Dead stock based on real velocity zeroing out
            dead_stock = filtered_inv[
                (filtered_inv["daily_velocity"] < 0.05) & 
                (filtered_inv["Stock Quantity"] > 0)
            ].copy()
            
            if not dead_stock.empty:
                st.error(f"Detected {len(dead_stock)} items with stagnant movement.")
                st.markdown(f"**Asset Value at Risk:** ৳{dead_stock['Value'].sum():,.0f}")
                
                st.dataframe(dead_stock[["Name", "Category", "Stock Quantity", "Value", "Trend"]].rename(
                    columns={"Value": "Capital Locked"}
                ).sort_values("Capital Locked", ascending=False), width="stretch", hide_index=True)
            else:
                st.success("No significant dead stock detected for this period.")

        with t5:
            st.markdown("##### 🤝 Bundle-Aware Inventory Intelligence")
            st.caption("Analyzes frequent product combinations to detect missing components (Orphan Stock).")
            
            if df_sales is None or df_sales.empty or "order_id" not in df_sales.columns or "item_name" not in df_sales.columns:
                st.info("Sales data is required to compute bundle intelligence.")
            else:
                # 1. Identify Top Bundles (Frequent Pairs)
                basket_df = df_sales.copy()
                if "_clean_name" not in basket_df.columns:
                    basket_df["_clean_name"] = basket_df["item_name"].astype(str).apply(get_clean_product_name)
                
                basket_df = basket_df.groupby("order_id")["_clean_name"].apply(list).reset_index()
                basket_df = basket_df[basket_df["_clean_name"].apply(len) > 1]

                if basket_df.empty:
                    st.info("No bundle history found in current sales window to analyze dependency.")
                else:
                    all_pairs = []
                    for products in basket_df["_clean_name"]:
                        unique_products = sorted(list(set(products)))
                        if len(unique_products) > 1:
                            all_pairs.extend(list(combinations(unique_products, 2)))
                    
                    top_pairs = Counter(all_pairs).most_common(10)
                    
                    if not top_pairs:
                        st.info("Not enough pair data.")
                    else:
                        # 2. Calculate Bundle Fulfillment Rate
                        full_count = 0
                        total_bundles = len(top_pairs)
                        orphan_skus = []
                        bundle_data = []

                        for pair, count in top_pairs:
                            stock_a = inventory[inventory["_clean_name"] == pair[0]]["Stock Quantity"].sum()
                            stock_b = inventory[inventory["_clean_name"] == pair[1]]["Stock Quantity"].sum()
                            
                            status = "✅ Fulfilled"
                            if stock_a > 0 and stock_b > 0:
                                full_count += 1
                            elif (stock_a > 0 and stock_b <= 0) or (stock_b > 0 and stock_a <= 0):
                                orphan_skus.append(pair[0] if stock_a > 0 else pair[1])
                                status = "⚠️ Orphaned"
                            else:
                                status = "❌ Both OOS"
                                
                            bundle_data.append({
                                "Product A": pair[0],
                                "Stock A": int(stock_a),
                                "Product B": pair[1],
                                "Stock B": int(stock_b),
                                "Co-Purchases": count,
                                "Status": status
                            })
                            
                        fulfillment_rate = (full_count / total_bundles * 100) if total_bundles > 0 else 0
                        unique_clean_names = inventory["_clean_name"].nunique()
                        orphan_pct = (len(set(orphan_skus)) / unique_clean_names * 100) if unique_clean_names > 0 else 0
                        
                        bm1, bm2, bm3 = st.columns(3)
                        with bm1: ui.icon_metric("Bundle Fulfillment", f"{fulfillment_rate:.0f}%", icon="📦")
                        with bm2: ui.icon_metric("Orphan Stock Rate", f"{orphan_pct:.1f}%", icon="⚠️", delta="Action Required" if orphan_pct > 10 else "Normal", delta_color="inverse")
                        with bm3: ui.icon_metric("Dependency Links", f"{total_bundles}", icon="🔗")
                        
                        if fulfillment_rate < 50:
                            st.error("⚠️ **Fulfillment Critical**: High rate of lost sales due to bundle imbalance (one item out of stock).")
                            
                        st.markdown("**Strategic Reorder Intelligence (Top Combinations)**")
                        st.dataframe(pd.DataFrame(bundle_data), width="stretch", hide_index=True)
                        
                        # Add Orphan Stock Intelligence
                        st.markdown("---")
                        st.markdown("##### 🚨 Critical Orphaned Stock")
                        st.caption("Items you have in stock, but their highly correlated paired item is Out of Stock.")
                        from BackEnd.services.inventory_intel import InventoryIntelligence
                        intel = InventoryIntelligence(df_sales, inventory)
                        orphans_df = intel.detect_orphan_stock()
                        if not orphans_df.empty:
                            st.error(f"Found {len(orphans_df)} stranded items due to out-of-stock partners.")
                            st.dataframe(orphans_df, width="stretch", hide_index=True)
                        else:
                            st.success("No critical orphan stock dependencies detected.")
    else:
        st.info("Category-wise breakdown is not yet available in the stock cache.")
        
        with t6:
            returns_df = st.session_state.get("returns_data", pd.DataFrame())
            _render_stock_timeline(inventory, df_sales, returns_df)
        
    st.markdown("---")
    
    # 3. Report Summary & Download
    d1, d2 = st.columns([2, 1])
    with d1:
        st.markdown("#### Strategic Inventory Snapshot")
        st.caption("Exports data based on active 'Inventory Sniper' filters and current velocity metrics.")
    with d2:
        # Apply active filters to the exported dataframe
        export_inv = inventory.copy()
        if active_cat: export_inv = export_inv[export_inv["Category"].str.startswith(active_cat, na=False)]
        if active_trend: export_inv = export_inv[export_inv["Trend"] == active_trend]
        if active_prod:
            export_inv["_display_name"] = export_inv["_clean_name"] + " [" + export_inv["SKU"].astype(str) + "]"
            export_inv = export_inv[export_inv["_display_name"] == active_prod]


def _get_global_date_range() -> tuple[date, date]:
    """Get start and end dates from global time window."""
    today = date.today()
    window = st.session_state.get("time_window", "Last Month")

    if window == "MTD":
        start_dt = today.replace(day=1)
        end_dt = today
    elif window == "YTD":
        start_dt = today.replace(month=1, day=1)
        end_dt = today
    elif window == "Custom Date Range":
        start_dt = st.session_state.get("wc_sync_start_date", today - timedelta(days=30))
        end_dt = st.session_state.get("wc_sync_end_date", today)
    else:
        window_map = {
            "Last Day": 1,
            "Last 3 Days": 3,
            "Last 4 Days": 4,
            "Last 7 Days": 7,
            "Last 15 Days": 15,
            "Last Month": 30,
            "Last 3 Months": 90,
            "Last Quarter": 90,
            "Last Half Year": 180,
            "Last 9 Months": 270,
            "Last Year": 365
        }
        days_back = window_map.get(window, 30)
        start_dt = today - timedelta(days=days_back)
        end_dt = today

    return start_dt, end_dt


def _render_stock_timeline(inventory_df: pd.DataFrame, sales_df: pd.DataFrame, returns_df: pd.DataFrame):
    """Renders a historical stock timeline and availability checker."""
    st.markdown("#### ⏳ Stock Timeline & Availability")
    st.caption("Analyze historical stock levels and see which products were available on a specific date.")

    # Part 1: Recent Stock-In Events (from returns)
    st.markdown("##### Recent Restocking Events (from Returns)")
    if returns_df.empty or 'returned_items' not in returns_df.columns:
        st.info("No returns data available to track restocking events.")
    else:
        restocked = returns_df[
            returns_df['inventory_updated'].astype(str).str.lower().isin(['yes', 'true', '1'])
        ].copy()

        if restocked.empty:
            st.info("No items marked as restocked from returns in the current dataset.")
        else:
            restocked_items = []
            for _, row in restocked.iterrows():
                items = row.get('returned_items', [])
                if not isinstance(items, list): continue
                for item in items:
                    if not isinstance(item, dict): continue
                    restocked_items.append({
                        "Date": row['date'],
                        "Order ID": row['order_id_raw'],
                        "Product": item.get('name', 'Unknown'),
                        "SKU": item.get('sku', 'N/A'),
                        "Qty": item.get('qty', 1)
                    })
            
            if restocked_items:
                restock_df = pd.DataFrame(restocked_items).sort_values("Date", ascending=False)
                st.dataframe(restock_df.head(10), hide_index=True, use_container_width=True, column_config={"Date": st.column_config.DateColumn("Date")})
            else:
                st.info("No individual items found in restocked returns.")

    st.divider()

    # Part 2: Historical Availability Snapshot
    st.markdown("##### Historical Stock Snapshot")
    
    start_dt, end_dt = _get_global_date_range()

    snapshot_date = st.date_input(
        "Select a date to view stock availability",
        value=end_dt,
        min_value=start_dt,
        max_value=end_dt,
        key=KeyManager.get_key("inventory", "snapshot_date")
    )
    snapshot_ts = pd.to_datetime(snapshot_date)

    if st.button("Show Stock on Selected Date", key=KeyManager.get_key("inventory", "show_snapshot_btn"), type="primary"):
        with st.spinner(f"Reconstructing stock levels for {snapshot_date.strftime('%Y-%m-%d')}..."):
            current_stock = inventory_df.set_index('SKU')['Stock Quantity'].to_dict()

            sales_after = pd.DataFrame()
            if sales_df is not None and not sales_df.empty and 'order_date' in sales_df.columns:
                sales_df['order_date'] = pd.to_datetime(sales_df['order_date'], errors='coerce').dt.tz_localize(None)
                sales_after = sales_df[sales_df['order_date'] > snapshot_ts].copy()
            sales_qty_after = sales_after.groupby('SKU')['qty'].sum() if not sales_after.empty else pd.Series(dtype='float64')

            returns_qty_after = pd.Series(dtype='float64')
            if not returns_df.empty and 'date' in returns_df.columns:
                returns_df['date'] = pd.to_datetime(returns_df['date'], errors='coerce').dt.tz_localize(None)
                restocked_after = returns_df[(returns_df['date'] > snapshot_ts) & (returns_df['inventory_updated'].astype(str).str.lower().isin(['yes', 'true', '1']))].copy()
                if not restocked_after.empty:
                    returned_items_after = [{'SKU': item['sku'], 'qty': item.get('qty', 1)} for _, row in restocked_after.iterrows() for item in (row.get('returned_items', []) if isinstance(row.get('returned_items', []), list) else []) if isinstance(item, dict) and 'sku' in item]
                    if returned_items_after:
                        returns_df_after = pd.DataFrame(returned_items_after)
                        returns_qty_after = returns_df_after.groupby('SKU')['qty'].sum()

            historical_stock_df = inventory_df[['Name', 'SKU', 'Category']].copy().set_index('SKU')
            historical_stock_df['stock_on_date'] = (historical_stock_df.index.map(current_stock).fillna(0) + historical_stock_df.index.map(sales_qty_after).fillna(0) - historical_stock_df.index.map(returns_qty_after).fillna(0)).astype(int)

            st.success(f"Found {len(historical_stock_df[historical_stock_df['stock_on_date'] > 0])} products available on {snapshot_date.strftime('%Y-%m-%d')}.")
            display_df = historical_stock_df[historical_stock_df['stock_on_date'] > 0].copy().reset_index()
            st.dataframe(display_df[['Name', 'SKU', 'Category', 'stock_on_date']].rename(columns={'stock_on_date': 'Estimated Stock'}), hide_index=True, use_container_width=True)