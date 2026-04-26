import pandas as pd
import streamlit as st
import plotly.express as px
import numpy as np
from datetime import datetime
from itertools import combinations
from collections import Counter
from FrontEnd.components import ui
from BackEnd.core.categories import get_category_for_sales, parse_sku_variants, get_clean_product_name, get_master_category_list, format_category_label, get_subcategory_name, classify_velocity_trend

def render_inventory_health(stock_df: pd.DataFrame, forecast_df: pd.DataFrame, df_sales: pd.DataFrame = None):
    c1, c2 = st.columns([3, 1])
    with c1:
        st.subheader("📦 Stock Insight")
    with c2:
        if st.button("🔄 Sync Products & Stock", use_container_width=True, help="Fetch latest published products, SKUs, and inventory counts."):
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
    
    # Initialize missing pillars for pricing
    for col in ["Regular Price", "Sale Price", "Price"]:
        if col not in inventory.columns:
            inventory[col] = 0.0
            
    inventory["Stock Quantity"] = pd.to_numeric(inventory.get("Stock Quantity", 0), errors="coerce").fillna(0)
    inventory["Price"] = pd.to_numeric(inventory.get("Price", 0), errors="coerce").fillna(0)
    inventory["Value"] = inventory["Stock Quantity"] * inventory["Price"]
    
    # Clean Names, Categories & Variants
    inventory["_clean_name"] = inventory["Name"].apply(get_clean_product_name)
    parsed_variants = inventory["Name"].apply(parse_sku_variants).tolist()
    inventory["_color"] = [p[0] for p in parsed_variants]
    inventory["_size"] = [p[1] for p in parsed_variants]
    
    # Enforce DEEN-BI standard categories over raw WooCommerce tags
    inventory["Category"] = inventory.apply(lambda x: get_category_for_sales(str(x.get("Name", "")) + " " + str(x.get("SKU", ""))), axis=1)
        
    # --- Real Velocity & Trend Calculation ---
    if df_sales is not None and not df_sales.empty:
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

    # Summary Metrics
    low_stock = inventory[inventory["Stock Quantity"] <= 5]
    m1, m2, m3 = st.columns(3)
    with m1: ui.icon_metric("Unique SKU Records", f"{len(inventory):,}", icon="🏷️")
    with m2: ui.icon_metric("Low Stock Alerts", f"{len(low_stock):,}", icon="⚠️")
    with m3: ui.icon_metric("Inventory Asset Value", f"৳{inventory['Value'].sum():,.0f}", icon="💰")
    
    st.divider()

    # 2. Inventory Sniper (Structured Search)
    st.markdown("#### 🎯 Inventory Sniper")
    st.caption("Search across categories and velocity trends to isolate specific SKU health.")
    
    f_c1, f_c2, f_c3 = st.columns(3)
    
    with f_c1:
        # Use master category list for consistent hierarchy display
        cat_list = get_master_category_list()
        sel_cat = st.selectbox("Category", ["All"] + cat_list, index=0, key="sniper_cat_select", format_func=format_category_label)
        active_cat = None if sel_cat == "All" else sel_cat

    with f_c2:
        avail_trends = sorted(inventory["Trend"].unique())
        sel_trend = st.selectbox("Trend Classification", ["All"] + avail_trends, index=0, key="sniper_trend_select")
        active_trend = None if sel_trend == "All" else sel_trend

    with f_c3:
        # Filter products by category & trend
        prod_options = inventory.copy()
        if active_cat: prod_options = prod_options[prod_options["Category"].str.startswith(active_cat, na=False)]
        if active_trend: prod_options = prod_options[prod_options["Trend"] == active_trend]
        
        # Build unique Name + SKU display entries
        prod_options["_display_name"] = prod_options["_clean_name"] + " [" + prod_options["SKU"].astype(str) + "]"
        
        avail_prods = sorted([str(p) for p in prod_options["_display_name"].unique() if str(p).strip()])
        sel_prod = st.selectbox("Product Selection", ["All"] + avail_prods, index=0, key="sniper_prod_select")
        active_prod = None if sel_prod == "All" else sel_prod

    # Determine Sniper Results
    if active_prod:
        # Match using the display name (which includes SKU) against the filtered options
        sniper_results = inventory.copy()
        sniper_results["_display_name"] = sniper_results["_clean_name"] + " [" + sniper_results["SKU"].astype(str) + "]"
        sniper_results = sniper_results[sniper_results["_display_name"] == active_prod]
        
        st.markdown(f"**Detailed Analysis for:** `{active_prod}`")
        
        if not sniper_results.empty:
            # Velocity and Trend display securely cast to scalar types
            item_trend = str(sniper_results["Trend"].iloc[0])
            item_velocity = float(sniper_results["daily_velocity"].iloc[0])
            
            k1, k2, k3 = st.columns(3)
            with k1: ui.icon_metric("Current Stock Balance", f"{int(sniper_results['Stock Quantity'].sum())}", icon="📦")
            with k2: ui.icon_metric("Sales Velocity", f"{item_velocity:.2f} units/day", icon="⚡")
            with k3: ui.icon_metric("Velocity Tier", item_trend, icon="📈")
            
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
        c_filter1, c_filter2 = st.columns([1, 2])
        with c_filter1:
            val_basis = st.radio(
                "Value Basis", 
                ["Market Value", "Regular Value", "Sale Value"], 
                index=0,
                horizontal=True
            )
        with c_filter2:
            group_basis = st.radio(
                "Aggregation Level",
                ["Main Category", "Sub-Category", "Master Product", "Variant"],
                index=1,
                horizontal=True
            )
        
        val_col_map = {
            "Market Value": "Value",
            "Regular Value": "Regular Value",
            "Sale Value": "Sale Value"
        }
        val_col = val_col_map.get(val_basis, "Value")

        # 3. Data Preparation
        valid_cats = list(get_master_category_list())
        inventory["Category"] = np.where(inventory["Category"].isin(valid_cats), inventory["Category"], "Others")
        inventory["Main Category"] = inventory["Category"].astype(str).str.split(" - ").str[0]
        inventory["Sub Category"] = inventory["Category"].apply(get_subcategory_name)
        inventory["Master Product"] = inventory["_clean_name"] + " [" + inventory["SKU"].astype(str) + "]"
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

        cat_agg = filtered_inv.groupby(group_col).agg(
            Selected_Value=(val_col, "sum"),
            Total_Units=("Stock Quantity", "sum"),
            SKU_Count=("Name", "count")
        ).reset_index()
        
        cat_agg = cat_agg.rename(columns={group_col: "Display Category"})
        
        # Independent limits for each metric so Volume isn't restricted by Value
        # Show all for Categories, limit for Products/Variants to keep charts readable
        display_limit = 150 if group_basis in ["Main Category", "Sub-Category"] else 50
        
        top_val_df = cat_agg.sort_values("Selected_Value", ascending=False).head(display_limit).sort_values("Selected_Value", ascending=True)
        top_unit_df = cat_agg.sort_values("Total_Units", ascending=False).head(display_limit).sort_values("Total_Units", ascending=True)
        top_sku_df = cat_agg.sort_values("SKU_Count", ascending=False).head(display_limit).sort_values("SKU_Count", ascending=True)

        # 4. Interactive Visuals
        t1, t2, t3, t4, t5 = st.tabs(["💰 Value & Volume", "📦 Breadth Analysis", "🛒 Smart Restock", "📉 Dead Stock", "🤝 Bundle Intel"])
        
        with t1:
            v1, v2 = st.columns(2)
            with v1:
                fig_unit_bar = ui.bar_chart(top_unit_df, x="Total_Units", y="Display Category", title=f"Total Unit Volume per {group_label}", color="Total_Units")
                fig_unit_bar.update_traces(texttemplate="%{x:,} Units", textposition="auto")
                fig_unit_bar.update_layout(height=max(450, len(top_unit_df) * 30))
                st.plotly_chart(fig_unit_bar, width="stretch")
            with v2:
                fig_val_bar = ui.bar_chart(top_val_df, x="Selected_Value", y="Display Category", title=f"Absolute {val_basis} per {group_label}", color="Selected_Value")
                fig_val_bar.update_traces(texttemplate="৳%{x:,.0f}", textposition="auto")
                fig_val_bar.update_layout(height=max(450, len(top_val_df) * 30))
                st.plotly_chart(fig_val_bar, width="stretch")
                
        with t2:
            breadth_label = "Variants" if group_basis == "Variant" else "SKUs"
            fig_sku_bar = ui.bar_chart(top_sku_df, x="SKU_Count", y="Display Category", title=f"{breadth_label} Breadth per {group_label}", color="SKU_Count")
            fig_sku_bar.update_traces(texttemplate=f"%{{x:,}} {breadth_label}", textposition="auto")
            fig_sku_bar.update_layout(height=max(450, len(top_sku_df) * 30))
            st.plotly_chart(fig_sku_bar, width="stretch")

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
            
            if df_sales is None or df_sales.empty:
                st.info("Sales data is required to compute bundle intelligence.")
            else:
                # 1. Identify Top Bundles (Frequent Pairs)
                basket_df = df_sales.copy()
                if "_clean_name" not in basket_df.columns:
                    basket_df["_clean_name"] = basket_df["item_name"].apply(get_clean_product_name)
                
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
        
        summary_metrics = {
            "Total SKU Records": len(export_inv),
            "Total Stock Quantity": export_inv["Stock Quantity"].sum(),
            "Total Asset Value (৳)": f"{export_inv['Value'].sum():,.2f}",
            "Low Stock Items": len(export_inv[export_inv["Stock Quantity"] <= 5]),
            "Fast Moving Items": len(export_inv[export_inv["Trend"] == "🔥 Fast Moving"]),
            "Dead Stock Items": len(export_inv[export_inv["Trend"] == "❄️ Non-Moving"]),
            "Report Generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        excel_bytes = ui.export_to_excel(
            export_inv.drop(columns=[c for c in export_inv.columns if c.startswith("_")], errors="ignore"), 
            sheet_name="Inventory Data",
            summary_metrics=summary_metrics
        )
        st.download_button(
            label="📊 Download Custom Report",
            data=excel_bytes,
            file_name=f"deen_inventory_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="inv_custom_export_btn"
        )
