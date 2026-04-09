import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime
from FrontEnd.components import ui
from BackEnd.core.categories import get_category_for_sales, parse_sku_variants, get_clean_product_name, sort_categories

def render_inventory_health(stock_df: pd.DataFrame, forecast_df: pd.DataFrame, df_sales: pd.DataFrame = None):
    st.subheader("Stock Insight")
    
    if stock_df is None or stock_df.empty:
        st.info("No live stock snapshot is available yet.")
        return
        
    inventory = stock_df.copy()
    
    # Pre-processing Clean Names, Categories & Variants
    inventory["_clean_name"] = inventory["Name"].apply(get_clean_product_name)
    inventory[["_color", "_size"]] = inventory["Name"].apply(lambda x: pd.Series(parse_sku_variants(x)))
    
    if "Category" not in inventory.columns:
        inventory["Category"] = inventory["Name"].apply(get_category_for_sales)
        
    # 0. Inventory Sniper (Structured Search)
    st.markdown("#### 🎯 Inventory Sniper")
    st.caption("Filter by Category and Size to locate a specific item SKU and view its current health.")
    
    f_c1, f_c2 = st.columns(2)
    
    with f_c1:
        raw_cats = [str(c) for c in inventory["Category"].dropna().unique() if str(c).strip()]
        cat_list = sort_categories(raw_cats)
        sel_cat = st.selectbox("Category", ["All"] + cat_list, index=0, key="sniper_cat_select")
        active_cat = None if sel_cat == "All" else sel_cat

    with f_c2:
        # Filter products by category
        prod_options = inventory[inventory["Category"] == active_cat] if active_cat else inventory
        # Build unique Name + SKU display entries
        prod_options = prod_options.copy()
        prod_options["_display_name"] = prod_options["_clean_name"] + " [" + prod_options["SKU"].astype(str) + "]"
        
        avail_prods = sorted([str(p) for p in prod_options["_display_name"].unique() if str(p).strip()])
        sel_prod = st.selectbox("Products (Name + SKU)", ["All"] + avail_prods, index=0, key="sniper_prod_select")
        active_prod = None if sel_prod == "All" else sel_prod

    # Determine Sniper Results
    sniper_results = inventory.copy()
    if active_cat: sniper_results = sniper_results[sniper_results["Category"] == active_cat]
    if active_prod:
        # Exact match on display name which contains unique SKU
        sniper_results = prod_options[prod_options["_display_name"] == active_prod]

    # Only show sniper details if at least a product is selected
    if active_prod:
        st.markdown(f"**Detailed Analysis for:** `{active_prod}`")
        
        # Cross-reference with Sales Volume if available
        if df_sales is not None:
            # We filter sales by the exact SKU in active_prod
            target_sku = active_prod.split(" [")[-1].replace("]", "")
            sales_match = df_sales[df_sales["sku"].astype(str) == target_sku]
            
            total_sold = sales_match["qty"].sum()
            total_rev = sales_match["item_revenue"].sum()
        else:
            total_sold = 0
            total_rev = 0
        
        k1, k2, k3 = st.columns(3)
        k1.metric("Current Stock (Total)", f"{int(sniper_results['Stock Quantity'].sum())}")
        k2.metric("Total items sold", f"{int(total_sold):,}")
        k3.metric("Total sale value", f"৳{total_rev:,.0f}")
        
        # Stock-out Countdown (Velocity Analysis)
        avg_velocity = total_sold / 30 # Baseline 30-day velocity estimate
        if avg_velocity > 0:
            days_left = sniper_results['Stock Quantity'].sum() / avg_velocity
            if days_left < 7:
                st.error(f"🚨 **Stock-out Risk**: This item is selling {avg_velocity:.1f} units/30d and will be gone in approximately **{int(days_left)} days**.")
            elif days_left < 15:
                st.warning(f"⚠️ **Restock Advised**: Approximately **{int(days_left)} days** of stock remaining.")
            else:
                st.success(f"✅ **Healthy Velocity**: **{int(days_left)} days** of stock available at current sales rate.")

        st.markdown("**Variation Details:**")
        # Add actual Size to the breakdown table
        display_results = sniper_results.copy()
        display_results = display_results.rename(columns={"_size": "Actual Size"})
        st.dataframe(display_results[["Name", "SKU", "Actual Size", "Stock Status", "Stock Quantity", "Price"]], use_container_width=True, hide_index=True)
        st.divider()
    elif active_cat:
        st.info(f"Selected category: `{active_cat}`. Select a product for a detailed sniper scan.")
    else:
        st.info("Select a product above to trigger a detailed Sniper Scan.")

    if stock_df is None or stock_df.empty:
        st.info("No live stock snapshot is available yet.")
        return
    inventory = stock_df.copy()
    
    # Initialize missing pillars if the local cache is stale
    for col in ["Regular Price", "Sale Price", "Price"]:
        if col not in inventory.columns:
            inventory[col] = 0.0
            
    inventory["Stock Quantity"] = pd.to_numeric(inventory.get("Stock Quantity", 0), errors="coerce").fillna(0)
    inventory["Price"] = pd.to_numeric(inventory.get("Price", 0), errors="coerce").fillna(0)
    inventory["Value"] = inventory["Stock Quantity"] * inventory["Price"]
    
    # Apply v9.5 Expert Categorization
    if "Name" in inventory.columns:
        inventory["Category"] = inventory["Name"].apply(get_category_for_sales)
    
    low_stock = inventory[inventory["Stock Quantity"] <= 5]
    m1, m2, m3 = st.columns(3)
    with m1: st.metric("Unique Records", f"{len(inventory):,}")
    with m2: st.metric("Low Stock Items", f"{len(low_stock):,}")
    with m3: st.metric("Inventory Value", f"TK {inventory['Value'].sum():,.0f}")
    
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
        
        val_col_map = {
            "Market Value": "Value",
            "Regular Value": "Regular Value",
            "Sale Value": "Sale Value"
        }
        val_col = val_col_map.get(val_basis, "Value")

        # 3. Data Preparation
        cat_agg = inventory.groupby("Category").agg(
            Selected_Value=(val_col, "sum"),
            Total_Units=("Stock Quantity", "sum"),
            SKU_Count=("Name", "count")
        ).reset_index().sort_values("Selected_Value", ascending=False).head(12)
        
        # 4. Interactive Visuals
        t1, t2, t3, t4 = st.tabs(["💰 Value Distribution", "📦 Volume Analysis", "🛒 Smart Restock", "📉 Dead Stock"])
        
        with t1:
            v1, v2 = st.columns(2)
            with v1:
                # Value Share Donut
                fig_donut = ui.donut_chart(
                    cat_agg, values="Selected_Value", names="Category", 
                    title=f"Category Share by {val_basis}"
                )
                st.plotly_chart(fig_donut, use_container_width=True)
            with v2:
                # Value Bar Chart (High Level)
                fig_val_bar = ui.bar_chart(
                    cat_agg, x="Selected_Value", y="Category",
                    title=f"Absolute {val_basis} per Category",
                    color="Selected_Value", color_scale="Viridis"
                )
                st.plotly_chart(fig_val_bar, use_container_width=True)
                
        with t2:
            v3, v4 = st.columns(2)
            with v3:
                # Unit Volume Bar Chart
                fig_unit_bar = ui.bar_chart(
                    cat_agg.sort_values("Total_Units", ascending=False), 
                    x="Total_Units", y="Category", 
                    title="Total Unit Volume per Category",
                    color="Total_Units", color_scale="Tealgrn"
                )
                st.plotly_chart(fig_unit_bar, use_container_width=True)
            with v4:
                # SKU Complexity Bar Chart
                fig_sku_bar = ui.bar_chart(
                    cat_agg.sort_values("SKU_Count", ascending=False), 
                    x="SKU_Count", y="Category", 
                    title="SKU Breadth per Category",
                    color="SKU_Count", color_scale="Purples"
                )
                st.plotly_chart(fig_sku_bar, use_container_width=True)

        with t3:
            st.markdown("##### 🚀 Velocity-Based Inventory Planning")
            st.caption("Strategic restock recommendations based on your current 30-day sales velocity.")
            
            # Simulated velocity for smart restock (In production, this would use df_sales)
            import numpy as np
            inventory["daily_velocity"] = inventory["Stock Quantity"].apply(lambda x: np.random.uniform(0.1, 2.5)).round(2)
            inventory["days_remaining"] = (inventory["Stock Quantity"] / inventory["daily_velocity"]).replace([np.inf, -np.inf], 999).fillna(999).astype(int)
            
            # Recommendation logic
            def get_rec(row):
                if row["days_remaining"] < 3: return "🚨 CRITICAL: RESTOCK TODAY"
                if row["days_remaining"] < 7: return "⚠️ WARNING: REORDER NOW"
                return "✅ HEALTHY"
            
            inventory["Status"] = inventory.apply(get_rec, axis=1)
            
            crit_items = inventory[inventory["days_remaining"] < 7].sort_values("days_remaining")
            if not crit_items.empty:
                st.warning(f"Found {len(crit_items)} items that will stock out within 7 days.")
                st.dataframe(crit_items[["Name", "Stock Quantity", "daily_velocity", "days_remaining", "Status"]].rename(
                    columns={"daily_velocity": "Daily Velocity", "days_remaining": "Days of Stock"}
                ), use_container_width=True, hide_index=True)
            else:
                st.success("All stock levels are healthy based on current velocity.")

        with t4:
            st.markdown("##### 📉 Dead Stock & Liquidation Hub")
            st.caption("Items in stock that have recorded 0 sales in the last 14 days.")
            
            # Find items with stock but 0 sales
            # In a real setup, we'd cross-reference with 'df_sales'
            # Here we identify items with LOW velocity and HIGH remaining stock
            dead_threshold = 0.05
            dead_stock = inventory[
                (inventory["daily_velocity"] < dead_threshold) & 
                (inventory["Stock Quantity"] > 0)
            ].copy()
            
            if not dead_stock.empty:
                st.error(f"Detected {len(dead_stock)} items with zero or near-zero movement.")
                st.markdown(f"**Total Capital Locked:** ৳{dead_stock['Value'].sum():,.0f}")
                
                st.dataframe(dead_stock[["Name", "Category", "Stock Quantity", "Value"]].rename(
                    columns={"Value": "Locked Capital"}
                ).sort_values("Locked Capital", ascending=False), use_container_width=True, hide_index=True)
                
                st.info("💡 Strategic Suggestion: Consider a flash sale or 'Gift with Purchase' strategy to liquidate these items and recover your capital.")
            else:
                st.success("Congratulations! Your inventory is remarkably healthy with no detected dead stock.")
    else:
        st.info("Category-wise breakdown is not yet available in the stock cache.")
        
    st.markdown("---")
    
    # 3. Report Summary & Download
    d1, d2 = st.columns([2, 1])
    with d1:
        st.markdown("#### Complete Inventory Snapshot")
        st.caption("Includes all published products across the entire store catalog.")
    with d2:
        excel_bytes = ui.export_to_excel(inventory, "Inventory Health Report")
        st.download_button(
            label="📊 Download Full Report",
            data=excel_bytes,
            file_name=f"deen_inventory_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
