import pandas as pd
import streamlit as st
import plotly.express as px
import numpy as np
from datetime import datetime
from FrontEnd.components import ui
from BackEnd.core.categories import get_category_for_sales, parse_sku_variants, get_clean_product_name, get_master_category_list, format_category_label

def render_inventory_health(stock_df: pd.DataFrame, forecast_df: pd.DataFrame, df_sales: pd.DataFrame = None):
    st.subheader("Stock Insight")
    
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
    inventory[["_color", "_size"]] = inventory["Name"].apply(lambda x: pd.Series(parse_sku_variants(x)))
    
    if "Category" not in inventory.columns:
        inventory["Category"] = inventory["Name"].apply(get_category_for_sales)
        
    # --- Real Velocity & Trend Calculation ---
    if df_sales is not None and not df_sales.empty:
        # Calculate days in selected window
        days_active = (df_sales["order_date"].max() - df_sales["order_date"].min()).days or 1
        
        # Group sales by SKU
        sku_sales = df_sales.groupby("sku")["qty"].sum().reset_index()
        sku_sales["daily_velocity"] = (sku_sales["qty"] / days_active).round(3)
        
        # Merge velocity into inventory
        inventory = inventory.merge(sku_sales[["sku", "daily_velocity"]], left_on="SKU", right_on="sku", how="left").drop(columns=["sku"])
        inventory["daily_velocity"] = inventory["daily_velocity"].fillna(0)
    else:
        inventory["daily_velocity"] = 0.0

    def classify_stock_trend(rate):
        if rate > 3.0: return "🔥 Fast Moving"
        if rate > 0.8: return "⚖️ Regular"
        if rate > 0.01: return "🐌 Slow Moving"
        return "❄️ Non-Moving"

    inventory["Trend"] = inventory["daily_velocity"].apply(classify_stock_trend)

    # Summary Metrics
    low_stock = inventory[inventory["Stock Quantity"] <= 5]
    m1, m2, m3 = st.columns(3)
    with m1: st.metric("Unique SKU Records", f"{len(inventory):,}")
    with m2: st.metric("Low Stock Alerts", f"{len(low_stock):,}")
    with m3: st.metric("Inventory Asset Value", f"৳{inventory['Value'].sum():,.0f}")
    
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
        
        # Velocity and Trend display
        item_trend = sniper_results["Trend"].iloc[0]
        item_velocity = sniper_results["daily_velocity"].iloc[0]
        
        k1, k2, k3 = st.columns(3)
        k1.metric("Current Stock Balance", f"{int(sniper_results['Stock Quantity'].sum())}")
        k2.metric("Sales Velocity", f"{item_velocity:.2f} units/day")
        k3.metric("Velocity Tier", item_trend)
        
        # Stock-out Countdown
        if item_velocity > 0:
            days_left = sniper_results['Stock Quantity'].sum() / item_velocity
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
                # Top-to-less Sort (Already sorted by Selected_Value)
                fig_donut = ui.donut_chart(cat_agg, values="Selected_Value", names="Category", title=f"Category Share by {val_basis}")
                st.plotly_chart(fig_donut, width="stretch")
            with v2:
                # Horizontal bar: ascending in code = largest at top in visual
                fig_val_bar = ui.bar_chart(cat_agg.sort_values("Selected_Value", ascending=True), x="Selected_Value", y="Category", title=f"Absolute {val_basis} per Category", color="Selected_Value")
                st.plotly_chart(fig_val_bar, width="stretch")
                
        with t2:
            v3, v4 = st.columns(2)
            with v3:
                # Vertical bar: descending
                fig_unit_bar = ui.bar_chart(cat_agg.sort_values("Total_Units", ascending=True), x="Total_Units", y="Category", title="Total Unit Volume per Category", color="Total_Units")
                st.plotly_chart(fig_unit_bar, width="stretch")
            with v4:
                # Vertical bar: descending
                fig_sku_bar = ui.bar_chart(cat_agg.sort_values("SKU_Count", ascending=True), x="SKU_Count", y="Category", title="SKU Breadth per Category", color="SKU_Count")
                st.plotly_chart(fig_sku_bar, width="stretch")

        with t3:
            st.markdown("##### 🚀 Velocity-Based Inventory Planning")
            st.caption("Strategic restock recommendations based on real sales velocity for the selected range.")
            
            # Use real daily_velocity calculated at the top
            inventory["days_remaining"] = (inventory["Stock Quantity"] / inventory["daily_velocity"]).replace([np.inf, -np.inf], 999).fillna(999).astype(int)
            
            # Recommendation logic
            def get_rec(row):
                if row["days_remaining"] < 3 and row["daily_velocity"] > 0: return "🚨 CRITICAL: RESTOCK TODAY"
                if row["days_remaining"] < 7 and row["daily_velocity"] > 0: return "⚠️ WARNING: REORDER NOW"
                return "✅ HEALTHY"
            
            inventory["Status"] = inventory.apply(get_rec, axis=1)
            
            crit_items = inventory[(inventory["days_remaining"] < 7) & (inventory["daily_velocity"] > 0)].sort_values("days_remaining")
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
            dead_stock = inventory[
                (inventory["daily_velocity"] < 0.05) & 
                (inventory["Stock Quantity"] > 0)
            ].copy()
            
            if not dead_stock.empty:
                st.error(f"Detected {len(dead_stock)} items with stagnant movement.")
                st.markdown(f"**Asset Value at Risk:** ৳{dead_stock['Value'].sum():,.0f}")
                
                st.dataframe(dead_stock[["Name", "Category", "Stock Quantity", "Value", "Trend"]].rename(
                    columns={"Value": "Capital Locked"}
                ).sort_values("Capital Locked", ascending=False), width="stretch", hide_index=True)
            else:
                st.success("No significant dead stock detected for this period.")
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
            width="stretch",
            key="inv_custom_export_btn"
        )
