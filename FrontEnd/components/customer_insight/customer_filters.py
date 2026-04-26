"""Customer filters component - uses EXISTING working data.

Provides UI controls for filtering customers by:
- Product purchases (multi-select from existing data)
- Total purchase amount range
- Total order count range

Note: Date range is controlled globally from the sidebar.
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Callable

import streamlit as st
import pandas as pd

from BackEnd.core.logging_config import get_logger


logger = get_logger("customer_filters")


def _get_sales_data() -> pd.DataFrame:
    """Get the active sales data from session state with variant parsing.
    
    Returns:
        Sales DataFrame with _clean_name, _size, _color columns parsed.
    """
    if "dashboard_data" not in st.session_state:
        return pd.DataFrame()
    
    # Use sales_active (operational dataset) which has Category column
    sales_df = st.session_state.dashboard_data.get("sales_active", pd.DataFrame())
    if sales_df.empty:
        # Fallback to sales_exec or sales
        sales_df = st.session_state.dashboard_data.get("sales_exec", pd.DataFrame())
    if sales_df.empty:
        sales_df = st.session_state.dashboard_data.get("sales", pd.DataFrame())
    
    if sales_df.empty:
        return pd.DataFrame()
    
    # Parse variants if not already done
    if "_ci_variant_parsed" not in sales_df.columns:
        from BackEnd.core.categories import parse_sku_variants, get_clean_product_name
        parsed_variants = sales_df["item_name"].apply(parse_sku_variants).tolist()
        sales_df["_color"] = [p[0] for p in parsed_variants]
        sales_df["_size"] = [p[1] for p in parsed_variants]
        sales_df["_clean_name"] = sales_df["item_name"].apply(get_clean_product_name)
        sales_df["_ci_variant_parsed"] = True
    
    return sales_df


def render_customer_filters(
    on_filter_change: Optional[Callable[[Dict[str, Any]], None]] = None,
    key_prefix: str = "ci_filters",
) -> Dict[str, Any]:
    """Render customer filter controls with hierarchical Category → Product → Size filters.
    
    Args:
        on_filter_change: Optional callback when filters change
        key_prefix: Prefix for Streamlit session state keys
        
    Returns:
        Dictionary with filter values
    """
    from BackEnd.core.categories import get_master_category_list, format_category_label
    
    st.caption("All filters work together with AND logic. Adjust and click Apply.")
    st.info("📅 Date range is controlled from the global sidebar (Business Intelligence > Custom Date Range)")
    
    # Get enriched sales data
    sales_df = _get_sales_data()
    
    # Initialize session state defaults
    if f"{key_prefix}_min_amount" not in st.session_state:
        st.session_state[f"{key_prefix}_min_amount"] = 0
    if f"{key_prefix}_max_amount" not in st.session_state:
        st.session_state[f"{key_prefix}_max_amount"] = 1000000
    if f"{key_prefix}_amount_select" not in st.session_state:
        st.session_state[f"{key_prefix}_amount_select"] = "Any amount"
    if f"{key_prefix}_min_orders" not in st.session_state:
        st.session_state[f"{key_prefix}_min_orders"] = 1
    if f"{key_prefix}_max_orders" not in st.session_state:
        st.session_state[f"{key_prefix}_max_orders"] = 1000
    if f"{key_prefix}_orders_select" not in st.session_state:
        st.session_state[f"{key_prefix}_orders_select"] = "Any (1 or more)"
    if f"{key_prefix}_filter_mode" not in st.session_state:
        st.session_state[f"{key_prefix}_filter_mode"] = "Customer total within range"
    
    # ──────────────────────────────────────────────────────────────
    # ROW 1: Hierarchical Product Filters (Category → SKU → Size)
    # ──────────────────────────────────────────────────────────────
    st.markdown("**📦 Category & Product Filters**")
    
    active_cats = []
    active_items = []
    active_sizes = []
    
    if not sales_df.empty and "Category" in sales_df.columns:
        f_c1, f_c2, f_c3 = st.columns(3)
        
        with f_c1:
            # 1. Category - use master list for consistent hierarchy display
            cat_list = get_master_category_list()
            sel_cats = st.multiselect(
                "Categories", ["All"] + cat_list, default=["All"],
                format_func=format_category_label,
                key=f"{key_prefix}_cats"
            )
            active_cats = [] if "All" in sel_cats or not sel_cats else sel_cats
        
        with f_c2:
            # 2. Products (Name + SKU) — cascaded from Category
            if active_cats:
                mask = pd.Series(False, index=sales_df.index)
                for cat in active_cats:
                    mask |= sales_df["Category"].str.startswith(cat, na=False)
                sku_options = sales_df[mask].copy()
            else:
                sku_options = sales_df.copy()
            
            sku_options["_display_name"] = sku_options["_clean_name"] + " [" + sku_options["sku"].astype(str) + "]"
            avail_items = sorted([
                str(s) for s in sku_options["_display_name"].unique()
                if str(s).strip() and "Unknown" not in str(s)
            ])
            sel_items = st.multiselect(
                "Products (Name + SKU)", ["All"] + avail_items, default=["All"],
                key=f"{key_prefix}_items"
            )
            active_items = [] if "All" in sel_items or not sel_items else sel_items
        
        with f_c3:
            # 3. Size — cascaded from Products
            if active_items:
                size_pool = sku_options[sku_options["_display_name"].isin(active_items)]
            else:
                size_pool = sku_options
            
            avail_sizes = sorted([str(s) for s in size_pool["_size"].dropna().unique() if str(s).strip()])
            sel_sizes = st.multiselect(
                "Variants (Size)", ["All"] + avail_sizes, default=["All"],
                key=f"{key_prefix}_sizes"
            )
            active_sizes = [] if "All" in sel_sizes or not sel_sizes else sel_sizes
    else:
        st.info("ℹ️ Load sales data from dashboard to enable product filters.")
    
    # ──────────────────────────────────────────────────────────────
    # ROW 2: Filter Mode
    # ──────────────────────────────────────────────────────────────
    st.markdown("**🎯 Filter Mode**")
    st.caption("Choose how Amount and Order filters are applied")
    filter_mode = st.radio(
        "Filter mode",
        options=[
            "Customer total within range",
            "Customer has at least one order in range"
        ],
        index=0,
        key=f"{key_prefix}_filter_mode_radio",
        horizontal=True,
        label_visibility="collapsed",
    )
    st.session_state[f"{key_prefix}_filter_mode"] = filter_mode
    
    if filter_mode == "Customer total within range":
        st.caption("✅ Only customers whose TOTAL spending/orders are within the selected range")
    else:
        st.caption("✅ Customers who have AT LEAST ONE order within the selected range (may have others outside)")
    
    # ──────────────────────────────────────────────────────────────
    # ROW 3: Amount and Order Count (2 columns)
    # ──────────────────────────────────────────────────────────────
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**💰 Amount (৳)**")
        st.caption("Select minimum amount")
        
        amount_options = [
            "Any amount",
            "Up to ৳1000",
            "Up to ৳1500",
            "Up to ৳2000",
            "Up to ৳2500",
            "Up to ৳3000",
            "Up to ৳3500",
            "Up to ৳4000",
            "Up to ৳5000",
            "Up to ৳7000",
            "Up to ৳10000",
            "Custom range..."
        ]
        
        selected_amount_option = st.selectbox(
            "Amount range",
            options=amount_options,
            index=0,
            key=f"{key_prefix}_amount_select",
            label_visibility="collapsed",
        )
        
        if selected_amount_option == "Any amount":
            min_amount, max_amount = 0, 10000000
        elif selected_amount_option == "Up to ৳1000":
            min_amount, max_amount = 0, 1000
        elif selected_amount_option == "Up to ৳1500":
            min_amount, max_amount = 0, 1500
        elif selected_amount_option == "Up to ৳2000":
            min_amount, max_amount = 0, 2000
        elif selected_amount_option == "Up to ৳2500":
            min_amount, max_amount = 0, 2500
        elif selected_amount_option == "Up to ৳3000":
            min_amount, max_amount = 0, 3000
        elif selected_amount_option == "Up to ৳3500":
            min_amount, max_amount = 0, 3500
        elif selected_amount_option == "Up to ৳4000":
            min_amount, max_amount = 0, 4000
        elif selected_amount_option == "Up to ৳5000":
            min_amount, max_amount = 0, 5000
        elif selected_amount_option == "Up to ৳7000":
            min_amount, max_amount = 0, 7000
        elif selected_amount_option == "Up to ৳10000":
            min_amount, max_amount = 0, 10000
        else:  # Custom range
            c1, c2 = st.columns(2)
            with c1:
                min_amount = st.number_input(
                    "Min ৳", min_value=0, max_value=1000000,
                    value=st.session_state.get(f"{key_prefix}_min_amount", 0),
                    step=100, key=f"{key_prefix}_min_amount_custom",
                )
            with c2:
                max_amount = st.number_input(
                    "Max ৳", min_value=0, max_value=10000000,
                    value=st.session_state.get(f"{key_prefix}_max_amount", 1000000),
                    step=1000, key=f"{key_prefix}_max_amount_custom",
                )
        
        st.session_state[f"{key_prefix}_min_amount"] = min_amount
        st.session_state[f"{key_prefix}_max_amount"] = max_amount
    
    with col2:
        st.markdown("**📊 Orders**")
        st.caption("Select order count range")
        
        order_options = [
            "Any (1 or more)",
            "Exactly 1",
            "Exactly 2",
            "Exactly 3",
            "More than 3",
            "Custom range..."
        ]
        
        selected_order_option = st.selectbox(
            "Order count",
            options=order_options,
            index=0,
            key=f"{key_prefix}_orders_select",
            label_visibility="collapsed",
        )
        
        if selected_order_option == "Any (1 or more)":
            min_orders, max_orders = 1, 50
        elif selected_order_option == "Exactly 1":
            min_orders, max_orders = 1, 1
        elif selected_order_option == "Exactly 2":
            min_orders, max_orders = 2, 2
        elif selected_order_option == "Exactly 3":
            min_orders, max_orders = 3, 3
        elif selected_order_option == "More than 3":
            min_orders, max_orders = 4, 50
        else:  # Custom range
            c1, c2 = st.columns(2)
            with c1:
                min_orders = st.number_input(
                    "Min", min_value=1, max_value=1000,
                    value=st.session_state.get(f"{key_prefix}_min_orders", 1),
                    step=1, key=f"{key_prefix}_min_orders_custom",
                )
            with c2:
                max_orders = st.number_input(
                    "Max", min_value=1, max_value=10000,
                    value=st.session_state.get(f"{key_prefix}_max_orders", 1000),
                    step=10, key=f"{key_prefix}_max_orders_custom",
                )
        
        st.session_state[f"{key_prefix}_min_orders"] = min_orders
        st.session_state[f"{key_prefix}_max_orders"] = max_orders
    
    # ──────────────────────────────────────────────────────────────
    # ROW 4: Action Buttons
    # ──────────────────────────────────────────────────────────────
    st.markdown("---")
    btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 2])
    
    with btn_col1:
        apply_clicked = st.button(
            "✅ Apply Filters",
            type="primary",
            use_container_width=True,
            key=f"{key_prefix}_apply",
        )
    
    with btn_col2:
        reset_clicked = st.button(
            "🔄 Reset",
            use_container_width=True,
            key=f"{key_prefix}_reset",
        )
    
    with btn_col3:
        st.caption("📊 Uses existing dashboard data")
    
    if reset_clicked:
        keys_to_clear = [
            f"{key_prefix}_cats",
            f"{key_prefix}_items",
            f"{key_prefix}_sizes",
            f"{key_prefix}_products",
            f"{key_prefix}_min_amount",
            f"{key_prefix}_max_amount",
            f"{key_prefix}_amount_select",
            f"{key_prefix}_min_orders",
            f"{key_prefix}_max_orders",
            f"{key_prefix}_orders_select",
            f"{key_prefix}_filter_mode",
        ]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
    
    # Build filter result
    filters = {
        "active_cats": active_cats,
        "active_items": active_items,
        "active_sizes": active_sizes,
        "min_amount": st.session_state.get(f"{key_prefix}_min_amount", 0),
        "max_amount": st.session_state.get(f"{key_prefix}_max_amount", 1000000),
        "min_orders": st.session_state.get(f"{key_prefix}_min_orders", 1),
        "max_orders": st.session_state.get(f"{key_prefix}_max_orders", 1000),
        "filter_mode": st.session_state.get(f"{key_prefix}_filter_mode", "Customer total within range"),
        "applied": apply_clicked,
    }
    
    if on_filter_change and apply_clicked:
        on_filter_change(filters)
    
    return filters


def apply_customer_filters(
    orders_df: pd.DataFrame,
    filters: Dict[str, Any],
) -> pd.DataFrame:
    """Apply hierarchical filter criteria to orders DataFrame.
    
    Args:
        orders_df: DataFrame with orders data from session state
        filters: Filter criteria from render_customer_filters()
            Keys: active_cats, active_items, active_sizes,
                  min_amount, max_amount, min_orders, max_orders, filter_mode
        
    Returns:
        Filtered DataFrame with unique customers
    """
    if orders_df.empty:
        return pd.DataFrame()
    
    df = orders_df.copy()
    
    # ── 1. Apply Category / Product / Size cascade filters ──
    active_cats = filters.get("active_cats", [])
    active_items = filters.get("active_items", [])
    active_sizes = filters.get("active_sizes", [])
    
    if active_cats and "Category" in df.columns:
        mask = pd.Series(False, index=df.index)
        for cat in active_cats:
            mask |= df["Category"].str.startswith(cat, na=False)
        df = df[mask]
    
    if active_items:
        # Parse variants if not done
        if "_clean_name" not in df.columns:
            from BackEnd.core.categories import get_clean_product_name
            df["_clean_name"] = df["item_name"].apply(get_clean_product_name)
        df["_display_name"] = df["_clean_name"] + " [" + df["sku"].astype(str) + "]"
        df = df[df["_display_name"].isin(active_items)]
    
    if active_sizes:
        if "_size" not in df.columns:
            from BackEnd.core.categories import parse_sku_variants
            parsed_variants = df["item_name"].apply(parse_sku_variants).tolist()
            df["_color"] = [p[0] for p in parsed_variants]
            df["_size"] = [p[1] for p in parsed_variants]
        df = df[df["_size"].isin(active_sizes)]
    
    if df.empty:
        return pd.DataFrame()
    
    # ── 2. Apply filter mode logic ──
    filter_mode = filters.get("filter_mode", "Customer total within range")
    
    if filter_mode == "Customer has at least one order in range" and "order_total" in df.columns:
        # Get unique orders
        unique_orders = df.drop_duplicates(subset=["order_id"]).copy()
        
        min_amt = filters.get("min_amount", 0)
        max_amt = filters.get("max_amount", 10000000)
        
        if min_amt > 0:
            unique_orders = unique_orders[unique_orders["order_total"] >= min_amt]
        if max_amt < 10000000:
            unique_orders = unique_orders[unique_orders["order_total"] <= max_amt]
        
        qualifying_customers = unique_orders["customer_key"].unique()
        df = df[df["customer_key"].isin(qualifying_customers)]
        
        qualifying_order_ids = unique_orders["order_id"].unique()
        df_filtered = df[df["order_id"].isin(qualifying_order_ids)]
        
        from BackEnd.services.customer_insights import generate_customer_insights_from_sales
        customers_df = generate_customer_insights_from_sales(df_filtered, include_rfm=True)
        
        min_ord = filters.get("min_orders", 1)
        max_ord = filters.get("max_orders", 10000)
        if min_ord > 1:
            customers_df = customers_df[customers_df["total_orders"] >= min_ord]
        if max_ord < 10000:
            customers_df = customers_df[customers_df["total_orders"] <= max_ord]
        
        return customers_df
    
    # ── MODE 1: Customer total within range (default) ──
    from BackEnd.services.customer_insights import generate_customer_insights_from_sales
    customers_df = generate_customer_insights_from_sales(df, include_rfm=True)
    
    # Column normalization
    if "customer_id" in customers_df.columns and "customer_key" not in customers_df.columns:
        customers_df["customer_key"] = customers_df["customer_id"]
    if "primary_name" in customers_df.columns and "name" not in customers_df.columns:
        customers_df["name"] = customers_df["primary_name"]
    if "total_revenue" in customers_df.columns and "total_value" not in customers_df.columns:
        customers_df["total_value"] = customers_df["total_revenue"]
    
    # Amount filter
    if filters.get("min_amount", 0) > 0:
        customers_df = customers_df[customers_df["total_value"] >= filters["min_amount"]]
    if filters.get("max_amount") and filters["max_amount"] < 10000000:
        customers_df = customers_df[customers_df["total_value"] <= filters["max_amount"]]
    
    # Order count filter
    if filters.get("min_orders", 1) > 1:
        customers_df = customers_df[customers_df["total_orders"] >= filters["min_orders"]]
    if filters.get("max_orders") and filters["max_orders"] < 10000:
        customers_df = customers_df[customers_df["total_orders"] <= filters["max_orders"]]
    
    logger.info(f"Filtered to {len(customers_df)} customers from {len(orders_df)} orders")
    
    return customers_df


def get_filtered_customers_summary(filters: Dict[str, Any]) -> pd.DataFrame:
    """Get filtered customer summary using EXISTING session state data.
    
    Args:
        filters: Filter criteria dictionary
        
    Returns:
        DataFrame with filtered customer summaries
    """
    # Use EXISTING working data from session state
    if "dashboard_data" not in st.session_state:
        return pd.DataFrame()
    
    sales_df = st.session_state.dashboard_data.get("sales_active", pd.DataFrame())
    
    if sales_df.empty:
        return pd.DataFrame()
    
    return apply_customer_filters(sales_df, filters)
