"""Customer selector component for choosing a customer from filtered list.

Provides UI for displaying and selecting customers with:
- Sortable data table display
- Radio button or clickable selection
- Unique identification by phone/email
"""

from __future__ import annotations

from typing import Optional, Callable, Dict, Any, List
from datetime import datetime

import streamlit as st
import pandas as pd

from BackEnd.utils.woocommerce_helpers import (
    format_currency,
    format_wc_date,
    calculate_customer_metrics,
    clean_phone,
    clean_email,
)
from BackEnd.core.logging_config import get_logger


logger = get_logger("customer_selector")


def render_customer_selector(
    customers_df: pd.DataFrame,
    on_select: Optional[Callable[[str], None]] = None,
    key_prefix: str = "ci_selector",
) -> Optional[str]:
    """Render customer selection table and handle selection.
    
    Args:
        customers_df: DataFrame with filtered customer data
        on_select: Optional callback when customer is selected
        key_prefix: Prefix for Streamlit session state keys
        
    Returns:
        Selected customer key or None if no selection
    """
    if customers_df.empty:
        st.info("📭 No customers match the current filters. Try adjusting your filter criteria.")
        return None
    
    st.markdown(f"### 👥 Matching Customers ({len(customers_df)})")
    
    # Prepare display data
    display_df = _prepare_customer_display(customers_df)
    
    # Sort options
    col1, col2 = st.columns([1, 3])
    with col1:
        sort_by = st.selectbox(
            "Sort by",
            options=[
                "Total Spent (High to Low)",
                "Total Spent (Low to High)",
                "Orders (High to Low)",
                "Orders (Low to High)",
                "Last Order (Recent First)",
                "Last Order (Oldest First)",
                "Name (A-Z)",
            ],
            key=f"{key_prefix}_sort",
        )
    
    with col2:
        # Search filter
        search_query = st.text_input(
            "🔍 Search by name, email, or phone",
            placeholder="Type to search...",
            key=f"{key_prefix}_search",
        )
    
    # Apply sorting
    display_df = _apply_sorting(display_df, sort_by)
    
    # Apply search filter
    if search_query:
        query = search_query.lower()
        mask = (
            display_df["Name"].str.lower().str.contains(query, na=False) |
            display_df["Email"].str.lower().str.contains(query, na=False) |
            display_df["Phone"].str.contains(query, na=False)
        )
        display_df = display_df[mask]
        
        if display_df.empty:
            st.warning("No customers match your search.")
            return None
    
    # Display customer table with selection
    st.caption("Click the radio button to select a customer for detailed report")
    
    # Create selection options
    selection_options = []
    for idx, row in display_df.iterrows():
        # Create label with key identifiers
        name = row["Name"] or "Unknown"
        phone = row["Phone"] or "No phone"
        email = row["Email"] or "No email"
        
        label = f"**{name}** | 📞 {phone} | 📧 {email}"
        selection_options.append((row["customer_key"], label))
    
    # Radio button selection
    selected_customer = st.radio(
        "Select a customer",
        options=[opt[0] for opt in selection_options],
        format_func=lambda x: next((opt[1] for opt in selection_options if opt[0] == x), x),
        key=f"{key_prefix}_radio",
        label_visibility="collapsed",
    )
    
    # Store selection in session state
    st.session_state[f"{key_prefix}_selected"] = selected_customer
    
    # Display detailed table below selection
    st.markdown("---")
    st.markdown("**Customer Details:**")
    
    # Format for display
    table_df = display_df[[
        "Name", "Email", "Phone", "Orders", "Total Spent",
        "Avg Order", "First Order", "Last Order"
    ]].copy()
    
    # Format currency columns
    for col in ["Total Spent", "Avg Order"]:
        table_df[col] = table_df[col].apply(format_currency)
    
    # Format dates
    for col in ["First Order", "Last Order"]:
        table_df[col] = table_df[col].apply(lambda x: format_wc_date(x, "%Y-%m-%d"))
    
    # Add row numbers
    table_df.insert(0, "#", range(1, len(table_df) + 1))
    
    # Display with highlighted selection
    st.dataframe(
        table_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "#": st.column_config.NumberColumn("#", width="small"),
            "Name": st.column_config.TextColumn("Name", width="medium"),
            "Email": st.column_config.TextColumn("Email", width="medium"),
            "Phone": st.column_config.TextColumn("Phone", width="medium"),
            "Orders": st.column_config.NumberColumn("Orders", width="small"),
            "Total Spent": st.column_config.TextColumn("Total Spent", width="medium"),
            "Avg Order": st.column_config.TextColumn("Avg Order", width="medium"),
            "First Order": st.column_config.DateColumn("First Order", width="medium"),
            "Last Order": st.column_config.DateColumn("Last Order", width="medium"),
        },
    )
    
    # Call callback if provided
    if on_select and selected_customer:
        on_select(selected_customer)
    
    return selected_customer


def _prepare_customer_display(customers_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare customer DataFrame for display.
    
    Args:
        customers_df: Raw customer data
        
    Returns:
        Formatted DataFrame for display
    """
    df = customers_df.copy()
    
    # Ensure required columns exist
    if "customer_key" not in df.columns:
        df["customer_key"] = df.index.astype(str)
    
    # Map columns if needed
    column_mapping = {
        "name": "Name",
        "unique_emails": "Email",
        "unique_phones": "Phone",
        "total_orders": "Orders",
        "total_value": "Total Spent",
        "avg_order_value": "Avg Order",
        "first_order_date": "First Order",
        "last_order_date": "Last Order",
    }
    
    # Rename columns that exist
    for old, new in column_mapping.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
    
    # Ensure all expected columns exist
    expected_cols = ["Name", "Email", "Phone", "Orders", "Total Spent", 
                     "Avg Order", "First Order", "Last Order", "customer_key"]
    
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""
    
    # Clean phone numbers
    df["Phone"] = df["Phone"].apply(lambda x: clean_phone(str(x)) if x else "")
    
    # Clean emails
    df["Email"] = df["Email"].apply(lambda x: clean_email(str(x)) if x else "")
    
    # Handle missing names
    df["Name"] = df["Name"].fillna("Unknown Customer")
    
    # Ensure numeric columns
    df["Orders"] = pd.to_numeric(df["Orders"], errors="coerce").fillna(0).astype(int)
    df["Total Spent"] = pd.to_numeric(df["Total Spent"], errors="coerce").fillna(0)
    df["Avg Order"] = pd.to_numeric(df["Avg Order"], errors="coerce").fillna(0)
    
    # Parse dates
    for col in ["First Order", "Last Order"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    
    return df


def _apply_sorting(df: pd.DataFrame, sort_by: str) -> pd.DataFrame:
    """Apply sorting to customer DataFrame.
    
    Args:
        df: Customer DataFrame
        sort_by: Sort option string
        
    Returns:
        Sorted DataFrame
    """
    ascending = False
    
    if "Low to High" in sort_by or "(A-Z)" in sort_by or "(Oldest First)" in sort_by:
        ascending = True
    
    sort_column_map = {
        "Total Spent": "Total Spent",
        "Orders": "Orders",
        "Last Order": "Last Order",
        "Name": "Name",
    }
    
    # Find the matching column
    sort_col = None
    for key, col in sort_column_map.items():
        if key in sort_by:
            sort_col = col
            break
    
    if sort_col and sort_col in df.columns:
        # Handle NaN values in sort
        df = df.sort_values(
            by=sort_col,
            ascending=ascending,
            na_position="last"
        )
    
    return df


def get_selected_customer_details(
    customer_key: str,
    customers_df: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    """Get full details for a selected customer.
    
    Args:
        customer_key: Customer unique key
        customers_df: Customer DataFrame
        
    Returns:
        Customer details dictionary or None
    """
    if not customer_key or customers_df.empty:
        return None
    
    match = customers_df[customers_df["customer_key"] == customer_key]
    
    if match.empty:
        return None
    
    return match.iloc[0].to_dict()
