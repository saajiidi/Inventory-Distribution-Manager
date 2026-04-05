"""Customer Insights Module - DuckDB-powered customer aggregation with RFM Analysis.

This module generates customer insights on-the-fly using DuckDB from the hybrid
data loading system. It implements the RFM Framework:
- Recency (R): Days since last order
- Frequency (F): Total number of orders
- Monetary (M): Total money spent

Additional metrics:
- Average Order Value (AOV)
- Purchase Cycle: Average days between orders
- Customer Lifetime Value (CLV)
- Favorite Product: Most purchased item
- RFM Score: Combined 1-5 score for each dimension
"""

import re
from datetime import datetime
from typing import Optional, Tuple

import duckdb
import pandas as pd
import streamlit as st

from BackEnd.services.hybrid_data_loader import load_hybrid_data


def normalize_name(name: str) -> str:
    """Normalize customer name: title case, strip extra spaces."""
    if pd.isna(name) or not name:
        return ""
    # Remove extra spaces, convert to title case
    name = str(name).strip()
    # Handle multiple spaces
    name = re.sub(r"\s+", " ", name)
    # Title case
    return name.title()


def clean_phone(phone: str) -> str:
    """Clean phone number: keep only digits, standardize format."""
    if pd.isna(phone) or not phone:
        return ""
    phone = str(phone).strip()
    # Keep only digits
    digits = re.sub(r"\D", "", phone)
    # If starts with 0, keep it; if starts with country code, keep it
    if len(digits) == 10 and digits.startswith("1"):
        digits = "0" + digits  # Bangladesh format
    return digits


def clean_email(email: str) -> str:
    """Clean email: lowercase, strip spaces."""
    if pd.isna(email) or not email:
        return ""
    return str(email).strip().lower()


def generate_customer_id(email: str, phone: str) -> str:
    """Generate unique customer ID based on email (primary) or phone (secondary).

    Priority:
    1. If email exists -> use email hash
    2. If phone exists -> use phone hash
    3. Otherwise -> generate anonymous ID
    """
    import hashlib

    clean_e = clean_email(email)
    clean_p = clean_phone(phone)

    if clean_e:
        # Email is primary identifier
        return hashlib.md5(clean_e.encode()).hexdigest()[:12]
    elif clean_p:
        # Phone is secondary identifier
        return hashlib.md5(clean_p.encode()).hexdigest()[:12]
    else:
        # No identifier available
        return "anon_" + hashlib.md5(str(pd.Timestamp.now()).encode()).hexdigest()[:8]


@st.cache_data(ttl=1800)  # Cache for 30 minutes
def generate_customer_insights(
    start_date: Optional[str] = None, end_date: Optional[str] = None,
    include_gsheet: bool = True, include_woocommerce: bool = True
) -> pd.DataFrame:
    """Generate customer insights table using DuckDB aggregation with RFM metrics.

    This function:
    1. Loads hybrid data (historical + live 2026)
    2. Normalizes names, phones, emails
    3. Generates unique customer IDs
    4. Aggregates all orders per customer with RFM metrics
    5. Calculates Recency, Frequency, Monetary scores (1-5 scale)
    6. Identifies favorite products and purchase cycles
    7. Segments customers into actionable buckets

    Args:
        start_date: Optional filter start date (YYYY-MM-DD)
        end_date: Optional filter end date (YYYY-MM-DD)
        include_gsheet: Whether to include Google Sheets data
        include_woocommerce: Whether to include WooCommerce data

    Returns:
        DataFrame with one row per unique customer including RFM metrics
    """
    # Load hybrid data
    df = load_hybrid_data(start_date, end_date, include_gsheet, include_woocommerce)

    if df.empty:
        return pd.DataFrame()

    # Find column mappings
    email_col = None
    phone_col = None
    name_col = None

    # Email column detection
    for col in ["Email", "email", "Customer Email", "E-mail", "e-mail"]:
        if col in df.columns:
            email_col = col
            break

    # Phone column detection
    for col in ["Phone", "phone", "Customer Phone", "Mobile", "mobile", "Contact"]:
        if col in df.columns:
            phone_col = col
            break

    # Name column detection
    for col in ["Customer Name", "customer_name", "Name", "name", "Full Name"]:
        if col in df.columns:
            name_col = col
            break

    # Generate customer IDs
    if email_col or phone_col:
        df["customer_id"] = df.apply(
            lambda row: generate_customer_id(
                row.get(email_col, "") if email_col else "",
                row.get(phone_col, "") if phone_col else "",
            ),
            axis=1,
        )
    else:
        # Fallback to order-based ID if no contact info
        order_col = "Order Number" if "Order Number" in df.columns else "order_id"
        df["customer_id"] = df[order_col].apply(lambda x: hash(str(x)) % 1000000)

    # Normalize data
    if name_col:
        df["normalized_name"] = df[name_col].apply(normalize_name)
    else:
        df["normalized_name"] = "Unknown"

    if email_col:
        df["clean_email"] = df[email_col].apply(clean_email)
    else:
        df["clean_email"] = ""

    if phone_col:
        df["clean_phone"] = df[phone_col].apply(clean_phone)
    else:
        df["clean_phone"] = ""

    # Use DuckDB for aggregation with RFM metrics
    con = duckdb.connect(database=":memory:")
    con.register("orders", df)

    # Aggregate customer data with RFM metrics
    query = """
        SELECT 
            customer_id,
            MAX(normalized_name) as primary_name,
            STRING_AGG(DISTINCT clean_email, ', ') as all_emails,
            STRING_AGG(DISTINCT clean_phone, ', ') as all_phones,
            COUNT(DISTINCT "Order Number") as total_orders,
            SUM(CAST("Order Total Amount" AS DOUBLE)) as total_revenue,
            MIN("Order Date") as first_order,
            MAX("Order Date") as last_order,
            AVG(CAST("Order Total Amount" AS DOUBLE)) as avg_order_value,
            -- RFM Metrics
            DATEDIFF('day', MAX(CAST("Order Date" AS DATE)), CURRENT_DATE) as recency_days,
            DATEDIFF('day', MIN(CAST("Order Date" AS DATE)), MAX(CAST("Order Date" AS DATE))) as customer_lifespan_days,
            -- Purchase Cycle (avg days between orders)
            CASE 
                WHEN COUNT(DISTINCT "Order Number") > 1 THEN 
                    ROUND(DATEDIFF('day', MIN(CAST("Order Date" AS DATE)), MAX(CAST("Order Date" AS DATE))) / (COUNT(DISTINCT "Order Number") - 1), 0)
                ELSE NULL
            END as purchase_cycle_days,
            -- Customer Lifetime Value (same as total_revenue for now)
            SUM(CAST("Order Total Amount" AS DOUBLE)) as clv
        FROM orders
        WHERE customer_id IS NOT NULL
        GROUP BY customer_id
        ORDER BY total_revenue DESC
    """

    try:
        result = con.execute(query).fetchdf()
    except Exception as e:
        # Fallback to pandas if DuckDB fails
        st.warning(f"DuckDB aggregation failed, using pandas: {e}")
        result = _pandas_rfm_aggregation(df)

    con.close()

    # Clean up: remove empty strings from aggregated fields
    result["all_emails"] = result["all_emails"].apply(
        lambda x: (
            ", ".join([e.strip() for e in str(x).split(",") if e.strip()]) if pd.notna(x) else ""
        )
    )
    result["all_phones"] = result["all_phones"].apply(
        lambda x: (
            ", ".join([p.strip() for p in str(x).split(",") if p.strip()]) if pd.notna(x) else ""
        )
    )

    # Calculate RFM Scores (1-5 scale) using quintiles
    result = calculate_rfm_scores(result)

    # Add segment classification
    result = classify_rfm_segments(result)

    # Get favorite products per customer
    favorite_products = get_favorite_products(df)
    if not favorite_products.empty:
        result = result.merge(favorite_products, on="customer_id", how="left")

    return result


def _pandas_rfm_aggregation(df: pd.DataFrame) -> pd.DataFrame:
    """Fallback pandas aggregation with RFM metrics when DuckDB fails."""
    # Ensure Order Date is datetime
    df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
    today = pd.Timestamp.now()

    agg_dict = {
        "normalized_name": "first",
        "clean_email": lambda x: ", ".join(x.dropna().unique()),
        "clean_phone": lambda x: ", ".join(x.dropna().unique()),
        "Order Number": "nunique",
        "Order Total Amount": "sum",
    }

    result = df.groupby("customer_id").agg(agg_dict).reset_index()
    result.columns = [
        "customer_id",
        "primary_name",
        "all_emails",
        "all_phones",
        "total_orders",
        "total_revenue",
    ]

    # Calculate order date metrics
    date_stats = df.groupby("customer_id").agg({"Order Date": ["min", "max"]}).reset_index()
    date_stats.columns = ["customer_id", "first_order", "last_order"]

    result = result.merge(date_stats, on="customer_id")

    # AOV - ensure numeric before division
    result["total_revenue"] = pd.to_numeric(result["total_revenue"], errors="coerce")
    result["total_orders"] = pd.to_numeric(result["total_orders"], errors="coerce")
    result["avg_order_value"] = result["total_revenue"] / result["total_orders"].replace(
        0, 1
    )  # Avoid divide by zero

    # RFM Metrics
    result["recency_days"] = (today - pd.to_datetime(result["last_order"])).dt.days
    result["customer_lifespan_days"] = (
        pd.to_datetime(result["last_order"]) - pd.to_datetime(result["first_order"])
    ).dt.days

    # Purchase cycle
    result["purchase_cycle_days"] = result.apply(
        lambda row: (
            row["customer_lifespan_days"] / (row["total_orders"] - 1)
            if row["total_orders"] > 1
            else None
        ),
        axis=1,
    )

    # CLV
    result["clv"] = result["total_revenue"]

    return result.sort_values("total_revenue", ascending=False)


def calculate_rfm_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate RFM scores (1-5 scale) for each customer.

    R (Recency): Lower days = Higher score (5 = bought recently)
    F (Frequency): Higher orders = Higher score (5 = frequent buyer)
    M (Monetary): Higher spend = Higher score (5 = big spender)

    Uses quintile-based scoring for even distribution.
    """
    result = df.copy()

    # Recency Score: Lower recency = Higher score (1-5)
    # Use qcut with duplicates='drop' to handle duplicate values
    try:
        result["r_score"] = pd.qcut(
            result["recency_days"], 5, labels=[5, 4, 3, 2, 1], duplicates="drop"
        )
    except ValueError:
        # If not enough unique values, use rank
        result["r_score"] = result["recency_days"].rank(pct=True)
        result["r_score"] = pd.cut(result["r_score"], 5, labels=[5, 4, 3, 2, 1])
    result["r_score"] = result["r_score"].astype(int)

    # Frequency Score: Higher frequency = Higher score (1-5)
    try:
        result["f_score"] = pd.qcut(
            result["total_orders"], 5, labels=[1, 2, 3, 4, 5], duplicates="drop"
        )
    except ValueError:
        result["f_score"] = result["total_orders"].rank(pct=True)
        result["f_score"] = pd.cut(result["f_score"], 5, labels=[1, 2, 3, 4, 5])
    result["f_score"] = result["f_score"].astype(int)

    # Monetary Score: Higher revenue = Higher score (1-5)
    try:
        result["m_score"] = pd.qcut(
            result["total_revenue"], 5, labels=[1, 2, 3, 4, 5], duplicates="drop"
        )
    except ValueError:
        result["m_score"] = result["total_revenue"].rank(pct=True)
        result["m_score"] = pd.cut(result["m_score"], 5, labels=[1, 2, 3, 4, 5])
    result["m_score"] = result["m_score"].astype(int)

    # Combined RFM Score (e.g., 555 = best, 111 = worst)
    result["rfm_score"] = (
        result["r_score"].astype(str)
        + result["f_score"].astype(str)
        + result["m_score"].astype(str)
    )

    # RFM Score average (for sorting/filtering)
    result["rfm_avg"] = (result["r_score"] + result["f_score"] + result["m_score"]) / 3

    return result


def get_favorite_products(df: pd.DataFrame) -> pd.DataFrame:
    """Get each customer's most frequently purchased product.

    Returns:
        DataFrame with customer_id and favorite_product columns
    """
    if "Item Name" not in df.columns and "item_name" not in df.columns:
        # No product data available
        return pd.DataFrame(columns=["customer_id", "favorite_product"])

    # Find the product column
    product_col = "Item Name" if "Item Name" in df.columns else "item_name"

    # Group by customer and product, count occurrences
    product_counts = df.groupby(["customer_id", product_col]).size().reset_index(name="count")

    # Get the most frequent product per customer
    favorite = product_counts.loc[product_counts.groupby("customer_id")["count"].idxmax()]
    favorite = favorite.rename(columns={product_col: "favorite_product"})

    return favorite[["customer_id", "favorite_product"]]


def classify_rfm_segments(df: pd.DataFrame) -> pd.DataFrame:
    """Classify customers into actionable segments based on RFM scores.

    Segments:
    - VIP: High RFM scores (R>=4, F>=4, M>=4)
    - New: Single order, recent (F=1, R<=2)
    - Potential Loyalist: Recent + decent frequency (R>=3, F>=2)
    - At Risk: Previously good but haven't bought in 60+ days (R>=4, F>=3 historically, now R>=3)
    - Churned: Haven't bought in 180+ days (R<=2)
    - Regular: Everyone else
    """
    result = df.copy()

    def get_segment(row):
        r, f, m = row["r_score"], row["f_score"], row["m_score"]
        recency = row["recency_days"]

        # Churned: Haven't bought in 180+ days (R=1)
        if recency > 180:
            return "💀 Churned"

        # VIP: Top tier in all dimensions (R>=4, F>=4, M>=4)
        if r >= 4 and f >= 4 and m >= 4:
            return "⭐ VIP"

        # New: First-time customer, bought recently (F=1, R<=2)
        if f == 1 and r <= 2:
            return "🆕 New"

        # At Risk: High value but haven't bought in 60-180 days (R=2-3, historically F>=3 or M>=4)
        if recency > 60 and (f >= 3 or m >= 4):
            return "⚠️ At Risk"

        # Potential Loyalist: Recent buyer with decent frequency (R>=3, F>=2)
        if r >= 3 and f >= 2:
            return "💰 Potential Loyalist"

        # Regular: Everyone else
        return "📦 Regular"

    result["segment"] = result.apply(get_segment, axis=1)

    return result


def get_customer_segments(df: pd.DataFrame) -> dict:
    """Segment customers based on RFM classification.

    Uses the pre-calculated 'segment' column from classify_rfm_segments().

    Returns:
        Dictionary with customer segments keyed by segment name
    """
    if df.empty or "segment" not in df.columns:
        return {}

    # Group by the RFM segment column
    segments = {}
    for segment_name in df["segment"].unique():
        segments[segment_name] = df[df["segment"] == segment_name].copy()

    # Sort by count (descending) for consistent display order
    return dict(sorted(segments.items(), key=lambda x: len(x[1]), reverse=True))


def search_customers(query: str, df: pd.DataFrame) -> pd.DataFrame:
    """Search customers by name, email, phone, segment, or RFM score.

    Args:
        query: Search string
        df: Customer insights DataFrame

    Returns:
        Filtered DataFrame matching search
    """
    if not query or df.empty:
        return df

    query = query.lower()

    # Check if searching for segment (e.g., "vip", "churned")
    segment_match = (
        df["segment"].str.lower().str.contains(query, na=False)
        if "segment" in df.columns
        else False
    )

    # Check if searching for RFM score (e.g., "555", "rfm:555")
    rfm_match = False
    if "rfm_score" in df.columns:
        if query.isdigit() and len(query) == 3:
            # Direct RFM score search
            rfm_match = df["rfm_score"] == query
        elif query.startswith("rfm:"):
            rfm_match = df["rfm_score"].str.contains(query.replace("rfm:", ""), na=False)

    # Basic text search on contact info
    text_match = (
        df["primary_name"].str.lower().str.contains(query, na=False)
        | df["all_emails"].str.lower().str.contains(query, na=False)
        | df["all_phones"].str.contains(query, na=False)
    )

    # Combine all match types
    if isinstance(segment_match, pd.Series):
        mask = text_match | segment_match | rfm_match
    else:
        mask = text_match | rfm_match

    return df[mask]


def get_segment_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Generate a summary table of all segments with key metrics.

    Returns:
        DataFrame with segment summary statistics
    """
    if df.empty or "segment" not in df.columns:
        return pd.DataFrame()

    summary = (
        df.groupby("segment")
        .agg(
            {
                "customer_id": "count",
                "total_revenue": ["sum", "mean"],
                "total_orders": "mean",
                "avg_order_value": "mean",
                "recency_days": "mean",
                "r_score": "mean",
                "f_score": "mean",
                "m_score": "mean",
            }
        )
        .reset_index()
    )

    # Flatten column names
    summary.columns = [
        "Segment",
        "Count",
        "Total Revenue",
        "Avg Revenue",
        "Avg Orders",
        "Avg AOV",
        "Avg Recency (days)",
        "Avg R Score",
        "Avg F Score",
        "Avg M Score",
    ]

    # Sort by total revenue descending
    summary = summary.sort_values("Total Revenue", ascending=False)

    return summary
