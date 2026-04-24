"""Customer insights service built on the normalized hybrid sales schema."""

from __future__ import annotations

import hashlib
import re
from typing import Optional

import pandas as pd
import streamlit as st

from BackEnd.services.hybrid_data_loader import load_full_woocommerce_history, load_hybrid_data
from BackEnd.utils.sales_schema import ensure_sales_schema
from BackEnd.utils.woocommerce_helpers import clean_phone, clean_email, normalize_name
from FrontEnd.utils.error_handler import log_error
from BackEnd.services.customer_manager import load_customer_mapping

CUSTOMER_BASE_COLUMNS = [
    "order_id",
    "order_date",
    "customer_name",
    "phone",
    "email",
    "item_name",
    "order_total",
    "customer_key",
]






def generate_customer_id(customer_key, email: str, phone: str, order_id: str = "") -> str:
    # 1. Registered Customer priority
    uid = str(customer_key).strip() if pd.notna(customer_key) else ""
    if uid and uid not in ["0", "0.0", "nan", "None"]:
        return f"reg_{uid}"
        
    # 2. Unregistered guests group by unique phone number first
    clean_p = clean_phone(phone)
    if clean_p:
        return f"guest_p_{clean_p}"
        
    # 3. Fallback to email or order_id
    clean_e = clean_email(email)
    if clean_e:
        return f"guest_e_{hashlib.md5(clean_e.encode()).hexdigest()[:8]}"
        
    return f"anon_{order_id}"


@st.cache_data(ttl=1800)
def generate_customer_insights(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_woocommerce: bool = True,
) -> pd.DataFrame:
    # Customer insights exclusively use WooCommerce order history.
    df = load_hybrid_data(start_date, end_date, include_woocommerce=include_woocommerce)
    full_history = load_full_woocommerce_history(end_date=end_date) if include_woocommerce else pd.DataFrame()
    return generate_customer_insights_from_sales(df, full_history_df=full_history)


def generate_customer_insights_from_sales(
    df: pd.DataFrame,
    full_history_df: pd.DataFrame | None = None,
    include_rfm: bool = True,
    include_favorites: bool = True,
) -> pd.DataFrame:
    if df.empty:
        res = pd.DataFrame(columns=[
            "customer_id", "primary_name", "all_emails", "all_phones", 
            "total_orders", "total_revenue", "avg_order_value", "first_order", 
            "last_order", "customer_lifespan_days", "recency_days", 
            "purchase_cycle_days", "clv", "segment", "r_score", "f_score", 
            "m_score", "rfm_score", "rfm_avg", "favorite_product"
        ])
        # Type enforcement for empty result
        res["first_order"] = pd.to_datetime(res["first_order"])
        res["last_order"] = pd.to_datetime(res["last_order"])
        res["total_revenue"] = pd.to_numeric(res["total_revenue"])
        res["total_orders"] = pd.to_numeric(res["total_orders"])
        res["recency_days"] = pd.to_numeric(res["recency_days"])
        return res

    df = _prepare_customer_identity(df)

    try:
        result = _aggregate_customer_metrics(df)
        current_window = result[
            [
                "customer_id",
                "total_orders",
                "total_revenue",
                "avg_order_value",
                "first_order",
                "last_order",
                "customer_lifespan_days",
                "recency_days",
                "purchase_cycle_days",
                "clv",
            ]
        ].copy()
        current_window = current_window.rename(
            columns={
                "total_orders": "current_orders",
                "total_revenue": "current_revenue",
                "avg_order_value": "current_avg_order_value",
                "first_order": "current_first_order",
                "last_order": "current_last_order",
                "customer_lifespan_days": "current_lifespan_days",
                "recency_days": "current_recency_days",
                "purchase_cycle_days": "current_purchase_cycle_days",
                "clv": "current_clv",
            }
        )

        if isinstance(full_history_df, pd.DataFrame) and not full_history_df.empty:
            history_prepared = _prepare_customer_identity(full_history_df)
            history_metrics = _aggregate_customer_metrics(history_prepared)
            if not history_metrics.empty:
                result = result.drop(
                    columns=[
                        "total_orders",
                        "total_revenue",
                        "avg_order_value",
                        "first_order",
                        "last_order",
                        "customer_lifespan_days",
                        "recency_days",
                        "purchase_cycle_days",
                        "clv",
                    ],
                    errors="ignore",
                ).merge(
                    history_metrics[
                        [
                            "customer_id",
                            "total_orders",
                            "total_revenue",
                            "avg_order_value",
                            "first_order",
                            "last_order",
                            "customer_lifespan_days",
                            "recency_days",
                            "purchase_cycle_days",
                            "clv",
                        ]
                    ],
                    on="customer_id",
                    how="left",
                )
        result = result.merge(current_window, on="customer_id", how="left")
        
        # --- RETURN RATE ENRICHMENT ---
        result = _enrich_with_returns(result, full_history_df)
        
        if include_rfm:
            result = calculate_rfm_scores(result)
            result = classify_rfm_segments(result)
        else:
            result["r_score"] = pd.NA
            result["f_score"] = pd.NA
            result["m_score"] = pd.NA
            result["rfm_score"] = ""
            result["rfm_avg"] = pd.NA
            result["segment"] = result.apply(_classify_without_rfm, axis=1)

        if include_favorites:
            favorite_products = get_favorite_products(df)
            if not favorite_products.empty:
                result = result.merge(favorite_products, on="customer_id", how="left")
        return result.sort_values("total_revenue", ascending=False).reset_index(drop=True)
    except Exception as exc:
        log_error(exc, context="Customer Insights Generation")
        return pd.DataFrame()



def _aggregate_customer_metrics(df: pd.DataFrame) -> pd.DataFrame:
    grouped = df.groupby("customer_id", dropna=False)
    result = grouped.agg(
        primary_name=("normalized_name", lambda s: next((v for v in s if str(v).strip()), "Unknown")),
        all_emails=("clean_email", lambda s: ", ".join(sorted({v for v in s if v}))),
        all_phones=("clean_phone", lambda s: ", ".join(sorted({v for v in s if v}))),
        total_orders=("order_id", lambda s: pd.Series(s).replace("", pd.NA).dropna().nunique()),
        total_revenue=("order_total", "sum"),
        first_order=("order_date", "min"),
        last_order=("order_date", "max"),
        avg_order_value=("order_total", "mean"),
        customer_lifespan_days=("order_date", lambda s: (s.max() - s.min()).days if s.notna().any() else 0),
    ).reset_index()

    today = pd.Timestamp.now().normalize()
    result["recency_days"] = (today - pd.to_datetime(result["last_order"], errors="coerce").dt.normalize()).dt.days.fillna(9999)
    result["purchase_cycle_days"] = result.apply(
        lambda row: round(row["customer_lifespan_days"] / (row["total_orders"] - 1), 0)
        if row["total_orders"] and row["total_orders"] > 1
        else pd.NA,
        axis=1,
    )
    result["clv"] = result["total_revenue"]
    return result


def _prepare_customer_identity(df: pd.DataFrame) -> pd.DataFrame:
    prepared = ensure_sales_schema(_select_customer_columns(df)).copy()
    prepared["normalized_name"] = prepared["customer_name"].apply(normalize_name)
    prepared["clean_email"] = prepared["email"].apply(clean_email)
    prepared["clean_phone"] = prepared["phone"].apply(clean_phone)
    
    # Try to use pre-computed mapping for stable customer IDs
    mapping_df = load_customer_mapping()
    if not mapping_df.empty:
        # Create lookup maps
        phone_to_id = {}
        email_to_id = {}
        for _, row in mapping_df.iterrows():
            cid = row["customer_id"]
            for p in str(row["phones"]).split(", "):
                if p: phone_to_id[p] = cid
            for e in str(row["emails"]).split(", "):
                if e: email_to_id[e] = cid
        
        def get_mapped_id(row):
            p = row["clean_phone"]
            e = row["clean_email"]
            return phone_to_id.get(p) or email_to_id.get(e) or generate_customer_id(
                row.get("customer_key"), e, p, row.get("order_id", "")
            )
            
        prepared["customer_id"] = prepared.apply(get_mapped_id, axis=1)
    else:
        prepared["customer_id"] = prepared.apply(
            lambda row: generate_customer_id(
                row.get("customer_key"),
                row.get("clean_email", ""), 
                row.get("clean_phone", ""), 
                row.get("order_id", "")
            ),
            axis=1,
        )
    return prepared


def _select_customer_columns(df: pd.DataFrame) -> pd.DataFrame:
    sales = ensure_sales_schema(df)
    available = [col for col in CUSTOMER_BASE_COLUMNS if col in sales.columns]
    if not available:
        return sales
    return sales[available].copy()


def _build_customer_first_order_history(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["customer_id", "first_order"])
    prepared = _prepare_customer_identity(df)
    if prepared.empty:
        res = pd.DataFrame(columns=["customer_id", "first_order"])
        res["first_order"] = pd.to_datetime(res["first_order"])
        return res
    history = (
        prepared.groupby("customer_id", dropna=False)
        .agg(first_order=("order_date", "min"))
        .reset_index()
    )
    return history



def calculate_rfm_scores(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    def _score(series: pd.Series, labels: list[int], ascending: bool) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce").fillna(series.max() if ascending else series.min())
        ranked = numeric.rank(method="average", pct=True, ascending=ascending)
        bins = pd.cut(ranked, bins=5, labels=labels, include_lowest=True)
        return bins.astype(int)

    result["r_score"] = _score(result["recency_days"], [5, 4, 3, 2, 1], ascending=False)
    result["f_score"] = _score(result["total_orders"], [1, 2, 3, 4, 5], ascending=True)
    result["m_score"] = _score(result["total_revenue"], [1, 2, 3, 4, 5], ascending=True)
    
    # Reliability Score based on return rate (lower return rate is better)
    if "return_rate" in result.columns:
        result["rel_score"] = _score(result["return_rate"], [5, 4, 3, 2, 1], ascending=False)
    else:
        result["rel_score"] = 5
        
    result["rfm_score"] = result["r_score"].astype(str) + result["f_score"].astype(str) + result["m_score"].astype(str)
    result["rfm_avg"] = (result["r_score"] + result["f_score"] + result["m_score"] + result["rel_score"]) / 4
    return result


def _enrich_with_returns(customers_df: pd.DataFrame, full_history_df: pd.DataFrame) -> pd.DataFrame:
    """Enrich customers DataFrame with return rate metrics."""
    returns_df = st.session_state.get("returns_data", pd.DataFrame())
    if returns_df is None or returns_df.empty or full_history_df is None or full_history_df.empty:
        customers_df["return_count"] = 0
        customers_df["return_rate"] = 0.0
        return customers_df
    
    try:
        # 1. Map return orders to customer_id using full_history_df
        # We need a mapping of order_id -> customer_id from ALL historical data
        id_map = _prepare_customer_identity(full_history_df)[["order_id", "customer_id"]].drop_duplicates()
        
        # 2. Extract order_id from returns
        ret_df = returns_df.copy()
        if "order_id" not in ret_df.columns:
            customers_df["return_count"] = 0
            customers_df["return_rate"] = 0.0
            return customers_df
            
        # 3. Merge returns with id_map to assign a customer_id to each return
        returns_mapped = ret_df.merge(id_map, on="order_id", how="inner")
        
        if returns_mapped.empty:
            customers_df["return_count"] = 0
            customers_df["return_rate"] = 0.0
            return customers_df
            
        # 4. Count returns per customer
        return_counts = returns_mapped.groupby("customer_id").size().reset_index(name="return_count")
        
        # 5. Merge with our target customers_df
        customers_df = customers_df.merge(return_counts, on="customer_id", how="left")
        customers_df["return_count"] = customers_df["return_count"].fillna(0).astype(int)
        
        # 6. Calculate return rate
        customers_df["return_rate"] = (customers_df["return_count"] / customers_df["total_orders"].replace(0, pd.NA)).fillna(0)
        
    except Exception as e:
        log_error(e, context="Return Rate Enrichment")
        customers_df["return_count"] = 0
        customers_df["return_rate"] = 0.0
        
    return customers_df



def get_favorite_products(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "item_name" not in df.columns:
        return pd.DataFrame(columns=["customer_id", "favorite_product"])
    counts = df.groupby(["customer_id", "item_name"]).size().reset_index(name="count")
    if counts.empty:
        return pd.DataFrame(columns=["customer_id", "favorite_product"])
    favorite = counts.loc[counts.groupby("customer_id")["count"].idxmax()].copy()
    favorite = favorite.rename(columns={"item_name": "favorite_product"})
    return favorite[["customer_id", "favorite_product"]]



def classify_rfm_segments(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    def get_segment(row: pd.Series) -> str:
        r, f, m, recency = row["r_score"], row["f_score"], row["m_score"], row["recency_days"]
        if recency > 180:
            return "Churned"
        if r >= 4 and f >= 4 and m >= 4:
            return "VIP"
        if f <= 2 and r >= 4:
            return "New"
        if recency > 60 and (f >= 3 or m >= 4):
            return "At Risk"
        if r >= 3 and f >= 2:
            return "Potential Loyalist"
        return "Regular"

    result["segment"] = result.apply(get_segment, axis=1)
    return result


def _classify_without_rfm(row: pd.Series) -> str:
    recency = pd.to_numeric(row.get("recency_days"), errors="coerce")
    total_orders = pd.to_numeric(row.get("total_orders"), errors="coerce")
    total_revenue = pd.to_numeric(row.get("total_revenue"), errors="coerce")

    if pd.notna(recency) and recency > 180:
        return "Churned"
    if pd.notna(total_orders) and total_orders <= 1:
        return "New"
    if pd.notna(recency) and recency > 60:
        return "At Risk"
    if pd.notna(total_orders) and total_orders >= 5 and pd.notna(total_revenue) and total_revenue > 0:
        return "VIP"
    if pd.notna(total_orders) and total_orders >= 2:
        return "Potential Loyalist"
    return "Regular"



def get_customer_segments(df: pd.DataFrame) -> dict:
    if df.empty or "segment" not in df.columns:
        return {}
    grouped = {segment_name: df[df["segment"] == segment_name].copy() for segment_name in df["segment"].dropna().unique()}
    return dict(sorted(grouped.items(), key=lambda item: len(item[1]), reverse=True))



def search_customers(query: str, df: pd.DataFrame) -> pd.DataFrame:
    if not query or df.empty:
        return df
    query = query.lower().strip()

    text_match = (
        df["primary_name"].astype(str).str.lower().str.contains(query, na=False)
        | df["all_emails"].astype(str).str.lower().str.contains(query, na=False)
        | df["all_phones"].astype(str).str.contains(query, na=False)
    )
    segment_match = df["segment"].astype(str).str.lower().str.contains(query, na=False) if "segment" in df.columns else False
    rfm_match = df["rfm_score"].astype(str).str.contains(query.replace("rfm:", ""), na=False) if "rfm_score" in df.columns and (query.startswith("rfm:") or (query.isdigit() and len(query) == 3)) else False

    if isinstance(segment_match, pd.Series):
        return df[text_match | segment_match | rfm_match]
    return df[text_match | rfm_match]



def get_segment_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "segment" not in df.columns:
        return pd.DataFrame()
    summary = (
        df.groupby("segment")
        .agg(
            Count=("customer_id", "count"),
            Total_Revenue=("total_revenue", "sum"),
            Avg_Revenue=("total_revenue", "mean"),
            Avg_Orders=("total_orders", "mean"),
            Avg_AOV=("avg_order_value", "mean"),
            Avg_Recency_days=("recency_days", "mean"),
            Avg_R_Score=("r_score", "mean"),
            Avg_F_Score=("f_score", "mean"),
            Avg_M_Score=("m_score", "mean"),
        )
        .reset_index()
    )
    return summary.rename(
        columns={
            "segment": "Segment",
            "Total_Revenue": "Total Revenue",
            "Avg_Revenue": "Avg Revenue",
            "Avg_Orders": "Avg Orders",
            "Avg_AOV": "Avg AOV",
            "Avg_Recency_days": "Avg Recency (days)",
            "Avg_R_Score": "Avg R Score",
            "Avg_F_Score": "Avg F Score",
            "Avg_M_Score": "Avg M Score",
        }
    ).sort_values("Total Revenue", ascending=False)


def generate_cohort_matrix(df: pd.DataFrame, period: str = 'M') -> pd.DataFrame:
    """
    Generates a retention cohort matrix.
    period: 'M' for monthly, 'W' for weekly.
    """
    if df.empty or 'order_date' not in df.columns:
        return pd.DataFrame()
        
    df = df.copy()
    df['order_date'] = pd.to_datetime(df['order_date'])
    # Need a consistent primary key for grouping
    if 'customer_id' not in df.columns:
        # Fallback to customer_key or hashing phone
        df['customer_id'] = df.apply(lambda row: row.get('customer_key', row.get('phone', 'anon')), axis=1)

    # Ensure order_date is datetimelike before using .dt
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    
    # Calculate cohort using transform, then ensure result is datetimelike
    min_dates = df.groupby('customer_id')['order_date'].transform('min')
    df['cohort'] = pd.to_datetime(min_dates, errors='coerce').dt.to_period(period)
    df['order_period'] = df['order_date'].dt.to_period(period)
    
    cohort_group = df.groupby(['cohort', 'order_period']).agg(n_customers=('customer_id', 'nunique')).reset_index()
    
    # Calculate period offset (Month 0, Month 1, etc.)
    cohort_group['period_number'] = (cohort_group.order_period.view(dtype='int64') - cohort_group.cohort.view(dtype='int64'))
    
    cohort_pivot = cohort_group.pivot_table(index='cohort', columns='period_number', values='n_customers')
    
    # Calculate percentage
    cohort_size = cohort_pivot.iloc[:, 0]
    retention_matrix = cohort_pivot.divide(cohort_size, axis=0) * 100
    
    return retention_matrix
