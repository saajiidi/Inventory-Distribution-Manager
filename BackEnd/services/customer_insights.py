"""Customer insights service built on the normalized hybrid sales schema."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Optional

import pandas as pd
import streamlit as st

from BackEnd.services.hybrid_data_loader import load_hybrid_data
from BackEnd.utils.sales_schema import ensure_sales_schema
from FrontEnd.utils.error_handler import log_error



def normalize_name(name: str) -> str:
    if pd.isna(name) or not name:
        return ""
    name = re.sub(r"\s+", " ", str(name).strip())
    return name.title()



def clean_phone(phone: str) -> str:
    if pd.isna(phone) or not phone:
        return ""
    digits = re.sub(r"\D", "", str(phone).strip())
    if len(digits) == 10 and digits.startswith("1"):
        digits = "0" + digits
    return digits



def clean_email(email: str) -> str:
    if pd.isna(email) or not email:
        return ""
    return str(email).strip().lower()



def generate_customer_id(email: str, phone: str, order_id: str = "") -> str:
    clean_e = clean_email(email)
    clean_p = clean_phone(phone)
    seed = clean_e or clean_p or str(order_id) or "anonymous"
    prefix = "anon_" if not (clean_e or clean_p) else "cust_"
    return prefix + hashlib.md5(seed.encode()).hexdigest()[:12]


@st.cache_data(ttl=1800)
def generate_customer_insights(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_gsheet: bool = True,
    include_woocommerce: bool = True,
) -> pd.DataFrame:
    df = load_hybrid_data(start_date, end_date, include_gsheet, include_woocommerce)
    if df.empty:
        return pd.DataFrame()

    df = ensure_sales_schema(df)
    df = df.copy()
    df["normalized_name"] = df["customer_name"].apply(normalize_name)
    df["clean_email"] = df["email"].apply(clean_email)
    df["clean_phone"] = df["phone"].apply(clean_phone)
    df["customer_id"] = df.apply(
        lambda row: generate_customer_id(row.get("clean_email", ""), row.get("clean_phone", ""), row.get("order_id", "")),
        axis=1,
    )

    try:
        result = _aggregate_customer_metrics(df)
        result = calculate_rfm_scores(result)
        result = classify_rfm_segments(result)
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
    result["rfm_score"] = result["r_score"].astype(str) + result["f_score"].astype(str) + result["m_score"].astype(str)
    result["rfm_avg"] = (result["r_score"] + result["f_score"] + result["m_score"]) / 3
    return result



def get_favorite_products(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "item_name" not in df.columns:
        return pd.DataFrame(columns=["customer_id", "favorite_product"])
    counts = df.groupby(["customer_id", "item_name"]).size().reset_index(name="count")
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
