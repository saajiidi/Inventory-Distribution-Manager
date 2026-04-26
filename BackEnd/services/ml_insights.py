"""Baseline ML-style intelligence for commerce operations.

This module avoids heavy dependencies and provides:
- demand forecasting for products
- customer risk / recommendation scoring
- anomaly detection for revenue, orders, and AOV

Artifacts are saved under data/models/ for reuse and inspection.
"""

from __future__ import annotations

import json
from datetime import datetime
from math import ceil

import pandas as pd

from BackEnd.core.paths import DATA_DIR
from BackEnd.utils.sales_schema import ensure_sales_schema

MODELS_DIR = DATA_DIR / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)


def generate_demand_forecast(
    df_sales: pd.DataFrame,
    horizon_days: int = 7,
    top_n: int = 12,
) -> pd.DataFrame:
    """Forecast next-period demand using recent weighted daily averages."""
    df = ensure_sales_schema(df_sales)
    df = df[df["order_date"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    df["order_day"] = df["order_date"].dt.normalize()
    product_daily = (
        df.groupby(["item_name", "order_day"], as_index=False)
        .agg(units=("qty", "sum"), revenue=("order_total", "sum"))
    )
    product_daily = product_daily[product_daily["item_name"].astype(str).str.strip() != ""]
    if product_daily.empty:
        return pd.DataFrame()

    dataset_min_date = df["order_day"].min()
    latest_day = product_daily["order_day"].max()
    last_7_cutoff = latest_day - pd.Timedelta(days=6)
    last_28_cutoff = latest_day - pd.Timedelta(days=27)

    available_days_7 = min(7, max(1, (latest_day - max(last_7_cutoff, dataset_min_date)).days + 1))
    available_days_28 = min(28, max(1, (latest_day - max(last_28_cutoff, dataset_min_date)).days + 1))

    rows: list[dict] = []
    for item_name, group in product_daily.groupby("item_name"):
        last_7 = group[group["order_day"] >= last_7_cutoff]
        last_28 = group[group["order_day"] >= last_28_cutoff]

        recent_7_daily = float(last_7["units"].sum() / max(available_days_7, 1))
        recent_28_daily = float(last_28["units"].sum() / max(available_days_28, 1))
        revenue_28_daily = float(last_28["revenue"].sum() / max(available_days_28, 1))
        weighted_daily = (recent_7_daily * 0.65) + (recent_28_daily * 0.35)
        trend_ratio = (recent_7_daily / recent_28_daily) if recent_28_daily > 0 else (1.5 if recent_7_daily > 0 else 1.0)
        forecast_units = max(weighted_daily * horizon_days, 0.0)
        suggested_buffer_units = ceil(forecast_units * 1.25)

        if trend_ratio >= 1.2:
            risk_level = "High Demand"
        elif trend_ratio <= 0.8:
            risk_level = "Cooling"
        else:
            risk_level = "Steady"

        rows.append(
            {
                "item_name": item_name,
                "recent_7d_daily_units": round(recent_7_daily, 2),
                "recent_28d_daily_units": round(recent_28_daily, 2),
                "forecast_daily_units": round(weighted_daily, 2),
                f"forecast_{horizon_days}d_units": round(forecast_units, 2),
                "trend_ratio": round(trend_ratio, 2),
                "avg_daily_revenue_28d": round(revenue_28_daily, 2),
                "suggested_buffer_units": suggested_buffer_units,
                "risk_level": risk_level,
                "reorder_comment": _build_reorder_comment(weighted_daily, trend_ratio, suggested_buffer_units),
            }
        )

    forecast_df = pd.DataFrame(rows).sort_values(
        [f"forecast_{horizon_days}d_units", "avg_daily_revenue_28d"],
        ascending=False,
    )
    forecast_df = forecast_df.head(top_n).reset_index(drop=True)
    _write_artifact("demand_forecast_latest.json", forecast_df.to_dict(orient="records"))
    return forecast_df


def score_customer_risk(df_customers: pd.DataFrame) -> pd.DataFrame:
    """Create churn and follow-up recommendations from customer metrics."""
    if df_customers is None or df_customers.empty:
        return pd.DataFrame()

    df = df_customers.copy()
    df["purchase_cycle_days"] = pd.to_numeric(df.get("purchase_cycle_days"), errors="coerce")
    df["recency_days"] = pd.to_numeric(df.get("recency_days"), errors="coerce").fillna(9999)
    df["avg_order_value"] = pd.to_numeric(df.get("avg_order_value"), errors="coerce").fillna(0)
    df["total_orders"] = pd.to_numeric(df.get("total_orders"), errors="coerce").fillna(0)
    df["total_revenue"] = pd.to_numeric(df.get("total_revenue"), errors="coerce").fillna(0)

    expected_window = df["purchase_cycle_days"].fillna(df["recency_days"].median()).clip(lower=7, upper=120)
    overdue_ratio = (df["recency_days"] / expected_window).clip(lower=0, upper=5)
    frequency_bonus = (df["total_orders"] / max(df["total_orders"].max(), 1)).clip(lower=0, upper=1)
    value_bonus = (df["total_revenue"] / max(df["total_revenue"].max(), 1)).clip(lower=0, upper=1)

    risk_score = ((overdue_ratio * 55) + ((1 - frequency_bonus) * 20) + ((1 - value_bonus) * 25)).clip(0, 100)
    df["risk_score"] = risk_score.round(1)
    df["risk_band"] = df["risk_score"].apply(
        lambda x: "High" if x >= 70 else ("Medium" if x >= 40 else "Low")
    )
    df["next_purchase_window_days"] = expected_window.round(0).astype("Int64")
    df["recommended_action"] = df.apply(_recommend_customer_action, axis=1)

    scored = df.sort_values(["risk_score", "total_revenue"], ascending=[False, False]).reset_index(drop=True)
    _write_artifact(
        "customer_risk_latest.json",
        scored[
            ["customer_id", "primary_name", "segment", "risk_score", "risk_band", "next_purchase_window_days", "recommended_action"]
        ].to_dict(orient="records"),
    )
    return scored


def detect_sales_anomalies(df_sales: pd.DataFrame, window: int = 7, z_threshold: float = 1.5) -> pd.DataFrame:
    """Detect unusual swings in key daily metrics using rolling z-score."""
    df = ensure_sales_schema(df_sales)
    df = df[df["order_date"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    df["order_day"] = df["order_date"].dt.normalize()
    daily = (
        df.groupby("order_day", as_index=False)
        .agg(revenue=("order_total", "sum"), orders=("order_id", "nunique"), items=("qty", "sum"))
        .sort_values("order_day")
    )
    daily["aov"] = daily["revenue"] / daily["orders"].replace(0, pd.NA)

    anomalies: list[dict] = []
    for metric in ["revenue", "orders", "aov"]:
        baseline = daily[metric].shift(1)
        rolling_mean = baseline.rolling(window=window, min_periods=max(3, window // 2)).mean()
        rolling_std = baseline.rolling(window=window, min_periods=max(3, window // 2)).std().replace(0, pd.NA)
        z_score = (daily[metric] - rolling_mean) / rolling_std

        flagged = daily.loc[z_score.abs() >= z_threshold, ["order_day", metric]].copy()
        if flagged.empty:
            continue
        flagged["metric"] = metric
        flagged["z_score"] = z_score[z_score.abs() >= z_threshold].round(2).values
        flagged["direction"] = flagged["z_score"].apply(lambda x: "Spike" if x > 0 else "Drop")
        flagged["commentary"] = flagged.apply(
            lambda row: f"{row['direction']} detected in {metric} on {row['order_day'].date()} (z={row['z_score']}).",
            axis=1,
        )
        anomalies.extend(flagged.to_dict(orient="records"))

    anomaly_df = pd.DataFrame(anomalies).sort_values("order_day", ascending=False).reset_index(drop=True) if anomalies else pd.DataFrame()
    _write_artifact("sales_anomalies_latest.json", anomaly_df.to_dict(orient="records"))
    return anomaly_df


def build_ml_insight_bundle(
    df_sales: pd.DataFrame,
    df_customers: pd.DataFrame,
    horizon_days: int = 7,
) -> dict[str, pd.DataFrame]:
    forecast = generate_demand_forecast(df_sales, horizon_days=horizon_days)
    customer_risk = score_customer_risk(df_customers)
    anomalies = detect_sales_anomalies(df_sales)
    return {
        "forecast": forecast,
        "customer_risk": customer_risk,
        "anomalies": anomalies,
    }


def _build_reorder_comment(weighted_daily: float, trend_ratio: float, suggested_buffer_units: int) -> str:
    if weighted_daily <= 0:
        return "No recent demand signal. Keep on watch rather than reordering immediately."
    if trend_ratio >= 1.2:
        return f"Demand is accelerating. Hold at least {suggested_buffer_units} buffer units for the next cycle."
    if trend_ratio <= 0.8:
        return "Demand is cooling. Refill conservatively and avoid overstock."
    return f"Demand is stable. A buffer of around {suggested_buffer_units} units is a sensible baseline."


def _recommend_customer_action(row: pd.Series) -> str:
    segment = str(row.get("segment", ""))
    risk_band = str(row.get("risk_band", ""))
    next_window = row.get("next_purchase_window_days")

    if segment == "VIP" and risk_band != "Low":
        return "Send a VIP recovery message with priority support and a limited-time private offer."
    if risk_band == "High":
        return "Launch a win-back message with a clear incentive and product reminder."
    if segment == "New":
        return "Trigger a second-order journey with best-sellers and social proof."
    if segment == "Potential Loyalist":
        return f"Recommend a follow-up campaign within {next_window} days with a bundle or loyalty benefit."
    return "Keep in regular retention flow and monitor for changes."


def _write_artifact(filename: str, payload: list[dict]) -> None:
    path = MODELS_DIR / filename
    path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "rows": payload,
            },
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )
