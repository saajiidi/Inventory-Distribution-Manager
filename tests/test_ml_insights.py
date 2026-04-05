import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from BackEnd.services import ml_insights


class TestMlInsights(unittest.TestCase):
    def test_generate_demand_forecast_returns_ranked_products(self):
        df = pd.DataFrame(
            {
                "order_date": pd.to_datetime(
                    [
                        "2026-04-01",
                        "2026-04-01",
                        "2026-04-02",
                        "2026-04-03",
                    ]
                ),
                "item_name": ["Polo", "Polo", "Panjabi", "Polo"],
                "qty": [3, 2, 4, 5],
                "order_total": [3000, 2000, 4000, 5000],
                "order_id": ["1", "2", "3", "4"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            models_dir = Path(tmpdir)
            with patch.object(ml_insights, "MODELS_DIR", models_dir):
                forecast = ml_insights.generate_demand_forecast(df, horizon_days=7)

        self.assertFalse(forecast.empty)
        self.assertIn("forecast_7d_units", forecast.columns)
        self.assertEqual(forecast.iloc[0]["item_name"], "Polo")

    def test_score_customer_risk_adds_action_fields(self):
        customers = pd.DataFrame(
            {
                "customer_id": ["c1", "c2"],
                "primary_name": ["Alice", "Bob"],
                "segment": ["VIP", "New"],
                "recency_days": [90, 10],
                "purchase_cycle_days": [30, 15],
                "avg_order_value": [2000, 800],
                "total_orders": [5, 1],
                "total_revenue": [10000, 800],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            models_dir = Path(tmpdir)
            with patch.object(ml_insights, "MODELS_DIR", models_dir):
                scored = ml_insights.score_customer_risk(customers)

        self.assertFalse(scored.empty)
        self.assertIn("risk_score", scored.columns)
        self.assertIn("recommended_action", scored.columns)
        self.assertEqual(scored.iloc[0]["primary_name"], "Alice")

    def test_detect_sales_anomalies_flags_large_spike(self):
        dates = pd.date_range("2026-04-01", periods=10, freq="D")
        revenue = [100, 110, 95, 105, 98, 102, 101, 99, 500, 103]
        df = pd.DataFrame(
            {
                "order_date": dates,
                "order_total": revenue,
                "order_id": [str(i) for i in range(10)],
                "qty": [1] * 10,
                "item_name": ["Item"] * 10,
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            models_dir = Path(tmpdir)
            with patch.object(ml_insights, "MODELS_DIR", models_dir):
                anomalies = ml_insights.detect_sales_anomalies(df, window=3)

        self.assertFalse(anomalies.empty)
        self.assertIn("metric", anomalies.columns)
        self.assertTrue((anomalies["metric"] == "revenue").any())


if __name__ == "__main__":
    unittest.main()
