import unittest
from datetime import datetime, timedelta

import pandas as pd

from FrontEnd.pages.cycle_analytics import (
    calculate_cycle_metrics,
    get_business_cycles,
    prepare_cycle_orders,
)


class TestCycleAnalytics(unittest.TestCase):
    def test_get_business_cycles_skips_friday_cutoff(self):
        current_start, current_end, previous_start, previous_end = get_business_cycles(
            datetime(2026, 4, 4, 18, 0, 0)
        )

        self.assertEqual(current_end, datetime(2026, 4, 4, 17, 0, 0))
        self.assertEqual(current_start, datetime(2026, 4, 2, 17, 0, 0))
        self.assertEqual(previous_end, datetime(2026, 4, 2, 17, 0, 0))
        self.assertEqual(previous_start, datetime(2026, 4, 1, 17, 0, 0))
        self.assertEqual(current_end - current_start, timedelta(days=2))

    def test_prepare_cycle_orders_rolls_line_items_into_single_order(self):
        df = pd.DataFrame(
            {
                "order_id": ["1001", "1001"],
                "order_date": [datetime(2026, 4, 4, 12, 0, 0), datetime(2026, 4, 4, 12, 0, 0)],
                "order_status": ["processing", "processing"],
                "qty": [1, 2],
                "item_name": ["Polo", "Pant"],
                "order_total": [1000, 1000],
                "customer_name": ["Jane", "Jane"],
                "city": ["Dhaka", "Dhaka"],
                "source": ["woocommerce_api", "woocommerce_api"],
            }
        )

        orders = prepare_cycle_orders(df)

        self.assertEqual(len(orders), 1)
        self.assertEqual(int(orders.iloc[0]["qty"]), 3)
        self.assertEqual(float(orders.iloc[0]["order_total"]), 1000.0)
        self.assertEqual(orders.iloc[0]["status_bucket"], "new")

    def test_calculate_cycle_metrics_uses_shipped_date_for_shipped_orders(self):
        cycle_start = datetime(2026, 4, 2, 17, 0, 0)
        cycle_end = datetime(2026, 4, 4, 17, 0, 0)

        df = pd.DataFrame(
            {
                "order_id": ["1001", "1001", "1002", "1003"],
                "order_date": [
                    datetime(2026, 4, 4, 12, 0, 0),
                    datetime(2026, 4, 4, 12, 0, 0),
                    datetime(2026, 4, 2, 10, 0, 0),
                    datetime(2026, 4, 4, 14, 0, 0),
                ],
                "shipped_date": [
                    pd.NaT,
                    pd.NaT,
                    datetime(2026, 4, 4, 9, 0, 0),
                    pd.NaT,
                ],
                "order_status": ["processing", "processing", "completed", "completed"],
                "qty": [1, 2, 1, 1],
                "item_name": ["Polo", "Pant", "Cap", "Shoes"],
                "order_total": [1000, 1000, 700, 1200],
                "customer_name": ["Jane", "Jane", "John", "Sara"],
                "city": ["Dhaka", "Dhaka", "Chittagong", "Sylhet"],
                "source": [
                    "woocommerce_api",
                    "woocommerce_api",
                    "woocommerce_api",
                    "woocommerce_api",
                ],
            }
        )

        orders = prepare_cycle_orders(df)
        new_metrics = calculate_cycle_metrics(orders, cycle_start, cycle_end, "new")
        shipped_metrics = calculate_cycle_metrics(orders, cycle_start, cycle_end, "shipped")

        self.assertEqual(new_metrics["num_orders"], 1)
        self.assertEqual(new_metrics["items_sold"], 3)
        self.assertEqual(new_metrics["revenue"], 1000.0)

        self.assertEqual(shipped_metrics["num_orders"], 2)
        self.assertEqual(shipped_metrics["items_sold"], 2)
        self.assertEqual(shipped_metrics["revenue"], 1900.0)


if __name__ == "__main__":
    unittest.main()
