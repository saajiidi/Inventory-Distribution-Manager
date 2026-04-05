import unittest
from unittest.mock import patch

import pandas as pd

from BackEnd.services import hybrid_data_loader


class _FakeWooService:
    def __init__(self):
        self.calls = []

    def fetch_all_historical_orders(self, after=None, before=None, status="any"):
        self.calls.append({"after": after, "before": before, "status": status})
        return pd.DataFrame(
            {
                "Order Number": ["1001"],
                "Order Date": ["2026-04-01 10:00:00"],
                "Customer Name": ["Jane"],
                "Qty": [1],
                "Item Name": ["Polo"],
                "Order Total Amount": [1200],
                "_source": ["woocommerce_api"],
            }
        )


class TestHybridDataLoader(unittest.TestCase):
    def test_woocommerce_loader_respects_selected_date_range(self):
        fake_service = _FakeWooService()

        with (
            patch.object(
                hybrid_data_loader.st,
                "secrets",
                {
                    "woocommerce": {
                        "store_url": "https://example.com",
                        "consumer_key": "ck_test",
                        "consumer_secret": "cs_test",
                    }
                },
            ),
            patch("BackEnd.services.woocommerce_service.WooCommerceService", return_value=fake_service),
        ):
            df = hybrid_data_loader.load_woocommerce_live_data(
                start_date="2026-04-01",
                end_date="2026-04-05",
            )

        self.assertFalse(df.empty)
        self.assertEqual(len(fake_service.calls), 1)
        self.assertEqual(fake_service.calls[0]["after"], "2026-04-01T00:00:00Z")
        self.assertEqual(fake_service.calls[0]["before"], "2026-04-05T23:59:59Z")


if __name__ == "__main__":
    unittest.main()
