import unittest

import pandas as pd

from BackEnd.utils.sales_schema import ensure_sales_schema


class TestSalesSchema(unittest.TestCase):
    def test_normalizes_woocommerce_columns(self):
        df = pd.DataFrame(
            {
                "Order Number": [101],
                "Order Date": ["2026-04-01 10:00:00"],
                "Customer Name": ["Jane Doe"],
                "Phone (Billing)": ["01700000000"],
                "Item Name": ["Classic Polo"],
                "Qty": [2],
                "Order Total Amount": [2500],
                "Order Status": ["processing"],
            }
        )

        normalized = ensure_sales_schema(df)

        self.assertEqual(normalized.loc[0, "order_id"], "101")
        self.assertEqual(normalized.loc[0, "customer_name"], "Jane Doe")
        self.assertEqual(normalized.loc[0, "item_name"], "Classic Polo")
        self.assertEqual(normalized.loc[0, "qty"], 2)
        self.assertEqual(normalized.loc[0, "order_total"], 2500)
        self.assertEqual(normalized.loc[0, "order_status"], "processing")
        self.assertTrue(pd.notna(normalized.loc[0, "order_date"]))

    def test_normalizes_legacy_sheet_columns(self):
        df = pd.DataFrame(
            {
                "Order ID": ["A-1"],
                "Date": ["2026-04-02"],
                "Full Name (Billing)": ["John Smith"],
                "Product Name (main)": ["Premium Panjabi"],
                "Quantity": [1],
                "Order Total": [1800],
            }
        )

        normalized = ensure_sales_schema(df)

        self.assertEqual(normalized.loc[0, "order_id"], "A-1")
        self.assertEqual(normalized.loc[0, "customer_name"], "John Smith")
        self.assertEqual(normalized.loc[0, "item_name"], "Premium Panjabi")
        self.assertEqual(normalized.loc[0, "qty"], 1)
        self.assertEqual(normalized.loc[0, "order_total"], 1800)


if __name__ == "__main__":
    unittest.main()
