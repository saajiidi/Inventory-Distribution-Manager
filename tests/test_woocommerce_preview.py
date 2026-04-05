import unittest

import pandas as pd

from FrontEnd.pages.woocommerce import _resolve_preview_columns


class TestWooCommercePreviewColumns(unittest.TestCase):
    def test_resolves_current_service_schema(self):
        df = pd.DataFrame(
            columns=[
                'Order Number',
                'Order Date',
                'Shipped Date',
                'Customer Name',
                'Tracking',
                'Item Name',
                'Qty',
                'Order Total Amount',
            ]
        )

        preview_cols = _resolve_preview_columns(df)

        self.assertEqual(
            preview_cols,
            [
                'Order Number',
                'Order Date',
                'Shipped Date',
                'Customer Name',
                'Tracking',
                'Item Name',
                'Qty',
                'Order Total Amount',
            ],
        )

    def test_prefers_legacy_labels_when_present(self):
        df = pd.DataFrame(
            columns=[
                'Order ID',
                'Order Date',
                'Shipped Date',
                'Full Name (Billing)',
                'Tracking',
                'Product Name (main)',
                'Quantity',
                'Order Total Amount',
            ]
        )

        preview_cols = _resolve_preview_columns(df)

        self.assertIn('Order ID', preview_cols)
        self.assertIn('Full Name (Billing)', preview_cols)
        self.assertIn('Product Name (main)', preview_cols)
        self.assertIn('Quantity', preview_cols)


if __name__ == '__main__':
    unittest.main()
