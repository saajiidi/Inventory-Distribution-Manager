import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.services import master_sales


class TestMasterSales(unittest.TestCase):
    def test_load_master_sales_dataset_uses_parquet_snapshot_when_workbook_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            snapshot_path = root / 'TotalOrder_TillLastTime.parquet'
            cache_path = root / 'historical_master.parquet'
            workbook_path = root / 'TotalOrder_TillLastTime.xlsx'

            snapshot_df = pd.DataFrame(
                {
                    '_src_tab': ['2025'],
                    '_row_fingerprint': ['snap-1'],
                    '_p_date': pd.to_datetime(['2026-01-01']),
                }
            )
            snapshot_df.to_parquet(snapshot_path, index=False)

            with patch.object(master_sales, 'CORE_PARQUET_SNAPSHOT_PATH', snapshot_path), \
                 patch.object(master_sales, 'MASTER_CACHE_FILE', cache_path), \
                 patch.object(master_sales, 'CORE_WORKBOOK_PATH', workbook_path), \
                 patch.object(master_sales, '_load_2026_delta', lambda base_df, force_refresh=False: pd.DataFrame(columns=base_df.columns)):
                result, msg = master_sales.load_master_sales_dataset()

            self.assertIsNotNone(result)
            self.assertEqual(len(result), 1)
            self.assertIn('Local Parquet snapshot loaded', msg)
            self.assertTrue(cache_path.exists())

    def test_load_master_sales_dataset_builds_snapshot_from_workbook(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            snapshot_path = root / 'TotalOrder_TillLastTime.parquet'
            cache_path = root / 'historical_master.parquet'
            workbook_path = root / 'TotalOrder_TillLastTime.xlsx'
            workbook_path.write_text('placeholder', encoding='utf-8')

            workbook_df = pd.DataFrame(
                {
                    '_src_tab': ['2026-tillLastTime'],
                    '_row_fingerprint': ['wb-1'],
                    '_p_date': pd.to_datetime(['2026-02-01']),
                }
            )

            with patch.object(master_sales, 'CORE_PARQUET_SNAPSHOT_PATH', snapshot_path), \
                 patch.object(master_sales, 'MASTER_CACHE_FILE', cache_path), \
                 patch.object(master_sales, 'CORE_WORKBOOK_PATH', workbook_path), \
                 patch.object(master_sales, '_load_core_workbook', lambda: (workbook_df, {'base_rows': 1, 'sheet_count': 3})), \
                 patch.object(master_sales, '_load_2026_delta', lambda base_df, force_refresh=False: pd.DataFrame(columns=base_df.columns)):
                result, msg = master_sales.load_master_sales_dataset(force_refresh=True)

            self.assertIsNotNone(result)
            self.assertEqual(len(result), 1)
            self.assertEqual('Workbook core loaded: 1 rows across 3 tabs + 0 new 2026 rows', msg)
            self.assertTrue(snapshot_path.exists())


if __name__ == '__main__':
    unittest.main()
