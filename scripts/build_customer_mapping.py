"""One-time historical build script for Customer Mapping.

Fetches all WooCommerce history and Google Sheets data to build the initial 
customer_first_order mapping using Union-Find deduplication.
"""

import sys
import os
import pandas as pd
from pathlib import Path

# Add project root to sys.path
root_path = Path(__file__).parent.parent.absolute()
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

from BackEnd.services.hybrid_data_loader import load_full_woocommerce_history
from BackEnd.services.customer_manager import build_customer_mapping, load_raw_customer_data
from BackEnd.core.logging_config import get_logger

logger = get_logger("build_mapping")

def main():
    logger.info("Starting historical customer mapping build...")
    
    # 1. Load full WooCommerce history
    logger.info("Fetching full WooCommerce history from local cache...")
    woo_df = load_full_woocommerce_history()
    if woo_df.empty:
        logger.warning("WooCommerce history is empty. Make sure you have synced data first.")
    else:
        logger.info(f"Loaded {len(woo_df)} WooCommerce order rows.")
        
    # 2. Load Google Sheets data
    logger.info("Fetching Google Sheets customer data...")
    gsheet_df = load_raw_customer_data()
    if gsheet_df.empty:
        logger.warning("Google Sheets data is empty or failed to load.")
    else:
        logger.info(f"Loaded {len(gsheet_df)} Google Sheets rows.")
        
    # 3. Build mapping
    logger.info("Running Union-Find deduplication and mapping build...")
    mapping_df = build_customer_mapping(woo_df, gsheet_df)
    
    logger.info("Build complete!")
    logger.info(f"Total Unique Customers identified: {len(mapping_df)}")
    logger.info(f"Mapping saved to BackEnd/cache/customer_first_order.parquet")

if __name__ == "__main__":
    main()
