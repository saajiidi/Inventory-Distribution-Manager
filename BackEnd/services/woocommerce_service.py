import pandas as pd
from woocommerce import API
import streamlit as st
from datetime import datetime
from typing import List, Dict, Any, Optional

class WooCommerceService:
    def __init__(self):
        """Initialize connection using Streamlit secrets."""
        try:
            self.wcapi = API(
                url=st.secrets["woocommerce"]["store_url"],
                consumer_key=st.secrets["woocommerce"]["consumer_key"],
                consumer_secret=st.secrets["woocommerce"]["consumer_secret"],
                version="wc/v3",
                timeout=120
            )
        except Exception as e:
            st.error(f"WooCommerce API Initialization Failed: {e}")
            self.wcapi = None

    def fetch_orders(self, page: int = 1, per_page: int = 100, status: str = "any", 
                     after: Optional[str] = None, before: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch orders from WooCommerce API."""
        if not self.wcapi:
            return []
            
        params = {
            "page": page,
            "per_page": per_page,
            "status": status
        }
        if after:
            params["after"] = after
        if before:
            params["before"] = before
            
        response = self.wcapi.get("orders", params=params)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to fetch orders: {response.status_code} - {response.text}")
            return []

    def fetch_all_historical_orders(self, after: Optional[str] = None, before: Optional[str] = None, status: str = "any") -> pd.DataFrame:
        """Fetch all historical orders recursively."""
        all_orders = []
        page = 1
        
        # Progress bar placeholder
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        while True:
            status_text.text(f"Fetching page {page} with status '{status}'...")
            orders = self.fetch_orders(page=page, after=after, before=before, status=status)
            if not orders:
                break
                
            all_orders.extend(orders)
            page += 1
            
            # Simple progress cap for safety or infinite loop prevention
            if page > 1000: 
                break
        
        status_text.text(f"Processing {len(all_orders)} orders...")
        return self.process_orders_to_df(all_orders)

    def process_orders_to_df(self, orders: List[Dict[str, Any]]) -> pd.DataFrame:
        """Flatten WooCommerce order JSON into the application's standard dataframe format."""
        flattened_data = []
        
        for order in orders:
            order_id = order.get("id")
            order_date = order.get("date_created")
            # Convert date
            if order_date:
                try:
                    dt = datetime.fromisoformat(order_date.replace("Z", "+00:00"))
                    order_date = dt.strftime("%Y-%m-%d %H:%M:%S")
                    year = dt.year
                except:
                    year = datetime.now().year
            else:
                year = datetime.now().year
                
            billing = order.get("billing", {})
            full_name = f"{billing.get('first_name', '')} {billing.get('last_name', '')}".strip()
            phone = billing.get("phone", "")
            address = f"{billing.get('address_1', '')} {billing.get('address_2', '')}".strip()
            city_state_zip = f"{billing.get('city', '')}, {billing.get('state', '')}, {billing.get('postcode', '')}".strip(", ")
            
            total_amount = order.get("total")
            payment_method = order.get("payment_method_title")
            shipped_date = order.get("date_modified")
            if shipped_date:
                try:
                    dt_mod = datetime.fromisoformat(shipped_date.replace("Z", "+00:00"))
                    shipped_date = dt_mod.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    pass

            # Tracking extraction
            tracking_number = "N/A"
            if order.get("meta_data"):
                for meta in order["meta_data"]:
                    if "tracking" in str(meta.get("key", "")).lower():
                        tracking_number = meta.get("value")
                        break
            
            # Line items
            for item in order.get("line_items", []):
                item_data = {
                    "Order Number": order_id,
                    "Order Date": order_date,
                    "Shipped Date": shipped_date,
                    "Order Status": order.get("status"),
                    "year": year,
                    "Customer Name": full_name,
                    "Phone (Billing)": phone,
                    "Address 1&2 (Billing)": address,
                    "City, State, Zip (Billing)": city_state_zip,
                    "Order Total Amount": total_amount,
                    "Payment Method Title": payment_method,
                    "Item Name": item.get("name"),
                    "SKU": item.get("sku"),
                    "Qty": item.get("quantity"),
                    "Item Cost": item.get("price"),
                    "Tracking": tracking_number,
                    "_source": "woocommerce_api"
                }
                flattened_data.append(item_data)
                
        return pd.DataFrame(flattened_data)

    def save_to_parquet(self, df: pd.DataFrame, base_path: str = "data"):
        """Save DataFrame to year-partitioned parquet files."""
        from pathlib import Path
        
        if df.empty:
            return
            
        base = Path(base_path)
        for year, group in df.groupby("year"):
            year_folder = base / f"year={year}"
            year_folder.mkdir(parents=True, exist_ok=True)
            
            file_path = year_folder / f"woo_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            group.to_parquet(file_path, index=False)
            st.success(f"Saved {len(group)} orders for year {year} to {file_path}")
