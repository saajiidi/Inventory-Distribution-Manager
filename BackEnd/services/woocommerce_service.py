import pandas as pd
from woocommerce import API
import streamlit as st
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse


def get_woocommerce_credentials() -> dict[str, str]:
    """Safely load WooCommerce credentials from Streamlit secrets or environment variables."""
    import os
    
    # 1. Try Streamlit secrets first
    try:
        woo = st.secrets.get("woocommerce", {})
        if woo:
            credentials = dict(woo)
            required = {"store_url", "consumer_key", "consumer_secret"}
            if required.issubset(credentials):
                return credentials
    except Exception:
        pass

    # 2. Fallback to environment variables
    env_credentials = {
        "store_url": os.getenv("WOOCOMMERCE_STORE_URL"),
        "consumer_key": os.getenv("WOOCOMMERCE_CONSUMER_KEY"),
        "consumer_secret": os.getenv("WOOCOMMERCE_CONSUMER_SECRET")
    }
    
    if all(env_credentials.values()):
        return env_credentials

    return {}


def get_woocommerce_store_label() -> str:
    credentials = get_woocommerce_credentials()
    if not credentials:
        return "Not connected"
    try:
        host = urlparse(credentials["store_url"]).netloc or credentials["store_url"]
        return host.replace("www.", "")
    except Exception:
        return "Connected store"


def _sanitize_api_error(error_text: str) -> str:
    if not error_text:
        return "Unknown API error"
    safe_text = str(error_text)
    credentials = get_woocommerce_credentials()
    for key_name in ("consumer_key", "consumer_secret"):
        value = credentials.get(key_name)
        if value:
            safe_text = safe_text.replace(value, "[redacted]")
    return safe_text


class WooCommerceService:
    def __init__(self, ui_enabled: bool = True):
        """Initialize connection using Streamlit secrets."""
        self.ui_enabled = ui_enabled
        try:
            credentials = get_woocommerce_credentials()
            if not credentials:
                self.wcapi = None
                return
            self.wcapi = API(
                url=credentials["store_url"],
                consumer_key=credentials["consumer_key"],
                consumer_secret=credentials["consumer_secret"],
                version="wc/v3",
                timeout=120
            )
        except Exception:
            if self.ui_enabled:
                st.error("WooCommerce API initialization failed. Please verify the store URL and API keys.")
            self.wcapi = None

    def fetch_orders(self, page: int = 1, per_page: int = 100, status: str = "any", 
                     after: Optional[str] = None, before: Optional[str] = None,
                     show_errors: bool = True) -> List[Dict[str, Any]]:
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
            if self.ui_enabled and show_errors:
                st.error(f"Failed to fetch orders: {response.status_code} - {_sanitize_api_error(response.text)}")
            return []

    def fetch_all_historical_orders(
        self,
        after: Optional[str] = None,
        before: Optional[str] = None,
        status: str = "any",
        show_progress: bool = True,
        show_errors: bool = True,
    ) -> pd.DataFrame:
        """Fetch all historical orders incrementally to stay within memory limits."""
        import gc
        
        all_dfs = []
        page = 1
        
        status_container = None
        if self.ui_enabled and show_progress:
            try:
                # Prevent Streamlit Thread crashes by verifying context
                from streamlit.runtime.scriptrunner import get_script_run_ctx
                if get_script_run_ctx() is not None:
                    status_container = st.empty()
            except Exception:
                pass
        
        if status_container:
            status_container.info(f"Initializing Smart Sync for status: {status}...")

        while True:
            orders_json = self.fetch_orders(
                page=page,
                after=after,
                before=before,
                status=status,
                show_errors=show_errors,
            )
            
            if not orders_json:
                break
            
            # 1. Process this page immediately to a lean DataFrame
            page_df = self.process_orders_to_df(orders_json)
            
            # 2. Append the lean DF
            all_dfs.append(page_df)
            
            # 3. CRITICAL: Clear the raw JSON from memory
            del orders_json
            gc.collect() 
            
            if status_container:
                status_container.text(f"Synced {page * 100} orders... (Memory Safe Mode)")
            
            page += 1
            if page > 1000: break # Safety cap
            
        if not all_dfs:
            return pd.DataFrame()

        # Final concatenation
        final_df = pd.concat(all_dfs, ignore_index=True)
        
        return final_df

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

            # Extraction of Coupons & Campaigns
            coupons = [c.get("code", "") for c in order.get("coupon_lines", [])]
            coupon_str = ", ".join(coupons) if coupons else "None"
            
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
                    "Coupons": coupon_str,
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

    def fetch_products(self, page: int = 1, per_page: int = 100) -> List[Dict[str, Any]]:
        """Fetch products from WooCommerce API."""
        if not self.wcapi:
            return []
            
        params = {
            "page": page,
            "per_page": per_page
        }
        response = self.wcapi.get("products", params=params)
        if response.status_code == 200:
            return response.json()
        return []

    def get_stock_report(self, show_errors: bool = True) -> pd.DataFrame:
        """Fetch all published products and extract stock counts using pagination headers."""
        if not self.wcapi:
            return pd.DataFrame()
            
        all_products = []
        page = 1
        per_page = 50
        
        while True:
            response = self.wcapi.get("products", params={
                "page": page,
                "per_page": per_page,
                "status": "publish", # Exclude drafts and private products
            })
            
            if response.status_code != 200:
                if self.ui_enabled and show_errors:
                    st.error(f"Failed to fetch products: {response.status_code} - {_sanitize_api_error(response.text)}")
                break
            
            products = response.json()
            if not products:
                break
            
            for p in products:
                stock_quantity = p.get("stock_quantity")
                stock_status = p.get("stock_status") or ("instock" if stock_quantity not in (None, "", 0) else "unknown")
                all_products.append({
                    "ID": p.get("id"),
                    "Name": p.get("name"),
                    "SKU": p.get("sku"),
                    "Stock Status": stock_status,
                    "Stock Quantity": stock_quantity or 0,
                    "Price": float(p.get("price", 0)) if p.get("price") else 0,
                    "Regular Price": float(p.get("regular_price", 0)) if p.get("regular_price") else 0,
                    "Sale Price": float(p.get("sale_price", 0)) if p.get("sale_price") else 0,
                    "Category": ", ".join([c.get("name") for c in p.get("categories", [])]),
                    "Manage Stock": bool(p.get("manage_stock")),
                    "Product Type": p.get("type", ""),
                })
            
            total_pages = int(response.headers.get('x-wp-totalpages', 1))
            if page >= total_pages:
                break
            page += 1
            if page > 200: break # Safety cap
            
        return pd.DataFrame(all_products)

    def fetch_orders_range(self, start_date: Any, end_date: Any) -> pd.DataFrame:
        """Fetch orders within a specific timestamp range."""
        from BackEnd.utils.woocommerce_helpers import to_iso8601
        
        after = to_iso8601(start_date)
        before = to_iso8601(end_date)
        
        return self.fetch_all_historical_orders(after=after, before=before, show_progress=False)

    def fetch_stock_inventory(self) -> pd.DataFrame:
        """Alias for get_stock_report to maintain compatibility with the hybrid loader."""
        return self.get_stock_report(show_errors=False)

    def get_registered_customer_count(self) -> int:
        """Fetch total count of registered customers using headers (efficient)."""
        if not self.wcapi:
            return 0
        try:
            # We only need 1 per_page to get the headers
            response = self.wcapi.get("customers", params={"per_page": 1})
            if response.status_code == 200:
                return int(response.headers.get('x-wp-total', 0))
            return 0
        except Exception:
            return 0

    def fetch_registered_customers(self, page: int = 1, per_page: int = 100) -> List[Dict[str, Any]]:
        """Fetch registered customer profiles."""
        if not self.wcapi:
            return []
        try:
            response = self.wcapi.get("customers", params={"page": page, "per_page": per_page})
            if response.status_code == 200:
                return response.json()
            return []
        except Exception:
            return []

    def query_stock_assistant(self, question: str, stock_df: pd.DataFrame) -> str:
        """Send question + stock data context to AI and return answer using OpenAI."""
        import openai
        
        # Determine API Key from environment or secrets
        try:
            api_key = st.secrets.get("OPENAI_API_KEY")
        except Exception:
            api_key = None
            
        if not api_key:
            return "Error: OPENAI_API_KEY is missing in Streamlit secrets."
        
        openai.api_key = api_key
        
        # Create a summary of the stock data for context
        stock_summary = f"""
        Total Products: {len(stock_df)}
        Out of Stock: {len(stock_df[stock_df['Stock Status'] == 'outofstock'])}
        Low Stock (≤5 units): {len(stock_df[stock_df['Stock Quantity'] <= 5])}
        Total Inventory Value: ${(stock_df['Stock Quantity'] * stock_df['Price']).sum():,.2f}
        
        Sample of products (first 10 rows):
        {stock_df.head(10).to_string()}
        
        Full stock data columns: {', '.join(stock_df.columns)}
        """
        
        system_prompt = """
        You are a helpful WooCommerce stock assistant. You have access to real-time product stock data.
        
        Your job is to answer questions about:
        - Product stock levels
        - Low stock alerts
        - Out of stock items
        - Inventory value
        - Product availability
        
        Always base your answers on the provided data. Be concise and helpful.
        If the user asks about a specific product not in the sample, search the full dataset logically.
        """
        
        try:
            client = openai.OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Stock Data Summary:\n{stock_summary}\n\nUser Question: {question}"}
                ],
                temperature=0.3,
                max_tokens=500
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"Error querying AI (OpenAI): {str(e)}"
