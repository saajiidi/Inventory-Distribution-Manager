import pandas as pd
from woocommerce import API
import streamlit as st
from datetime import datetime
from typing import List, Dict, Any, Optional


def get_woocommerce_credentials() -> dict[str, str]:
    """Safely load WooCommerce credentials from Streamlit secrets."""
    try:
        woo = st.secrets.get("woocommerce", {})
    except Exception:
        return {}

    if not woo:
        return {}

    credentials = dict(woo)
    required = {"store_url", "consumer_key", "consumer_secret"}
    if not required.issubset(credentials):
        return {}
    return credentials


class WooCommerceService:
    def __init__(self):
        """Initialize connection using Streamlit secrets."""
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

    def get_stock_report(self) -> pd.DataFrame:
        """Fetch all products and extract stock counts using pagination headers."""
        if not self.wcapi:
            return pd.DataFrame()
            
        all_products = []
        page = 1
        per_page = 50
        
        while True:
            response = self.wcapi.get("products", params={
                "page": page,
                "per_page": per_page,
                "stock_status": "instock,outofstock,onbackorder"
            })
            
            if response.status_code != 200:
                break
            
            products = response.json()
            if not products:
                break
            
            for p in products:
                all_products.append({
                    "ID": p.get("id"),
                    "Name": p.get("name"),
                    "SKU": p.get("sku"),
                    "Stock Status": p.get("stock_status"),
                    "Stock Quantity": p.get("stock_quantity") or 0,
                    "Price": float(p.get("price", 0)) if p.get("price") else 0,
                    "Category": ", ".join([c.get("name") for c in p.get("categories", [])])
                })
            
            total_pages = int(response.headers.get('x-wp-totalpages', 1))
            if page >= total_pages:
                break
            page += 1
            if page > 200: break # Safety cap
            
        return pd.DataFrame(all_products)

    def query_stock_assistant(self, question: str, stock_df: pd.DataFrame) -> str:
        """Send question + stock data context to AI and return answer using OpenAI."""
        import openai
        
        # Determine API Key from environment or secrets
        api_key = st.secrets.get("OPENAI_API_KEY")
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
