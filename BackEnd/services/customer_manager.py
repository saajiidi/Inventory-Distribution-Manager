"""Customer Data Manager Service v2.

Handles extraction, deduplication, and consolidation of customer records
using a robust Union-Find approach for both WooCommerce and Google Sheets.
"""

import pandas as pd
import os
import re
import hashlib
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path
from BackEnd.core.logging_config import get_logger

logger = get_logger("customer_manager")

CUSTOMER_DATA_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vTBDukmkRJGgHjCRIAAwGmlWaiPwESXSp9UBXm3_sbs37bk2HxavPc62aobmL1cGWUfAKE4Zd6yJySO"
    "/pub?output=csv"
)

CACHE_DIR = Path("BackEnd/cache")
CUSTOMER_CACHE_FILE = CACHE_DIR / "consolidated_customers.parquet"
MAPPING_FILE = CACHE_DIR / "customer_first_order.parquet"

class UnionFind:
    """Disjoint Set Union (Union-Find) with path compression and union by rank."""
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, i):
        if i not in self.parent:
            self.parent[i] = i
            self.rank[i] = 0
            return i
        if self.parent[i] == i:
            return i
        self.parent[i] = self.find(self.parent[i])
        return self.parent[i]

    def union(self, i, j):
        root_i = self.find(i)
        root_j = self.find(j)
        if root_i != root_j:
            if self.rank[root_i] < self.rank[root_j]:
                self.parent[root_i] = root_j
            elif self.rank[root_i] > self.rank[root_j]:
                self.parent[root_j] = root_i
            else:
                self.parent[root_i] = root_j
                self.rank[root_j] += 1
            return True
        return False

    def get_clusters(self) -> Dict[Any, List[Any]]:
        clusters = {}
        for node in self.parent:
            root = self.find(node)
            if root not in clusters:
                clusters[root] = []
            clusters[root].append(node)
        return clusters

def clean_phone(phone: str) -> str:
    if pd.isna(phone) or not phone:
        return ""
    digits = re.sub(r"\D", "", str(phone).strip())
    # Standardize BD numbers if needed
    if len(digits) == 10 and digits.startswith("1"):
        digits = "0" + digits
    return digits

def clean_email(email: str) -> str:
    if pd.isna(email) or not email:
        return ""
    return str(email).strip().lower()

def load_raw_customer_data(url: str = CUSTOMER_DATA_URL) -> pd.DataFrame:
    """Load raw data from the provided Google Sheet CSV URL."""
    try:
        df = pd.read_csv(url)
        logger.info(f"Loaded {len(df)} rows of raw customer data from Google Sheets.")
        return df
    except Exception as e:
        logger.error(f"Failed to load customer data from URL: {e}")
        return pd.DataFrame()

def build_customer_mapping(orders_df: pd.DataFrame, gsheet_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Historical build of customer mapping using Union-Find.
    Merges WooCommerce orders and Google Sheets data.
    """
    uf = UnionFind()
    
    # 1. Collect all rows to process
    rows = []
    
    # Process WooCommerce orders
    if not orders_df.empty:
        for _, row in orders_df.iterrows():
            phone = clean_phone(row.get("phone", ""))
            email = clean_email(row.get("email", ""))
            name = str(row.get("customer_name", row.get("name", ""))).strip()
            date = pd.to_datetime(row.get("order_date"), errors='coerce')
            
            rows.append({
                "phone": phone,
                "email": email,
                "name": name,
                "date": date,
                "source": "woo"
            })
            
            # Union nodes
            if phone and email:
                uf.union(f"p:{phone}", f"e:{email}")
            elif phone:
                uf.find(f"p:{phone}")
            elif email:
                uf.find(f"e:{email}")

    # Process Google Sheets data
    if gsheet_df is not None and not gsheet_df.empty:
        # Standardize columns for gsheet
        temp_df = gsheet_df.copy()
        for col in temp_df.columns:
            c_low = col.lower()
            if any(kw in c_low for kw in ["phone", "contact", "mobile"]):
                temp_df = temp_df.rename(columns={col: "phone"})
            elif any(kw in c_low for kw in ["email", "mail"]):
                temp_df = temp_df.rename(columns={col: "email"})
            elif any(kw in c_low for kw in ["name", "customer"]):
                temp_df = temp_df.rename(columns={col: "name"})
            elif any(kw in c_low for kw in ["date", "time"]):
                temp_df = temp_df.rename(columns={col: "date"})

        for _, row in temp_df.iterrows():
            phone = clean_phone(row.get("phone", ""))
            email = clean_email(row.get("email", ""))
            name = str(row.get("name", "")).strip()
            date = pd.to_datetime(row.get("date"), errors='coerce')
            
            rows.append({
                "phone": phone,
                "email": email,
                "name": name,
                "date": date,
                "source": "gsheet"
            })
            
            if phone and email:
                uf.union(f"p:{phone}", f"e:{email}")
            elif phone:
                uf.find(f"p:{phone}")
            elif email:
                uf.find(f"e:{email}")

    # 2. Assign rows to clusters
    cluster_data = {} # root_id -> list of rows
    
    for row in rows:
        phone = row["phone"]
        email = row["email"]
        
        node = None
        if phone: node = f"p:{phone}"
        elif email: node = f"e:{email}"
        
        if node:
            root = uf.find(node)
            if root not in cluster_data:
                cluster_data[root] = []
            cluster_data[root].append(row)
        else:
            # Anonymous order - treat as unique cluster
            anon_id = f"anon_{hashlib.md5(str(row['name']).encode()).hexdigest()[:8]}"
            cluster_data[anon_id] = [row]

    # 3. Aggregate clusters into persistent customer records
    consolidated = []
    for root, cluster_rows in cluster_data.items():
        phones = sorted(list(set(r["phone"] for r in cluster_rows if r["phone"])))
        emails = sorted(list(set(r["email"] for r in cluster_rows if r["email"])))
        names = sorted(list(set(r["name"] for r in cluster_rows if r["name"] and r["name"].lower() != 'nan')))
        dates = [r["date"] for r in cluster_rows if pd.notna(r["date"])]
        
        first_order = min(dates) if dates else pd.NaT
        last_order = max(dates) if dates else pd.NaT
        
        # Use a stable hash of the root or phone as customer_id
        customer_id = hashlib.md5(str(root).encode()).hexdigest()[:12]
        
        consolidated.append({
            "customer_id": customer_id,
            "primary_name": names[0] if names else "Unknown",
            "secondary_names": ", ".join(names[1:]) if len(names) > 1 else "",
            "phones": ", ".join(phones),
            "primary_phone": phones[0] if phones else "",
            "emails": ", ".join(emails),
            "primary_email": emails[0] if emails else "",
            "first_order_date": first_order,
            "last_order_date": last_order,
            "total_orders": len(cluster_rows),
            "source": ", ".join(list(set(r["source"] for r in cluster_rows)))
        })
        
    result_df = pd.DataFrame(consolidated)
    save_mapping(result_df)
    return result_df

def update_customer_mapping(new_orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    Incremental update function.
    Fetches orders > last_processed_date and merges with existing mapping.
    """
    existing_df = load_customer_mapping()
    if existing_df.empty:
        return build_customer_mapping(new_orders_df)
    
    # Simple incremental logic:
    # 1. For each new order, check if phone/email matches existing customers
    # 2. If yes, update last_order_date
    # 3. If no, create new customer
    
    # Create lookup maps for speed
    phone_to_id = {}
    email_to_id = {}
    for _, row in existing_df.iterrows():
        cid = row["customer_id"]
        for p in str(row["phones"]).split(", "):
            if p: phone_to_id[p] = cid
        for e in str(row["emails"]).split(", "):
            if e: email_to_id[e] = cid
            
    updates = []
    new_customers = []
    
    for _, row in new_orders_df.iterrows():
        phone = clean_phone(row.get("phone", ""))
        email = clean_email(row.get("email", ""))
        date = pd.to_datetime(row.get("order_date"), errors='coerce')
        
        cid = phone_to_id.get(phone) or email_to_id.get(email)
        
        if cid:
            # Update existing
            idx = existing_df.index[existing_df['customer_id'] == cid].tolist()[0]
            if pd.isna(existing_df.at[idx, 'last_order_date']) or date > existing_df.at[idx, 'last_order_date']:
                existing_df.at[idx, 'last_order_date'] = date
            existing_df.at[idx, 'total_orders'] += 1
        else:
            # New customer
            new_cid = hashlib.md5(f"p:{phone}e:{email}".encode()).hexdigest()[:12]
            name = str(row.get("customer_name", row.get("name", "Unknown"))).strip()
            
            new_rec = {
                "customer_id": new_cid,
                "primary_name": name,
                "secondary_names": "",
                "phones": phone,
                "primary_phone": phone,
                "emails": email,
                "primary_email": email,
                "first_order_date": date,
                "last_order_date": date,
                "total_orders": 1,
                "source": "woo_incremental"
            }
            new_customers.append(new_rec)
            # Update lookups to avoid duplicates in this batch
            if phone: phone_to_id[phone] = new_cid
            if email: email_to_id[email] = new_cid

    if new_customers:
        existing_df = pd.concat([existing_df, pd.DataFrame(new_customers)], ignore_index=True)
        
    save_mapping(existing_df)
    return existing_df

def save_mapping(df: pd.DataFrame):
    """Save the pre-computed mapping to parquet."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(MAPPING_FILE, index=False)
    logger.info(f"Saved {len(df)} customer mappings to {MAPPING_FILE}")

def load_customer_mapping() -> pd.DataFrame:
    """Load the pre-computed mapping from cache."""
    if MAPPING_FILE.exists():
        df = pd.read_parquet(MAPPING_FILE)
        # Ensure dates are datetime
        df['first_order_date'] = pd.to_datetime(df['first_order_date'])
        df['last_order_date'] = pd.to_datetime(df['last_order_date'])
        return df
    return pd.DataFrame()

def get_customer_metrics(start_date: date, end_date: date) -> Dict[str, int]:
    """
    Fast metrics calculation using the pre-computed mapping.
    """
    df = load_customer_mapping()
    if df.empty:
        return {"total_customers": 0, "new_customers": 0}
    
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    
    # Cumulative total up to end_date
    total_customers = len(df[df['first_order_date'] <= end_ts])
    
    # New customers in range
    new_customers = len(df[(df['first_order_date'] >= start_ts) & (df['first_order_date'] <= end_ts)])
    
    return {
        "total_customers": total_customers,
        "new_customers": new_customers
    }

# Legacy function shim for backward compatibility
def consolidate_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Shim for legacy code to use the new Union-Find logic."""
    # If the input DF is the raw gsheet data, we can just use our new builder
    consolidated = build_customer_mapping(pd.DataFrame(), gsheet_df=df)
    
    # Rename columns to match what the old Customer Intelligence page expects
    mapping = {
        "primary_name": "Prominent Name",
        "secondary_names": "Secondary Names",
        "primary_phone": "Primary Phone",
        "phones": "Secondary Phones", # The old code expects comma-separated list here
        "primary_email": "Primary Email",
        "emails": "Secondary Emails",
        "total_orders": "Total Orders"
    }
    return consolidated.rename(columns=mapping)

def load_cached_customers() -> pd.DataFrame:
    """Shim for legacy code."""
    return load_customer_mapping()

def verify_with_woocommerce(customer_df: pd.DataFrame, woo_sales_df: pd.DataFrame) -> pd.DataFrame:
    """Verify customer existence and data in WooCommerce using standardized schema."""
    if customer_df.empty or woo_sales_df is None or woo_sales_df.empty:
        return customer_df
    
    df = customer_df.copy()
    
    # Standardize woo data for matching (using canonical schema names: phone, email)
    woo_phones = set()
    if 'phone' in woo_sales_df.columns:
        woo_phones = set(woo_sales_df['phone'].astype(str).str.replace(r'\D', '', regex=True).str.strip())
        
    woo_emails = set()
    if 'email' in woo_sales_df.columns:
        woo_emails = set(woo_sales_df['email'].astype(str).str.lower().str.strip())
    
    def check_verified(row):
        # Handle both old and new column names
        p_val = row.get('Primary Phone', row.get('primary_phone', ''))
        e_val = row.get('Primary Email', row.get('primary_email', ''))
        
        phone = str(p_val).replace(r'\D', '', regex=True)
        email = str(e_val).lower()
        
        if (phone and phone in woo_phones) or (email and email in woo_emails):
            return "Yes"
        return "No"

    df['Verified'] = df.apply(check_verified, axis=1)
    return df

def save_consolidated_data(df: pd.DataFrame):
    """Shim for legacy code."""
    # Convert back to internal schema if needed, but save_mapping handles generic DFs too
    save_mapping(df)

def export_to_excel(df: pd.DataFrame, filename: str):
    """Shim for legacy code."""
    try:
        df.to_excel(filename, index=False, engine='openpyxl')
        logger.info(f"Exported customer report to {filename}")
        return True
    except Exception as e:
        logger.error(f"Failed to export Excel: {e}")
        return False
