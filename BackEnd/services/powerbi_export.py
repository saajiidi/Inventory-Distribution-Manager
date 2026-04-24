import pandas as pd
import io
from typing import Dict, Any, Tuple
from datetime import date, timedelta

import logging
logger = logging.getLogger(__name__)

def build_star_schema(data: Dict[str, Any], returns_df: pd.DataFrame) -> Tuple[bytes, str]:
    """Builds a Power BI optimized Star Schema from the dashboard data."""
    sales_df = data.get("sales_active", pd.DataFrame())
    
    output = io.BytesIO()
    
    # We will use ExcelWriter to write multiple sheets
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        
        # 1. Dim_Date
        if not sales_df.empty and 'date' in sales_df.columns:
            min_date = sales_df['date'].min()
            max_date = sales_df['date'].max()
        else:
            min_date = pd.to_datetime('2023-01-01')
            max_date = pd.to_datetime('today')
            
        dim_date = pd.DataFrame({'Date': pd.date_range(start=min_date, end=max_date)})
        dim_date['DateKey'] = dim_date['Date'].dt.strftime('%Y%m%d').astype(int)
        dim_date['Year'] = dim_date['Date'].dt.year
        dim_date['Month'] = dim_date['Date'].dt.month
        dim_date['MonthName'] = dim_date['Date'].dt.strftime('%B')
        dim_date['Quarter'] = dim_date['Date'].dt.quarter
        dim_date['DayOfWeek'] = dim_date['Date'].dt.day_name()
        dim_date['IsWeekend'] = dim_date['DayOfWeek'].isin(['Saturday', 'Sunday'])
        
        dim_date.to_excel(writer, sheet_name='Dim_Date', index=False)
        
        # 2. Dim_Product
        if not sales_df.empty:
            # We assume sales_df has 'sku', 'Category', 'product_name'
            prod_cols = []
            for col in ['sku', 'product_name', 'Category', 'product_type']:
                if col in sales_df.columns:
                    prod_cols.append(col)
                    
            if prod_cols:
                dim_product = sales_df[prod_cols].dropna(subset=[prod_cols[0]]).drop_duplicates()
                dim_product.reset_index(drop=True, inplace=True)
                dim_product.index.name = 'ProductKey'
                dim_product = dim_product.reset_index()
                dim_product.to_excel(writer, sheet_name='Dim_Product', index=False)
                
                # Merge ProductKey back to sales for Fact_Sales
                if 'sku' in prod_cols:
                    sales_df = sales_df.merge(dim_product[['ProductKey', 'sku']], on='sku', how='left')
                elif 'product_name' in prod_cols:
                    sales_df = sales_df.merge(dim_product[['ProductKey', 'product_name']], on='product_name', how='left')

        # 3. Dim_Customer
        if 'customer_key' in sales_df.columns:
            cust_cols = ['customer_key']
            for col in ['billing_city', 'billing_state', 'customer_type', 'segment']:
                if col in sales_df.columns:
                    cust_cols.append(col)
                    
            dim_customer = sales_df[cust_cols].dropna(subset=['customer_key']).drop_duplicates()
            dim_customer.reset_index(drop=True, inplace=True)
            dim_customer.index.name = 'CustomerKey'
            dim_customer = dim_customer.reset_index()
            dim_customer.to_excel(writer, sheet_name='Dim_Customer', index=False)
            
            sales_df = sales_df.merge(dim_customer[['CustomerKey', 'customer_key']], on='customer_key', how='left')
        elif 'billing_first_name' in sales_df.columns:
             dim_customer = sales_df[['billing_phone', 'billing_first_name', 'billing_city']].drop_duplicates(subset=['billing_phone'])
             dim_customer.reset_index(drop=True, inplace=True)
             dim_customer.index.name = 'CustomerKey'
             dim_customer = dim_customer.reset_index()
             dim_customer.to_excel(writer, sheet_name='Dim_Customer', index=False)
             
             sales_df = sales_df.merge(dim_customer[['CustomerKey', 'billing_phone']], on='billing_phone', how='left')

        # 4. Fact_Sales
        if not sales_df.empty:
            fact_cols = []
            
            if 'date' in sales_df.columns:
                sales_df['DateKey'] = sales_df['date'].dt.strftime('%Y%m%d').astype(int)
                fact_cols.append('DateKey')
            
            if 'order_id' in sales_df.columns: fact_cols.append('order_id')
            if 'CustomerKey' in sales_df.columns: fact_cols.append('CustomerKey')
            if 'ProductKey' in sales_df.columns: fact_cols.append('ProductKey')
            
            # Metrics
            for col in ['qty', 'item_revenue', 'order_total', 'discount_total', 'shipping_total']:
                if col in sales_df.columns:
                    fact_cols.append(col)
                    
            fact_sales = sales_df[fact_cols].copy()
            fact_sales.to_excel(writer, sheet_name='Fact_Sales', index=False)
            
        # 5. Fact_Returns
        if returns_df is not None and not returns_df.empty:
            fact_returns = returns_df.copy()
            if 'date' in fact_returns.columns:
                fact_returns['DateKey'] = pd.to_datetime(fact_returns['date']).dt.strftime('%Y%m%d').fillna(0).astype(int)
                
            cols_to_keep = [c for c in ['order_id', 'DateKey', 'issue_type', 'return_reason', 'partial_amount', 'courier'] if c in fact_returns.columns]
            fact_returns = fact_returns[cols_to_keep]
            fact_returns.to_excel(writer, sheet_name='Fact_Returns', index=False)

    return output.getvalue(), "PBISchema"
