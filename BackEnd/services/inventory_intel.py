import pandas as pd
import numpy as np

class InventoryIntelligence:
    """Enterprise join logic for Supply Chain & Sales Affinity."""
    
    def __init__(self, sales_df: pd.DataFrame, stock_df: pd.DataFrame):
        self.sales_df = sales_df
        self.stock_df = stock_df
        
        # Robust column discovery for Sales
        self.prod_col = self._find_col(sales_df, ["item_name", "Product Name", "Item Name", "Product"])
        
        # Robust column discovery for Stock
        self.stock_prod_col = self._find_col(stock_df, ["Name", "Product Name", "Item Name", "Product"])
        self.stock_qty_col = self._find_col(stock_df, ["Stock Quantity", "Stock", "Quantity", "Qty"])

        # Pre-calculate stock quantities for fast lookups
        if not self.stock_df.empty and self.stock_prod_col in self.stock_df.columns and self.stock_qty_col in self.stock_df.columns:
            temp_stock = self.stock_df.copy()
            temp_stock[self.stock_qty_col] = pd.to_numeric(temp_stock[self.stock_qty_col], errors='coerce').fillna(0)
            
            # The stock_df (inventory) from inventory.py has _clean_name
            if "_clean_name" in temp_stock.columns:
                self.stock_lookup = temp_stock.groupby("_clean_name")[self.stock_qty_col].sum().to_dict()
            else: # Fallback if clean name not pre-calculated
                from BackEnd.core.categories import get_clean_product_name
                temp_stock["_clean_name"] = temp_stock[self.stock_prod_col].apply(get_clean_product_name)
                self.stock_lookup = temp_stock.groupby("_clean_name")[self.stock_qty_col].sum().to_dict()
        else:
            self.stock_lookup = {}

    def _find_col(self, df, candidates):
        cols = {str(c).lower().strip(): c for c in df.columns}
        for cand in candidates:
            if cand.lower() in cols:
                return cols[cand.lower()]
        # Fallback to first column if no match
        return df.columns[0] if not df.empty else "N/A"

    def calculate_bundle_fulfillment(self, top_pairs: list):
        """
        Calculates fulfillment potential for detected product bundles.
        top_pairs: list of dicts with {'A', 'B'}
        """
        results = []
        for pair in top_pairs:
            item_a = pair['A']
            item_b = pair['B']
            
            stock_a = pd.to_numeric(self.stock_df[self.stock_df[self.stock_prod_col] == item_a][self.stock_qty_col], errors='coerce').fillna(0).sum()
            stock_b = pd.to_numeric(self.stock_df[self.stock_df[self.stock_prod_col] == item_b][self.stock_qty_col], errors='coerce').fillna(0).sum()
            
            complete_sets = min(stock_a, stock_b)
            weak_link = item_a if stock_a < stock_b else item_b
            
            results.append({
                "Bundle": f"{item_a} + {item_b}",
                "Sets_Available": complete_sets,
                "Weak_Link": weak_link,
                "Balance_Ratio": min(stock_a, stock_b) / max(stock_a, stock_b) if max(stock_a, stock_b) > 0 else 0
            })
            
        