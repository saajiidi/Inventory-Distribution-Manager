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
            
            stock_a = self.stock_df[self.stock_df[self.stock_prod_col] == item_a][self.stock_qty_col].sum()
            stock_b = self.stock_df[self.stock_df[self.stock_prod_col] == item_b][self.stock_qty_col].sum()
            
            complete_sets = min(stock_a, stock_b)
            weak_link = item_a if stock_a < stock_b else item_b
            
            results.append({
                "Bundle": f"{item_a} + {item_b}",
                "Sets_Available": complete_sets,
                "Weak_Link": weak_link,
                "Balance_Ratio": min(stock_a, stock_b) / max(stock_a, stock_b) if max(stock_a, stock_b) > 0 else 0
            })
            
        return pd.DataFrame(results)

    def detect_orphan_stock(self, lift_threshold=1.2):
        """Finds items (Orphan Stock) where the affinity partner is OOS."""
        # 1. Get high-lift associations (simplified here, in practice use affinity_engine)
        # Assuming we have access to rule list...
        pass # To be integrated with affinity_engine

    def component_dependency_ratio(self, item_a: str, item_b: str):
        """Calculates how dependent Item A is on Item B for sales."""
        orders_a = self.sales_df[self.sales_df[self.prod_col] == item_a]['order_id'].unique()
        if len(orders_a) == 0: return 0.0
        
        orders_both = self.sales_df[
            (self.sales_df['order_id'].isin(orders_a)) & 
            (self.sales_df[self.prod_col] == item_b)
        ]['order_id'].nunique()
        
        return orders_both / len(orders_a)
