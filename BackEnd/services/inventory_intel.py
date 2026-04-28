import pandas as pd
import numpy as np
from BackEnd.core.categories import get_clean_product_name

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
            # The stock_df (inventory) from inventory.py has _clean_name
            if "_clean_name" in self.stock_df.columns:
                self.stock_lookup = self.stock_df.groupby("_clean_name")[self.stock_qty_col].sum().to_dict()
            else: # Fallback if clean name not pre-calculated
                temp_stock = self.stock_df.copy()
                temp_stock["_clean_name"] = temp_stock[self.stock_prod_col].apply(get_clean_product_name)
                self.stock_lookup = temp_stock.groupby("_clean_name")[self.stock_qty_col].sum().to_dict()
        else:
            self.stock_lookup = {}

    def detect_orphan_stock(self, min_support=0.005, min_lift=1.2):
        """Finds items (Orphan Stock) where the affinity partner is OOS."""
        try:
            from BackEnd.services.affinity_engine import MarketBasketEngine
        except ImportError:
            return pd.DataFrame()
            
        engine = MarketBasketEngine(self.sales_df)
        rules = engine.get_associations(min_support=min_support, min_lift=min_lift)
        
        if rules.empty:
            return pd.DataFrame()
            
        orphans = []
        for _, rule in rules.iterrows():
            # The affinity engine may use full names, so we clean them for stock lookup
            item_a = get_clean_product_name(rule['Antecedent'])
            item_b = get_clean_product_name(rule['Consequent'])
            
            stock_a = self.stock_lookup.get(item_a, 0)
            stock_b = self.stock_lookup.get(item_b, 0)
            
            # If A is in stock but B is out of stock, A is orphaned
            if stock_a > 0 and stock_b <= 0:
                orphans.append({"In_Stock_Item": item_a, "Stock_Qty": int(stock_a), "Missing_Partner": item_b, "Lift (Correlation)": round(rule['Lift'], 2), "Action": f"Restock {item_b} to unblock {item_a} sales"})
            # If B is in stock but A is out of stock, B is orphaned
            elif stock_b > 0 and stock_a <= 0:
                orphans.append({"In_Stock_Item": item_b, "Stock_Qty": int(stock_b), "Missing_Partner": item_a, "Lift (Correlation)": round(rule['Lift'], 2), "Action": f"Restock {item_a} to unblock {item_b} sales"})
                
        return pd.DataFrame(orphans).sort_values("Lift (Correlation)", ascending=False).drop_duplicates(subset=["In_Stock_Item"]) if orphans else pd.DataFrame()

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

    def component_dependency_ratio(self, item_a: str, item_b: str):
        """Calculates how dependent Item A is on Item B for sales."""
        orders_a = self.sales_df[self.sales_df[self.prod_col] == item_a]['order_id'].unique()
        if len(orders_a) == 0: return 0.0
        
        orders_both = self.sales_df[
            (self.sales_df['order_id'].isin(orders_a)) & 
            (self.sales_df[self.prod_col] == item_b)
        ]['order_id'].nunique()
        
        return orders_both / len(orders_a)
