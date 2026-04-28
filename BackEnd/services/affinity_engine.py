import pandas as pd
import numpy as np
from itertools import combinations
from collections import Counter
from BackEnd.utils.sales_schema import ensure_sales_schema

class MarketBasketEngine:
    """Core logic for discovering product affinities and association rules."""
    
    def __init__(self, sales_df: pd.DataFrame):
        self.df = ensure_sales_schema(sales_df).copy()
        # Find product column
        self.prod_col = "item_name"
        self.order_col = "order_id"
        
    def get_associations(self, min_support=0.005, min_lift=1.1):
        """Calculates association rules (Antecedent -> Consequent)."""
        empty_df = pd.DataFrame(columns=["Antecedent", "Consequent", "Support", "Confidence", "Lift", "Frequency"])
        
        if self.df.empty or self.prod_col not in self.df.columns:
            return empty_df
            
        total_orders = self.df[self.order_col].nunique()
        if total_orders == 0:
            return empty_df
            
        # 1. Base Basket Setup & Item Frequencies
        basket_df = self.df[[self.order_col, self.prod_col]].drop_duplicates()
        item_counts = basket_df[self.prod_col].value_counts()
        
        # 2. Vectorized Self-Join to get pairs directly
        pairs = pd.merge(basket_df, basket_df, on=self.order_col)
        pairs = pairs[pairs[f"{self.prod_col}_x"] != pairs[f"{self.prod_col}_y"]]
        
        if pairs.empty:
            return empty_df
            
        # Count pair frequencies
        pair_counts = pairs.groupby([f"{self.prod_col}_x", f"{self.prod_col}_y"]).size().reset_index(name="Frequency")
        pair_counts.rename(columns={f"{self.prod_col}_x": "Antecedent", f"{self.prod_col}_y": "Consequent"}, inplace=True)
        
        # 3. Calculate Metrics
        pair_counts["Support"] = pair_counts["Frequency"] / total_orders
        rules = pair_counts[pair_counts["Support"] >= min_support].copy()
        
        if rules.empty:
            return empty_df
            
        rules["Support_A"] = rules["Antecedent"].map(item_counts) / total_orders
        rules["Support_B"] = rules["Consequent"].map(item_counts) / total_orders
        
        rules["Confidence"] = rules["Support"] / rules["Support_A"]
        rules["Lift"] = rules["Confidence"] / rules["Support_B"]
        
        rules = rules[rules["Lift"] >= min_lift]
        rules.drop(columns=["Support_A", "Support_B"], inplace=True)
        
        if rules.empty:
            return empty_df

        return pd.DataFrame(rules).sort_values("Lift", ascending=False)

    def get_attachment_rate(self, target_product: str):
        """Calculates the attachment rate KPI for a specific product."""
        order_ids_with_target = self.df[self.df[self.prod_col] == target_product][self.order_col].unique()
        if len(order_ids_with_target) == 0:
            return 0.0
            
        # Orders containing the target
        df_target_orders = self.df[self.df[self.order_col].isin(order_ids_with_target)]
        
        # Count other products in these orders
        attachments = df_target_orders[df_target_orders[self.prod_col] != target_product][self.prod_col].value_counts()
        
        if attachments.empty:
            return 0.0
            
        # Top attached item rate
        top_item = attachments.index[0]
        rate = attachments.iloc[0] / len(order_ids_with_target)
        
        return {
            "target": target_product,
            "top_attachment": top_item,
            "rate": rate,
            "orders_count": len(order_ids_with_target)
        }
