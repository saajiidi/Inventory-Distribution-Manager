import pandas as pd
import numpy as np
from itertools import combinations
from collections import Counter

class MarketBasketEngine:
    """Core logic for discovering product affinities and association rules."""
    
    def __init__(self, sales_df: pd.DataFrame):
        self.df = sales_df.copy()
        # Find product column
        self.prod_col = "item_name" if "item_name" in self.df.columns else "Product Name"
        self.order_col = "order_id" if "order_id" in self.df.columns else "Order ID"
        
    def get_associations(self, min_support=0.01, min_lift=1.1):
        """Calculates association rules (Antecedent -> Consequent)."""
        if self.df.empty or self.prod_col not in self.df.columns:
            return pd.DataFrame()
            
        # Group by order to get baskets
        baskets = self.df.groupby(self.order_col)[self.prod_col].apply(set).tolist()
        total_orders = len(baskets)
        
        if total_orders == 0:
            return pd.DataFrame()
            
        # 1. Frequency of individual items
        item_counts = Counter()
        for basket in baskets:
            item_counts.update(basket)
            
        # 2. Frequency of pairs
        pair_counts = Counter()
        for basket in baskets:
            if len(basket) > 1:
                pair_counts.update(combinations(sorted(basket), 2))
                
        # 3. Calculate metrics
        rules = []
        for (item_a, item_b), count in pair_counts.items():
            support = count / total_orders
            if support < min_support:
                continue
                
            support_a = item_counts[item_a] / total_orders
            support_b = item_counts[item_b] / total_orders
            
            # Rule A -> B
            conf_a_to_b = count / item_counts[item_a]
            lift = conf_a_to_b / support_b
            
            if lift >= min_lift:
                rules.append({
                    "Antecedent": item_a,
                    "Consequent": item_b,
                    "Support": support,
                    "Confidence": conf_a_to_b,
                    "Lift": lift,
                    "Frequency": count
                })
        
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
