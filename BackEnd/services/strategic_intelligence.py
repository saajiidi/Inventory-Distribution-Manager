import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List

def detect_business_anomalies(sales_df: pd.DataFrame, returns_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Detects operational risks, courier drops, and revenue leakage markers."""
    anomalies = []
    
    if sales_df.empty:
        return anomalies

    # 1. Revenue Leakage: "Ghost" Orders (Stale Pendings)
    # Orders created > 5 days ago still in 'pending' or 'on-hold'
    today = datetime.now()
    if 'order_date' in sales_df.columns:
        stale_cutoff = today - timedelta(days=5)
        stale_orders = sales_df[
            (sales_df['order_date'] < stale_cutoff) & 
            (sales_df['order_status'].str.lower().isin(['pending', 'on-hold', 'pending-payment']))
        ]
        if not stale_orders.empty:
            leakage = stale_orders['item_revenue'].sum()
            anomalies.append({
                "level": "CRITICAL",
                "category": "Revenue Leakage",
                "title": f"৳{leakage:,.0f} stuck in Stale Orders",
                "description": f"{len(stale_orders['order_id'].unique())} orders are older than 5 days but not yet processed.",
                "action": "Review Pending queue in WooCommerce."
            })

    # 2. Inventory Velocity Risk (Stock-out Prediction)
    if 'qty' in sales_df.columns and 'sku' in sales_df.columns:
        # Simple velocity: total qty sold in last 7 days / 7
        recent_sales = sales_df[sales_df['order_date'] > (today - timedelta(days=7))]
        velocity = recent_sales.groupby('sku')['qty'].sum() / 7
        
        # Note: We don't have 'stock_quantity' in the current sales_df usually, 
        # but if we did, we'd flag SKUs where stock / velocity < 2 days.
        # For now, we flag SKUs with extreme surges.
        avg_velocity = velocity.mean()
        surging_skus = velocity[velocity > (avg_velocity * 3)]
        if not surging_skus.empty:
            anomalies.append({
                "level": "WARNING",
                "category": "Inventory Surge",
                "title": f"High Velocity Surge detected for {len(surging_skus)} SKUs",
                "description": f"Top items like {surging_skus.index[0]} are selling 3x faster than average.",
                "action": "Check stock levels immediately."
            })

    # 3. Courier Performance Drops
    if not returns_df.empty and 'courier' in returns_df.columns:
        # Check return rate per courier in last 14 days
        returns_df['date'] = pd.to_datetime(returns_df['date'])
        recent_returns = returns_df[returns_df['date'] > (today - timedelta(days=14))]
        
        if not recent_returns.empty:
            courier_stats = recent_returns.groupby('courier').size()
            avg_returns = courier_stats.mean()
            poor_couriers = courier_stats[courier_stats > (avg_returns * 1.5)]
            
            for courier, count in poor_couriers.items():
                anomalies.append({
                    "level": "WARNING",
                    "category": "Logistics Risk",
                    "title": f"Abnormal Returns from {courier}",
                    "description": f"Return volume is {(count/avg_returns):.1f}x higher than courier average.",
                    "action": f"Investigate delivery handling for {courier}."
                })

    return anomalies

def generate_executive_narrative(sales_df: pd.DataFrame, returns_df: pd.DataFrame, current_rev: float, prev_rev: float) -> List[str]:
    """Generates strategy-focused bullet points for the CEO briefing."""
    narrative = []
    
    # 1. Growth Context
    delta_pct = ((current_rev - prev_rev) / prev_rev * 100) if prev_rev > 0 else 0
    
    if delta_pct > 0:
        phrase = "trending **higher**" if delta_pct < 100 else "**surging**"
        narrative.append(f"📈 **Momentum:** Revenue is {phrase} by **{delta_pct:.1f}%** compared to the prior period.")
    elif delta_pct < 0:
        narrative.append(f"📉 **Alert:** Revenue has dipped by **{abs(delta_pct):.1f}%**. Suggest reviewing recent marketing spend.")
    else:
        narrative.append(f"⚖️ **Stablity:** Revenue is holding steady with 0% variance from the previous window.")

    # 2. Category Intelligence
    if 'Category' in sales_df.columns:
        top_cat = sales_df.groupby('Category')['item_revenue'].sum().idxmax()
        narrative.append(f"💎 **Hero Category:** **{top_cat}** continues to anchor your net sales, contributing the highest volume.")

    # 3. Efficiency Tip
    if not returns_df.empty:
        total_ret = len(returns_df)
        partial_ret = len(returns_df[returns_df['issue_type'] == 'Partial'])
        pct_partial = (partial_ret / total_ret * 100) if total_ret > 0 else 0
        if pct_partial > 15:
            narrative.append(f"💡 **Efficiency Idea:** High 'Partial' order volume ({pct_partial:.1f}%) suggests customers are hesitant. Consider post-purchase reassurance calls.")

    return narrative

def calculate_rfm_churn_risk(sales_df: pd.DataFrame) -> pd.DataFrame:
    """Computes RFM scores and identifies Churn Risk for VIPs."""
    if sales_df.empty or 'customer_key' not in sales_df.columns:
        return pd.DataFrame()

    today = datetime.now()
    
    # Aggregate by customer
    rfm = sales_df.groupby('customer_key').agg({
        'order_date': lambda x: (today - x.max()).days, # Recency
        'order_id': 'nunique',                         # Frequency
        'item_revenue': 'sum'                          # Monetary
    }).reset_index()
    
    rfm.columns = ['customer_key', 'recency', 'frequency', 'monetary']
    
    # Define VIPs (e.g., spent > 10,000 or bought > 3 times)
    is_vip = (rfm['monetary'] > 10000) | (rfm['frequency'] >= 3)
    rfm['status'] = 'Standard'
    rfm.loc[is_vip, 'status'] = 'VIP'
    
    # Churn Risk: VIP and Recency > 30 days
    rfm['risk_level'] = 'Low'
    rfm.loc[(rfm['status'] == 'VIP') & (rfm['recency'] > 30), 'risk_level'] = 'High (Churn Risk)'
    rfm.loc[(rfm['status'] == 'VIP') & (rfm['recency'] > 60), 'risk_level'] = 'CRITICAL (Lost VIP?)'
    
    return rfm[rfm['status'] == 'VIP'].sort_values('recency', ascending=False)
