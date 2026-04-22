import pandas as pd
from datetime import datetime
from FrontEnd.pages.cycle_analytics import prepare_cycle_orders, calculate_cycle_metrics

cycle_start = datetime(2026, 4, 2, 17, 0, 0)
cycle_end = datetime(2026, 4, 4, 17, 0, 0)

df = pd.DataFrame({
    'order_id': ['1001', '1001', '1002', '1003'],
    'order_date': [datetime(2026, 4, 4, 12, 0, 0), datetime(2026, 4, 4, 12, 0, 0), datetime(2026, 4, 2, 10, 0, 0), datetime(2026, 4, 4, 14, 0, 0)],
    'shipped_date': [pd.NaT, pd.NaT, datetime(2026, 4, 4, 9, 0, 0), pd.NaT],
    'order_status': ['processing', 'processing', 'completed', 'completed'],
    'qty': [1, 2, 1, 1],
    'item_name': ['Polo', 'Pant', 'Cap', 'Shoes'],
    'order_total': [1000, 1000, 700, 1200],
    'customer_name': ['Jane', 'Jane', 'John', 'Sara'],
    'city': ['Dhaka', 'Dhaka', 'Chittagong', 'Sylhet'],
    'source': ['woocommerce_api', 'woocommerce_api', 'woocommerce_api', 'woocommerce_api'],
})

orders = prepare_cycle_orders(df)
print('Orders columns:', orders.columns.tolist())
print()
print('Orders:')
for idx, row in orders.iterrows():
    print(f"  {row['order_id']}: status={row['status_bucket']}, metric_date={row['metric_date']}, order_date={row['order_date']}, shipped_date={row['shipped_date']}, qty={row['qty']}")
print()

print(f"cycle_start: {cycle_start}")
print(f"cycle_end: {cycle_end}")
print()

new_orders = orders[orders['status_bucket'] == 'new']
print(f"New orders: {len(new_orders)}")
for idx, row in new_orders.iterrows():
    in_range = cycle_start < row['metric_date'] <= cycle_end if pd.notna(row['metric_date']) else False
    print(f"  {row['order_id']}: metric_date={row['metric_date']}, in_range={in_range}")
