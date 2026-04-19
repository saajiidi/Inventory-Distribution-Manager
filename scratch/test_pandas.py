import pandas as pd
import numpy as np

# Test empty DF with datetime col
df = pd.DataFrame(columns=['order_date', 'item_revenue'])
df['order_date'] = pd.to_datetime(df['order_date'])
df['item_revenue'] = pd.to_numeric(df['item_revenue'])

print(f"Dtype: {df['order_date'].dtype}")

try:
    daily_gross = df.groupby(df['order_date'].dt.date)['item_revenue'].sum().reset_index()
    print("Success empty groupby")
    print(daily_gross)
except Exception as e:
    print(f"Failed empty groupby: {e}")

# Test empty DF with object col (the old way)
df_obj = pd.DataFrame(columns=['order_date', 'item_revenue'])
print(f"Dtype Obj: {df_obj['order_date'].dtype}")
try:
    df_obj['order_date'].dt.date
except Exception as e:
    print(f"Expected failure for object dtype: {e}")
