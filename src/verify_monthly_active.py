import pandas as pd
from pathlib import Path
import sys

path = Path(__file__).resolve().parents[1] / 'exports' / 'csv' / 'orders.csv'
if not path.exists():
    print('orders.csv not found at', path)
    sys.exit(1)

df = pd.read_csv(path)
if df.empty:
    print('orders.csv is empty')
    sys.exit(0)

df['ym'] = df['date_id'].astype(str).str[:6]
s = df.groupby('ym')['customer_id'].nunique().sort_index()
print('months:', len(s))
print('min:', int(s.min()), 'max:', int(s.max()))
print(s.to_string())
