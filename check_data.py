import sqlite3, pandas as pd
conn = sqlite3.connect('data/trends.db')
df = pd.read_sql('SELECT * FROM product_snapshots LIMIT 3', conn)
print('=== COLUMNS ===')
print(list(df.columns))
print()
for col in df.columns:
    val = df[col].iloc[0] if len(df) > 0 else 'empty'
    print(f'{col}: {val}')
print()
total = pd.read_sql('SELECT COUNT(*) as c FROM product_snapshots', conn)['c'][0]
old_p = pd.read_sql('SELECT COUNT(*) as c FROM product_snapshots WHERE old_price > 0', conn)['c'][0]
roz_cat = pd.read_sql("SELECT COUNT(*) as c FROM product_snapshots WHERE rozetka_category != ''", conn)['c'][0]
cats = pd.read_sql('SELECT DISTINCT category FROM product_snapshots', conn)['category'].tolist()
print(f'Total: {total}')
print(f'old_price > 0: {old_p}')
print(f'rozetka_category filled: {roz_cat}')
print(f'Categories: {cats}')
conn.close()
