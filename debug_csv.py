# debug_csv.py
import pandas as pd

df = pd.read_csv("data/bronze/complaints_raw.csv.zip", nrows=5)

print(df.columns.tolist())
print(df.iloc[0].to_dict())
potential_names = [c for c in df.columns if 'narrative' in c.lower()]
print(f"Potential narrative columns found: {potential_names}")