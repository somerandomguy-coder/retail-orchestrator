import pandas as pd
from pathlib import Path

file_path = Path(r"..\data\raw\transactions_2026-10-01.parquet")
print(file_path)

df = pd.read_parquet(file_path)
print(df.head())
