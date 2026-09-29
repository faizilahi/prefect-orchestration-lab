from pathlib import Path
import pandas as pd
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"; DATA.mkdir(parents=True, exist_ok=True)
pd.DataFrame({
    "sale_id": [f"S{i:05d}" for i in range(10000)],
    "amount": [round(10 + (i % 90), 2) for i in range(10000)],
    "sale_date": "2024-05-20",
}).to_csv(DATA / "source_sales.csv", index=False)
print("source 10000")
