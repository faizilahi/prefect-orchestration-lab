from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "images"
OUT.mkdir(parents=True, exist_ok=True)
p = ROOT / "data" / "processed" / "finance_reconciliation.csv"
if p.exists():
    df = pd.read_csv(p)
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(df["ledger_date"], df["variance_usd"], marker="o")
    ax.set_title("Finance control variance (orchestration output)")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(OUT / "variance.png", dpi=120)
    plt.close(fig)
