from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

RNG = np.random.default_rng(12)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, default=Path("data/raw"))
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    retail = pd.DataFrame(
        {
            "order_id": [f"O{i:06d}" for i in range(1, 2001)],
            "store_id": RNG.choice([f"S{i}" for i in range(1, 11)], 2000),
            "amount_usd": np.round(RNG.uniform(5, 250, 2000), 2),
        }
    )
    finance = pd.DataFrame(
        {
            "ledger_date": pd.date_range("2024-01-01", periods=30, freq="D").strftime("%Y-%m-%d"),
            "control_total_usd": np.round(RNG.uniform(40000, 65000, 30), 2),
        }
    )
    retail.to_csv(args.out_dir / "retail_orders.csv", index=False)
    finance.to_csv(args.out_dir / "finance_control_totals.csv", index=False)
    print(f"Wrote retail + finance raw data to {args.out_dir}")


if __name__ == "__main__":
    main()
