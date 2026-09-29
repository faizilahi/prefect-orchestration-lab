import json, sys, hashlib
from pathlib import Path
import pandas as pd
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from flow_def import FLOW
from partial_load import buggy_retry_load, clean_load
from gate import idempotency_gate
DATA, OUT = ROOT / "data", ROOT / "output"
OUT.mkdir(parents=True, exist_ok=True)

def batch_key(df):
    return hashlib.sha256(",".join(df["sale_id"].astype(str)).encode()).hexdigest()[:16]

def main():
    src = pd.read_csv(DATA / "source_sales.csv")
    partial = buggy_retry_load(src)
    clean = clean_load(src)
    gate_bad = idempotency_gate(len(src), len(partial), batch_key(src), "mismatched")
    gate_good = idempotency_gate(len(src), len(clean), batch_key(src), batch_key(clean))
    partial.to_csv(OUT / "dest_partial.csv", index=False)
    clean.to_csv(OUT / "dest_clean.csv", index=False)
    summary = {"flow": FLOW["name"], "partial_dest_rows": int(len(partial)),
               "clean_dest_rows": int(len(clean)), "gate_blocked_partial": not gate_bad["gate_passed"],
               "gate_passed_clean": gate_good["gate_passed"]}
    pd.DataFrame([summary]).to_csv(OUT / "flow_summary.csv", index=False)
    print(json.dumps(summary, indent=2))
if __name__ == "__main__":
    main()
