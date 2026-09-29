"""Fallback orchestrator if Prefect is unavailable — same task graph, plain Python."""
from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd

LOG = logging.getLogger("local_orchestrator")
ROOT = Path(__file__).resolve().parents[1]


def with_retry(fn, retries=3, delay=0.5):
    last = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 — teaching lab
            last = exc
            LOG.warning("Attempt %s failed: %s", attempt, exc)
            time.sleep(delay)
    raise last  # type: ignore[misc]


def retail_ingest(raw: Path, out: Path) -> Path:
    df = pd.read_csv(raw)
    summary = df.groupby("store_id", as_index=False)["amount_usd"].sum()
    out.mkdir(parents=True, exist_ok=True)
    dest = out / "retail_store_totals.csv"
    summary.to_csv(dest, index=False)
    LOG.info("Retail ingest wrote %s rows", len(summary))
    return dest


def finance_control(raw: Path, out: Path, retail_totals: Path) -> Path:
    fin = pd.read_csv(raw)
    retail_sum = pd.read_csv(retail_totals)["amount_usd"].sum()
    fin["retail_rollups_usd"] = retail_sum
    fin["variance_usd"] = fin["control_total_usd"] - retail_sum / len(fin)
    out.mkdir(parents=True, exist_ok=True)
    dest = out / "finance_reconciliation.csv"
    fin.to_csv(dest, index=False)
    LOG.info("Finance control wrote %s days", len(fin))
    return dest


def run_all() -> None:
    logging.basicConfig(level=logging.INFO)
    raw = ROOT / "data" / "raw"
    processed = ROOT / "data" / "processed"
    rt = with_retry(lambda: retail_ingest(raw / "retail_orders.csv", processed))
    with_retry(lambda: finance_control(raw / "finance_control_totals.csv", processed, rt))
    print("Local orchestrator completed both pipelines.")


if __name__ == "__main__":
    run_all()
