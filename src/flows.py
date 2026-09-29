"""Prefect flows orchestrating retail ingest + finance control totals."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

try:
    from prefect import flow, get_run_logger, task  # type: ignore

    _PREFECT_OK = True
except Exception:  # pragma: no cover — broken/partial installs
    flow = task = None
    _PREFECT_OK = False


def _retail_ingest_impl(raw: Path, out: Path) -> Path:
    df = pd.read_csv(raw)
    summary = df.groupby("store_id", as_index=False)["amount_usd"].sum()
    out.mkdir(parents=True, exist_ok=True)
    dest = out / "retail_store_totals.csv"
    summary.to_csv(dest, index=False)
    return dest


def _finance_control_impl(raw: Path, out: Path, retail_totals: Path) -> Path:
    fin = pd.read_csv(raw)
    retail_sum = pd.read_csv(retail_totals)["amount_usd"].sum()
    fin["retail_rollups_usd"] = retail_sum
    fin["variance_usd"] = fin["control_total_usd"] - retail_sum / len(fin)
    out.mkdir(parents=True, exist_ok=True)
    dest = out / "finance_reconciliation.csv"
    fin.to_csv(dest, index=False)
    return dest


if task is not None:

    @task(retries=2, retry_delay_seconds=1, log_prints=True)
    def retail_ingest_task(raw: Path, out: Path) -> Path:
        logger = get_run_logger()
        dest = _retail_ingest_impl(raw, out)
        logger.info("Retail ingest complete: %s", dest)
        return dest

    @task(retries=2, retry_delay_seconds=1, log_prints=True)
    def finance_control_task(raw: Path, out: Path, retail_totals: Path) -> Path:
        logger = get_run_logger()
        dest = _finance_control_impl(raw, out, retail_totals)
        logger.info("Finance control complete: %s", dest)
        return dest

    @flow(name="dual-pipeline-orchestration-lab")
    def dual_pipeline_flow() -> None:
        raw = ROOT / "data" / "raw"
        processed = ROOT / "data" / "processed"
        rt = retail_ingest_task(raw / "retail_orders.csv", processed)
        finance_control_task(raw / "finance_control_totals.csv", processed, rt)


def run_prefect_flow() -> None:
    if not _PREFECT_OK or flow is None:
        raise ImportError("Prefect not installed")
    dual_pipeline_flow()


if __name__ == "__main__":
    run_prefect_flow()
