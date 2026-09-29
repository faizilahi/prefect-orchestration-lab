# Flow Retry That Hid a Partial Load

[Faiz Elahi](https://www.linkedin.com/in/faizilahi) — [pendataco.com](https://pendataco.com) — [github.com/faizilahi](https://github.com/faizilahi)

Synthetic data only. No vendor-customer employment claim.

Prefect-style flow `load_daily_sales` retried a failed DB write and marked the
run **success** while the first attempt had already inserted **60%** of the
batch. Without an idempotency gate, destination rows ballooned to **16,000**
against a **10,000**-row source.

## The flow

Tasks: extract → validate → load → certify. Retry policy: 2 retries on load.

## The partial load

Attempt 1 wrote rows 1–6000 then timed out. Attempt 2 wrote 1–10000 again.
Net destination: **16000** rows.

## The gate

`src/gate.py` requires destination count == source count and a batch
idempotency key. After truncate-and-reload with the gate, count is **10000**.

```powershell
pip install -r requirements.txt
python scripts/generate_synthetic_data.py
python src/run_flow.py
```
