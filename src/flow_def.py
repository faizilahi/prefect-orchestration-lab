FLOW = {
    "name": "load_daily_sales",
    "tasks": ["extract", "validate", "load", "certify"],
    "load_retries": 2,
}
