import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main() -> None:
    try:
        from flows import run_prefect_flow

        run_prefect_flow()
        print("MODE: prefect")
    except Exception as exc:  # noqa: BLE001
        print(f"Prefect path failed ({exc}); falling back to local orchestrator.")
        from local_orchestrator import run_all

        run_all()
        print("MODE: local_orchestrator")


if __name__ == "__main__":
    main()
