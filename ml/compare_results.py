"""
Module 6 — LSTM RUL Prediction
Report: pulls together LSTM (raw vs engineered) and baseline (SVR,
Random Forest) results into a single comparison table per sub-dataset.
This is the artifact to drop straight into the IEEE paper's results
section.

Usage:
    python -m ml.compare_results
"""

from __future__ import annotations

import glob
import json

from ml.config import CHECKPOINT_DIR, SUB_DATASETS


def _latest_metadata(config_name: str) -> dict | None:
    pattern = str(CHECKPOINT_DIR / config_name / "lstm_*_metadata.json")
    files = sorted(glob.glob(pattern))
    if not files:
        return None
    with open(files[-1]) as f:
        return json.load(f)


def _baseline_results(config_name: str) -> dict | None:
    path = CHECKPOINT_DIR / config_name / "baselines_results.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def main() -> None:
    rows = []
    for config_name in ["raw", "engineered"]:
        lstm_meta = _latest_metadata(config_name)
        baseline = _baseline_results(config_name)

        for fd in SUB_DATASETS:
            row = {"sub_dataset": fd, "feature_config": config_name}
            if lstm_meta and fd in lstm_meta.get("per_fd_test_results", {}):
                r = lstm_meta["per_fd_test_results"][fd]
                row["lstm_rmse"] = round(r["rmse"], 2)
                row["lstm_mae"] = round(r["mae"], 2)
            if baseline:
                if fd in baseline["models"].get("random_forest", {}):
                    r = baseline["models"]["random_forest"][fd]
                    row["rf_rmse"] = round(r["rmse"], 2)
                if fd in baseline["models"].get("svr", {}):
                    r = baseline["models"]["svr"][fd]
                    row["svr_rmse"] = round(r["rmse"], 2)
            rows.append(row)

    if not rows or all(len(r) <= 2 for r in rows):
        print("No results found yet — run ml.models.train and ml.models.baselines first.")
        return

    headers = ["sub_dataset", "feature_config", "lstm_rmse", "lstm_mae", "rf_rmse", "svr_rmse"]
    print(f"\n{'  '.join(h.upper().ljust(14) for h in headers)}")
    print("-" * 14 * len(headers))
    for row in rows:
        print("  ".join(str(row.get(h, "-")).ljust(14) for h in headers))


if __name__ == "__main__":
    main()
