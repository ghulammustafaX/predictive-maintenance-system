"""
Module 6 — LSTM RUL Prediction
Sub-module: Baseline Comparisons

Benchmarks the LSTM against classical, non-sequential models (SVR,
Random Forest) to quantify the advantage (or lack thereof) of deep
learning for this task. These baselines see only the LAST cycle's
feature vector of each window (not the full 30-cycle sequence) — that's
the fairest classical-model comparison, since SVR/RF have no native way
to consume a sequence; feeding them a flattened 30-step window would
just be a much higher-dimensional version of the same single-snapshot
information without adding temporal modeling.

Usage:
    python -m ml.models.baselines --config raw
    python -m ml.models.baselines --config engineered
"""

from __future__ import annotations

import argparse
import json

import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR

from ml.build_combined_dataset import COMBINED_DIR
from ml.config import CHECKPOINT_DIR, RANDOM_SEED, SUB_DATASETS
from ml.models.train import rmse_mae

# SVR scales poorly with dataset size; subsample the combined training
# set for SVR only (Random Forest handles the full set fine).
SVR_TRAIN_SAMPLE = 20000


def _load_last_cycle(npz_path):
    data = np.load(npz_path)
    X_last = data["X"][:, -1, :]  # (n_windows, num_features)
    return X_last, data["y"]


def run_baselines(config_name: str) -> dict:
    data_dir = COMBINED_DIR / config_name
    X_train, y_train = _load_last_cycle(data_dir / "train.npz")
    print(f"\n=== Baseline Comparisons [{config_name}] — train windows: {len(y_train)}, "
          f"features: {X_train.shape[1]} ===")

    rng = np.random.default_rng(RANDOM_SEED)
    results: dict = {"config_name": config_name, "models": {}}

    # --- Random Forest ---
    rf = RandomForestRegressor(n_estimators=150, max_depth=12, random_state=RANDOM_SEED, n_jobs=-1)
    rf.fit(X_train, y_train)
    results["models"]["random_forest"] = _evaluate_per_fd(rf, data_dir)

    # --- SVR (subsampled — kernel SVMs scale ~O(n^2-n^3)) ---
    if len(X_train) > SVR_TRAIN_SAMPLE:
        idx = rng.choice(len(X_train), SVR_TRAIN_SAMPLE, replace=False)
        X_svr, y_svr = X_train[idx], y_train[idx]
    else:
        X_svr, y_svr = X_train, y_train
    svr = SVR(kernel="rbf", C=10.0, epsilon=1.0)
    svr.fit(X_svr, y_svr)
    results["models"]["svr"] = _evaluate_per_fd(svr, data_dir)

    ckpt_dir = CHECKPOINT_DIR / config_name
    ckpt_dir.mkdir(parents=True, exist_ok=True)
    out_path = ckpt_dir / "baselines_results.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Saved baseline results: {out_path}")
    return results


def _evaluate_per_fd(model, data_dir) -> dict:
    per_fd = {}
    for fd in SUB_DATASETS:
        test_path = data_dir / f"test_{fd}.npz"
        if not test_path.exists():
            continue
        X_test, y_test = _load_last_cycle(test_path)
        preds = model.predict(X_test)
        rmse, mae = rmse_mae(preds, y_test)
        per_fd[fd] = {"rmse": rmse, "mae": mae, "n_test_windows": len(y_test)}
        print(f"  {model.__class__.__name__:>20s} [{fd}]: RMSE={rmse:.3f}, MAE={mae:.3f}")
    return per_fd


def main() -> None:
    parser = argparse.ArgumentParser(description="Run classical baseline models for comparison")
    parser.add_argument("--config", choices=["raw", "engineered"], required=True)
    args = parser.parse_args()
    run_baselines(args.config)


if __name__ == "__main__":
    main()
