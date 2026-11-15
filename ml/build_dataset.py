"""
Module 4 — Data Preprocessing
Runner script: builds the full preprocessing pipeline end-to-end for all
four C-MAPSS sub-datasets (FD001-FD004) and saves windowed tensors +
fitted scalers to disk, ready for Module 5 (Feature Engineering) and
Module 6 (LSTM training).

Usage:
    python -m ml.build_dataset                # all 4 sub-datasets
    python -m ml.build_dataset --sub FD001     # single sub-dataset
"""

from __future__ import annotations

import argparse

import numpy as np

from ml.config import OUTLIER_Z_THRESHOLD, PROCESSED_DIR, SUB_DATASETS, WINDOW_SIZE
from ml.data import cmapss_loader as loader
from ml.data import preprocessing as prep


def build_for_sub_dataset(sub_dataset: str) -> None:
    print(f"\n=== Building Module 4 pipeline for {sub_dataset} ===")

    # 1. Dataset Loader
    train_raw = loader.load_train(sub_dataset)
    test_raw = loader.load_test(sub_dataset)
    true_rul = loader.load_rul(sub_dataset)
    print(f"  Loaded train: {train_raw.shape}, test: {test_raw.shape}, "
          f"RUL entries: {len(true_rul)}")

    # 2. Missing Value Handler and RUL labels
    train_labeled = prep.assign_rul_train(prep.handle_missing_values(train_raw))
    test_labeled = prep.assign_rul_test(
        prep.handle_missing_values(test_raw), true_rul
    )

    # 3. Train/Validation Split (by unit ID before fitting any statistics)
    train_split, val_split = prep.split_by_unit(train_labeled)
    stats = prep.fit_training_stats(train_split, sub_dataset)
    medians = {column: values["median"] for column, values in stats.items()}
    train_split = prep.clip_outliers(
        prep.handle_missing_values(train_split, medians),
        OUTLIER_Z_THRESHOLD,
        stats,
    )
    val_split = prep.clip_outliers(
        prep.handle_missing_values(val_split, medians),
        OUTLIER_Z_THRESHOLD,
        stats,
    )
    test_labeled = prep.clip_outliers(
        prep.handle_missing_values(test_labeled, medians),
        OUTLIER_Z_THRESHOLD,
        stats,
    )
    print(f"  Train units: {train_split['unit_id'].nunique()}, "
          f"Val units: {val_split['unit_id'].nunique()}, "
          f"Test units: {test_labeled['unit_id'].nunique()}")

    # 4. Min-Max Normalization (fit on train split only)
    scaler = prep.fit_scaler(train_split, sub_dataset)
    train_scaled = prep.apply_scaler(train_split, scaler)
    val_scaled = prep.apply_scaler(val_split, scaler)
    test_scaled = prep.apply_scaler(test_labeled, scaler)

    # 5. Sliding Window Generator
    train_windows = prep.generate_windows(train_scaled, WINDOW_SIZE, last_window_only=False)
    val_windows = prep.generate_windows(val_scaled, WINDOW_SIZE, last_window_only=False)
    test_windows = prep.generate_windows(test_scaled, WINDOW_SIZE, last_window_only=True)

    print(f"  Windows -> train: {train_windows.X.shape}, "
          f"val: {val_windows.X.shape}, test: {test_windows.X.shape}")

    # Persist
    out_dir = PROCESSED_DIR / sub_dataset
    out_dir.mkdir(parents=True, exist_ok=True)
    np.savez(
        out_dir / "train.npz",
        X=train_windows.X, y=train_windows.y, unit_ids=train_windows.unit_ids,
    )
    np.savez(
        out_dir / "val.npz",
        X=val_windows.X, y=val_windows.y, unit_ids=val_windows.unit_ids,
    )
    np.savez(
        out_dir / "test.npz",
        X=test_windows.X, y=test_windows.y, unit_ids=test_windows.unit_ids,
    )
    print(f"  Saved to {out_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Module 4 preprocessing pipeline")
    parser.add_argument(
        "--sub", choices=SUB_DATASETS, default=None,
        help="Build a single sub-dataset only (default: all four)",
    )
    args = parser.parse_args()

    targets = [args.sub] if args.sub else SUB_DATASETS
    for sd in targets:
        build_for_sub_dataset(sd)

    print("\nModule 4 preprocessing complete.")


if __name__ == "__main__":
    main()
