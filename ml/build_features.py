"""
Module 5 — Feature Engineering
Runner script: rebuilds the Module 4 per-cycle pipeline (load -> clean ->
label -> split -> scale), then branches into TWO parallel outputs per
sub-dataset:

    raw        - same 24 features as Module 4 (3 settings + 21 sensors)
    engineered - raw 24 features + selected rolling/trend/health-index
                 features (Sensor Correlation Analysis drops redundant
                 ones before Feature Selection picks the top-K by mutual
                 information with RUL)

Both are windowed and saved separately, so Module 6 can train an LSTM on
each and report an ablation comparison (raw vs. engineered) in the IEEE
paper.

Usage:
    python -m ml.build_features                # all 4 sub-datasets
    python -m ml.build_features --sub FD001     # single sub-dataset
    python -m ml.build_features --top-k 20      # fewer engineered features
"""

from __future__ import annotations

import argparse

import numpy as np

from ml.config import (
    OUTLIER_Z_THRESHOLD,
    SETTING_COLUMNS,
    SUB_DATASETS,
    WINDOW_SIZE,
)
from ml.data import cmapss_loader as loader
from ml.data import preprocessing as prep
from ml.features import feature_engineering as feateng

DEFAULT_TOP_K = 30


def _prep_split_scaled(sub_dataset: str):
    """Reproduce Module 4 steps 1-6 (load through scaling) and return the
    three scaled per-cycle DataFrames: train, val, test."""
    train_raw = loader.load_train(sub_dataset)
    test_raw = loader.load_test(sub_dataset)
    true_rul = loader.load_rul(sub_dataset)

    train_clean = prep.clip_outliers(prep.handle_missing_values(train_raw), OUTLIER_Z_THRESHOLD)
    test_clean = prep.clip_outliers(prep.handle_missing_values(test_raw), OUTLIER_Z_THRESHOLD)

    train_labeled = prep.assign_rul_train(train_clean)
    test_labeled = prep.assign_rul_test(test_clean, true_rul)

    train_split, val_split = prep.split_by_unit(train_labeled)

    scaler = prep.fit_scaler(train_split, sub_dataset)
    train_scaled = prep.apply_scaler(train_split, scaler)
    val_scaled = prep.apply_scaler(val_split, scaler)
    test_scaled = prep.apply_scaler(test_labeled, scaler)
    return train_scaled, val_scaled, test_scaled


def _save_windows(out_dir, split_name: str, windows) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    np.savez(
        out_dir / f"{split_name}.npz",
        X=windows.X, y=windows.y, unit_ids=windows.unit_ids,
    )


def build_for_sub_dataset(sub_dataset: str, top_k: int = DEFAULT_TOP_K) -> None:
    from ml.config import FEATURE_STORE_DIR, PROCESSED_DIR

    print(f"\n=== Building Module 5 features for {sub_dataset} ===")
    train_scaled, val_scaled, test_scaled = _prep_split_scaled(sub_dataset)
    raw_feature_columns = list(prep.FEATURE_COLUMNS)

    # --- Raw-only branch (identical to Module 4 output) ---
    raw_dir = PROCESSED_DIR / sub_dataset / "raw"
    for split_name, split_df in [("train", train_scaled), ("val", val_scaled), ("test", test_scaled)]:
        last_only = split_name == "test"
        windows = prep.generate_windows(split_df, WINDOW_SIZE, raw_feature_columns, last_only)
        _save_windows(raw_dir, split_name, windows)
    print(f"  Raw-only windows saved to {raw_dir}")

    # --- Engineered branch ---
    # Engineer on train first to learn which engineered columns survive
    # correlation pruning + feature selection, then apply the SAME
    # column list to val/test (never re-select per split — that would
    # leak information and break reproducibility between splits).
    train_eng, kept_engineered = feateng.engineer_features(train_scaled)
    # Only ENGINEERED columns compete for the top_k budget — raw settings/
    # sensors are always kept regardless of MI rank, so including them in
    # the ranking pool just steals slots from engineered features without
    # changing what ends up in the final set.
    if kept_engineered:
        selected_engineered = feateng.select_top_features(
            train_eng, kept_engineered, target_column="rul",
            top_k=min(top_k, len(kept_engineered)),
        )
    else:
        selected_engineered = []
    final_columns = list(dict.fromkeys(raw_feature_columns + selected_engineered))

    print(f"  Engineered candidates: {len(kept_engineered) + len(raw_feature_columns)} -> "
          f"kept after correlation pruning: {len(kept_engineered)} -> "
          f"final feature set: {len(final_columns)} "
          f"({len(raw_feature_columns)} raw + {len(selected_engineered)} engineered)")

    val_eng, _ = feateng.engineer_features(val_scaled)
    test_eng, _ = feateng.engineer_features(test_scaled)

    eng_dir = PROCESSED_DIR / sub_dataset / "engineered"
    for split_name, split_df in [("train", train_eng), ("val", val_eng), ("test", test_eng)]:
        last_only = split_name == "test"
        windows = prep.generate_windows(split_df, WINDOW_SIZE, final_columns, last_only)
        _save_windows(eng_dir, split_name, windows)
    print(f"  Engineered windows saved to {eng_dir}")

    # Feature Store: cache the selected column list + raw engineered train df
    feateng.save_feature_set(train_eng[["unit_id", "time_cycles"] + final_columns + ["rul"]],
                              sub_dataset, "train", "engineered")
    np.save(FEATURE_STORE_DIR / sub_dataset / "selected_columns.npy", np.array(final_columns))


def main() -> None:
    parser = argparse.ArgumentParser(description="Module 5 feature engineering pipeline")
    parser.add_argument("--sub", choices=SUB_DATASETS, default=None,
                         help="Build a single sub-dataset only (default: all four)")
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K,
                         help="Number of engineered features to keep after selection")
    args = parser.parse_args()

    targets = [args.sub] if args.sub else SUB_DATASETS
    for sd in targets:
        build_for_sub_dataset(sd, args.top_k)

    print("\nModule 5 feature engineering complete.")


if __name__ == "__main__":
    main()
