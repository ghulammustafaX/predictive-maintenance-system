"""
Module 6 — LSTM RUL Prediction
Prep step: build a COMBINED training set across all four C-MAPSS
sub-datasets (FD001-FD004) for a single general model.

Why this needs its own builder (rather than reusing Module 5's output
directly): Module 5 selects the top-K engineered features PER
sub-dataset independently, so FD001's 30 selected engineered columns are
not the same columns as FD002's. Concatenating windows across
sub-datasets requires every window to have identical feature columns in
the same order, so this script:

  1. Re-runs preprocessing (load/clean/label/split/scale) per sub-dataset
     (cheap, deterministic — same as Module 4/5).
  2. Computes engineered features per sub-dataset WITHOUT per-dataset
     pruning/selection yet.
  3. Pools a sample from all four sub-datasets' engineered training data
     and runs correlation pruning + mutual-information selection ONCE,
     producing a single global engineered column list.
  4. Re-windows every sub-dataset's train/val/test using that one global
     column list, then concatenates train and val windows across all
     four sub-datasets. Test windows are kept SEPARATE per sub-dataset,
     so Module 6's evaluation can still report RMSE/MAE per FD001-FD004
     (matching how C-MAPSS results are reported in the literature) even
     though training itself is combined.

Output:
    ml/saved/combined/raw/{train,val}.npz                  (24 features)
    ml/saved/combined/engineered/{train,val}.npz            (24 + top_k)
    ml/saved/combined/engineered/test_<FD>.npz   (per sub-dataset, global cols)
    ml/saved/combined/raw/test_<FD>.npz          (per sub-dataset, raw cols)
    ml/saved/combined/global_engineered_columns.npy
"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from ml.build_features import _prep_split_scaled
from ml.config import ML_ROOT, SUB_DATASETS, WINDOW_SIZE
from ml.data import preprocessing as prep
from ml.features import feature_engineering as feateng

COMBINED_DIR = ML_ROOT / "saved" / "combined"
DEFAULT_TOP_K = 30
POOL_SAMPLE_PER_FD = 4000  # rows sampled per sub-dataset for global selection


def _save_windows(out_dir, filename, windows) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    np.savez(out_dir / filename, X=windows.X, y=windows.y, unit_ids=windows.unit_ids)


def build_combined(top_k: int = DEFAULT_TOP_K, sub_datasets: list[str] | None = None) -> None:
    sub_datasets = sub_datasets or SUB_DATASETS
    raw_cols = list(prep.FEATURE_COLUMNS)

    per_fd_scaled = {}     # FD -> (train_scaled, val_scaled, test_scaled)
    per_fd_engineered = {}  # FD -> (train_eng, val_eng, test_eng, kept_engineered_fd)

    print("=== Step 1/4: preprocessing + per-FD feature engineering ===")
    pooled_samples = []
    for fd in sub_datasets:
        train_scaled, val_scaled, test_scaled = _prep_split_scaled(fd)
        per_fd_scaled[fd] = (train_scaled, val_scaled, test_scaled)

        train_eng, kept_fd = feateng.engineer_features(train_scaled)
        val_eng, _ = feateng.engineer_features(val_scaled)
        test_eng, _ = feateng.engineer_features(test_scaled)
        per_fd_engineered[fd] = (train_eng, val_eng, test_eng, kept_fd)

        sample_n = min(POOL_SAMPLE_PER_FD, len(train_eng))
        pooled_samples.append(train_eng.sample(sample_n, random_state=42))
        print(f"  {fd}: train rows {len(train_eng)}, engineered candidates after "
              f"per-FD pruning: {len(kept_fd)}")

    print("\n=== Step 2/4: global engineered feature selection (pooled across all FDs) ===")
    pooled_df = pd.concat(pooled_samples, ignore_index=True)
    # Union of every sub-dataset's surviving engineered columns is the
    # candidate pool for global selection — a column only needs to have
    # survived correlation pruning in AT LEAST one sub-dataset to be
    # considered (some sensors are near-constant only under certain
    # operating conditions).
    candidate_union: list[str] = []
    seen = set()
    for _, _, _, kept_fd in per_fd_engineered.values():
        for c in kept_fd:
            if c not in seen:
                candidate_union.append(c)
                seen.add(c)

    global_selected = feateng.select_top_features(
        pooled_df, candidate_union, target_column="rul",
        top_k=min(top_k, len(candidate_union)),
    )
    global_engineered_columns = global_selected
    global_columns = raw_cols + global_engineered_columns
    print(f"  Candidate union across all FDs: {len(candidate_union)} -> "
          f"global selection: {len(global_engineered_columns)} engineered "
          f"-> final schema: {len(global_columns)} features "
          f"({len(raw_cols)} raw + {len(global_engineered_columns)} engineered)")

    COMBINED_DIR.mkdir(parents=True, exist_ok=True)
    np.save(COMBINED_DIR / "global_engineered_columns.npy", np.array(global_engineered_columns))

    print("\n=== Step 3/4: windowing with the global schema, per FD ===")
    raw_train_chunks, raw_val_chunks = [], []
    eng_train_chunks, eng_val_chunks = [], []

    for fd in sub_datasets:
        train_scaled, val_scaled, test_scaled = per_fd_scaled[fd]
        train_eng, val_eng, test_eng, _ = per_fd_engineered[fd]

        # Raw branch
        raw_train_w = prep.generate_windows(train_scaled, WINDOW_SIZE, raw_cols, False)
        raw_val_w = prep.generate_windows(val_scaled, WINDOW_SIZE, raw_cols, False)
        raw_test_w = prep.generate_windows(test_scaled, WINDOW_SIZE, raw_cols, True)
        raw_train_chunks.append(raw_train_w)
        raw_val_chunks.append(raw_val_w)
        _save_windows(COMBINED_DIR / "raw", f"test_{fd}.npz", raw_test_w)

        # Engineered branch (global column list — pad any FD missing a
        # globally-selected column with 0, since a column dropped by one
        # FD's per-FD pruning may still be globally useful for others)
        for df_ in (train_eng, val_eng, test_eng):
            for col in global_engineered_columns:
                if col not in df_.columns:
                    df_[col] = 0.0

        eng_train_w = prep.generate_windows(train_eng, WINDOW_SIZE, global_columns, False)
        eng_val_w = prep.generate_windows(val_eng, WINDOW_SIZE, global_columns, False)
        eng_test_w = prep.generate_windows(test_eng, WINDOW_SIZE, global_columns, True)
        eng_train_chunks.append(eng_train_w)
        eng_val_chunks.append(eng_val_w)
        _save_windows(COMBINED_DIR / "engineered", f"test_{fd}.npz", eng_test_w)

        print(f"  {fd}: raw train {raw_train_w.X.shape}, engineered train {eng_train_w.X.shape}")

    print("\n=== Step 4/4: concatenating combined train/val sets ===")

    def _concat(chunks):
        return (
            np.concatenate([c.X for c in chunks], axis=0),
            np.concatenate([c.y for c in chunks], axis=0),
            np.concatenate([c.unit_ids for c in chunks], axis=0),
        )

    for name, chunks_train, chunks_val, out_dir in [
        ("raw", raw_train_chunks, raw_val_chunks, COMBINED_DIR / "raw"),
        ("engineered", eng_train_chunks, eng_val_chunks, COMBINED_DIR / "engineered"),
    ]:
        X_tr, y_tr, u_tr = _concat(chunks_train)
        X_va, y_va, u_va = _concat(chunks_val)
        out_dir.mkdir(parents=True, exist_ok=True)
        np.savez(out_dir / "train.npz", X=X_tr, y=y_tr, unit_ids=u_tr)
        np.savez(out_dir / "val.npz", X=X_va, y=y_va, unit_ids=u_va)
        print(f"  {name}: combined train {X_tr.shape}, combined val {X_va.shape}")

    print("\nCombined dataset build complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build combined dataset across all C-MAPSS sub-datasets")
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    args = parser.parse_args()
    build_combined(top_k=args.top_k)


if __name__ == "__main__":
    main()
