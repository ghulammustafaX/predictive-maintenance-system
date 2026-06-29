"""
Module 4 — Data Preprocessing
Sub-modules: Missing Value Handler, Outlier Detection & Removal,
Min-Max Normalization, Sliding Window Generator, RUL Label Assignment,
Train/Validation Split.

Design notes
------------
- C-MAPSS is a clean simulated dataset, so missing-value handling is a
  lightweight safety net, not heavy imputation.
- Normalization is fit on TRAIN data only and reused (via MinMaxScaler
  pickle) on test/inference data, to avoid data leakage.
- RUL labels use the standard piecewise-linear scheme: RUL is capped at
  RUL_CAP, since an engine's degradation is assumed negligible far from
  failure (this is the convention used across published C-MAPSS work).
- Train/validation split is done by engine UNIT ID, never by row, since
  splitting rows would leak information across time within the same
  engine's run-to-failure sequence.
"""

from __future__ import annotations

import pickle
from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from ml.config import (
    RANDOM_SEED,
    RUL_CAP,
    SCALER_DIR,
    SENSOR_COLUMNS,
    SETTING_COLUMNS,
    VALIDATION_UNIT_FRACTION,
    WINDOW_SIZE,
)

FEATURE_COLUMNS = SETTING_COLUMNS + SENSOR_COLUMNS


# ---------------------------------------------------------------------------
# Sub-module: Missing Value Handler
# ---------------------------------------------------------------------------
def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Forward-fill missing sensor/setting readings per engine unit, then
    back-fill any leading gaps. C-MAPSS rarely has true gaps, but live
    Kafka/InfluxDB ingestion (Module 2/3) can occasionally drop a reading,
    so this must also work on streamed data, not just the static files."""
    df = df.sort_values(["unit_id", "time_cycles"]).copy()
    df[FEATURE_COLUMNS] = (
        df.groupby("unit_id")[FEATURE_COLUMNS]
        .apply(lambda g: g.ffill().bfill())
        .reset_index(drop=True)
    )
    remaining_na = df[FEATURE_COLUMNS].isna().sum().sum()
    if remaining_na:
        # Entire unit missing a column — fall back to global column median
        # rather than dropping the unit outright.
        df[FEATURE_COLUMNS] = df[FEATURE_COLUMNS].fillna(df[FEATURE_COLUMNS].median())
    return df


# ---------------------------------------------------------------------------
# Sub-module: Outlier Detection & Removal
# ---------------------------------------------------------------------------
def clip_outliers(df: pd.DataFrame, z_threshold: float) -> pd.DataFrame:
    """Clip (not drop) sensor values beyond `z_threshold` standard
    deviations from each sensor's column mean. Clipping is preferred over
    row removal so we never break a unit's time-series continuity, which
    the Sliding Window Generator and LSTM both depend on."""
    df = df.copy()
    for col in SENSOR_COLUMNS:
        mean, std = df[col].mean(), df[col].std()
        if std == 0 or np.isnan(std):
            continue  # constant sensor, nothing to clip
        lower, upper = mean - z_threshold * std, mean + z_threshold * std
        df[col] = df[col].clip(lower, upper)
    return df


# ---------------------------------------------------------------------------
# Sub-module: Min-Max Normalization
# ---------------------------------------------------------------------------
def fit_scaler(train_df: pd.DataFrame, sub_dataset: str) -> MinMaxScaler:
    """Fit a MinMaxScaler on TRAIN data only and persist it to disk so the
    exact same transform can be reused for test data and for live
    inference windows coming off the Kafka/InfluxDB pipeline."""
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaler.fit(train_df[FEATURE_COLUMNS])
    scaler_path = SCALER_DIR / f"scaler_{sub_dataset}.pkl"
    with open(scaler_path, "wb") as f:
        pickle.dump(scaler, f)
    return scaler


def load_scaler(sub_dataset: str) -> MinMaxScaler:
    scaler_path = SCALER_DIR / f"scaler_{sub_dataset}.pkl"
    with open(scaler_path, "rb") as f:
        return pickle.load(f)


def apply_scaler(df: pd.DataFrame, scaler: MinMaxScaler) -> pd.DataFrame:
    df = df.copy()
    df[FEATURE_COLUMNS] = scaler.transform(df[FEATURE_COLUMNS])
    return df


# ---------------------------------------------------------------------------
# Sub-module: RUL Label Assignment
# ---------------------------------------------------------------------------
def assign_rul_train(df: pd.DataFrame, rul_cap: int = RUL_CAP) -> pd.DataFrame:
    """For TRAIN data, RUL at each row = (unit's final cycle) - (current
    cycle), since training engines run all the way to failure. Capped
    using the standard piecewise-linear scheme."""
    df = df.copy()
    max_cycle = df.groupby("unit_id")["time_cycles"].transform("max")
    df["rul"] = (max_cycle - df["time_cycles"]).clip(upper=rul_cap)
    return df


def assign_rul_test(df: pd.DataFrame, true_rul: pd.Series, rul_cap: int = RUL_CAP) -> pd.DataFrame:
    """For TEST data, sequences are truncated before failure, so the true
    remaining life at the END of each sequence is given by `true_rul`
    (from RUL_FD00X.txt). RUL at each row = true_rul[unit] + (unit's final
    observed cycle - current cycle)."""
    df = df.copy()
    max_cycle = df.groupby("unit_id")["time_cycles"].transform("max")
    unit_true_rul = df["unit_id"].map(true_rul)
    df["rul"] = (unit_true_rul + (max_cycle - df["time_cycles"])).clip(upper=rul_cap)
    return df


# ---------------------------------------------------------------------------
# Sub-module: Sliding Window Generator
# ---------------------------------------------------------------------------
@dataclass
class WindowedDataset:
    X: np.ndarray  # shape: (num_windows, window_size, num_features)
    y: np.ndarray  # shape: (num_windows,) — RUL at the last cycle of each window
    unit_ids: np.ndarray  # shape: (num_windows,) — which engine unit each window belongs to


def generate_windows(
    df: pd.DataFrame,
    window_size: int = WINDOW_SIZE,
    feature_columns: list[str] | None = None,
    last_window_only: bool = False,
) -> WindowedDataset:
    """Convert per-cycle rows into fixed-length sliding windows for LSTM
    consumption. `last_window_only=True` is used for TEST data, matching
    the standard C-MAPSS RUL benchmark protocol (one prediction per test
    engine, using its most recent `window_size` cycles).

    Engines with fewer than `window_size` cycles are left-padded by
    repeating their first row, so short test sequences still produce a
    usable window instead of being silently dropped.
    """
    feature_columns = feature_columns or FEATURE_COLUMNS
    X_chunks, y_chunks, unit_chunks = [], [], []

    for unit_id, group in df.groupby("unit_id"):
        group = group.sort_values("time_cycles")
        values = group[feature_columns].to_numpy(dtype=np.float32)
        ruls = group["rul"].to_numpy(dtype=np.float32)

        if len(values) < window_size:
            pad_count = window_size - len(values)
            pad = np.repeat(values[:1], pad_count, axis=0)
            values = np.vstack([pad, values])
            ruls = np.concatenate([np.repeat(ruls[:1], pad_count), ruls])

        if last_window_only:
            X_chunks.append(values[-window_size:][np.newaxis, :, :])
            y_chunks.append(ruls[-1:])
            unit_chunks.append(np.array([unit_id]))
        else:
            # Vectorized sliding windows via stride tricks instead of a
            # per-window Python loop — this is the part that made FD002/
            # FD004 (tens of thousands of rows) crawl.
            n_windows = len(values) - window_size + 1
            num_features = values.shape[1]
            windows = np.lib.stride_tricks.sliding_window_view(
                values, window_shape=window_size, axis=0
            )  # shape: (n_windows, num_features, window_size)
            windows = np.transpose(windows, (0, 2, 1))  # -> (n_windows, window_size, num_features)
            X_chunks.append(windows)
            y_chunks.append(ruls[window_size - 1 :])
            unit_chunks.append(np.full(n_windows, unit_id))

    return WindowedDataset(
        X=np.concatenate(X_chunks, axis=0).astype(np.float32),
        y=np.concatenate(y_chunks, axis=0).astype(np.float32),
        unit_ids=np.concatenate(unit_chunks, axis=0),
    )


# ---------------------------------------------------------------------------
# Sub-module: Train/Validation Split
# ---------------------------------------------------------------------------
def split_by_unit(
    df: pd.DataFrame,
    val_fraction: float = VALIDATION_UNIT_FRACTION,
    seed: int = RANDOM_SEED,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split by engine UNIT ID (not by row) so no engine's cycles appear
    in both train and validation — prevents temporal leakage."""
    rng = np.random.default_rng(seed)
    unit_ids = df["unit_id"].unique()
    rng.shuffle(unit_ids)

    n_val = max(1, int(len(unit_ids) * val_fraction))
    val_units = set(unit_ids[:n_val])

    val_df = df[df["unit_id"].isin(val_units)]
    train_df = df[~df["unit_id"].isin(val_units)]
    return train_df, val_df
