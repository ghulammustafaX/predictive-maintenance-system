"""
Module 5 — Feature Engineering
Sub-modules: Rolling Statistics Computation, Trend Indicators, Sensor
Correlation Analysis, Health Index Construction, Feature Selection,
Feature Store.

Design notes
------------
- All functions here operate on the per-cycle DataFrame produced by
  Module 4 *after* scaling, so engineered features are derived from the
  same [0,1]-normalized sensor values the LSTM will eventually see.
- Rolling/trend features are computed per engine unit (grouped), never
  across unit boundaries, since consecutive rows from two different
  engines are not a real time series.
- This module deliberately does NOT decide whether to use the engineered
  features — that choice (raw-only vs. raw+engineered) is made by the
  caller (build_features.py), so the same engineering logic supports the
  ablation comparison for the IEEE paper.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.feature_selection import mutual_info_regression

from ml.config import FEATURE_STORE_DIR, SENSOR_COLUMNS, SETTING_COLUMNS

ROLLING_WINDOW = 5  # cycles, for rolling mean/std/min/max


# ---------------------------------------------------------------------------
# Sub-module: Rolling Statistics Computation
# ---------------------------------------------------------------------------
def add_rolling_statistics(df: pd.DataFrame, window: int = ROLLING_WINDOW) -> pd.DataFrame:
    """Rolling mean, std, min, max per sensor channel, computed within
    each engine unit's own cycle history (min_periods=1 so early cycles
    still get a value instead of NaN)."""
    df = df.sort_values(["unit_id", "time_cycles"]).copy()
    grouped = df.groupby("unit_id")[SENSOR_COLUMNS]

    for stat_name, agg_fn in [("mean", "mean"), ("std", "std"), ("min", "min"), ("max", "max")]:
        rolled = grouped.transform(lambda s: s.rolling(window, min_periods=1).agg(agg_fn))
        rolled.columns = [f"{c}_roll_{stat_name}" for c in SENSOR_COLUMNS]
        df = pd.concat([df, rolled], axis=1)

    # Rolling std on a single-value window is NaN; fill with 0 (no observed variance yet).
    std_cols = [f"{c}_roll_std" for c in SENSOR_COLUMNS]
    df[std_cols] = df[std_cols].fillna(0.0)
    return df


# ---------------------------------------------------------------------------
# Sub-module: Trend Indicators
# ---------------------------------------------------------------------------
def add_trend_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """First-order difference (cycle-over-cycle delta) per sensor,
    capturing rate-of-change/degradation velocity. First cycle of each
    unit has no prior reading, so its delta is set to 0."""
    df = df.sort_values(["unit_id", "time_cycles"]).copy()
    deltas = df.groupby("unit_id")[SENSOR_COLUMNS].diff().fillna(0.0)
    deltas.columns = [f"{c}_delta" for c in SENSOR_COLUMNS]
    return pd.concat([df, deltas], axis=1)


# ---------------------------------------------------------------------------
# Sub-module: Sensor Correlation Analysis
# ---------------------------------------------------------------------------
def find_correlated_columns(df: pd.DataFrame, columns: list[str], threshold: float = 0.95) -> list[str]:
    """Identify columns that are highly correlated with an
    already-kept column, so one of each pair can be dropped. Also flags
    near-constant (effectively zero-variance) columns, which carry no
    predictive signal and only add noise to the LSTM input."""
    sub = df[columns]
    variances = sub.var()
    # Widened from 1e-8: sensors with extremely low but nonzero variance
    # still trigger divide-by-zero warnings in corr() due to floating
    # point underflow in the standard deviation, without carrying any
    # real signal. 1e-6 catches those too.
    near_constant = variances[variances < 1e-6].index.tolist()

    with np.errstate(invalid="ignore", divide="ignore"):
        corr = sub.drop(columns=near_constant, errors="ignore").corr().abs()
    corr = corr.fillna(0.0)
    to_drop: list[str] = list(near_constant)
    seen: set[str] = set(near_constant)

    for col in corr.columns:
        if col in seen:
            continue
        correlated_with = corr.index[(corr[col] > threshold) & (corr.index != col)].tolist()
        for other in correlated_with:
            if other not in seen:
                to_drop.append(other)
                seen.add(other)

    return to_drop


# ---------------------------------------------------------------------------
# Sub-module: Health Index Construction
# ---------------------------------------------------------------------------
def add_health_index(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sensor channels into a single composite degradation
    score per cycle. Sensors are signed by their correlation with RUL
    within the training data so each contributes in a consistent
    "higher index = healthier" direction, then averaged. Simple by
    design — this is a interpretable baseline feature, not a learned
    embedding; the LSTM itself is where the real modeling happens."""
    df = df.copy()
    # Exclude zero-variance sensors entirely rather than letting them
    # contribute a constant offset to every row's health index.
    valid_sensors = [c for c in SENSOR_COLUMNS if df[c].var() > 1e-6]

    if "rul" in df.columns and valid_sensors:
        with np.errstate(invalid="ignore", divide="ignore"):
            correlations = df[valid_sensors].corrwith(df["rul"]).fillna(0.0)
        signs = correlations.apply(lambda c: 1.0 if c >= 0 else -1.0)
    else:
        signs = pd.Series(1.0, index=valid_sensors)

    if valid_sensors:
        signed = df[valid_sensors].mul(signs, axis=1)
        df["health_index"] = signed.mean(axis=1)
    else:
        df["health_index"] = 0.0
    return df


# ---------------------------------------------------------------------------
# Sub-module: Feature Selection
# ---------------------------------------------------------------------------
def select_top_features(
    df: pd.DataFrame,
    candidate_columns: list[str],
    target_column: str = "rul",
    top_k: int = 30,
    sample_size: int = 5000,
) -> list[str]:
    """Rank candidate engineered columns by mutual information with RUL
    and keep the top_k. Mutual information (rather than plain
    correlation) is used because it captures non-linear relationships,
    which matter here since degradation is rarely linear in raw sensor
    units."""
    sample = df if len(df) <= sample_size else df.sample(sample_size, random_state=42)
    X = sample[candidate_columns].fillna(0.0)
    y = sample[target_column]

    scores = mutual_info_regression(X, y, random_state=42)
    ranked = sorted(zip(candidate_columns, scores), key=lambda t: t[1], reverse=True)
    return [col for col, _ in ranked[:top_k]]


# ---------------------------------------------------------------------------
# Sub-module: Feature Store
# ---------------------------------------------------------------------------
def save_feature_set(df: pd.DataFrame, sub_dataset: str, split: str, config: str) -> None:
    """Cache an engineered feature DataFrame so repeated training runs
    don't need to recompute rolling stats / trend / health index from
    scratch every time."""
    out_dir = FEATURE_STORE_DIR / sub_dataset
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / f"{split}_{config}.parquet", index=False)


def load_feature_set(sub_dataset: str, split: str, config: str) -> pd.DataFrame:
    path = FEATURE_STORE_DIR / sub_dataset / f"{split}_{config}.parquet"
    return pd.read_parquet(path)


# ---------------------------------------------------------------------------
# Orchestration: build the full engineered feature set for one DataFrame
# ---------------------------------------------------------------------------
def engineer_features(df: pd.DataFrame, correlation_threshold: float = 0.95) -> tuple[pd.DataFrame, list[str]]:
    """Run all engineering sub-modules in sequence and return the
    enriched DataFrame plus the final list of engineered feature column
    names (after correlation-based pruning)."""
    df = add_rolling_statistics(df)
    df = add_trend_indicators(df)
    df = add_health_index(df)

    engineered_columns = (
        [f"{c}_roll_mean" for c in SENSOR_COLUMNS]
        + [f"{c}_roll_std" for c in SENSOR_COLUMNS]
        + [f"{c}_roll_min" for c in SENSOR_COLUMNS]
        + [f"{c}_roll_max" for c in SENSOR_COLUMNS]
        + [f"{c}_delta" for c in SENSOR_COLUMNS]
        + ["health_index"]
    )

    to_drop = find_correlated_columns(df, engineered_columns, threshold=correlation_threshold)
    kept_columns = [c for c in engineered_columns if c not in to_drop]
    return df, kept_columns
