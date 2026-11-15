from __future__ import annotations

import json

import numpy as np
import pandas as pd

from ml.data.preprocessing import (
    FEATURE_COLUMNS,
    clip_outliers,
    fit_training_stats,
    handle_missing_values,
    load_training_stats,
)


def _frame() -> pd.DataFrame:
    columns = ["unit_id", "time_cycles", *FEATURE_COLUMNS]
    rows = []
    for unit_id in (1, 2):
        for cycle in (1, 2, 3):
            rows.append([unit_id, cycle, *([float(cycle)] * len(FEATURE_COLUMNS))])
    return pd.DataFrame(rows, columns=columns)


def test_missing_values_forward_fill_then_training_median() -> None:
    df = _frame()
    df.loc[(df["unit_id"] == 1) & (df["time_cycles"] == 2), "sensor_1"] = np.nan
    df["sensor_2"] = np.nan
    result = handle_missing_values(df, {"sensor_2": 99.0})
    assert result.loc[1, "sensor_1"] == 1.0
    assert result.loc[0, "sensor_2"] == 99.0


def test_training_stats_persist_mean_std_and_median(tmp_path, monkeypatch) -> None:
    import ml.data.preprocessing as preprocessing

    monkeypatch.setattr(preprocessing, "SCALER_DIR", tmp_path)
    stats = fit_training_stats(_frame(), "FD001")
    loaded = load_training_stats("FD001")
    assert set(loaded["sensor_1"]) == {"mean", "std", "median"}
    assert loaded == stats
    payload = json.loads((tmp_path / "scaler_FD001_stats.json").read_text())
    assert payload["dataset_id"] == "FD001"


def test_outlier_clipping_uses_persisted_training_stats() -> None:
    df = _frame()
    df.loc[0, "sensor_1"] = 1000.0
    stats = {column: {"mean": 1.0, "std": 2.0, "median": 1.0} for column in FEATURE_COLUMNS}
    result = clip_outliers(df, 5.0, stats)
    assert result.loc[0, "sensor_1"] == 11.0
