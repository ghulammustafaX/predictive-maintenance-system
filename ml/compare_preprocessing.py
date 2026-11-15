"""Compare current preprocessing artifact roots without changing artifacts.

Usage:
    python -m ml.compare_preprocessing

The comparison uses the first complete 30-cycle window from each C-MAPSS
test dataset and applies the same explicitly selected transformation policy
to the original and A1 artifact roots. It then runs both transformed windows
through the existing raw checkpoint. The current artifact set is
``ml/saved/scalers``; ``ml/saved_v1_pre_leakage_fix`` is the preserved
baseline. This is a comparison of current artifact roots, not proof of the
historical preprocessing policy used to train that checkpoint. Historical
checkpoint/preprocessing provenance remains incomplete, and historical RMSE
must not be attributed to the A1 pairing; that pairing remains experimental
until evaluated.
"""

from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

import numpy as np

from ml.config import (
    CHECKPOINT_DIR,
    SCALER_DIR,
    STREAMING_ARTIFACT_DIR,
    SUB_DATASETS,
    WINDOW_SIZE,
)
from ml.data import cmapss_loader
from ml.data.preprocessing import (
    FEATURE_COLUMNS,
    apply_scaler,
    clip_outliers,
    handle_missing_values,
    load_scaler,
    load_training_stats,
)
from ml.models.inference import RULInferenceEngine


def _window(dataset_id: str) -> pd.DataFrame:
    frame = cmapss_loader.load_test(dataset_id)
    unit_id = frame["unit_id"].iloc[0]
    return frame[frame["unit_id"] == unit_id].sort_values("time_cycles").head(WINDOW_SIZE)


def _transform(frame, artifact_dir: Path) -> np.ndarray:
    stats = load_training_stats(frame.attrs["dataset_id"], artifact_dir)
    scaler = load_scaler(frame.attrs["dataset_id"], artifact_dir)
    prepared = handle_missing_values(
        frame.copy(),
        {column: values["median"] for column, values in stats.items()},
        backfill=False,
    )
    prepared = clip_outliers(prepared, 5.0, stats)
    return apply_scaler(prepared, scaler)[FEATURE_COLUMNS].to_numpy(dtype=np.float32)


def compare() -> dict[str, dict[str, float | int | list[float]]]:
    checkpoint = CHECKPOINT_DIR / "raw" / "lstm_raw_20260626T082955Z.pt"
    engine = RULInferenceEngine(checkpoint, device="cpu")
    results: dict[str, dict[str, float | int | list[float]]] = {}
    for dataset_id in SUB_DATASETS:
        frame = _window(dataset_id)
        frame.attrs["dataset_id"] = dataset_id
        original = _transform(frame, SCALER_DIR)
        a1 = _transform(frame, STREAMING_ARTIFACT_DIR)
        original_prediction = engine.predict(original)
        a1_prediction = engine.predict(a1)
        delta = np.abs(original - a1)
        results[dataset_id] = {
            "changed_values": int(np.count_nonzero(delta)),
            "max_abs_feature_delta": float(delta.max()),
            "mean_abs_feature_delta": float(delta.mean()),
            "original_prediction": float(original_prediction),
            "a1_prediction": float(a1_prediction),
            "prediction_delta": float(abs(original_prediction - a1_prediction)),
        }
    return results


def main() -> None:
    print(json.dumps(compare(), indent=2))


if __name__ == "__main__":
    main()
