"""Deterministic synthetic scaler fixtures for preprocessing unit tests.

These fixtures exercise the production transform code without pretending to
be the hash-validated production scaler binaries. The committed training
statistics JSON files are copied unchanged; MinMaxScaler instances are fitted
on small deterministic matrices derived from those statistics.
"""
from __future__ import annotations

import json
import pickle
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from ml.data.preprocessing import FEATURE_COLUMNS

DATASETS = ("FD001", "FD002", "FD003", "FD004")


def create_synthetic_preprocessing_artifacts(
    destination: Path,
    repository_root: Path,
) -> Path:
    source = repository_root / "ml" / "saved_v2_a1" / "scalers"
    target = destination / "scalers"
    target.mkdir(parents=True, exist_ok=True)

    for dataset in DATASETS:
        stats_source = source / f"scaler_{dataset}_stats.json"
        if not stats_source.is_file():
            raise FileNotFoundError(f"Tracked training statistics missing: {stats_source}")
        shutil.copyfile(stats_source, target / stats_source.name)
        payload = json.loads(stats_source.read_text(encoding="utf-8"))
        features = payload["features"]

        low = []
        high = []
        for column in FEATURE_COLUMNS:
            mean = float(features[column]["mean"])
            std = max(abs(float(features[column]["std"])), 1e-8)
            low.append(mean - std)
            high.append(mean + std)

        matrix = np.asarray([low, high], dtype=np.float64)
        scaler = MinMaxScaler(feature_range=(0, 1)).fit(
            pd.DataFrame(matrix, columns=FEATURE_COLUMNS)
        )
        with (target / f"scaler_{dataset}.pkl").open("wb") as stream:
            pickle.dump(scaler, stream, protocol=pickle.HIGHEST_PROTOCOL)

    return target
