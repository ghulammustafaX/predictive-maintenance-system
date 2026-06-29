"""
Module 4 — Data Preprocessing
Sub-module: Dataset Loader

Reads and parses NASA C-MAPSS FD001-FD004 dataset files: training,
testing, and RUL ground-truth files. This is the offline counterpart to
Module 1's Sensor Stream Emulator — Module 1 replays this same data live
through MQTT/Kafka, while this loader reads it directly from disk for
fast, deterministic model training.
"""

from __future__ import annotations

import pandas as pd

from ml.config import ALL_COLUMNS, RAW_DATA_DIR, SUB_DATASETS


def _read_space_delimited(path) -> pd.DataFrame:
    """C-MAPSS .txt files are whitespace-delimited with trailing spaces
    and no header row."""
    df = pd.read_csv(path, sep=r"\s+", header=None, engine="python")
    # Some files have 2 trailing empty columns from trailing whitespace.
    df = df.iloc[:, : len(ALL_COLUMNS)]
    df.columns = ALL_COLUMNS
    return df


def load_train(sub_dataset: str) -> pd.DataFrame:
    """Load train_FD00X.txt. Contains full run-to-failure sequences."""
    _validate_sub_dataset(sub_dataset)
    path = RAW_DATA_DIR / f"train_{sub_dataset}.txt"
    return _read_space_delimited(path)


def load_test(sub_dataset: str) -> pd.DataFrame:
    """Load test_FD00X.txt. Contains truncated sequences (RUL must be
    supplied separately via load_rul)."""
    _validate_sub_dataset(sub_dataset)
    path = RAW_DATA_DIR / f"test_{sub_dataset}.txt"
    return _read_space_delimited(path)


def load_rul(sub_dataset: str) -> pd.Series:
    """Load RUL_FD00X.txt — one RUL ground-truth value per test engine
    unit, in unit-ID order (1, 2, 3, ...)."""
    _validate_sub_dataset(sub_dataset)
    path = RAW_DATA_DIR / f"RUL_{sub_dataset}.txt"
    rul = pd.read_csv(path, sep=r"\s+", header=None, engine="python")
    rul.columns = ["rul"]
    rul.index = rul.index + 1  # unit IDs are 1-indexed
    rul.index.name = "unit_id"
    return rul["rul"]


def load_all_train(sub_datasets: list[str] | None = None) -> dict[str, pd.DataFrame]:
    """Load train data for multiple sub-datasets at once. Returns a dict
    keyed by sub-dataset name (e.g. 'FD001') rather than concatenating,
    since unit IDs are not globally unique across sub-datasets and each
    sub-dataset has different operating-condition complexity."""
    sub_datasets = sub_datasets or SUB_DATASETS
    return {sd: load_train(sd) for sd in sub_datasets}


def load_all_test(sub_datasets: list[str] | None = None) -> dict[str, tuple[pd.DataFrame, pd.Series]]:
    """Load test data + RUL ground truth for multiple sub-datasets."""
    sub_datasets = sub_datasets or SUB_DATASETS
    return {sd: (load_test(sd), load_rul(sd)) for sd in sub_datasets}


def _validate_sub_dataset(sub_dataset: str) -> None:
    if sub_dataset not in SUB_DATASETS:
        raise ValueError(
            f"Unknown sub-dataset '{sub_dataset}'. Expected one of {SUB_DATASETS}."
        )
