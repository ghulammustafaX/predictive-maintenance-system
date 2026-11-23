"""
Module 6 — LSTM RUL Prediction
Helper: thin torch Dataset wrapper around the windowed .npz files
produced by build_combined_dataset.py (and build_dataset.py /
build_features.py for single-sub-dataset use).
"""

from __future__ import annotations

import numpy as np
import torch
from torch.utils.data import Dataset


class WindowedNpzDataset(Dataset):
    def __init__(self, npz_path, *, expected_features=None):
        """Load windows; training opts into shape and finite-value validation.

        No feature semantics are inferred from legacy NPZ files.
        """
        with np.load(npz_path, allow_pickle=False) as data:
            if expected_features is not None and not {"X", "y"}.issubset(data.files):
                raise ValueError(f"Training dataset requires X and y: {npz_path}")
            X, y = data["X"], data["y"]
            if expected_features is not None:
                from ml.config import WINDOW_SIZE

                if (X.ndim != 3 or X.shape[1:] != (WINDOW_SIZE, expected_features)
                        or X.shape[0] == 0 or y.ndim != 1 or y.shape[0] != X.shape[0]
                        or X.dtype.kind not in 'iuf' or y.dtype.kind not in 'iuf'
                        or not np.isfinite(X).all() or not np.isfinite(y).all()):
                    raise ValueError(f"Invalid training windows or labels: {npz_path}")
                for key in ("unit_ids", "dataset_ids"):
                    if key in data and (data[key].ndim != 1 or len(data[key]) != len(y)):
                        raise ValueError(f"Invalid {key} length: {npz_path}")
            self.X = torch.from_numpy(X).float()
            self.y = torch.from_numpy(y).float()
            if expected_features is not None and (not torch.isfinite(self.X).all() or not torch.isfinite(self.y).all()):
                raise ValueError(f"Training values exceed float32 range: {npz_path}")

    def __len__(self) -> int:
        return len(self.y)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]
