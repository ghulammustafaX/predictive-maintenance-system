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
    def __init__(self, npz_path):
        data = np.load(npz_path)
        self.X = torch.from_numpy(data["X"]).float()
        self.y = torch.from_numpy(data["y"]).float()

    def __len__(self) -> int:
        return len(self.y)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]
