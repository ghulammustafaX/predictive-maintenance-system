"""
Module 6 — LSTM RUL Prediction
Sub-module: Real-Time Inference Engine

Accepts a live feature window (already preprocessed by Module 4's
scaler, optionally enriched by Module 5's feature engineering) and
returns an RUL prediction in milliseconds. This is the bridge between
the offline-trained checkpoint and the live Kafka/InfluxDB streaming
pipeline (Modules 1-3).
"""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import torch

from ml.config import WINDOW_SIZE
from ml.models.lstm import LSTMConfig, RULLSTM


class RULInferenceEngine:
    """Loads a trained checkpoint once, then serves repeated predictions
    cheaply. Intended to be instantiated ONCE at FastAPI startup, not
    per-request."""

    def __init__(self, checkpoint_path: str | Path, device: str | None = None):
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        checkpoint = torch.load(checkpoint_path, map_location=self.device)
        self.config = LSTMConfig(**checkpoint["config"])
        self.model = RULLSTM(self.config).to(self.device)
        self.model.load_state_dict(checkpoint["model_state"])
        self.model.eval()

    def predict(self, window: np.ndarray) -> float:
        """`window` must be shape (WINDOW_SIZE, num_features), already
        scaled/feature-engineered to match the checkpoint's training
        input_size. Returns a single RUL prediction in cycles."""
        if window.shape != (WINDOW_SIZE, self.config.input_size):
            raise ValueError(
                f"Expected window shape ({WINDOW_SIZE}, {self.config.input_size}), "
                f"got {window.shape}. Did you apply the same scaler/feature "
                f"engineering used at training time?"
            )
        x = torch.from_numpy(window).float().unsqueeze(0).to(self.device)  # (1, window, features)
        with torch.no_grad():
            pred = self.model(x).item()
        return max(0.0, pred)  # RUL can't be negative

    def predict_batch(self, windows: np.ndarray) -> np.ndarray:
        """`windows` shape: (n, WINDOW_SIZE, num_features)."""
        x = torch.from_numpy(windows).float().to(self.device)
        with torch.no_grad():
            preds = self.model(x).cpu().numpy()
        return np.clip(preds, a_min=0.0, a_max=None)

    def benchmark_latency(self, n_calls: int = 100) -> dict:
        """Sanity-check the 'milliseconds' claim in the scope doc — run
        n_calls single-window predictions and report timing stats."""
        dummy = np.random.rand(WINDOW_SIZE, self.config.input_size).astype(np.float32)
        # Warm up (first call includes lazy CUDA/op initialization).
        self.predict(dummy)
        timings = []
        for _ in range(n_calls):
            t0 = time.perf_counter()
            self.predict(dummy)
            timings.append((time.perf_counter() - t0) * 1000)
        timings = np.array(timings)
        return {
            "mean_ms": float(timings.mean()),
            "p95_ms": float(np.percentile(timings, 95)),
            "max_ms": float(timings.max()),
        }
