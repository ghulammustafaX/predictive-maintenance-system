from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import torch

from ml.config import WINDOW_SIZE
from ml.models.baselines import _evaluate_per_fd
from ml.models.inference import RULInferenceEngine
from ml.models.lstm import LSTMConfig, RULLSTM


def test_model_forward_pass_shape() -> None:
    model = RULLSTM(LSTMConfig(input_size=4, hidden_size=8, num_layers=2, dropout=0.1))
    result = model(torch.randn(3, WINDOW_SIZE, 4))
    assert result.shape == (3,)


def test_checkpoint_save_load_round_trip(tmp_path: Path) -> None:
    config = LSTMConfig(input_size=4, hidden_size=8, num_layers=2, dropout=0.1)
    model = RULLSTM(config)
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer)
    path = tmp_path / "model.pt"
    torch.save({
        "model_state": model.state_dict(),
        "config": config.to_dict(),
        "optimizer_state": optimizer.state_dict(),
        "scheduler_state": scheduler.state_dict(),
    }, path)
    loaded = torch.load(path, map_location="cpu", weights_only=False)
    restored = RULLSTM(LSTMConfig(**loaded["config"]))
    restored.load_state_dict(loaded["model_state"])
    model.eval()
    restored.eval()
    assert torch.equal(
        model(torch.ones(1, WINDOW_SIZE, 4)),
        restored(torch.ones(1, WINDOW_SIZE, 4)),
    )


def test_inference_output_non_negative_and_shape(tmp_path: Path) -> None:
    config = LSTMConfig(input_size=4, hidden_size=8, num_layers=2, dropout=0.1)
    model = RULLSTM(config)
    with torch.no_grad():
        for parameter in model.parameters():
            parameter.fill_(-1)
    checkpoint = tmp_path / "model.pt"
    torch.save({"model_state": model.state_dict(), "config": config.to_dict()}, checkpoint)
    engine = RULInferenceEngine(checkpoint, device="cpu")
    window = np.ones((WINDOW_SIZE, 4), dtype=np.float32)
    assert engine.predict(window) >= 0
    assert engine.predict_batch(window[None, ...]).shape == (1,)


def test_baselines_runner_evaluates_per_dataset(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    for fd in ("FD001", "FD002", "FD003", "FD004"):
        np.savez(data_dir / f"test_{fd}.npz", X=np.ones((2, WINDOW_SIZE, 2)), y=np.ones(2))

    class ConstantModel:
        def predict(self, values: np.ndarray) -> np.ndarray:
            return np.ones(len(values))

    results = _evaluate_per_fd(ConstantModel(), data_dir)
    assert set(results) == {"FD001", "FD002", "FD003", "FD004"}
    assert all(item["rmse"] == 0 for item in results.values())
