from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest
import torch

from ml.config import STREAMING_ARTIFACT_DIR, WINDOW_SIZE
from ml.data.preprocessing import FEATURE_COLUMNS
from ml.models.compatibility import (
    CompatibilityError,
    validate_model_artifacts,
    validate_preprocessing_artifacts,
)
from ml.models.inference import RULInferenceEngine

CHECKPOINT = Path("ml/saved/checkpoints/raw/lstm_raw_20260626T082955Z.pt")


def _copy_artifacts(tmp_path: Path) -> Path:
    target = tmp_path / "scalers"
    shutil.copytree(STREAMING_ARTIFACT_DIR, target)
    return target


def _manifest(path: Path) -> dict:
    return json.loads((path / "preprocessing_manifest.json").read_text())


def _write_manifest(path: Path, payload: dict) -> None:
    (path / "preprocessing_manifest.json").write_text(json.dumps(payload))


def test_validated_model_and_artifacts_succeed() -> None:
    report = validate_model_artifacts(CHECKPOINT, STREAMING_ARTIFACT_DIR)
    assert report["schema_version"] == "raw-24-v1"
    assert report["window_size"] == WINDOW_SIZE
    assert report["validation"]["status"] == "accuracy_and_normal_input_parity_validated"


def test_compatibility_import_does_not_require_pandas() -> None:
    script = """
import sys

class BlockPandas:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "pandas" or fullname.startswith("pandas."):
            raise ModuleNotFoundError("pandas intentionally blocked")
        return None

sys.meta_path.insert(0, BlockPandas())
import ml.models.compatibility
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("field", ["checkpoint_sha256", "schema_version", "window_size"])
def test_incorrect_manifest_contract_fails(tmp_path: Path, field: str) -> None:
    artifacts = _copy_artifacts(tmp_path)
    payload = _manifest(artifacts)
    payload[field] = "wrong" if field != "window_size" else 31
    _write_manifest(artifacts, payload)
    with pytest.raises(CompatibilityError):
        validate_model_artifacts(CHECKPOINT, artifacts)


def test_incorrect_artifact_hash_fails(tmp_path: Path) -> None:
    artifacts = _copy_artifacts(tmp_path)
    payload = _manifest(artifacts)
    payload["artifact_sha256"]["FD001"]["scaler"] = "0" * 64
    _write_manifest(artifacts, payload)
    with pytest.raises(CompatibilityError, match="hash mismatch"):
        validate_preprocessing_artifacts(artifacts)


def test_incorrect_feature_order_fails(tmp_path: Path) -> None:
    artifacts = _copy_artifacts(tmp_path)
    payload = _manifest(artifacts)
    payload["feature_order"] = list(reversed(FEATURE_COLUMNS))
    _write_manifest(artifacts, payload)
    with pytest.raises(CompatibilityError, match="feature order"):
        validate_preprocessing_artifacts(artifacts)


def test_missing_dataset_scaler_fails(tmp_path: Path) -> None:
    artifacts = _copy_artifacts(tmp_path)
    (artifacts / "scaler_FD004.pkl").unlink()
    with pytest.raises(CompatibilityError, match="Missing FD dataset scaler"):
        validate_preprocessing_artifacts(artifacts)


def test_non_finite_inference_inputs_are_rejected(tmp_path: Path) -> None:
    config = {"input_size": 2, "hidden_size": 4, "num_layers": 1, "dropout": 0.0}
    from ml.models.lstm import LSTMConfig, RULLSTM

    model = RULLSTM(LSTMConfig(**config))
    checkpoint = tmp_path / "model.pt"
    torch.save({"model_state": model.state_dict(), "config": config}, checkpoint)
    engine = RULInferenceEngine(checkpoint, device="cpu")
    window = np.zeros((WINDOW_SIZE, 2), dtype=np.float32)
    window[0, 0] = np.nan
    with pytest.raises(ValueError, match="non-finite"):
        engine.predict(window)
    with pytest.raises(ValueError, match="non-finite"):
        engine.predict_batch(window[None, ...])
