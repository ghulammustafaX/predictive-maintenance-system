"""Compatibility checks for the validated raw Module 6 deployment."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ml.config import (
    SENSOR_COLUMNS,
    SETTING_COLUMNS,
    STREAMING_ARTIFACT_DIR,
    SUB_DATASETS,
    WINDOW_SIZE,
)

FEATURE_COLUMNS = SETTING_COLUMNS + SENSOR_COLUMNS

SCHEMA_VERSION = "raw-24-v1"
REQUIRED_MANIFEST_FIELDS = (
    "artifact_version",
    "datasets",
    "feature_order",
    "schema_version",
    "window_size",
    "missing_value_policy",
    "outlier_policy",
    "scaler_policy",
    "checkpoint_sha256",
    "artifact_sha256",
    "validation",
)


class CompatibilityError(ValueError):
    """Raised when model and preprocessing artifacts cannot be used together."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_manifest(artifact_dir: Path) -> dict[str, Any]:
    path = artifact_dir / "preprocessing_manifest.json"
    if not path.is_file():
        raise CompatibilityError("Preprocessing manifest is missing")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CompatibilityError("Preprocessing manifest is unreadable") from exc
    missing = [field for field in REQUIRED_MANIFEST_FIELDS if field not in payload]
    if missing:
        raise CompatibilityError(
            f"Preprocessing manifest is missing required fields: {', '.join(missing)}"
        )
    return payload


def validate_preprocessing_artifacts(
    artifact_dir: str | Path,
) -> dict[str, Any]:
    """Validate the manifest, feature contract, and all dataset artifacts."""
    root = Path(artifact_dir)
    manifest = _read_manifest(root)
    if manifest["schema_version"] != SCHEMA_VERSION:
        raise CompatibilityError("Preprocessing schema version is not raw-24-v1")
    if manifest["feature_order"] != FEATURE_COLUMNS:
        raise CompatibilityError("Preprocessing feature order does not match raw-24-v1")
    if manifest["window_size"] != WINDOW_SIZE:
        raise CompatibilityError("Preprocessing window size is not 30 cycles")
    if manifest["datasets"] != SUB_DATASETS:
        raise CompatibilityError("Preprocessing dataset list is incomplete or reordered")

    hashes = manifest["artifact_sha256"]
    for dataset in SUB_DATASETS:
        for suffix, label in ((".pkl", "scaler"), ("_stats.json", "statistics")):
            path = root / f"scaler_{dataset}{suffix}"
            if not path.is_file():
                raise CompatibilityError(f"Missing FD dataset {label} artifact")
            expected = hashes.get(dataset, {}).get(label)
            if not isinstance(expected, str) or sha256_file(path) != expected:
                raise CompatibilityError(f"FD dataset {label} artifact hash mismatch")
    return manifest


def validate_model_artifacts(
    checkpoint_path: str | Path,
    artifact_dir: str | Path,
) -> dict[str, Any]:
    """Validate a checkpoint against the manifest without loading model weights."""
    root = Path(artifact_dir)
    manifest = validate_preprocessing_artifacts(root)
    checkpoint = Path(checkpoint_path)
    if not checkpoint.is_file():
        raise CompatibilityError("LSTM checkpoint is missing")
    if sha256_file(checkpoint) != manifest["checkpoint_sha256"]:
        raise CompatibilityError("LSTM checkpoint hash mismatch")

    try:
        import torch

        payload = torch.load(checkpoint, map_location="cpu", weights_only=False)
        input_size = int(payload["config"]["input_size"])
    except (KeyError, TypeError, ValueError, OSError, RuntimeError) as exc:
        raise CompatibilityError("LSTM checkpoint metadata is invalid") from exc
    if input_size != len(FEATURE_COLUMNS):
        raise CompatibilityError("LSTM checkpoint input size is not 24")
    return {
        "schema_version": manifest["schema_version"],
        "feature_order": list(manifest["feature_order"]),
        "window_size": manifest["window_size"],
        "checkpoint_sha256": manifest["checkpoint_sha256"],
        "artifact_sha256": manifest["artifact_sha256"],
        "validation": manifest["validation"],
    }
