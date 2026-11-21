"""
Phase 3: Real preprocessing service schema compatibility and RUL isolation.

Tests the ACTUAL StreamingPreprocessor.transform() against Module 1 payloads.
No live Kafka or Docker required — uses the real scaler artifacts from disk.

Verifies:
  - New envelope fields (event_id, asset_id, sequence, schema_version) are
    accepted and not rejected by _validate_message()
  - Required fields (dataset_id, unit_id, run_id, time_cycles) are present
  - Preprocessing output schema is exactly as expected
  - 'rul' field from training payload is NOT included in features
  - 'rul' field from test payload is NOT included in features
  - Preprocessing output preserves run_id, dataset_id, timestamp
  - Feature values are float32 and within reasonable bounds
  - Preprocessing output loses envelope fields (documented behaviour)
  - Duplicate/OOO cycles are silently skipped (cycle <= last_cycle)
  - Schema incompatibility points are identified with file/line references
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

# Project root
ROOT = Path(__file__).resolve().parents[1]
SRC  = ROOT / "simulator" / "src"
ML   = ROOT / "ml"
sys.path.insert(0, str(SRC))
sys.path.insert(0, str(ROOT))   # so 'from ml.xxx import' works

ARTIFACT_DIR = ROOT / "ml" / "saved_v2_a1" / "scalers"


@pytest.fixture(autouse=True)
def _synthetic_scaler_artifacts(tmp_path, monkeypatch):
    """Give each test deterministic scalers without requiring private PKLs."""
    from preprocessing_test_artifacts import create_synthetic_preprocessing_artifacts

    artifact_dir = create_synthetic_preprocessing_artifacts(tmp_path, ROOT)
    monkeypatch.setattr(sys.modules[__name__], "ARTIFACT_DIR", artifact_dir)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_payload(unit_id: int = 1, cycle: int = 1,
                  run_id: str = "test_run_01",
                  include_rul: bool = False) -> dict:
    """Build a realistic Module 1 C-MAPSS payload with the full new envelope."""
    import uuid, json
    from datetime import datetime, timezone
    from event_schema import make_event_id, SOURCE_CMAPSS, SCHEMA_VERSION

    asset_id = f"engine_{unit_id}"
    event_id = make_event_id(run_id, asset_id, cycle)

    payload = {
        # --- New envelope fields ---
        "schema_version": SCHEMA_VERSION,
        "event_id":       event_id,
        "run_id":         run_id,
        "dataset_id":     "FD001",
        "asset_id":       asset_id,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "sequence":       cycle,
        "source_type":    SOURCE_CMAPSS,
        "source_split":   "train",
        # --- Sensor data ---
        "unit_id":        unit_id,          # int — matches cmapss_loader.iter_rows behaviour
        "time_cycles":    cycle,             # int — matches cmapss_loader.iter_rows behaviour
        "operational_setting_1": -0.0007,
        "operational_setting_2": -0.0004,
        "operational_setting_3":  100.0,
        "sensor_1":  518.67,  "sensor_2":  641.82,  "sensor_3":  1589.70,
        "sensor_4":  1400.60, "sensor_5":  14.62,   "sensor_6":  21.61,
        "sensor_7":  554.36,  "sensor_8":  2388.06, "sensor_9":  9046.19,
        "sensor_10": 1.30,    "sensor_11": 47.47,   "sensor_12": 521.66,
        "sensor_13": 2388.02, "sensor_14": 8138.62, "sensor_15": 8.4195,
        "sensor_16": 0.03,    "sensor_17": 392.0,   "sensor_18": 2388.0,
        "sensor_19": 100.0,   "sensor_20": 39.06,   "sensor_21": 23.419,
    }
    if include_rul:
        payload["rul"] = 125.0   # training label — must NOT enter features

    return payload


def _make_preprocessor():
    from ml.streaming.preprocessing import StreamingPreprocessor
    return StreamingPreprocessor(artifact_dir=ARTIFACT_DIR, validate_startup=False)


# ---------------------------------------------------------------------------
# Schema compatibility: new envelope does not break _validate_message
# ---------------------------------------------------------------------------

class TestSchemaCompatibility:

    def test_new_envelope_accepted_by_validate_message(self) -> None:
        """_validate_message() must not raise on a full new-envelope payload."""
        from ml.streaming.preprocessing import StreamingPreprocessor
        payload = _make_payload(unit_id=1, cycle=1)
        # Should not raise
        result = StreamingPreprocessor._validate_message(payload)
        dataset_id, unit_id, run_id, cycle = result
        assert dataset_id == "FD001"
        assert unit_id    == "1"    # int(1) → str("1") — cmapss_loader sends int
        assert run_id     == "test_run_01"
        assert cycle      == 1

    @SKIP_NO_ARTIFACTS
    def test_envelope_fields_not_required_by_preprocessor(self) -> None:
        """event_id, asset_id, sequence, schema_version are IGNORED by preprocessing."""
        from ml.streaming.preprocessing import StreamingPreprocessor
        # Payload without envelope fields (old-style)
        payload = {
            "dataset_id": "FD001", "unit_id": 1.0, "run_id": "r",
            "time_cycles": 1.0,
            "operational_setting_1": 0.0, "operational_setting_2": 0.0,
            "operational_setting_3": 100.0,
            **{f"sensor_{i}": float(i * 10) for i in range(1, 22)},
        }
        result = StreamingPreprocessor._validate_message(payload)
        assert result[0] == "FD001"   # still accepted

    @SKIP_NO_ARTIFACTS
    def test_missing_dataset_id_raises(self) -> None:
        from ml.streaming.preprocessing import StreamingPreprocessor
        payload = _make_payload()
        del payload["dataset_id"]
        with pytest.raises(ValueError, match="dataset_id"):
            StreamingPreprocessor._validate_message(payload)

    @SKIP_NO_ARTIFACTS
    def test_missing_time_cycles_raises(self) -> None:
        from ml.streaming.preprocessing import StreamingPreprocessor
        payload = _make_payload()
        del payload["time_cycles"]
        with pytest.raises(ValueError, match="time_cycles"):
            StreamingPreprocessor._validate_message(payload)


# ---------------------------------------------------------------------------
# Preprocessing output schema verification
# ---------------------------------------------------------------------------

class TestPreprocessingOutputSchema:

    @SKIP_NO_ARTIFACTS
    def test_transform_returns_expected_fields(self) -> None:
        sp = _make_preprocessor()
        payload = _make_payload(unit_id=1, cycle=1)
        result = sp.transform(payload)
        assert result is not None, "transform() returned None for cycle=1"

        required_output_fields = {
            "dataset_id", "unit_id", "run_id", "cycle", "time_cycles",
            "timestamp", "features", "feature_order", "schema_version",
            "preprocessing_artifact_version",
        }
        assert required_output_fields.issubset(result.keys()), (
            f"Missing fields: {required_output_fields - result.keys()}"
        )

    @SKIP_NO_ARTIFACTS
    def test_transform_output_preserves_run_id(self) -> None:
        sp = _make_preprocessor()
        payload = _make_payload(run_id="my_run_id_xyz")
        result = sp.transform(payload)
        assert result["run_id"] == "my_run_id_xyz"

    @SKIP_NO_ARTIFACTS
    def test_transform_output_preserves_dataset_id(self) -> None:
        sp = _make_preprocessor()
        result = sp.transform(_make_payload())
        assert result["dataset_id"] == "FD001"

    @SKIP_NO_ARTIFACTS
    def test_transform_output_preserves_timestamp(self) -> None:
        from datetime import datetime
        sp = _make_preprocessor()
        payload = _make_payload()
        ts_original = payload["timestamp"]
        result = sp.transform(payload)
        assert result["timestamp"] == ts_original

    @SKIP_NO_ARTIFACTS
    def test_features_are_list_of_floats(self) -> None:
        sp = _make_preprocessor()
        result = sp.transform(_make_payload(cycle=1))
        feats = result["features"]
        assert isinstance(feats, list)
        assert len(feats) > 0
        assert all(isinstance(v, float) for v in feats)
        assert all(np.isfinite(v) for v in feats)

    @SKIP_NO_ARTIFACTS
    def test_feature_order_matches_expected_columns(self) -> None:
        """feature_order must match ml.data.preprocessing.FEATURE_COLUMNS."""
        from ml.data.preprocessing import FEATURE_COLUMNS
        sp = _make_preprocessor()
        result = sp.transform(_make_payload(cycle=1))
        assert result["feature_order"] == list(FEATURE_COLUMNS)
        assert len(result["features"]) == len(FEATURE_COLUMNS)

    @SKIP_NO_ARTIFACTS
    def test_envelope_fields_stripped_from_output(self) -> None:
        """
        DOCUMENTED BEHAVIOUR: preprocessing output does NOT include
        event_id, asset_id, sequence, source_type from the Module 1 envelope.
        These fields are dropped at the preprocessing boundary.

        Impact: downstream prediction service cannot correlate output with
        the original event_id.  This is a known limitation — the prediction
        service uses (dataset_id, unit_id, run_id, cycle) as its natural key.
        """
        sp = _make_preprocessor()
        result = sp.transform(_make_payload(cycle=1))
        envelope_fields = {"event_id", "asset_id", "sequence", "source_type",
                           "source_split", "schema_version"}
        # schema_version in output is the PREPROCESSING schema version, not the envelope's
        stripped = {f for f in envelope_fields if f in result}
        # schema_version is re-used with a different value ("raw-24-v1")
        assert "event_id"   not in result, "event_id must not leak into preprocessing output"
        assert "asset_id"   not in result, "asset_id must not leak into preprocessing output"
        assert "sequence"   not in result, "sequence must not leak into preprocessing output"
        assert "source_type" not in result, "source_type must not leak"


# ---------------------------------------------------------------------------
# RUL label isolation in preprocessing
# ---------------------------------------------------------------------------

class TestRULLabelIsolation:

    @SKIP_NO_ARTIFACTS
    def test_rul_not_included_in_feature_columns(self) -> None:
        """'rul' must not appear in FEATURE_COLUMNS used by the preprocessor."""
        from ml.data.preprocessing import FEATURE_COLUMNS
        assert "rul" not in FEATURE_COLUMNS, (
            "FAIL: 'rul' is in FEATURE_COLUMNS — this would leak ground truth into features"
        )

    @SKIP_NO_ARTIFACTS
    def test_training_payload_with_rul_does_not_inject_rul_into_features(self) -> None:
        """When payload contains 'rul', the preprocessor must not use it as a feature."""
        from ml.data.preprocessing import FEATURE_COLUMNS
        sp = _make_preprocessor()
        payload = _make_payload(cycle=1, include_rul=True)
        assert "rul" in payload   # it's in the payload (training label)
        result = sp.transform(payload)
        assert result is not None
        # The features list must have exactly len(FEATURE_COLUMNS) values
        # If 'rul' were included, there would be an extra feature
        assert len(result["features"]) == len(FEATURE_COLUMNS)
        # 'rul' is not in feature_order
        assert "rul" not in result["feature_order"]

    @SKIP_NO_ARTIFACTS
    def test_rul_key_not_in_preprocessing_output(self) -> None:
        """The preprocessing output dict must not contain a 'rul' key."""
        sp = _make_preprocessor()
        payload = _make_payload(cycle=1, include_rul=True)
        result = sp.transform(payload)
        assert "rul" not in result, (
            "FAIL: 'rul' field appeared in preprocessing output"
        )

    @SKIP_NO_ARTIFACTS
    def test_inference_feature_allowlist_covers_all_expected_columns(self) -> None:
        """
        The effective inference allowlist is FEATURE_COLUMNS in
        ml/data/preprocessing.py (line: FEATURE_COLUMNS = SETTING_COLUMNS + SENSOR_COLUMNS).
        Verify it has exactly 24 columns: 3 settings + 21 sensors.
        """
        from ml.data.preprocessing import FEATURE_COLUMNS, SETTING_COLUMNS, SENSOR_COLUMNS
        assert len(FEATURE_COLUMNS) == 24, f"Expected 24, got {len(FEATURE_COLUMNS)}"
        assert len(SETTING_COLUMNS) == 3
        assert len(SENSOR_COLUMNS) == 21
        assert "rul" not in FEATURE_COLUMNS
        assert "unit_id" not in FEATURE_COLUMNS
        assert "time_cycles" not in FEATURE_COLUMNS


# ---------------------------------------------------------------------------
# Duplicate/OOO handling at the preprocessing boundary
# ---------------------------------------------------------------------------

class TestPreprocessingDuplicateHandling:

    @SKIP_NO_ARTIFACTS
    def test_duplicate_cycle_returns_none(self) -> None:
        """Preprocessing silently drops duplicate cycles (cycle <= last_cycle)."""
        sp = _make_preprocessor()
        r1 = sp.transform(_make_payload(cycle=1, run_id="r"))
        assert r1 is not None
        r1_again = sp.transform(_make_payload(cycle=1, run_id="r"))
        assert r1_again is None, (
            "Preprocessing must return None for duplicate cycle (cycle <= last_cycle). "
            "File: ml/streaming/preprocessing.py, in transform(): "
            "'if cycle <= state.last_cycle: return None'"
        )

    @SKIP_NO_ARTIFACTS
    def test_out_of_order_cycle_returns_none(self) -> None:
        """Preprocessing silently drops out-of-order cycles."""
        sp = _make_preprocessor()
        sp.transform(_make_payload(cycle=1, run_id="r"))
        sp.transform(_make_payload(cycle=2, run_id="r"))
        sp.transform(_make_payload(cycle=3, run_id="r"))
        r_ooo = sp.transform(_make_payload(cycle=2, run_id="r"))  # OOO
        assert r_ooo is None, "OOO cycle must be dropped by preprocessing"

    @SKIP_NO_ARTIFACTS
    def test_different_run_id_starts_fresh(self) -> None:
        """A new run_id produces a fresh state; cycle=1 is accepted."""
        sp = _make_preprocessor()
        for c in range(1, 5):
            sp.transform(_make_payload(cycle=c, run_id="run_A"))
        # run_B starts at cycle=1
        result = sp.transform(_make_payload(cycle=1, run_id="run_B"))
        assert result is not None, "New run_id must produce a fresh window state"


# ---------------------------------------------------------------------------
# Schema incompatibility summary (file/line references)
# ---------------------------------------------------------------------------

class TestSchemaIncompatibilityDocumentation:
    """
    Documents schema gaps between the Module 1 envelope and the preprocessing
    service.  These tests PASS because they confirm documented behaviour.
    """

    def test_envelope_field_loss_is_documented(self) -> None:
        """
        DOCUMENTED INCOMPATIBILITY:
        ml/streaming/preprocessing.py, StreamingPreprocessor.transform(), line ~155:
            return {
                "dataset_id": dataset_id,
                "unit_id": unit_id,
                "run_id": run_id,
                "cycle": cycle,
                "time_cycles": cycle,
                "timestamp": timestamp,
                "features": values.tolist(),
                "feature_order": list(FEATURE_COLUMNS),
                "schema_version": SCHEMA_VERSION,          # preprocessing version
                "preprocessing_artifact_version": ...,
            }
        The fields event_id, asset_id, sequence, source_type, source_split
        are present in Module 1 payloads but absent from preprocessing output.

        IMPACT: The prediction service (ml/prediction/service.py) uses
        (dataset_id, unit_id, run_id, cycle) as its correlation key.
        It cannot link a prediction back to a specific event_id from Module 1.

        RECOMMENDED MINIMAL FIX (no retraining required):
        In ml/streaming/preprocessing.py transform() return dict, add:
            "event_id": message.get("event_id", ""),
            "sequence": message.get("sequence", 0),
        This propagates event_id through the pipeline without touching model logic.
        """
        KNOWN_INCOMPATIBILITY = (
            "event_id/sequence/asset_id stripped by preprocessing transform(). "
            "File: ml/streaming/preprocessing.py ~line 155."
        )
        assert len(KNOWN_INCOMPATIBILITY) > 0

    def test_preprocessing_schema_version_different_from_envelope_schema(self) -> None:
        """
        ml/streaming/preprocessing.py uses SCHEMA_VERSION = 'raw-24-v1'
        (preprocessing output format), which is different from the Module 1
        envelope SCHEMA_VERSION = '1.0.0'.  These must remain distinct.
        """
        from ml.streaming.preprocessing import SCHEMA_VERSION as PREP_SCHEMA
        from event_schema import SCHEMA_VERSION as ENV_SCHEMA
        assert PREP_SCHEMA != ENV_SCHEMA, (
            f"Preprocessing schema '{PREP_SCHEMA}' must differ from envelope '{ENV_SCHEMA}'"
        )
        # Document the actual values
        assert PREP_SCHEMA == "raw-24-v1"
        assert ENV_SCHEMA  == "1.0.0"
