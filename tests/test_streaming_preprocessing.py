from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ml.config import OUTLIER_Z_THRESHOLD
from ml.data import preprocessing as batch
from ml.streaming.preprocessing import SCHEMA_VERSION, StreamingPreprocessor


def _frame(unit_id: int = 1, cycle: int = 1) -> pd.DataFrame:
    values = {column: float(cycle) for column in batch.FEATURE_COLUMNS}
    return pd.DataFrame([{"unit_id": unit_id, "time_cycles": cycle, **values}])


def _artifacts(tmp_path, monkeypatch):
    monkeypatch.setattr(batch, "SCALER_DIR", tmp_path)
    train = pd.concat([_frame(cycle=1), _frame(cycle=2)], ignore_index=True)
    stats = batch.fit_training_stats(train, "FD001")
    batch.fit_scaler(train, "FD001")
    return stats


def test_stream_matches_batch_on_complete_ordered_input(tmp_path, monkeypatch):
    _artifacts(tmp_path, monkeypatch)
    processor = StreamingPreprocessor(tmp_path, validate_startup=False)
    source = _frame(cycle=2).iloc[0].to_dict()
    expected = batch.apply_scaler(
        batch.clip_outliers(
            batch.handle_missing_values(_frame(cycle=2), backfill=False),
            OUTLIER_Z_THRESHOLD,
            batch.load_training_stats("FD001", tmp_path),
        ),
        batch.load_scaler("FD001", tmp_path),
    )[batch.FEATURE_COLUMNS].to_numpy(dtype=np.float32)[0]
    output = processor.transform({"dataset_id": "FD001", "run_id": "run-a", **source})
    assert output is not None
    np.testing.assert_array_equal(np.asarray(output["features"], dtype=np.float32), expected)
    assert output["feature_order"] == batch.FEATURE_COLUMNS
    assert output["schema_version"] == SCHEMA_VERSION


def test_stream_leading_gaps_use_training_medians(tmp_path, monkeypatch):
    stats = _artifacts(tmp_path, monkeypatch)
    processor = StreamingPreprocessor(tmp_path, validate_startup=False)
    message = {"dataset_id": "FD001", "unit_id": 1, "run_id": "run-a", "time_cycles": 1}
    output = processor.transform(message)
    assert output is not None
    expected_raw = np.asarray([stats[column]["median"] for column in batch.FEATURE_COLUMNS])
    expected_frame = pd.DataFrame([expected_raw], columns=batch.FEATURE_COLUMNS)
    expected = batch.load_scaler("FD001", tmp_path).transform(expected_frame)[0]
    expected = expected.astype(np.float32)
    np.testing.assert_array_equal(np.asarray(output["features"], dtype=np.float32), expected)


def test_stream_state_isolated_by_dataset_and_unit(tmp_path, monkeypatch):
    _artifacts(tmp_path, monkeypatch)
    processor = StreamingPreprocessor(tmp_path, validate_startup=False)
    first = {"dataset_id": "FD001", "unit_id": 1, "run_id": "run-a", "time_cycles": 1, **_frame(cycle=1).iloc[0].to_dict()}
    first.pop("unit_id")
    first.pop("time_cycles")
    first = {"dataset_id": "FD001", "unit_id": 1, "time_cycles": 1, **first}
    second = {**first, "unit_id": 2, "time_cycles": 2, "sensor_1": None}
    output = processor.transform(first)
    isolated = processor.transform(second)
    assert output is not None and isolated is not None
    assert isolated["features"] != output["features"]


@pytest.mark.parametrize(
    "message",
    [
        {"unit_id": 1, "time_cycles": 1},
        {"dataset_id": "FD999", "unit_id": 1, "time_cycles": 1},
        {"dataset_id": "FD001", "time_cycles": 1},
    ],
)
def test_invalid_messages_are_rejected(tmp_path, monkeypatch, message):
    _artifacts(tmp_path, monkeypatch)
    assert StreamingPreprocessor(tmp_path, validate_startup=False).process_message(message) is None


def test_duplicate_out_of_order_and_restart_cycles(tmp_path, monkeypatch):
    _artifacts(tmp_path, monkeypatch)
    processor = StreamingPreprocessor(tmp_path, restart_cycle=1, validate_startup=False)
    base = {"dataset_id": "FD001", "unit_id": 1, "run_id": "run-a", **_frame(cycle=2).iloc[0].to_dict()}
    base.pop("unit_id")
    base.pop("time_cycles")
    assert processor.transform({"dataset_id": "FD001", "unit_id": 1, "run_id": "run-a", "time_cycles": 2, **base})
    assert processor.transform({"dataset_id": "FD001", "unit_id": 1, "run_id": "run-a", "time_cycles": 2, **base}) is None
    assert processor.transform({"dataset_id": "FD001", "unit_id": 1, "run_id": "run-a", "time_cycles": 1, **base}) is not None
    assert processor.transform({"dataset_id": "FD001", "unit_id": 1, "run_id": "run-a", "time_cycles": 1, **base}) is None
    assert processor.state_count == 1


def test_stream_matches_batch_for_out_of_range_values(tmp_path, monkeypatch):
    _artifacts(tmp_path, monkeypatch)
    processor = StreamingPreprocessor(tmp_path, validate_startup=False)
    source = _frame(cycle=2).iloc[0].to_dict()
    source["sensor_2"] = 10_000.0
    raw = _frame(cycle=2)
    raw.loc[0, "sensor_2"] = 10_000.0
    expected = batch.apply_scaler(
        batch.clip_outliers(
            batch.handle_missing_values(raw, backfill=False),
            OUTLIER_Z_THRESHOLD,
            batch.load_training_stats("FD001", tmp_path),
        ),
        batch.load_scaler("FD001", tmp_path),
    )[batch.FEATURE_COLUMNS].to_numpy(dtype=np.float32)[0]
    output = processor.transform(
        {"dataset_id": "FD001", "run_id": "run-a", **source}
    )
    assert output is not None
    np.testing.assert_array_equal(
        np.asarray(output["features"], dtype=np.float32), expected
    )


def test_stream_run_id_isolates_restart_state(tmp_path, monkeypatch):
    _artifacts(tmp_path, monkeypatch)
    processor = StreamingPreprocessor(tmp_path, validate_startup=False)
    first = {
        "dataset_id": "FD001",
        "unit_id": 1,
        "run_id": "run-a",
        "time_cycles": 1,
        **_frame(cycle=1).iloc[0].to_dict(),
    }
    second = {**first, "run_id": "run-b", "sensor_1": None}
    assert processor.transform(first) is not None
    output = processor.transform(second)
    assert output is not None
    assert processor.state_count == 2
    assert output["run_id"] == "run-b"
