from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "backend"))

from app.routers.rul import (
    _engine_parts,
    _prediction_from_row,
    _safe_run_id,
    _time_range,
)
from app.routers.fleet import _select_fleet_rows, temporary_fleet_status


def test_engine_id_maps_to_dataset_and_unit():
    assert _engine_parts("FD001-001") == ("FD001", 1, "FD001-001")


def test_invalid_run_id_is_rejected():
    with pytest.raises(Exception):
        _safe_run_id("other/run")


def test_prediction_contract_preserves_run_metadata():
    prediction = _prediction_from_row(
        {
            "rul": 117.5,
            "cycle": 30.0,
            "timestamp": "2026-11-12T00:15:29+00:00",
            "run_id": "acceptance-fd001-unit1-20261112T0015Z",
            "window_size": 30.0,
            "model_version": "lstm_raw",
            "preprocessing_artifact_version": "a1-v2",
        }
    )
    assert prediction == {
        "rul_cycles": 117.5,
        "cycle": 30,
        "timestamp": "2026-11-12T00:15:29+00:00",
        "run_id": "acceptance-fd001-unit1-20261112T0015Z",
        "window_size": 30,
        "model_version": "lstm_raw",
        "preprocessing_artifact_version": "a1-v2",
    }


def test_empty_prediction_row_is_ignored():
    assert _prediction_from_row({"timestamp": "2026-11-12T00:15:29+00:00"}) is None


def test_time_range_normalizes_naive_utc_and_enforces_limit():
    start = datetime(2026, 11, 12, 0, 0, 0)
    end = datetime(2026, 11, 12, 1, 0, 0)
    normalized_start, normalized_end = _time_range(start, end)
    assert normalized_start.tzinfo == timezone.utc
    assert normalized_end - normalized_start == timedelta(hours=1)


def test_future_fleet_timestamp_is_not_marked_live():
    future = datetime.now(timezone.utc) + timedelta(minutes=5)
    assert temporary_fleet_status(future, 40) == "future"


def test_current_fleet_row_wins_over_future_test_row():
    current = {"dataset_id": "FD001", "unit_id": "1", "_value": 39}
    future = {"dataset_id": "FD001", "unit_id": "1", "_value": 40}
    assert _select_fleet_rows([current], [future]) == [current]


def test_future_only_fleet_row_remains_discoverable():
    future = {"dataset_id": "FD001", "unit_id": "1", "_value": 40}
    assert _select_fleet_rows([], [future]) == [future]
