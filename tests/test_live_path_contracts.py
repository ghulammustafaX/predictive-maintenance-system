from __future__ import annotations

from pathlib import Path

import numpy as np
import torch
import pytest

from ml.config import WINDOW_SIZE
from ml.data.preprocessing import FEATURE_COLUMNS
from ml.models.lstm import LSTMConfig, RULLSTM
from ml.prediction.service import PredictionService
from ml.prediction import main as prediction_main


class _Future:
    def get(self, timeout: int):
        return {"ok": True}


class _Producer:
    def __init__(self):
        self.messages = []

    def send(self, topic, value):
        self.messages.append((topic, value))
        return _Future()


class _Write:
    def __init__(self):
        self.records = []

    def write(self, **kwargs):
        self.records.append(kwargs)


class _Engine:
    def predict(self, window):
        assert window.shape == (WINDOW_SIZE, len(FEATURE_COLUMNS))
        return 12.5


def _service(tmp_path: Path) -> PredictionService:
    config = LSTMConfig(input_size=len(FEATURE_COLUMNS), hidden_size=4, num_layers=1)
    model = RULLSTM(config)
    checkpoint = tmp_path / "raw-v1.pt"
    torch.save({"model_state": model.state_dict(), "config": config.to_dict()}, checkpoint)
    service = PredictionService(checkpoint_path=checkpoint)
    service.engine = _Engine()
    service.producer = _Producer()
    service.influx_write = _Write()
    service.influx_bucket = "sensor-data"
    service.influx = type("Influx", (), {"org": "pms-org"})()
    return service


def _message(cycle: int, unit_id: int = 7, dataset_id: str = "FD001", run_id: str = "run-a") -> dict:
    return {
        "dataset_id": dataset_id,
        "unit_id": unit_id,
        "run_id": run_id,
        "cycle": cycle,
        "timestamp": "2026-10-30T12:00:00+00:00",
        "features": [float(cycle)] * len(FEATURE_COLUMNS),
        "feature_order": list(FEATURE_COLUMNS),
        "preprocessing_artifact_version": "a1-v2",
    }


def test_prediction_warms_up_at_thirty_cycles_and_publishes_contract(tmp_path):
    service = _service(tmp_path)
    for cycle in range(1, WINDOW_SIZE):
        assert service.process_record(_message(cycle)) is None
    assert service.process_record(_message(WINDOW_SIZE)) == 12.5

    producer = service.producer
    assert len(producer.messages) == 1
    topic, payload = producer.messages[0]
    assert topic == "rul-predictions"
    assert payload == {
        "dataset_id": "FD001",
        "unit_id": 7,
        "run_id": "run-a",
        "cycle": WINDOW_SIZE,
        "source_timestamp": "2026-10-30T12:00:00+00:00",
        "rul": 12.5,
        "window_size": WINDOW_SIZE,
        "model_version": "raw-v1",
        "preprocessing_artifact_version": "a1-v2",
    }
    assert len(service.influx_write.records) == 1
    line = service.influx_write.records[0]["record"].to_line_protocol()
    assert "rul_predictions" in line
    assert "dataset_id=FD001" in line
    assert "unit_id=7" in line
    assert "run_id=run-a" in line
    assert "rul=12.5" in line
    assert "cycle=30i" in line
    assert 'model_version="raw-v1"' in line


def test_prediction_isolates_dataset_and_unit_and_rejects_bad_schema(tmp_path):
    service = _service(tmp_path)
    for cycle in range(1, WINDOW_SIZE + 1):
        service.process_record(_message(cycle, unit_id=1))
    for cycle in range(1, WINDOW_SIZE + 1):
        service.process_record(_message(cycle, unit_id=1, dataset_id="FD002", run_id="run-a"))
    assert len(service.producer.messages) == 2
    bad = _message(1)
    bad["feature_order"] = list(reversed(FEATURE_COLUMNS))
    try:
        service.process_record(bad)
    except ValueError as exc:
        assert "feature_order" in str(exc)
    else:
        raise AssertionError("invalid feature order was accepted")


def test_duplicate_and_out_of_order_prediction_cycles_are_skipped(tmp_path):
    service = _service(tmp_path)
    for cycle in range(1, WINDOW_SIZE + 1):
        service.process_record(_message(cycle))
    assert service.process_record(_message(WINDOW_SIZE)) is None
    assert service.process_record(_message(WINDOW_SIZE - 1)) is None
    assert service.process_record(_message(1)) is None
    assert len(service.producer.messages) == 1


def test_prediction_run_id_isolates_restart_window(tmp_path):
    service = _service(tmp_path)
    for cycle in range(1, WINDOW_SIZE + 1):
        service.process_record(_message(cycle, run_id="run-a"))
    for cycle in range(1, WINDOW_SIZE + 1):
        service.process_record(_message(cycle, run_id="run-b"))
    assert len(service.producer.messages) == 2
    assert {payload["run_id"] for _, payload in service.producer.messages} == {
        "run-a",
        "run-b",
    }


def test_prediction_cycle_reset_restarts_window(tmp_path):
    service = _service(tmp_path)
    for cycle in range(1, WINDOW_SIZE + 1):
        service.process_record(_message(cycle, run_id="run-a"))
    assert service.process_record(_message(1, run_id="run-a")) is None
    for cycle in range(2, WINDOW_SIZE + 1):
        service.process_record(_message(cycle, run_id="run-a"))
    assert len(service.producer.messages) == 2


def test_ready_returns_503_when_dependencies_are_not_ready(monkeypatch):
    class _NotReady:
        def readiness_status(self):
            return {
                "ready": False,
                "model_loaded": True,
                "consumer_thread_alive": False,
                "kafka_initialized": False,
                "influx_configured": True,
                "influx_reachable": False,
                "influx_write_permission_verified": False,
                "model_version": "raw-v1",
                "reason": None,
            }

    monkeypatch.setattr(prediction_main, "service", _NotReady())
    with pytest.raises(prediction_main.HTTPException) as exc_info:
        prediction_main.ready()
    assert exc_info.value.status_code == 503


def test_ready_returns_full_readiness_when_all_dependencies_are_ready(monkeypatch):
    class _Ready:
        def readiness_status(self):
            return {
                "ready": True,
                "model_loaded": True,
                "consumer_thread_alive": True,
                "kafka_initialized": True,
                "influx_configured": True,
                "influx_reachable": True,
                "influx_write_permission_verified": False,
                "model_version": "raw-v1",
                "reason": None,
            }

    monkeypatch.setattr(prediction_main, "service", _Ready())
    assert prediction_main.ready()["ready"] is True
