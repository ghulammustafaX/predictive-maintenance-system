"""Offline contract checks for Module 03 opt-in batch writer.

Uses mocks only; real Kafka and InfluxDB verification runs separately in CI.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion" / "src"))
from kafka_batch_consumer import handle_partition, persist_points, require_safe_config


def message(offset, valid=True, partition=0):
    value = {
        "schema_version": "1.0.0", "event_id": f"evt-{offset}",
        "run_id": "batch-test", "dataset_id": "FD001", "asset_id": "engine_1",
        "unit_id": 1, "time_cycles": offset + 1, "sequence": offset + 1,
        "source_type": "cmapss", "timestamp": "2026-11-21T10:00:00Z",
        "operational_setting_1": 0.1, "operational_setting_2": 0.2,
        "operational_setting_3": 100.0, "sensor_1": 5.0,
    } if valid else {"bad": True}
    return SimpleNamespace(topic="test-topic", partition=partition, offset=offset,
                           value=json.dumps(value).encode())


def commit_offsets(consumer):
    return [list(call.args[0].values())[0].offset for call in consumer.commit.call_args_list]


def test_batch_write_commits_last_offset_only_after_ack():
    consumer = MagicMock()
    writer = MagicMock()
    producer = MagicMock()
    handle_partition(consumer, writer, producer, [message(i) for i in range(5)], batch_size=3)
    assert [len(c.kwargs["record"]) for c in writer.write.call_args_list] == [3, 2]
    assert commit_offsets(consumer) == [3, 5]
    producer.send.assert_not_called()


def test_failed_batch_does_not_commit_or_dlq():
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    writer.write.side_effect = RuntimeError("influx timeout")
    with patch("kafka_batch_consumer.time.sleep"):
        with pytest.raises(RuntimeError):
            handle_partition(consumer, writer, producer, [message(i) for i in range(3)], batch_size=3)
    consumer.commit.assert_not_called()
    producer.send.assert_not_called()


def test_invalid_barrier_flushes_previous_and_commits_after_dlq():
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    order = []
    writer.write.side_effect = lambda **kwargs: order.append("write")
    consumer.commit.side_effect = lambda *a, **k: order.append("commit")
    producer.send.return_value.get.side_effect = lambda **kwargs: order.append("dlq-ack")
    handle_partition(consumer, writer, producer,
                     [message(0), message(1, valid=False), message(2)], batch_size=3)
    assert order == ["write", "commit", "dlq-ack", "commit", "write", "commit"]
    assert commit_offsets(consumer) == [1, 2, 3]


def test_failed_dlq_does_not_ack_invalid_record_or_later_points():
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    producer.send.return_value.get.side_effect = TimeoutError("DLQ failure")
    with pytest.raises(TimeoutError):
        handle_partition(consumer, writer, producer,
                         [message(0), message(1, valid=False), message(2)], batch_size=3)
    assert commit_offsets(consumer) == [1]
    assert writer.write.call_count == 1


def test_commit_error_stops_before_next_batch():
    consumer, writer, producer = MagicMock(), MagicMock(), MagicMock()
    consumer.commit.side_effect = RuntimeError("commit failed")
    with pytest.raises(RuntimeError):
        handle_partition(consumer, writer, producer, [message(i) for i in range(4)], batch_size=2)
    assert writer.write.call_count == 1
    producer.send.assert_not_called()


def test_auth_error_does_not_retry():
    writer = MagicMock()
    exc = RuntimeError("unauthorized")
    exc.status = 401
    writer.write.side_effect = exc
    with pytest.raises(RuntimeError):
        persist_points(writer, [MagicMock()])
    assert writer.write.call_count == 1


def test_requires_explicit_worker_confirmation(monkeypatch):
    monkeypatch.delenv("PMS_BATCH_WORKER_CONFIRM", raising=False)
    with pytest.raises(RuntimeError, match="Refusing to start"):
        require_safe_config()
