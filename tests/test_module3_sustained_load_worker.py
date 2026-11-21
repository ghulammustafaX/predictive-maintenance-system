"""Regression checks for the test-only sustained-load measurement wrapper."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


def load_worker(monkeypatch, points, persist):
    settings = {
        "MODULE3_LOAD_GUARD": "m3-isolated-sustained-load-only",
        "KAFKA_BROKER": "kafka:9092",
        "INFLUXDB_URL": "http://influxdb:8086",
        "INFLUXDB_TOKEN": "m3-isolated-token",
        "INFLUXDB_ORG": "m3-isolated-org",
        "INFLUXDB_BUCKET": "m3-isolated-bucket",
        "MODULE3_LOAD_MODE": "batch",
        "MODULE3_LOAD_EXPECTED_RECORDS": "1",
    }
    for key, value in settings.items():
        monkeypatch.setenv(key, value)

    class Consumer:
        def subscribe(self, **kwargs): pass
        def poll(self, **kwargs): return {}
        def assignment(self): return set()
        def close(self): pass

    class Worker:
        def __init__(self, *args, **kwargs): self.completed_records = 0
        def enqueue_polled_records(self, records): pass
        def process_ready(self, **kwargs):
            if self.completed_records:
                return 0
            try:
                batch.persist_points(None, points)
            except Exception as exc:
                raise AssertionError("persist callback raised into worker") from exc
            self.completed_records = 1
            return 1
        @staticmethod
        def _remove_committed_prefix(progress, messages): pass

    legacy = SimpleNamespace(
        create_influxdb_client=lambda: (SimpleNamespace(close=lambda: None), None),
        create_dead_letter_producer=lambda: SimpleNamespace(close=lambda: None),
        SENSOR_TOPIC="test", CONSUMER_GROUP="test", POLL_TIMEOUT_MS=1, MAX_POLL_RECORDS=1,
    )
    batch = SimpleNamespace(
        create_batch_consumer=Consumer, PartitionIsolatingBatchWorker=Worker,
        persist_points=persist, commit_through=lambda *args: None,
        BATCH_SIZE=10, RETRY_BASE_SECONDS=1, RETRY_MAX_SECONDS=2,
    )
    monkeypatch.setitem(sys.modules, "kafka_consumer", legacy)
    monkeypatch.setitem(sys.modules, "kafka_batch_consumer", batch)
    if sys.platform == "win32":
        monkeypatch.setitem(sys.modules, "resource", SimpleNamespace(
            RUSAGE_SELF=0, getrusage=lambda _: SimpleNamespace(ru_utime=0, ru_stime=0, ru_maxrss=0)
        ))
    spec = importlib.util.spec_from_file_location(
        "module3_load_worker_test", Path(__file__).with_name("module3_sustained_load_worker.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, batch


def test_batch_records_sizes_after_success_and_restores_wrappers(monkeypatch):
    writes = []
    def persist(api, points): writes.append(list(points))
    module, batch = load_worker(monkeypatch, [1, 2, 3], persist)
    summary = module.run_batch()
    assert writes == [[1, 2, 3]]
    assert summary["points_per_write"]["calls"] == 1
    assert summary["points_per_write"]["average"] == 3
    assert summary["write_latency_ms"]["calls"] == 1
    assert batch.persist_points is persist


def test_metric_failure_does_not_escape_persistence_callback(monkeypatch):
    writes = []
    class BadLength(list):
        def __len__(self): raise ValueError("broken metric")
    def persist(api, points): writes.append("confirmed")
    module, batch = load_worker(monkeypatch, BadLength([1]), persist)
    with pytest.raises(RuntimeError, match="batch write measurement failed: ValueError: broken metric"):
        module.run_batch()
    assert writes == ["confirmed"]
    assert batch.persist_points is persist


def test_failed_persistence_does_not_record_success(monkeypatch):
    attempts = []
    def persist(api, points):
        attempts.append("attempt")
        raise OSError("influx unavailable")
    module, batch = load_worker(monkeypatch, [1, 2], persist)
    with pytest.raises(AssertionError, match="persist callback raised into worker") as error:
        module.run_batch()
    assert isinstance(error.value.__cause__, OSError)
    assert attempts == ["attempt"]
    assert batch.persist_points is persist


def test_publisher_paces_equal_bounded_bursts(monkeypatch, tmp_path):
    settings = {
        "MODULE3_LOAD_PROBE_GUARD": "m3-isolated-sustained-load-probe-only",
        "KAFKA_BROKER": "kafka:9092", "INFLUXDB_URL": "http://influxdb:8086",
        "INFLUXDB_TOKEN": "m3-isolated-token", "INFLUXDB_ORG": "m3-isolated-org",
        "INFLUXDB_BUCKET": "m3-isolated-bucket", "MODULE3_LOAD_RECORDS": "100",
        "MODULE3_LOAD_RATE_PER_SECOND": "200", "MODULE3_LOAD_RESULTS_DIR": str(tmp_path),
    }
    for key, value in settings.items(): monkeypatch.setenv(key, value)
    spec = importlib.util.spec_from_file_location(
        "module3_load_probe_test", Path(__file__).with_name("module3_sustained_load_probe.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sent, acknowledgments, sleeps = [], [], []
    clock = [0.0]
    def sleep(delay):
        assert len(acknowledgments) == len(sent)
        sleeps.append((len(sent), delay))
        clock[0] += delay
    module.time = SimpleNamespace(monotonic=lambda: clock[0], sleep=sleep)
    class Producer:
        def __init__(self, **kwargs): pass
        def send(self, topic, **kwargs):
            sent.append((topic, kwargs["partition"], kwargs["value"]["sequence"]))
            return SimpleNamespace(get=lambda timeout: acknowledgments.append(timeout))
        def flush(self, **kwargs): pass
        def close(self, **kwargs): pass
    module.KafkaProducer = Producer
    module.publish()
    assert acknowledgments == [15] * 200
    assert [count for count, _ in sleeps] == [160, 200]
    assert sum(delay for _, delay in sleeps) == pytest.approx(0.5)
    for topic in module.TOPICS.values():
        records = [(partition, seq) for name, partition, seq in sent if name == topic]
        assert len(records) == 100
        assert [seq for _, seq in records] == list(range(1, 101))
        assert [sum(partition == p for partition, _ in records) for p in range(4)] == [25] * 4
