"""Offline observability contract for the optional Module 03 batch worker.

These tests never connect to external infrastructure or production resources.
"""
from __future__ import annotations

import logging

import pytest

from test_module3_batch_faults import (
    _committed_partition_offsets,
    _message_for_run,
    _partition_worker,
)


def test_retry_counter_alert_deduplicates_and_recovers(caplog):
    now = [0.0]
    worker, consumer, writer, _, tp0, tp1, paused = _partition_worker(
        clock=lambda: now[0],
    )
    worker.alert_retry_threshold = 3
    worker.alert_age_seconds = 60
    remaining = [3]

    def persist(**kwargs):
        run_ids = {p._tags["run_id"] for p in kwargs["record"]}
        if "alert-p0" in run_ids and remaining[0]:
            remaining[0] -= 1
            raise TimeoutError("partition outage")

    writer.write.side_effect = persist
    with caplog.at_level(logging.WARNING, logger="pms.ingestion.batch"):
        worker.process_polled_records({
            tp0: [_message_for_run(0, 0, "alert-p0"), _message_for_run(1, 0, "alert-p0")],
            tp1: [_message_for_run(0, 1, "healthy-p1"), _message_for_run(1, 1, "healthy-p1")],
        })
        assert worker.metrics_snapshot()["alerts_emitted"] == 0
        assert worker.metrics_snapshot()["failing_partitions"] == 1
        assert tp0 in paused and tp1 not in paused
        assert _committed_partition_offsets(consumer) == [(1, 2)]

        now[0] = 2.0
        worker.process_ready()
        assert worker.metrics_snapshot()["alerts_emitted"] == 0
        now[0] = 6.0
        worker.process_ready()
        assert worker.metrics_snapshot()["alerts_emitted"] == 1

        # Polling while still paused should not emit another alert for this incident.
        now[0] = 7.0
        worker.process_ready()
        assert worker.metrics_snapshot()["alerts_emitted"] == 1

        now[0] = 14.0
        worker.process_ready()

    alerts = [r for r in caplog.records if "M3_PARTITION_ALERT" in r.message]
    assert len(alerts) == 1
    assert "partition=0" in alerts[0].message
    assert "stage=write" in alerts[0].message
    snapshot = worker.metrics_snapshot()
    assert snapshot["alerts_emitted"] == 1
    assert snapshot["recoveries"] == 1
    assert snapshot["failing_partitions"] == 0
    assert snapshot["queued_records"] == 0
    assert snapshot["completed_records"] == 4
    assert snapshot["total_failures_by_stage"] == {"write": 3}
    assert all(not p["alert_emitted"] for p in snapshot["partitions"])
    assert _committed_partition_offsets(consumer) == [(1, 2), (0, 2)]


def test_retry_age_alert_without_many_attempts(caplog):
    now = [0.0]
    worker, _, writer, _, tp0, _, _ = _partition_worker(clock=lambda: now[0])
    worker.alert_retry_threshold = 10
    worker.alert_age_seconds = 1
    writer.write.side_effect = TimeoutError("unavailable")
    worker.process_polled_records({
        tp0: [_message_for_run(0, 0, "age-p0"), _message_for_run(1, 0, "age-p0")],
    })
    assert worker.metrics_snapshot()["alerts_emitted"] == 0
    with caplog.at_level(logging.WARNING, logger="pms.ingestion.batch"):
        now[0] = 1.5
        # The retry is scheduled at 2s, but alert age should still be checked.
        worker.process_ready()
        worker.process_ready()
    assert worker.metrics_snapshot()["alerts_emitted"] == 1
    assert len([r for r in caplog.records if "M3_PARTITION_ALERT" in r.message]) == 1
    state = worker.metrics_snapshot()["partitions"][0]
    assert state["failure_age_seconds"] == 1.5
    assert state["retry_wait_seconds"] == 0.5
    assert state["retry_attempt"] == 1


def test_snapshot_tracks_commit_failure_separately_and_keeps_healthy_progress():
    now = [0.0]
    worker, consumer, writer, _, tp0, tp1, paused = _partition_worker(clock=lambda: now[0])
    original_commit = consumer.commit.side_effect
    failure = [True]

    def commit(offsets, timeout_ms):
        tp = next(iter(offsets))
        if tp == tp0 and failure[0]:
            failure[0] = False
            raise TimeoutError("commit acknowledgement lost")
        return original_commit(offsets, timeout_ms)

    consumer.commit.side_effect = commit
    worker.process_polled_records({
        tp0: [_message_for_run(0, 0, "commit-p0"), _message_for_run(1, 0, "commit-p0")],
        tp1: [_message_for_run(0, 1, "commit-p1"), _message_for_run(1, 1, "commit-p1")],
    })
    snapshot = worker.metrics_snapshot()
    assert snapshot["total_failures_by_stage"] == {"commit_valid": 1}
    assert snapshot["failing_partitions"] == 1
    assert snapshot["queued_records"] == 2
    assert snapshot["partitions"][0]["stage"] == "commit_valid"
    assert tp0 in paused and tp1 not in paused
    assert writer.write.call_count == 2

    now[0] = 2.0
    worker.process_ready()
    snapshot = worker.metrics_snapshot()
    assert snapshot["recoveries"] == 1
    assert snapshot["failing_partitions"] == 0
    assert snapshot["total_failures_by_stage"] == {"commit_valid": 1}
    assert writer.write.call_count == 2
    assert _committed_partition_offsets(consumer) == [(1, 2), (0, 2)]


def test_revocation_clears_stale_partition_failure_and_counts_pending_work():
    now = [0.0]
    worker, consumer, writer, _, tp0, _, _ = _partition_worker(clock=lambda: now[0])
    writer.write.side_effect = TimeoutError("down")
    worker.process_polled_records({
        tp0: [_message_for_run(0, 0, "revoke-p0"), _message_for_run(1, 0, "revoke-p0")],
    })
    assert worker.metrics_snapshot()["failing_partitions"] == 1
    worker.on_partitions_revoked([tp0])
    snapshot = worker.metrics_snapshot()
    assert snapshot["revocations_with_pending_work"] == 1
    assert snapshot["failing_partitions"] == 0
    assert snapshot["partitions"] == []
    assert consumer.commit.call_count == 0
    worker.on_partitions_assigned([tp0])
    assert worker.metrics_snapshot()["partitions"][0]["retry_attempt"] == 0


def test_observability_config_rejects_invalid_thresholds(monkeypatch):
    from kafka_batch_consumer import PartitionIsolatingBatchWorker, require_safe_config
    from unittest.mock import MagicMock

    consumer, writer, dlq = MagicMock(), MagicMock(), MagicMock()
    with pytest.raises(ValueError, match="alert threshold"):
        PartitionIsolatingBatchWorker(consumer, writer, dlq, alert_retry_threshold=0)
    with pytest.raises(ValueError, match="alert threshold"):
        PartitionIsolatingBatchWorker(consumer, writer, dlq, alert_age_seconds=0)
    monkeypatch.setenv("PMS_BATCH_WORKER_CONFIRM", "opt-in-isolated-batch-worker")
    monkeypatch.setattr("kafka_batch_consumer.ALERT_RETRY_THRESHOLD", 0)
    with pytest.raises(ValueError, match="alert thresholds"):
        require_safe_config()
