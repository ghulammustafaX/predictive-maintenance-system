"""Offline checks for the optional batch worker's bounded drain."""
import logging
import signal
import time
from unittest.mock import MagicMock

import pytest
from test_module3_batch_faults import _message_for_run, _partition_worker
import kafka_batch_consumer as batch


def test_shutdown_drains_queued_records_and_pauses_partition():
    worker, consumer, writer, _, tp, _, paused = _partition_worker()
    consumer.poll.return_value = {}
    worker.enqueue_polled_records({tp: [_message_for_run(0, 0, "drain")]})
    batch.drain_worker(worker, worker.clock() + 10)
    assert tp in paused
    assert worker.completed_records == 1
    assert consumer.commit.call_count == 1
    assert writer.write.call_count == 1


def test_expired_shutdown_leaves_queue_uncommitted_and_reports_offset(caplog):
    worker, consumer, writer, _, tp, _, _ = _partition_worker()
    worker.enqueue_polled_records({tp: [_message_for_run(7, 0, "timeout")]})
    with caplog.at_level(logging.WARNING):
        batch.drain_worker(worker, worker.clock() - 1)
    consumer.commit.assert_not_called()
    writer.write.assert_not_called()
    assert "offset=7" in caplog.text
    assert "stage=queued" in caplog.text


def test_write_completing_after_deadline_does_not_commit():
    now = [0.0]
    worker, consumer, writer, _, tp, _, _ = _partition_worker(clock=lambda: now[0])
    consumer.poll.return_value = {}
    writer.write.side_effect = lambda **kwargs: now.__setitem__(0, 2.0)
    worker.enqueue_polled_records({tp: [_message_for_run(0, 0, "slow-write")]})
    batch.drain_worker(worker, 1.0)
    writer.write.assert_called_once()
    consumer.commit.assert_not_called()
    assert worker.partitions[tp].pending.stage == "commit_valid"


def test_shutdown_confirms_dlq_before_committing_invalid_record():
    worker, consumer, _, dlq, tp, _, _ = _partition_worker()
    consumer.poll.return_value = {}
    record = _message_for_run(0, 0, "bad")
    record.value = b"{bad json"
    events = []
    dlq.send.return_value.get.side_effect = lambda **kwargs: events.append("dlq")
    consumer.commit.side_effect = lambda *args, **kwargs: events.append("commit")
    worker.enqueue_polled_records({tp: [record]})
    batch.drain_worker(worker, worker.clock() + 10)
    assert events == ["dlq", "commit"]


def test_failed_shutdown_write_is_replayed_by_fresh_worker():
    worker, consumer, writer, _, tp, _, _ = _partition_worker(clock=time.monotonic)
    consumer.poll.return_value = {}
    writer.write.side_effect = TimeoutError("unavailable")
    record = _message_for_run(0, 0, "replay")
    worker.enqueue_polled_records({tp: [record]})
    batch.drain_worker(worker, worker.clock() + 0.05)
    consumer.commit.assert_not_called()
    replacement, replacement_consumer, replacement_writer, _, tp2, _, _ = _partition_worker()
    replacement_consumer.poll.return_value = {}
    replacement.enqueue_polled_records({tp2: [record]})
    batch.drain_worker(replacement, replacement.clock() + 10)
    replacement_writer.write.assert_called_once()
    assert replacement_consumer.commit.call_count == 1


def test_sigterm_idle_releases_resources_even_if_flush_fails(monkeypatch):
    worker, consumer, _, producer, _, _, _ = _partition_worker(clock=time.monotonic)
    client = MagicMock()
    handlers = {}
    def install(sig, handler):
        handlers[sig] = handler
        return signal.SIG_DFL
    monkeypatch.setattr(batch.signal, "signal", install)
    monkeypatch.setattr(batch, "require_safe_config", lambda: None)
    monkeypatch.setattr(batch, "create_batch_consumer", lambda: consumer)
    monkeypatch.setattr(batch.legacy, "create_influxdb_client", lambda: (client, worker.write_api))
    monkeypatch.setattr(batch, "create_batch_dlq_producer", lambda: producer)
    monkeypatch.setattr(batch, "PartitionIsolatingBatchWorker", lambda *args: worker)
    def poll(**kwargs):
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        return {}
    consumer.poll.side_effect = poll
    producer.flush.side_effect = RuntimeError("flush failed")
    with pytest.raises(RuntimeError, match="shutdown incomplete"):
        batch.run()
    producer.close.assert_called_once()
    consumer.close.assert_called_once()
    client.close.assert_called_once()
    consumer.commit.assert_not_called()


def _run_with_signal_fixture(monkeypatch):
    worker, consumer, writer, producer, tp, _, _ = _partition_worker(clock=time.monotonic)
    client = MagicMock()
    handlers = {}
    monkeypatch.setattr(batch.signal, "signal", lambda sig, handler: handlers.setdefault(sig, handler) or signal.SIG_DFL)
    monkeypatch.setattr(batch, "require_safe_config", lambda: None)
    monkeypatch.setattr(batch, "create_batch_consumer", lambda: consumer)
    monkeypatch.setattr(batch.legacy, "create_influxdb_client", lambda: (client, writer))
    monkeypatch.setattr(batch, "create_batch_dlq_producer", lambda: producer)
    monkeypatch.setattr(batch, "PartitionIsolatingBatchWorker", lambda *args: worker)
    monkeypatch.setattr(batch, "SHUTDOWN_DRAIN_SECONDS", 0.01)
    record = _message_for_run(0, 0, "signal")
    consumer.poll.side_effect = [{tp: [record]}]
    return worker, consumer, writer, producer, client, handlers


def test_sigterm_during_blocked_write_does_not_commit_after_deadline(monkeypatch):
    worker, consumer, writer, _, _, handlers = _run_with_signal_fixture(monkeypatch)
    def blocked_write(**kwargs):
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        time.sleep(0.03)
    writer.write.side_effect = blocked_write
    with pytest.raises(RuntimeError, match="shutdown incomplete"):
        batch.run()
    consumer.commit.assert_not_called()
    assert worker.partitions[next(iter(worker.partitions))].pending.stage == "commit_valid"


def test_sigterm_during_commit_does_not_repeat_confirmed_write(monkeypatch):
    worker, consumer, writer, _, _, handlers = _run_with_signal_fixture(monkeypatch)
    def blocked_commit(*args, **kwargs):
        handlers[signal.SIGTERM](signal.SIGTERM, None)
        time.sleep(0.03)
    consumer.commit.side_effect = blocked_commit
    batch.run()
    writer.write.assert_called_once()
    consumer.commit.assert_called_once()
    assert worker.completed_records == 1


def test_dlq_timeout_does_not_commit_during_shutdown():
    worker, consumer, _, dlq, tp, _, _ = _partition_worker(clock=time.monotonic)
    consumer.poll.return_value = {}
    dlq.send.return_value.get.side_effect = TimeoutError("DLQ acknowledgement timed out")
    worker.enqueue_polled_records({tp: [_message_for_run(0, 0, "bad", valid=False)]})
    batch.drain_worker(worker, time.monotonic() + 0.03)
    consumer.commit.assert_not_called()
    assert worker.partitions[tp].pending.stage == "dlq"


def test_multiple_cleanup_failures_do_not_mask_original_exception(monkeypatch):
    _, consumer, _, producer, client, _ = _run_with_signal_fixture(monkeypatch)
    original = RuntimeError("poll failure")
    consumer.poll.side_effect = original
    producer.flush.side_effect = RuntimeError("flush failure")
    producer.close.side_effect = RuntimeError("producer close failure")
    consumer.close.side_effect = RuntimeError("consumer close failure")
    client.close.side_effect = RuntimeError("influx close failure")
    with pytest.raises(RuntimeError, match="poll failure") as caught:
        batch.run()
    assert caught.value is original
    producer.flush.assert_called_once()
    producer.close.assert_called_once()
    consumer.close.assert_called_once()
    client.close.assert_called_once()


def test_dlq_producer_has_bounded_send_and_durable_ack(monkeypatch):
    factory = MagicMock()
    monkeypatch.setattr(batch.legacy, "KafkaProducer", factory)
    batch.create_batch_dlq_producer()
    assert factory.call_args.kwargs["max_block_ms"] == batch.DLQ_MAX_BLOCK_MS
    assert factory.call_args.kwargs["acks"] == "all"


@pytest.mark.parametrize("failure", ["LeaveGroup request failed: timeout", "transport close failed"])
def test_kafka_logged_close_errors_report_incomplete(monkeypatch, caplog, failure):
    _, consumer, _, producer, client, handlers = _run_with_signal_fixture(monkeypatch)
    consumer.poll.side_effect = lambda **kwargs: (handlers[signal.SIGTERM](signal.SIGTERM, None) or {})
    consumer.close.side_effect = lambda **kwargs: logging.getLogger("kafka.coordinator").error(failure)
    with caplog.at_level(logging.INFO), pytest.raises(RuntimeError, match="shutdown incomplete"):
        batch.run()
    assert '"status": "incomplete"' in caplog.text
    assert failure in caplog.text
    producer.flush.assert_called_once()
    client.close.assert_called_once()


def test_clean_close_reports_group_departure_unverified(monkeypatch, caplog):
    _, consumer, _, _, _, handlers = _run_with_signal_fixture(monkeypatch)
    consumer.poll.side_effect = lambda **kwargs: (handlers[signal.SIGTERM](signal.SIGTERM, None) or {})
    with caplog.at_level(logging.INFO):
        batch.run()
    assert '"group_departure": "unverified"' in caplog.text
    assert '"status": "close_completed_group_unverified"' in caplog.text
