"""Polling-thread position recovery after Kafka partition reassignment."""
from test_module3_batch_faults import _message_for_run, _partition_worker
from kafka_batch_consumer import drain_worker


def retained_worker(committed=3, position=6):
    worker, consumer, writer, dlq, tp0, tp1, paused = _partition_worker()
    consumer.committed.return_value = committed
    consumer.position.return_value = position
    consumer.config = {"auto_offset_reset": "earliest"}
    worker.enqueue_polled_records({tp0: [_message_for_run(i, 0, "r") for i in (3, 4, 5)]})
    worker.on_partitions_revoked([tp0])
    worker.on_partitions_assigned([tp0, tp1])
    return worker, consumer, writer, tp0, tp1, paused


def test_same_owner_seeks_committed_and_resumes_paused_partition():
    worker, consumer, _, tp0, _, paused = retained_worker()
    worker.process_ready()
    consumer.seek.assert_called_once_with(tp0, 3)
    assert tp0 not in paused
    assert tp0 not in worker.recovery_partitions
    consumer.commit.assert_not_called()


def test_matching_position_does_not_seek():
    worker, consumer, _, _, _, _ = retained_worker(position=3)
    worker.process_ready()
    consumer.seek.assert_not_called()


def test_recovery_uses_latest_broker_commit():
    worker, consumer, _, tp0, _, _ = retained_worker(committed=5, position=6)
    worker.process_ready()
    consumer.seek.assert_called_once_with(tp0, 5)


def test_position_behind_newer_commit_seeks_forward_not_back():
    worker, consumer, _, tp0, _, _ = retained_worker(committed=7, position=6)
    worker.process_ready()
    consumer.seek.assert_called_once_with(tp0, 7)


def test_reassignment_during_commit_lookup_does_not_seek_stale_owner():
    worker, consumer, _, tp0, tp1, _ = retained_worker()
    def lose_and_reassign(*args, **kwargs):
        worker.on_partitions_revoked([tp0])
        worker.on_partitions_assigned([tp0, tp1])
        return 3
    consumer.committed.side_effect = lose_and_reassign
    worker.process_ready()
    consumer.seek.assert_not_called()
    assert tp0 in worker.recovery_partitions


def test_missing_commit_uses_configured_reset_policy():
    worker, consumer, _, tp0, _, _ = retained_worker(committed=None)
    worker.process_ready()
    consumer.seek_to_beginning.assert_called_once_with(tp0)
    consumer.seek.assert_not_called()


def test_latest_reset_policy_is_preserved():
    worker, consumer, _, tp0, _, _ = retained_worker(committed=None)
    consumer.config = {"auto_offset_reset": "latest"}
    worker.process_ready()
    consumer.seek_to_end.assert_called_once_with(tp0)


def test_stale_polled_records_are_discarded_then_refetched_and_committed():
    worker, consumer, writer, tp0, _, _ = retained_worker()
    records = [_message_for_run(i, 0, "r") for i in (3, 4, 5)]
    worker.enqueue_polled_records({tp0: records})
    assert not worker.partitions[tp0].records
    worker.process_ready()
    consumer.commit.assert_not_called()
    worker.process_polled_records({tp0: records})
    worker.process_ready()
    assert writer.write.call_count == 2
    assert consumer._successful_commits == [(0, 5), (0, 6)]


def test_other_partition_keeps_progress_during_recovery_failure():
    worker, consumer, writer, tp0, tp1, paused = retained_worker()
    consumer.committed.side_effect = TimeoutError("coordinator unavailable")
    worker.process_polled_records({tp1: [_message_for_run(i, 1, "healthy") for i in (0, 1)]})
    assert tp0 in paused
    assert tp1 not in paused
    assert writer.write.call_count == 1
    assert consumer._successful_commits == [(1, 2)]


def test_different_owner_does_not_use_stale_prior_owner_marker():
    worker, consumer, _, tp0, tp1, _ = retained_worker()
    worker.on_partitions_assigned([tp1])
    assert tp0 not in worker.recovery_partitions
    replacement, other, _, _, _, _, _ = _partition_worker()
    replacement.on_partitions_assigned([tp0])
    replacement.process_ready()
    other.seek.assert_not_called()


def test_repeated_reassignments_reconcile_each_discarded_batch():
    worker, consumer, _, tp0, tp1, _ = retained_worker()
    worker.process_ready()
    worker.enqueue_polled_records({tp0: [_message_for_run(6, 0, "r")]})
    worker.on_partitions_revoked([tp0])
    worker.on_partitions_assigned([tp0, tp1])
    consumer.committed.return_value = 6
    consumer.position.return_value = 7
    worker.process_ready()
    assert [c.args for c in consumer.seek.call_args_list] == [(tp0, 3), (tp0, 6)]


def test_output_failure_is_discarded_and_recovered_without_commit():
    worker, consumer, writer, _, tp0, tp1, _ = _partition_worker()
    consumer.committed.return_value = 0
    consumer.position.return_value = 2
    writer.write.side_effect = TimeoutError("influx unavailable")
    worker.process_polled_records({tp0: [_message_for_run(i, 0, "r") for i in (0, 1)]})
    consumer.commit.assert_not_called()
    worker.on_partitions_revoked([tp0])
    worker.on_partitions_assigned([tp0, tp1])
    writer.write.side_effect = None
    worker.process_ready()
    consumer.seek.assert_called_once_with(tp0, 0)
    consumer.commit.assert_not_called()


def test_revocation_during_confirmed_write_prevents_stale_owner_commit():
    worker, consumer, writer, _, tp0, _, _ = _partition_worker()
    writer.write.side_effect = lambda **kwargs: worker.on_partitions_revoked([tp0])
    worker.process_polled_records({tp0: [_message_for_run(i, 0, "r") for i in (0, 1)]})
    writer.write.assert_called_once()
    consumer.commit.assert_not_called()


def test_shutdown_reports_unreconciled_discarded_work_as_incomplete(caplog):
    worker, consumer, _, tp0, _, _ = retained_worker()
    assert drain_worker(worker, -1) is False
    assert tp0 in worker.recovery_partitions
    assert "stage=position_recovery" in caplog.text
    consumer.commit.assert_not_called()
    consumer.seek.assert_not_called()
