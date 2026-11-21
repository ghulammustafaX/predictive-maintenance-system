"""Fault-injection tests for opt-in Module 03 batch writer.

These tests never connect to a broker or mutate production state. They model
ambiguous persistence, retry exhaustion, commit failure, and partitions.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion" / "src"))
from kafka_batch_consumer import handle_partition
from test_module3_batch_writer import message, commit_offsets


def test_ambiguous_ack_after_durable_write_replays_same_point_identities():
    consumer, writer, dlq = MagicMock(), MagicMock(), MagicMock()
    persisted = set()
    calls = []

    def write(**kw):
        identities = []
        for point in kw["record"]:
            identity = (point._name, tuple(sorted(point._tags.items())), str(point._time))
            identities.append(identity)
            persisted.add(identity)
        calls.append(identities)
        # Simulate Influx accepting all points but connection dropping before
        # client sees the acknowledgment. No offset may advance at this point.
        if len(calls) == 1:
            assert consumer.commit.call_count == 0
            raise TimeoutError("acknowledgement lost AFTER durable write")

    writer.write.side_effect = write
    with patch("kafka_batch_consumer.time.sleep"):
        handle_partition(consumer, writer, dlq, [message(i) for i in range(3)], batch_size=3)
    assert len(calls) == 2
    assert calls[0] == calls[1]
    assert len(persisted) == 3
    assert commit_offsets(consumer) == [3]


def test_exhausted_ambiguous_write_never_commits():
    consumer, writer, dlq = MagicMock(), MagicMock(), MagicMock()
    writer.write.side_effect = TimeoutError("ambiguous write")
    with patch("kafka_batch_consumer.time.sleep"):
        with pytest.raises(TimeoutError):
            handle_partition(consumer, writer, dlq, [message(i) for i in range(3)], batch_size=3)
    consumer.commit.assert_not_called()
    dlq.send.assert_not_called()


def test_commit_failure_after_confirmed_write_replays_without_skipping():
    consumer, writer, dlq = MagicMock(), MagicMock(), MagicMock()
    first = [message(i) for i in range(2)]
    consumer.commit.side_effect = RuntimeError("commit response unavailable")
    with pytest.raises(RuntimeError, match="commit response unavailable"):
        handle_partition(consumer, writer, dlq, first + [message(2)], batch_size=2)
    assert writer.write.call_count == 1
    # A new consumer replays from the previous committed offset. Reconstructed
    # point identities must match for the same underlying messages.
    first_identities = [(p._name, dict(p._tags), p._time) for p in writer.write.call_args.kwargs["record"]]
    replay_consumer = MagicMock()
    replay_writer = MagicMock()
    handle_partition(replay_consumer, replay_writer, dlq, first, batch_size=2)
    replay_identities = [(p._name, dict(p._tags), p._time) for p in replay_writer.write.call_args.kwargs["record"]]
    assert first_identities == replay_identities
    assert commit_offsets(replay_consumer) == [2]


def test_failed_dlq_publication_after_flushed_prefix_blocks_all_later_offsets():
    consumer, writer, dlq = MagicMock(), MagicMock(), MagicMock()
    dlq.send.return_value.get.side_effect = TimeoutError("DLQ ack unavailable")
    with pytest.raises(TimeoutError):
        handle_partition(consumer, writer, dlq,
                         [message(0), message(1, valid=False), message(2), message(3)],
                         batch_size=2)
    assert commit_offsets(consumer) == [1]
    assert writer.write.call_count == 1


def test_multi_partition_commit_identity_cannot_cross_advance():
    consumer, writer, dlq = MagicMock(), MagicMock(), MagicMock()
    handle_partition(consumer, writer, dlq,
                     [message(10, partition=0), message(11, partition=0)], batch_size=2)
    handle_partition(consumer, writer, dlq,
                     [message(20, partition=1), message(21, partition=1)], batch_size=2)
    commits = [list(call.args[0].items())[0] for call in consumer.commit.call_args_list]
    assert [(tp.partition, value.offset) for tp, value in commits] == [(0, 12), (1, 22)]


def test_single_point_writer_and_batch_writer_produce_same_schema():
    from kafka_consumer import build_point
    sample = message(9)
    import json
    payload = json.loads(sample.value)
    original = build_point(payload)
    consumer, writer, dlq = MagicMock(), MagicMock(), MagicMock()
    handle_partition(consumer, writer, dlq, [sample, message(10)], batch_size=2)
    batched = writer.write.call_args.kwargs["record"][0]
    assert original._name == batched._name
    assert original._tags == batched._tags
    assert original._fields == batched._fields
    assert original._time == batched._time
