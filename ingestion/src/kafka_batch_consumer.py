"""Opt-in bounded synchronous batch writer with partition-level fault isolation.

The frozen legacy Module 02 consumer remains the production default. This
worker is opt-in and must not run concurrently with the legacy consumer in the
same source topic/group. Kafka consumer operations remain on one thread:
failed partitions are paused and retried while poll continues serving healthy
partitions and group heartbeats.
"""
from __future__ import annotations

import logging
import os
import time
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata, TopicPartition

import kafka_consumer as legacy

logger = logging.getLogger("pms.ingestion.batch")

BATCH_SIZE = int(os.getenv("PMS_INFLUX_BATCH_SIZE", "20"))
MAX_BATCH_SIZE = 200
CONFIRM = "opt-in-isolated-batch-worker"
RETRY_BASE_SECONDS = float(os.getenv("PMS_PARTITION_RETRY_BASE_SECONDS", "1"))
RETRY_MAX_SECONDS = float(os.getenv("PMS_PARTITION_RETRY_MAX_SECONDS", "30"))


def require_safe_config() -> None:
    if os.environ.get("PMS_BATCH_WORKER_CONFIRM") != CONFIRM:
        raise RuntimeError("Refusing to start: PMS_BATCH_WORKER_CONFIRM is required")
    if not 2 <= BATCH_SIZE <= MAX_BATCH_SIZE:
        raise ValueError(f"PMS_INFLUX_BATCH_SIZE must be in [2,{MAX_BATCH_SIZE}]")
    if legacy.MAX_POLL_RECORDS < BATCH_SIZE:
        raise ValueError("KAFKA_MAX_POLL_RECORDS must be >= PMS_INFLUX_BATCH_SIZE")
    if RETRY_BASE_SECONDS <= 0 or RETRY_MAX_SECONDS < RETRY_BASE_SECONDS:
        raise ValueError("Partition retry backoff must be positive and max >= base")


def persist_points(write_api: Any, points: Sequence[Any]) -> None:
    """Attempt one synchronous write; retry scheduling belongs to the poll loop."""
    if points:
        write_api.write(
            bucket=legacy.INFLUXDB_BUCKET,
            org=legacy.INFLUXDB_ORG,
            record=list(points),
        )


def persist_points_with_retries(write_api: Any, points: Sequence[Any]) -> None:
    """Compatibility helper for direct partition tests, not used by run()."""
    for attempt in range(legacy.MAX_WRITE_RETRIES):
        try:
            persist_points(write_api, points)
            return
        except Exception as exc:
            if getattr(exc, "status", None) in (401, 403):
                raise
            if attempt == legacy.MAX_WRITE_RETRIES - 1:
                raise
            time.sleep(legacy.RETRY_BACKOFF_SECONDS * (2 ** attempt))


def commit_through(consumer: Any, message: Any) -> None:
    """Commit the next offset for exactly one fully handled partition prefix."""
    tp = TopicPartition(message.topic, message.partition)
    consumer.commit(
        {tp: OffsetAndMetadata(message.offset + 1, None, -1)},
        timeout_ms=legacy.COMMIT_TIMEOUT_MS,
    )


def handle_partition(consumer: Any, write_api: Any, dlq_producer: Any, messages: Sequence[Any],
                     batch_size: int = BATCH_SIZE) -> None:
    """Synchronous compatibility helper used by focused unit tests.

    The long-running service uses PartitionIsolatingBatchWorker below so an
    exhausted write/commit/DLQ operation cannot terminate healthy partitions.
    """
    if not 2 <= batch_size <= MAX_BATCH_SIZE:
        raise ValueError("Invalid batch size")
    pending_points: list[Any] = []
    last_valid = None

    def flush() -> None:
        nonlocal last_valid
        if not pending_points:
            return
        persist_points_with_retries(write_api, pending_points)
        commit_through(consumer, last_valid)
        pending_points.clear()
        last_valid = None

    for message in messages:
        try:
            payload = legacy.decode_message(message.value)
            point = legacy.build_point(payload)
        except (ValueError, TypeError, UnicodeDecodeError) as exc:
            flush()
            legacy.dead_letter(dlq_producer, message, exc)
            commit_through(consumer, message)
            continue
        pending_points.append(point)
        last_valid = message
        if len(pending_points) >= batch_size:
            flush()
    flush()


@dataclass
class PendingBatch:
    """One ordered partition prefix and the exact operation still to complete."""

    valid_messages: list[Any]
    points: list[Any]
    invalid_message: Any | None = None
    stage: str = "write"


@dataclass
class PartitionProgress:
    """In-memory queue and retry state for one currently assigned partition."""

    records: deque[Any] = field(default_factory=deque)
    pending: PendingBatch | None = None
    retry_attempt: int = 0
    retry_at: float = 0.0
    failure_paused: bool = False
    backpressure_paused: bool = False


class PartitionIsolatingBatchWorker:
    """Pause/retry only the failing partition while polling all healthy ones.

    All KafkaConsumer calls, including poll, pause, resume, and commit, are
    performed by the caller's single consumer thread. Uncommitted work is
    intentionally discarded on revocation so the new owner replays from the
    broker's committed offset. InfluxDB point identities are deterministic,
    therefore a replay may repeat a write but converges to the same point.
    """

    def __init__(
        self,
        consumer: Any,
        write_api: Any,
        dlq_producer: Any,
        batch_size: int = BATCH_SIZE,
        retry_base_seconds: float = RETRY_BASE_SECONDS,
        retry_max_seconds: float = RETRY_MAX_SECONDS,
        clock: Any = time.monotonic,
    ) -> None:
        if not 2 <= batch_size <= MAX_BATCH_SIZE:
            raise ValueError("Invalid batch size")
        if retry_base_seconds <= 0 or retry_max_seconds < retry_base_seconds:
            raise ValueError("Invalid partition retry backoff")
        self.consumer = consumer
        self.write_api = write_api
        self.dlq_producer = dlq_producer
        self.batch_size = batch_size
        self.retry_base_seconds = retry_base_seconds
        self.retry_max_seconds = retry_max_seconds
        self.clock = clock
        self.partitions: dict[TopicPartition, PartitionProgress] = {}
        self.completed_records = 0

    def on_partitions_revoked(self, revoked: Sequence[TopicPartition]) -> None:
        """Drop uncommitted local work before Kafka transfers ownership."""
        for tp in revoked:
            progress = self.partitions.pop(tp, None)
            if progress is not None:
                queued = len(progress.records)
                stage = progress.pending.stage if progress.pending else "queued"
                logger.warning(
                    "Partition revoked; discarding uncommitted local work topic=%s partition=%s "
                    "queued=%d stage=%s; new owner will replay from Kafka committed offset",
                    tp.topic, tp.partition, queued, stage,
                )

    def on_partitions_assigned(self, assigned: Sequence[TopicPartition]) -> None:
        """Initialize state for new ownership and clear stale local pause flags."""
        for tp in assigned:
            self.partitions.setdefault(tp, PartitionProgress())
        paused = set(self.consumer.paused())
        to_resume = [tp for tp in assigned if tp in paused]
        if to_resume:
            self.consumer.resume(*to_resume)
            logger.info("Resumed newly assigned partitions count=%d", len(to_resume))

    def process_polled_records(self, records: dict[TopicPartition, Sequence[Any]]) -> None:
        """Queue one poll result, then advance each assigned partition once."""
        for tp, messages in records.items():
            progress = self.partitions.setdefault(tp, PartitionProgress())
            progress.records.extend(messages)
            progress.backpressure_paused = len(progress.records) >= self.batch_size
            self._sync_pause(tp, progress)
        self.process_ready()

    def process_ready(self) -> None:
        """Make one bounded processing attempt per ready partition per poll."""
        now = self.clock()
        for tp, progress in list(self.partitions.items()):
            if progress.pending is None and progress.records:
                progress.pending = self._prepare_pending(progress)
            if progress.pending is not None and now >= progress.retry_at:
                try:
                    self._advance_pending(tp, progress)
                except Exception:
                    progress.retry_attempt += 1
                    delay = min(
                        self.retry_base_seconds * (2 ** (progress.retry_attempt - 1)),
                        self.retry_max_seconds,
                    )
                    progress.retry_at = self.clock() + delay
                    progress.failure_paused = True
                    logger.exception(
                        "Partition operation failed; pausing only this partition "
                        "topic=%s partition=%s stage=%s attempt=%d retry_in_seconds=%.3f",
                        tp.topic, tp.partition,
                        progress.pending.stage if progress.pending else "unknown",
                        progress.retry_attempt, delay,
                    )
                else:
                    if progress.pending is None:
                        progress.retry_attempt = 0
                        progress.retry_at = 0.0
                        progress.failure_paused = False
            progress.backpressure_paused = len(progress.records) >= self.batch_size
            self._sync_pause(tp, progress)

    def _prepare_pending(self, progress: PartitionProgress) -> PendingBatch:
        valid_messages: list[Any] = []
        points: list[Any] = []
        invalid_message = None
        for message in list(progress.records)[:self.batch_size]:
            try:
                payload = legacy.decode_message(message.value)
                point = legacy.build_point(payload)
            except (ValueError, TypeError, UnicodeDecodeError):
                invalid_message = message
                break
            valid_messages.append(message)
            points.append(point)
        stage = "write" if points else "dlq"
        return PendingBatch(valid_messages, points, invalid_message, stage)

    def _advance_pending(self, tp: TopicPartition, progress: PartitionProgress) -> None:
        pending = progress.pending
        if pending is None:
            return
        while True:
            if pending.stage == "write":
                persist_points(self.write_api, pending.points)
                pending.stage = "commit_valid"
            elif pending.stage == "commit_valid":
                if not pending.valid_messages:
                    pending.stage = "dlq"
                    continue
                commit_through(self.consumer, pending.valid_messages[-1])
                self._remove_committed_prefix(progress, pending.valid_messages)
                pending.valid_messages.clear()
                pending.points.clear()
                if pending.invalid_message is not None:
                    pending.stage = "dlq"
                else:
                    progress.pending = None
                    self.completed_records += 1
                    return
            elif pending.stage == "dlq":
                if pending.invalid_message is None:
                    progress.pending = None
                    return
                message = pending.invalid_message
                try:
                    payload = legacy.decode_message(message.value)
                    error = ValueError("record rejected by telemetry validation")
                except (ValueError, TypeError, UnicodeDecodeError) as exc:
                    error = exc
                else:
                    # A point-construction error is also a permanent record error.
                    try:
                        legacy.build_point(payload)
                    except (ValueError, TypeError, UnicodeDecodeError) as exc:
                        error = exc
                legacy.dead_letter(self.dlq_producer, message, error)
                pending.stage = "commit_dlq"
            elif pending.stage == "commit_dlq":
                message = pending.invalid_message
                if message is None:
                    raise RuntimeError("DLQ commit stage has no invalid record")
                commit_through(self.consumer, message)
                self._remove_committed_prefix(progress, [message])
                progress.pending = None
                self.completed_records += len(pending.valid_messages) + 1
                return
            else:
                raise RuntimeError(f"Unknown pending partition stage: {pending.stage}")

    @staticmethod
    def _remove_committed_prefix(progress: PartitionProgress, messages: Sequence[Any]) -> None:
        for expected in messages:
            if not progress.records:
                raise RuntimeError("Committed partition queue unexpectedly empty")
            actual = progress.records[0]
            if (actual.topic, actual.partition, actual.offset) != (
                expected.topic, expected.partition, expected.offset
            ):
                raise RuntimeError(
                    "Committed prefix does not match queued partition head: "
                    f"expected={expected.offset} actual={actual.offset}"
                )
            progress.records.popleft()

    def _sync_pause(self, tp: TopicPartition, progress: PartitionProgress) -> None:
        if tp not in set(self.consumer.assignment()):
            return
        should_pause = progress.failure_paused or progress.backpressure_paused
        paused = set(self.consumer.paused())
        if should_pause and tp not in paused:
            self.consumer.pause(tp)
            logger.info(
                "Paused partition topic=%s partition=%s failure=%s backpressure=%s",
                tp.topic, tp.partition, progress.failure_paused, progress.backpressure_paused,
            )
        elif not should_pause and tp in paused:
            self.consumer.resume(tp)
            logger.info("Resumed partition topic=%s partition=%s", tp.topic, tp.partition)


def create_batch_consumer(listener: PartitionIsolatingBatchWorker) -> Any:
    """Create a manually committed consumer with rebalance callbacks."""
    logger.info("Connecting batch consumer broker=%s topic=%s", legacy.KAFKA_BROKER, legacy.SENSOR_TOPIC)
    for attempt in range(30):
        try:
            consumer = legacy.KafkaConsumer(
                bootstrap_servers=legacy.KAFKA_BROKER,
                group_id=legacy.CONSUMER_GROUP,
                value_deserializer=lambda value: value,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                max_poll_records=legacy.MAX_POLL_RECORDS,
                max_poll_interval_ms=legacy.MAX_POLL_INTERVAL_MS,
            )
            consumer.subscribe(topics=[legacy.SENSOR_TOPIC], listener=listener)
            logger.info("Batch Kafka consumer connected")
            return consumer
        except KafkaError:
            logger.exception("Batch Kafka connection failed attempt=%d", attempt + 1)
            if attempt < 29:
                time.sleep(2)
    raise RuntimeError("Batch Kafka connection failed after 30 attempts")


def run() -> None:
    require_safe_config()
    # Create the consumer first, then attach the worker as its rebalance listener.
    consumer = legacy.KafkaConsumer(
        bootstrap_servers=legacy.KAFKA_BROKER,
        group_id=legacy.CONSUMER_GROUP,
        value_deserializer=lambda value: value,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=legacy.MAX_POLL_RECORDS,
        max_poll_interval_ms=legacy.MAX_POLL_INTERVAL_MS,
    )
    client = None
    producer = None
    try:
        client, write_api = legacy.create_influxdb_client()
        producer = legacy.create_dead_letter_producer()
        worker = PartitionIsolatingBatchWorker(consumer, write_api, producer)
        consumer.subscribe(topics=[legacy.SENSOR_TOPIC], listener=worker)
        logger.info(
            "Partition-isolating batch writer started batch_size=%s group=%s retry_base=%.2f retry_max=%.2f",
            BATCH_SIZE, legacy.CONSUMER_GROUP, RETRY_BASE_SECONDS, RETRY_MAX_SECONDS,
        )
        while True:
            records = consumer.poll(
                timeout_ms=legacy.POLL_TIMEOUT_MS,
                max_records=legacy.MAX_POLL_RECORDS,
            )
            worker.process_polled_records(records)
            if worker.completed_records and worker.completed_records % 100 == 0:
                logger.info("Completed source records=%d", worker.completed_records)
    except KeyboardInterrupt:
        logger.info("Batch writer stopped by operator")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
        consumer.close()
        if client is not None:
            client.close()


if __name__ == "__main__":
    run()
