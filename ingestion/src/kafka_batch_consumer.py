"""Opt-in bounded synchronous batch writer, retaining the frozen Module 02 contract.

Never start this worker alongside the legacy consumer using the same Kafka
consumer group. No production Compose service points at this entrypoint.
"""
from __future__ import annotations

import logging
import os
import time
from collections.abc import Sequence
from typing import Any

from kafka.structs import OffsetAndMetadata, TopicPartition

import kafka_consumer as legacy

logger = logging.getLogger("pms.ingestion.batch")

BATCH_SIZE = int(os.getenv("PMS_INFLUX_BATCH_SIZE", "20"))
MAX_BATCH_SIZE = 200
CONFIRM = "opt-in-isolated-batch-worker"


def require_safe_config() -> None:
    if os.environ.get("PMS_BATCH_WORKER_CONFIRM") != CONFIRM:
        raise RuntimeError("Refusing to start: PMS_BATCH_WORKER_CONFIRM is required")
    if not 2 <= BATCH_SIZE <= MAX_BATCH_SIZE:
        raise ValueError(f"PMS_INFLUX_BATCH_SIZE must be in [2,{MAX_BATCH_SIZE}]")
    if legacy.MAX_POLL_RECORDS < BATCH_SIZE:
        raise ValueError("KAFKA_MAX_POLL_RECORDS must be >= PMS_INFLUX_BATCH_SIZE")


def persist_points(write_api: Any, points: Sequence[Any]) -> None:
    """Synchronous all-or-error call. Never commits on failed/unknown result."""
    if not points:
        return
    for attempt in range(legacy.MAX_WRITE_RETRIES):
        try:
            write_api.write(
                bucket=legacy.INFLUXDB_BUCKET,
                org=legacy.INFLUXDB_ORG,
                record=list(points),
            )
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
    """Process one Kafka partition in order with malformed-message barriers.

    - Decode/build points before the write. Invalid records must not cause a
      valid point batch to be lost or source offsets to advance prematurely.
    - Commit only after the full current batch is synchronously acknowledged.
    - Before DLQ, flush and commit the preceding valid batch; then publish the
      malformed record to DLQ and commit it only after broker acknowledgement.
    - If write, DLQ, or commit fails, stop; replay from last committed offset.
      InfluxDB identities from the accepted writer are deterministic.
    """
    if not 2 <= batch_size <= MAX_BATCH_SIZE:
        raise ValueError("Invalid batch size")
    pending_points: list[Any] = []
    last_valid = None

    def flush() -> None:
        nonlocal last_valid
        if not pending_points:
            return
        persist_points(write_api, pending_points)
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


def run() -> None:
    require_safe_config()
    consumer = legacy.create_kafka_consumer()
    client = None
    write_api = None
    producer = None
    try:
        client, write_api = legacy.create_influxdb_client()
        producer = legacy.create_dead_letter_producer()
        logger.info("Opt-in batch writer started, batch_size=%s group=%s", BATCH_SIZE, legacy.CONSUMER_GROUP)
        while True:
            partitions = consumer.poll(
                timeout_ms=legacy.POLL_TIMEOUT_MS,
                max_records=legacy.MAX_POLL_RECORDS,
            )
            for _, records in partitions.items():
                handle_partition(consumer, write_api, producer, records, BATCH_SIZE)
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
