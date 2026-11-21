"""Test-only sustained-load worker for isolated Module 03 acceptance.

This process exercises the real legacy or partition-isolating consumer logic.
It is guarded so it cannot connect to non-test Kafka or InfluxDB endpoints.
"""
from __future__ import annotations

import json
import os
import resource
import statistics
import sys
import time
from pathlib import Path
from typing import Any

EXPECTED_GUARD = {
    "MODULE3_LOAD_GUARD": "m3-isolated-sustained-load-only",
    "KAFKA_BROKER": "kafka:9092",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
}
for key, expected in EXPECTED_GUARD.items():
    if os.environ.get(key) != expected:
        raise SystemExit(f"REFUSED: invalid sustained-load setting {key}")

MODE = os.environ.get("MODULE3_LOAD_MODE", "")
if MODE not in {"legacy", "batch"}:
    raise SystemExit("REFUSED: MODULE3_LOAD_MODE must be legacy or batch")
EXPECTED_RECORDS = int(os.environ.get("MODULE3_LOAD_EXPECTED_RECORDS", "3000"))
MAX_RUNTIME_SECONDS = int(os.environ.get("MODULE3_LOAD_MAX_RUNTIME_SECONDS", "600"))
RESULTS_DIR = Path(os.environ.get("MODULE3_LOAD_RESULTS_DIR", "/results"))
if EXPECTED_RECORDS < 1 or MAX_RUNTIME_SECONDS < 30:
    raise SystemExit("REFUSED: invalid sustained-load bounds")

sys.path.insert(0, "/app/src")
import kafka_consumer as legacy  # noqa: E402

if MODE == "batch":
    import kafka_batch_consumer as batch  # noqa: E402


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 3)


def message_timestamp_ms(message: Any) -> int | None:
    value = getattr(message, "timestamp", None)
    return int(value) if value is not None and int(value) >= 0 else None


def offset_snapshot(consumer: Any) -> dict[str, Any]:
    assignment = sorted(
        consumer.assignment(),
        key=lambda tp: (tp.topic, tp.partition),
    )
    committed: dict[str, int | None] = {}
    end_offsets: dict[str, int] = {}
    for tp in assignment:
        key = f"{tp.topic}:{tp.partition}"
        metadata = consumer.committed(tp)
        committed[key] = int(metadata) if isinstance(metadata, int) else (
            int(metadata.offset) if metadata is not None else None
        )
    if assignment:
        for tp, offset in consumer.end_offsets(assignment).items():
            end_offsets[f"{tp.topic}:{tp.partition}"] = int(offset)
    total_lag = 0
    for key, end in end_offsets.items():
        current = committed.get(key)
        if current is None:
            total_lag += end
        else:
            total_lag += max(0, end - current)
    return {
        "committed_offsets": committed,
        "end_offsets": end_offsets,
        "total_lag": total_lag,
        "assigned_partitions": len(assignment),
    }


def make_summary(
    count: int,
    started_at: float | None,
    ended_at: float,
    e2e_ms: list[float],
    write_ms: list[float],
    write_batch_sizes: list[int],
    commit_ms: list[float],
    offsets: dict[str, Any],
    cpu_before: float,
    rss_before_kb: int,
) -> dict[str, Any]:
    elapsed = max(ended_at - (started_at if started_at is not None else ended_at), 0.001)
    usage = resource.getrusage(resource.RUSAGE_SELF)
    cpu_after = usage.ru_utime + usage.ru_stime
    return {
        "mode": MODE,
        "topic": legacy.SENSOR_TOPIC,
        "consumer_group": legacy.CONSUMER_GROUP,
        "expected_records": EXPECTED_RECORDS,
        "processed_records": count,
        "elapsed_seconds": round(elapsed, 3),
        "records_per_second": round(count / elapsed, 3),
        "end_to_end_latency_ms": {
            "p50": percentile(e2e_ms, 0.50),
            "p95": percentile(e2e_ms, 0.95),
            "p99": percentile(e2e_ms, 0.99),
            "samples": len(e2e_ms),
        },
        "write_latency_ms": {
            "p50": percentile(write_ms, 0.50),
            "p95": percentile(write_ms, 0.95),
            "p99": percentile(write_ms, 0.99),
            "calls": len(write_ms),
        },
        "points_per_write": {
            "average": round(sum(write_batch_sizes) / len(write_batch_sizes), 3) if write_batch_sizes else 0,
            "p50": percentile([float(size) for size in write_batch_sizes], 0.50),
            "p95": percentile([float(size) for size in write_batch_sizes], 0.95),
            "max": max(write_batch_sizes, default=0),
            "calls": len(write_batch_sizes),
        },
        "commit_latency_ms": {
            "p50": percentile(commit_ms, 0.50),
            "p95": percentile(commit_ms, 0.95),
            "p99": percentile(commit_ms, 0.99),
            "calls": len(commit_ms),
        },
        "cpu_seconds": round(max(0.0, cpu_after - cpu_before), 3),
        "max_rss_kb": int(usage.ru_maxrss),
        "offsets": offsets,
    }


def run_legacy() -> dict[str, Any]:
    consumer = legacy.create_kafka_consumer()
    client = None
    producer = None
    write_ms: list[float] = []
    write_batch_sizes: list[int] = []
    commit_ms: list[float] = []
    e2e_ms: list[float] = []
    count = 0
    started_at: float | None = None
    cpu_before_usage = resource.getrusage(resource.RUSAGE_SELF)
    cpu_before = cpu_before_usage.ru_utime + cpu_before_usage.ru_stime
    rss_before_kb = int(cpu_before_usage.ru_maxrss)
    deadline = time.monotonic() + MAX_RUNTIME_SECONDS

    try:
        client, write_api = legacy.create_influxdb_client()
        producer = legacy.create_dead_letter_producer()
        original_process = legacy.process_message
        original_write = legacy.write_to_influxdb
        original_commit = legacy.commit_message

        def measured_process(write_api_arg: Any, producer_arg: Any, message: Any) -> None:
            nonlocal started_at
            if started_at is None:
                started_at = time.monotonic()
            original_process(write_api_arg, producer_arg, message)

        def measured_write(write_api_arg: Any, payload: Any) -> bool:
            before = time.perf_counter()
            result = original_write(write_api_arg, payload)
            write_ms.append((time.perf_counter() - before) * 1000)
            write_batch_sizes.append(1)
            return result

        def measured_commit(consumer_arg: Any, message: Any) -> None:
            nonlocal count
            before = time.perf_counter()
            original_commit(consumer_arg, message)
            commit_ms.append((time.perf_counter() - before) * 1000)
            timestamp = message_timestamp_ms(message)
            if timestamp is not None:
                e2e_ms.append(max(0.0, time.time() * 1000 - timestamp))
            count += 1

        legacy.process_message = measured_process
        legacy.write_to_influxdb = measured_write
        legacy.commit_message = measured_commit

        while count < EXPECTED_RECORDS:
            if time.monotonic() > deadline:
                raise TimeoutError(f"legacy worker timed out after {count} records")
            records = consumer.poll(
                timeout_ms=legacy.POLL_TIMEOUT_MS,
                max_records=legacy.MAX_POLL_RECORDS,
            )
            for partition_records in records.values():
                for message in partition_records:
                    legacy.process_message(write_api, producer, message)
                    legacy.commit_message(consumer, message)
                    if count % 500 == 0:
                        print(f"M3_LOAD_PROGRESS mode=legacy committed={count}", flush=True)
                    if count >= EXPECTED_RECORDS:
                        break
                if count >= EXPECTED_RECORDS:
                    break

        offsets = offset_snapshot(consumer)
        if offsets["total_lag"] != 0:
            raise AssertionError(f"legacy consumer completed with lag: {offsets}")
        ended_at = time.monotonic()
        return make_summary(
            count, started_at, ended_at, e2e_ms, write_ms, write_batch_sizes, commit_ms,
            offsets, cpu_before, rss_before_kb,
        )
    finally:
        producer and producer.close()
        consumer.close()
        client and client.close()


def run_batch() -> dict[str, Any]:
    consumer = batch.create_batch_consumer()
    client = None
    producer = None
    write_ms: list[float] = []
    write_batch_sizes: list[int] = []
    measurement_errors: list[str] = []
    commit_ms: list[float] = []
    e2e_ms: list[float] = []
    started_at: float | None = None
    cpu_before_usage = resource.getrusage(resource.RUSAGE_SELF)
    cpu_before = cpu_before_usage.ru_utime + cpu_before_usage.ru_stime
    rss_before_kb = int(cpu_before_usage.ru_maxrss)
    deadline = time.monotonic() + MAX_RUNTIME_SECONDS
    original_persist = None
    original_commit = None
    original_remove = None

    try:
        client, write_api = legacy.create_influxdb_client()
        producer = legacy.create_dead_letter_producer()
        worker = batch.PartitionIsolatingBatchWorker(
            consumer,
            write_api,
            producer,
            batch_size=batch.BATCH_SIZE,
            retry_base_seconds=batch.RETRY_BASE_SECONDS,
            retry_max_seconds=batch.RETRY_MAX_SECONDS,
        )
        consumer.subscribe(topics=[legacy.SENSOR_TOPIC], listener=worker)

        original_enqueue = worker.enqueue_polled_records
        original_persist = batch.persist_points
        original_commit = batch.commit_through
        original_remove = batch.PartitionIsolatingBatchWorker._remove_committed_prefix

        def measured_enqueue(records: dict[Any, list[Any]]) -> None:
            nonlocal started_at
            if started_at is None and any(records.values()):
                started_at = time.monotonic()
            original_enqueue(records)

        def measured_persist(write_api_arg: Any, points: Any) -> None:
            before = time.perf_counter()
            original_persist(write_api_arg, points)
            # Never surface instrumentation errors as failed Influx writes: the
            # write has already succeeded and the worker must not retry it.
            try:
                write_batch_sizes.append(len(points))
                write_ms.append((time.perf_counter() - before) * 1000)
            except Exception as exc:
                measurement_errors.append(f"{type(exc).__name__}: {exc}")

        def measured_commit(consumer_arg: Any, message: Any) -> None:
            before = time.perf_counter()
            original_commit(consumer_arg, message)
            commit_ms.append((time.perf_counter() - before) * 1000)

        def measured_remove(progress: Any, messages: Any) -> None:
            acknowledged_at_ms = time.time() * 1000
            for message in messages:
                timestamp = message_timestamp_ms(message)
                if timestamp is not None:
                    e2e_ms.append(max(0.0, acknowledged_at_ms - timestamp))
            original_remove(progress, messages)

        worker.enqueue_polled_records = measured_enqueue
        batch.persist_points = measured_persist
        batch.commit_through = measured_commit
        batch.PartitionIsolatingBatchWorker._remove_committed_prefix = staticmethod(measured_remove)

        while worker.completed_records < EXPECTED_RECORDS:
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"batch worker timed out after {worker.completed_records} records"
                )
            records = consumer.poll(
                timeout_ms=legacy.POLL_TIMEOUT_MS,
                max_records=legacy.MAX_POLL_RECORDS,
            )
            worker.enqueue_polled_records(records)
            worker.process_ready(max_partitions=1)
            if measurement_errors:
                raise RuntimeError(f"batch write measurement failed: {measurement_errors[0]}")
            while True:
                if time.monotonic() > deadline:
                    raise TimeoutError(f"batch worker timed out after {worker.completed_records} records")
                more = consumer.poll(timeout_ms=0, max_records=legacy.MAX_POLL_RECORDS)
                worker.enqueue_polled_records(more)
                processed = worker.process_ready(max_partitions=1)
                if measurement_errors:
                    raise RuntimeError(f"batch write measurement failed: {measurement_errors[0]}")
                if not more and processed == 0:
                    break

        offsets = offset_snapshot(consumer)
        if offsets["total_lag"] != 0:
            raise AssertionError(f"batch consumer completed with lag: {offsets}")
        ended_at = time.monotonic()
        summary = make_summary(
            worker.completed_records, started_at, ended_at, e2e_ms, write_ms,
            write_batch_sizes, commit_ms,
            offsets, cpu_before, rss_before_kb,
        )
        return summary
    finally:
        if original_persist is not None:
            batch.persist_points = original_persist
        if original_commit is not None:
            batch.commit_through = original_commit
        if original_remove is not None:
            batch.PartitionIsolatingBatchWorker._remove_committed_prefix = staticmethod(original_remove)
        producer and producer.close()
        consumer.close()
        client and client.close()


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    summary = run_legacy() if MODE == "legacy" else run_batch()
    path = RESULTS_DIR / f"{MODE}.json"
    path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print("M3_SUSTAINED_LOAD_RESULT " + json.dumps(summary, sort_keys=True), flush=True)
    if summary["processed_records"] != EXPECTED_RECORDS:
        raise AssertionError("worker did not process the expected record count")
    if summary["offsets"]["total_lag"] != 0:
        raise AssertionError("worker did not finish with zero consumer lag")
    print(f"PASS: {MODE} sustained-load worker processed {EXPECTED_RECORDS} records", flush=True)


if __name__ == "__main__":
    main()
