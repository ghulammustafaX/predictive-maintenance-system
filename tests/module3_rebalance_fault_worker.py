"""Test-only worker that exposes the real Kafka rebalance/commit boundary.

The underlying batch consumer and InfluxDB write function remain unchanged.
The worker blocks only after the first synchronous write, until a second
consumer has joined the same real Kafka group and the broker reports
PreparingRebalance. Its subsequent commit must be rejected by the broker.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

EXPECTED = {
    "MODULE3_REBALANCE_FAULT_GUARD": "m3-isolated-rebalance-fault-only",
    "PMS_BATCH_WORKER_CONFIRM": "opt-in-isolated-batch-worker",
    "PMS_INFLUX_BATCH_SIZE": "3",
    "KAFKA_BROKER": "kafka:9092",
    "SENSOR_TOPIC": "m3-rebalance-telemetry",
    "CONSUMER_GROUP": "m3-rebalance-group",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
}
for key, value in EXPECTED.items():
    if os.environ.get(key) != value:
        raise SystemExit(f"REFUSED: invalid isolated rebalance worker setting {key}")

COORDINATION = Path("/coordination")
COORDINATION.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, "/app/src")
import kafka_batch_consumer as batch

original_persist = batch.persist_points
original_commit = batch.commit_through
blocked_once = False


def persist_then_wait(write_api, points):
    global blocked_once
    original_persist(write_api, points)
    if blocked_once:
        return
    blocked_once = True
    (COORDINATION / "influx_write_acknowledged").write_text("confirmed\n")
    print("M3_REBALANCE_INFLUX_WRITE_ACKNOWLEDGED", flush=True)
    deadline = time.monotonic() + 120
    ready = COORDINATION / "peer_joined_during_processing"
    while time.monotonic() < deadline and not ready.exists():
        time.sleep(0.1)
    if not ready.exists():
        raise TimeoutError("Second consumer did not join during the in-flight write")
    print("M3_REBALANCE_PEER_JOINED_WHILE_BATCH_PENDING", flush=True)


def commit_and_record_rejection(consumer, message):
    try:
        original_commit(consumer, message)
    except Exception as exc:
        name = type(exc).__name__
        (COORDINATION / "stale_commit_rejected").write_text(name + "\n")
        print(f"M3_REBALANCE_STALE_COMMIT_REJECTED={name}", flush=True)
        raise
    (COORDINATION / "stale_commit_unexpectedly_succeeded").write_text("unexpected\n")
    print("M3_REBALANCE_STALE_COMMIT_UNEXPECTEDLY_SUCCEEDED", flush=True)
    raise AssertionError("Kafka accepted the commit while the group was rebalancing")


batch.persist_points = persist_then_wait
batch.commit_through = commit_and_record_rejection

if __name__ == "__main__":
    batch.run()
