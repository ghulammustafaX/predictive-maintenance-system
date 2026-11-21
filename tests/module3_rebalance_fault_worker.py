"""Test-only worker coordinating a real Kafka rebalance after durable write.

The unchanged batch worker persists a three-record batch, pauses while a
second member joins, then commits that already-persisted batch before its next
poll completes reassignment. This verifies the safe write -> commit -> poll
ordering, not a broker rejection during PreparingRebalance.
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
from kafka.admin import KafkaAdminClient

original_persist = batch.persist_points
original_commit = batch.commit_through
blocked_once = False
verified_commit = False


def current_group_state() -> tuple[str, int]:
    admin = KafkaAdminClient(bootstrap_servers=os.environ["KAFKA_BROKER"], request_timeout_ms=10000)
    try:
        description = admin.describe_groups([os.environ["CONSUMER_GROUP"]])[os.environ["CONSUMER_GROUP"]]
        state = str(description.get("state", description.get("group_state", "")))
        return state.replace("_", "").replace("-", "").lower(), len(description.get("members", []))
    finally:
        admin.close()


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
    state, members = current_group_state()
    if state != "preparingrebalance" or members < 2:
        raise AssertionError(f"Expected broker PreparingRebalance with two members, got {state}/{members}")
    print("M3_REBALANCE_PEER_JOINED_WHILE_BATCH_PENDING", flush=True)


def commit_after_confirmed_write(consumer, message):
    global verified_commit
    if not verified_commit:
        state, members = current_group_state()
        if state != "preparingrebalance" or members < 2:
            raise AssertionError(f"Commit was not attempted during observed rebalance: {state}/{members}")
        original_commit(consumer, message)
        verified_commit = True
        (COORDINATION / "write_then_commit_confirmed").write_text(
            f"topic={message.topic} partition={message.partition} next_offset={message.offset + 1}\n"
        )
        print(
            f"M3_REBALANCE_COMMIT_SUCCEEDED_WHILE_PREPARING partition={message.partition} "
            f"next_offset={message.offset + 1}",
            flush=True,
        )
        return
    original_commit(consumer, message)


batch.persist_points = persist_then_wait
batch.commit_through = commit_after_confirmed_write

if __name__ == "__main__":
    batch.run()
