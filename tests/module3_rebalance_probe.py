"""Real Kafka + InfluxDB multi-partition rebalance and replay acceptance probe.

Every operation is guarded for the disposable Module 03 CI stack. This probe
creates two explicit source partitions, observes the broker group state, and
checks committed offsets plus deterministic InfluxDB point identities.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from influxdb_client import InfluxDBClient
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition

REQUIRED = {
    "MODULE3_REBALANCE_PROBE_GUARD": "m3-isolated-rebalance-probe-only",
    "KAFKA_BROKER": "kafka:9092",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
}
for key, value in REQUIRED.items():
    if os.getenv(key) != value:
        raise SystemExit(f"REFUSED: invalid isolated rebalance probe setting {key}")

TOPIC = "m3-rebalance-telemetry"
GROUP = "m3-rebalance-group"
RUN_P0 = "m3-rebalance-p0-20261121"
RUN_P1 = "m3-rebalance-p1-20261121"
COUNT = 6
COORDINATION = Path("/coordination")
COORDINATION.mkdir(parents=True, exist_ok=True)
BASE = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=3)
ALLOWED_COMMIT_ERRORS = {
    "RebalanceInProgressError",
    "CommitFailedError",
    "UnknownMemberIdError",
    "IllegalGenerationError",
}


def offsets() -> tuple[dict[int, int | None], dict[int, int]]:
    observer = KafkaConsumer(
        bootstrap_servers=REQUIRED["KAFKA_BROKER"],
        group_id=GROUP,
        enable_auto_commit=False,
        request_timeout_ms=10000,
    )
    try:
        partitions = [TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)]
        observer.assign(partitions)
        committed = {p.partition: observer.committed(p) for p in partitions}
        ends = {p.partition: v for p, v in observer.end_offsets(partitions).items()}
        return committed, ends
    finally:
        observer.close()


def influx_count(client, run_id: str) -> int:
    query = f'''from(bucket: "{REQUIRED["INFLUXDB_BUCKET"]}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry" and
      r._field == "time_cycles" and r.run_id == "{run_id}")
  |> group(columns: [])
  |> count()
'''
    records = [r for table in client.query_api().query(query, org=REQUIRED["INFLUXDB_ORG"])
               for r in table.records]
    return int(records[0].get_value()) if records else 0


def publish_partition(partition: int, run_id: str, unit_id: int) -> None:
    producer = KafkaProducer(
        bootstrap_servers=REQUIRED["KAFKA_BROKER"],
        acks="all",
        retries=3,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    try:
        futures = []
        for cycle in range(1, COUNT + 1):
            payload = {
                "schema_version": "1.0.0",
                "event_id": f"{run_id}-{cycle}",
                "run_id": run_id,
                "dataset_id": "FD001",
                "asset_id": f"engine_{unit_id}",
                "unit_id": unit_id,
                "time_cycles": cycle,
                "sequence": cycle,
                "source_type": "cmapss",
                "timestamp": (BASE + timedelta(seconds=cycle)).isoformat(),
                "operational_setting_1": 0.1,
                "operational_setting_2": 0.2,
                "operational_setting_3": 100.0,
                "sensor_1": float(cycle),
            }
            futures.append(producer.send(TOPIC, partition=partition, value=payload))
        for future in futures:
            future.get(timeout=20)
        producer.flush(timeout=20)
        print(f"M3_REBALANCE_PUBLISHED partition={partition} records={COUNT}", flush=True)
    finally:
        producer.close()


def group_state(admin: KafkaAdminClient) -> tuple[str, int]:
    result = admin.describe_groups([GROUP])[GROUP]
    state = str(result.get("state", result.get("group_state", "")))
    members = result.get("members", [])
    return state.replace("_", "").replace("-", "").lower(), len(members)


def publish_p0(client) -> None:
    committed, ends = offsets()
    assert ends == {0: 0, 1: 0}, f"Expected empty two-partition topic, got {ends}"
    assert committed == {0: None, 1: None}, f"Unexpected pre-existing group offsets: {committed}"
    assert influx_count(client, RUN_P0) == 0 and influx_count(client, RUN_P1) == 0
    publish_partition(0, RUN_P0, 903)


def await_write_ack(client) -> None:
    marker = COORDINATION / "influx_write_acknowledged"
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if marker.exists():
            break
        time.sleep(0.1)
    assert marker.exists(), "Fault worker did not acknowledge its first InfluxDB batch"
    count = influx_count(client, RUN_P0)
    assert count == 3, f"Expected first 3-record batch stored before rebalance, got {count}"
    committed, _ = offsets()
    assert committed[0] in (None, 0), f"Worker committed before the peer joined: {committed}"
    print("PASS: first partition batch is stored while its source offset remains pending", flush=True)


def trigger_rebalance(client) -> None:
    admin = KafkaAdminClient(bootstrap_servers=REQUIRED["KAFKA_BROKER"], request_timeout_ms=10000)
    try:
        deadline = time.monotonic() + 90
        observed = None
        while time.monotonic() < deadline:
            try:
                observed = group_state(admin)
                if observed[0] == "preparingrebalance" and observed[1] >= 2:
                    break
            except Exception:
                pass
            time.sleep(0.25)
        assert observed == ("preparingrebalance", 2), (
            f"Broker never showed two members in PreparingRebalance: {observed}"
        )
        committed, _ = offsets()
        assert committed[0] in (None, 0), f"Unexpected partition-0 commit during rebalance: {committed}"
        assert influx_count(client, RUN_P0) == 3
        # Add healthy-partition work while the old owner is blocked inside its
        # already acknowledged partition-0 batch and the group is rebalancing.
        publish_partition(1, RUN_P1, 904)
        (COORDINATION / "peer_joined_during_processing").write_text(
            "PreparingRebalance members=2\n"
        )
        print("PASS: real Kafka group entered PreparingRebalance with two members", flush=True)
    finally:
        admin.close()


def verify_recovery(client) -> None:
    rejected = COORDINATION / "stale_commit_rejected"
    unexpected = COORDINATION / "stale_commit_unexpectedly_succeeded"
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and not rejected.exists() and not unexpected.exists():
        time.sleep(0.1)
    assert not unexpected.exists(), "Kafka unexpectedly accepted the old owner's commit"
    assert rejected.exists(), "Old owner did not record a broker-rejected commit"
    error_name = rejected.read_text().strip()
    assert error_name in ALLOWED_COMMIT_ERRORS, f"Unexpected stale commit rejection: {error_name}"
    print(f"PASS: broker rejected stale in-flight commit with {error_name}", flush=True)

    deadline = time.monotonic() + 120
    last = None
    while time.monotonic() < deadline:
        committed, ends = offsets()
        counts = (influx_count(client, RUN_P0), influx_count(client, RUN_P1))
        last = (committed, ends, counts)
        if committed == {0: COUNT, 1: COUNT} and ends == {0: COUNT, 1: COUNT} and counts == (COUNT, COUNT):
            break
        time.sleep(1)
    else:
        raise AssertionError(f"Partitions did not converge after reassignment: {last}")

    assert influx_count(client, RUN_P0) == COUNT
    assert influx_count(client, RUN_P1) == COUNT
    print("PASS: partition 0 replayed to six unique points and offset 6", flush=True)
    print("PASS: partition 1 progressed after rebalance to six unique points and offset 6", flush=True)
    print("PASS: both Kafka partitions have zero lag and no lost records", flush=True)
    print("MODULE 03 MULTI-PARTITION REBALANCE AND STALE-COMMIT RECOVERY: PASS", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("publish-p0", "await-write-ack", "trigger-rebalance", "verify-recovery"))
    phase = parser.parse_args().phase
    with InfluxDBClient(
        url=REQUIRED["INFLUXDB_URL"],
        token=REQUIRED["INFLUXDB_TOKEN"],
        org=REQUIRED["INFLUXDB_ORG"],
        timeout=10000,
    ) as client:
        if phase == "publish-p0":
            publish_p0(client)
        elif phase == "await-write-ack":
            await_write_ack(client)
        elif phase == "trigger-rebalance":
            trigger_rebalance(client)
        else:
            verify_recovery(client)


if __name__ == "__main__":
    main()
