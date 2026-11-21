"""Real Kafka + InfluxDB acceptance probe for partition-specific failure isolation."""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from influxdb_client import InfluxDBClient
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition

EXPECTED = {
    "MODULE3_PARTITION_FAULT_PROBE_GUARD": "m3-isolated-partition-fault-probe-only",
    "KAFKA_BROKER": "kafka:9092",
    "SENSOR_TOPIC": "m3-partition-fault-telemetry",
    "CONSUMER_GROUP": "m3-partition-fault-group",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
}
for key, value in EXPECTED.items():
    if os.environ.get(key) != value:
        raise SystemExit(f"REFUSED: invalid partition-fault probe setting {key}")

COORDINATION = Path("/coordination")
FAILURE_MARKER = COORDINATION / "partition0_write_failed"
FAILURE_COUNT = COORDINATION / "partition0_failure_count"
RELEASE_MARKER = COORDINATION / "release_partition0"
RUN_P0 = "m3-partition-fault-p0"
RUN_P1 = "m3-partition-fault-p1"
STAMP = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=2)
TP0 = TopicPartition(EXPECTED["SENSOR_TOPIC"], 0)
TP1 = TopicPartition(EXPECTED["SENSOR_TOPIC"], 1)


def event(partition: int, cycle: int) -> dict:
    run_id = RUN_P0 if partition == 0 else RUN_P1
    return {
        "schema_version": "1.0.0",
        "event_id": f"{run_id}-{cycle}",
        "run_id": run_id,
        "dataset_id": "FD001",
        "asset_id": f"engine_{partition + 1}",
        "unit_id": partition + 1,
        "time_cycles": cycle,
        "sequence": cycle,
        "source_type": "cmapss",
        "timestamp": (STAMP + timedelta(seconds=cycle)).isoformat(),
        "operational_setting_1": 0.1,
        "operational_setting_2": 0.2,
        "operational_setting_3": 100.0,
        "sensor_2": float(cycle * 2),
    }


def committed_offsets() -> dict[int, int | None]:
    observer = KafkaConsumer(
        bootstrap_servers=EXPECTED["KAFKA_BROKER"],
        group_id=EXPECTED["CONSUMER_GROUP"],
        enable_auto_commit=False,
        request_timeout_ms=10000,
    )
    try:
        observer.assign([TP0, TP1])
        return {0: observer.committed(TP0), 1: observer.committed(TP1)}
    finally:
        observer.close()


def points_for(client: InfluxDBClient, run_id: str) -> list:
    query = f'''from(bucket: "{EXPECTED["INFLUXDB_BUCKET"]}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry" and r._field == "time_cycles")
  |> filter(fn: (r) => r.run_id == "{run_id}")
'''
    return [
        record
        for table in client.query_api().query(query, org=EXPECTED["INFLUXDB_ORG"])
        for record in table.records
    ]


def wait_for(predicate, description: str, timeout: int = 90) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return
        time.sleep(0.5)
    raise AssertionError(f"Timed out waiting for {description}; last={last!r}")


def prepare() -> None:
    COORDINATION.mkdir(parents=True, exist_ok=True)
    FAILURE_MARKER.unlink(missing_ok=True)
    FAILURE_COUNT.unlink(missing_ok=True)
    RELEASE_MARKER.unlink(missing_ok=True)
    print("PASS: isolated coordination markers reset", flush=True)


def verify() -> None:
    producer = KafkaProducer(
        bootstrap_servers=EXPECTED["KAFKA_BROKER"],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3,
    )
    client = InfluxDBClient(
        url=EXPECTED["INFLUXDB_URL"],
        org=EXPECTED["INFLUXDB_ORG"],
        token=EXPECTED["INFLUXDB_TOKEN"],
        timeout=10000,
    )
    try:
        for cycle in range(1, 4):
            producer.send(EXPECTED["SENSOR_TOPIC"], value=event(0, cycle), key=f"{RUN_P0}-{cycle}".encode(), partition=0).get(timeout=20)
            producer.send(EXPECTED["SENSOR_TOPIC"], value=event(1, cycle), key=f"{RUN_P1}-{cycle}".encode(), partition=1).get(timeout=20)
        producer.flush()

        wait_for(
            lambda: FAILURE_COUNT.exists() and int(FAILURE_COUNT.read_text(encoding="utf-8")) >= 3,
            "three repeated partition 0 output failures",
        )

        def healthy_partition_only():
            offsets = committed_offsets()
            p0 = points_for(client, RUN_P0)
            p1 = points_for(client, RUN_P1)
            return (
                offsets.get(0) in (None, 0)
                and offsets.get(1) == 3
                and len(p0) == 0
                and len(p1) == 3
            )

        wait_for(healthy_partition_only, "partition 1 commits while partition 0 is paused")
        before = committed_offsets()
        assert before[0] in (None, 0) and before[1] == 3, before
        assert len(points_for(client, RUN_P0)) == 0
        assert len(points_for(client, RUN_P1)) == 3
        print("PASS: partition 0 stayed uncommitted with no points after three repeated write failures", flush=True)
        print("PASS: healthy partition 1 committed offset 3 and persisted all three points", flush=True)

        assert int(FAILURE_COUNT.read_text(encoding="utf-8")) >= 3
        print("PASS: repeated partition-local failures did not block healthy partition progress", flush=True)

        RELEASE_MARKER.write_text("release\n", encoding="utf-8")

        def both_partitions_recovered():
            offsets = committed_offsets()
            p0 = points_for(client, RUN_P0)
            p1 = points_for(client, RUN_P1)
            return offsets == {0: 3, 1: 3} and len(p0) == 3 and len(p1) == 3

        wait_for(both_partitions_recovered, "partition 0 retry and both partitions at offset 3")
        p0 = points_for(client, RUN_P0)
        p1 = points_for(client, RUN_P1)
        ids0 = {record.values["cycle"] for record in p0}
        ids1 = {record.values["cycle"] for record in p1}
        assert ids0 == {"1", "2", "3"}, ids0
        assert ids1 == {"1", "2", "3"}, ids1
        print("PASS: failed partition 0 resumed and committed offset 3 after output recovery", flush=True)
        print("PASS: both partitions contain exactly three deterministic points with no skipped cycles", flush=True)
        print("MODULE 03 PARTITION FAILURE ISOLATION: PASS", flush=True)
    finally:
        producer.close()
        client.close()


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in {"prepare", "verify"}:
        raise SystemExit("usage: module3_partition_fault_probe.py prepare|verify")
    prepare() if sys.argv[1] == "prepare" else verify()
