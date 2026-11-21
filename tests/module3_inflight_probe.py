"""Guarded Kafka and InfluxDB assertions for a real post-write, pre-commit SIGKILL.

Dedicated CI Kafka topic/group and deterministic InfluxDB identities only.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone

from influxdb_client import InfluxDBClient
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition

REQUIRED = {
    "MODULE3_INFLIGHT_PROBE_GUARD": "m3-isolated-inflight-probe-only",
    "KAFKA_BROKER": "kafka:9092",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
}
for key, value in REQUIRED.items():
    if os.getenv(key) != value:
        raise SystemExit(f"REFUSED: invalid isolated probe setting {key}")

TOPIC = "m3-inflight-telemetry"
GROUP = "m3-inflight-group"
RUN = "m3-crash-after-ack-20261121"
COUNT = 6
BASE = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=2)


def offset() -> int | None:
    observer = KafkaConsumer(
        bootstrap_servers=REQUIRED["KAFKA_BROKER"], group_id=GROUP,
        enable_auto_commit=False, request_timeout_ms=10000,
    )
    try:
        tp = TopicPartition(TOPIC, 0)
        observer.assign([tp])
        return observer.committed(tp)
    finally:
        observer.close()


def count(client) -> int:
    q = f'''from(bucket: "{REQUIRED["INFLUXDB_BUCKET"]}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry" and
      r._field == "time_cycles" and r.run_id == "{RUN}")
  |> group(columns: [])
  |> count()
'''
    records = [r for table in client.query_api().query(q, org=REQUIRED["INFLUXDB_ORG"])
               for r in table.records]
    return int(records[0].get_value()) if records else 0


def publish() -> None:
    producer = KafkaProducer(
        bootstrap_servers=REQUIRED["KAFKA_BROKER"],
        acks="all", retries=3,
        value_serializer=lambda item: json.dumps(item).encode("utf-8"),
    )
    try:
        futures = []
        for i in range(1, COUNT + 1):
            event = {
                "schema_version": "1.0.0", "event_id": f"{RUN}-{i}",
                "run_id": RUN, "dataset_id": "FD002",
                "asset_id": "engine_903", "unit_id": 903,
                "time_cycles": i, "sequence": i,
                "source_type": "cmapss",
                "timestamp": (BASE + timedelta(seconds=i)).isoformat(),
                "operational_setting_1": 0.1, "operational_setting_2": 0.2,
                "operational_setting_3": 100.0, "sensor_1": float(i),
            }
            futures.append(producer.send(TOPIC, key=RUN.encode(), value=event))
        for future in futures:
            future.get(timeout=20)
        producer.flush(timeout=20)
        print("M3_INFLIGHT_PUBLISHED=6", flush=True)
    finally:
        producer.close()


def before_restart(client) -> None:
    deadline = time.monotonic() + 60
    last = None
    while time.monotonic() < deadline:
        last = (count(client), offset())
        if last[0] == COUNT:
            break
        time.sleep(1)
    else:
        raise AssertionError(f"Write did not complete before process fault: {last}")
    # The worker must be confirmed dead by Compose and log marker in CI.
    time.sleep(1)
    observed = offset()
    assert observed in (None, 0), f"Kafka committed despite pre-commit SIGKILL: {observed}"
    assert count(client) == COUNT
    print("PASS: InfluxDB persisted 6 records but Kafka source offset remained uncommitted", flush=True)


def after_recovery(client) -> None:
    deadline = time.monotonic() + 90
    last = None
    while time.monotonic() < deadline:
        last = (count(client), offset())
        if last == (COUNT, COUNT):
            break
        if last[0] > COUNT or (last[1] is not None and last[1] > COUNT):
            raise AssertionError(f"Replay advanced beyond source records: {last}")
        time.sleep(1)
    else:
        raise AssertionError(f"Replayed batch did not commit and converge: {last}")
    print("PASS: original batch replayed after real process kill", flush=True)
    print("PASS: InfluxDB contains 6 unique points and Kafka committed offset 6", flush=True)
    print("MODULE 03 IN-FLIGHT WRITE/COMMIT CRASH RECOVERY: PASS", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("publish", "before-restart", "after-recovery"))
    phase = parser.parse_args().phase
    with InfluxDBClient(
        url=REQUIRED["INFLUXDB_URL"], token=REQUIRED["INFLUXDB_TOKEN"],
        org=REQUIRED["INFLUXDB_ORG"], timeout=10000,
    ) as client:
        if phase == "publish":
            assert count(client) == 0, "Expected clean isolated run"
            assert offset() in (None, 0), "Expected clean source offsets"
            publish()
        elif phase == "before-restart":
            before_restart(client)
        else:
            after_recovery(client)


if __name__ == "__main__":
    main()
