"""Guarded end-to-end Kafka -> InfluxDB comparison and real SIGKILL restart test.

Runs ONLY in disposable Module 03 Compose. Modes:
 benchmark: submit 120 identical-schema source records to each isolated worker;
            verify both storage and committed Kafka offsets.
 during-outage: after externally SIGKILLing batch worker, queue 20 more and
                show no consumer progress while stopped.
 after-restart: after externally restarting batch worker, confirm recovery.
This does NOT test a kill between a write and commit (that needs fault injection).
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

GUARD = {
    "MODULE3_E2E_GUARD": "m3-isolated-e2e-only",
    "KAFKA_BROKER": "kafka:9092",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
}
for key, expected in GUARD.items():
    if os.environ.get(key) != expected:
        raise SystemExit(f"REFUSED: unexpected E2E configuration: {key}")

CASES = {
    "legacy": ("m3-e2e-legacy-telemetry", "m3-e2e-legacy-group", "m3-e2e-legacy-run"),
    "batch": ("m3-e2e-batch-telemetry", "m3-e2e-batch-group", "m3-e2e-batch-run"),
}
INITIAL_RECORDS = 120
RESTART_RECORDS = 20
BASE = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=3)


def make_event(run: str, cycle: int) -> dict:
    return {
        "schema_version": "1.0.0",
        "event_id": f"{run}-{cycle}", "run_id": run,
        "dataset_id": "FD002", "asset_id": "engine_902",
        "unit_id": 902, "time_cycles": cycle, "sequence": cycle,
        "source_type": "cmapss",
        "timestamp": (BASE + timedelta(milliseconds=cycle * 100)).isoformat(),
        "operational_setting_1": 0.1, "operational_setting_2": 0.2,
        "operational_setting_3": 100.0, "sensor_1": float(cycle),
        "sensor_21": float(cycle * 2),
    }


def committed_offset(topic: str, group: str) -> int | None:
    # A manually assigned observer does not join or rebalance the active group.
    observer = KafkaConsumer(
        bootstrap_servers=GUARD["KAFKA_BROKER"], group_id=group,
        enable_auto_commit=False, request_timeout_ms=10000,
    )
    try:
        tp = TopicPartition(topic, 0)
        observer.assign([tp])
        return observer.committed(tp)
    finally:
        observer.close()


def stored_count(client: InfluxDBClient, run: str) -> int:
    query = f'''from(bucket: "{GUARD["INFLUXDB_BUCKET"]}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry" and
            r._field == "time_cycles" and r.run_id == "{run}")
  |> group(columns: [])
  |> count()
'''
    records = [r for table in client.query_api().query(query, org=GUARD["INFLUXDB_ORG"])
               for r in table.records]
    return int(records[0].get_value()) if records else 0


def publish(topic: str, run: str, start_cycle: int, end_cycle: int) -> float:
    producer = KafkaProducer(
        bootstrap_servers=GUARD["KAFKA_BROKER"],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all", retries=3, request_timeout_ms=10000,
    )
    started = time.monotonic()
    try:
        futures = [producer.send(topic, key=run.encode(), value=make_event(run, cycle))
                   for cycle in range(start_cycle, end_cycle + 1)]
        for future in futures:
            future.get(timeout=20)
        producer.flush(timeout=20)
    finally:
        producer.close()
    return time.monotonic() - started


def wait_for(client: InfluxDBClient, topic: str, group: str, run: str, count: int):
    deadline = time.monotonic() + 110
    last = (None, None)
    while time.monotonic() < deadline:
        last = (stored_count(client, run), committed_offset(topic, group))
        if last == (count, count):
            return
        if last[0] > count or (last[1] is not None and last[1] > count):
            raise AssertionError(f"Unexpected overrun {last}, expected {count}")
        time.sleep(1)
    raise AssertionError(f"Timed out waiting for stored={count}, committed={count}, found={last}")


def phase_benchmark(client: InfluxDBClient) -> None:
    results = []
    for mode in ("legacy", "batch"):
        topic, group, run = CASES[mode]
        assert stored_count(client, run) == 0, "Expected clean isolated topic/run"
        initial_offset = committed_offset(topic, group)
        assert initial_offset in (None, 0), f"Unexpected starting Kafka offset: {initial_offset}"
        started = time.monotonic()
        publishing_seconds = publish(topic, run, 1, INITIAL_RECORDS)
        wait_for(client, topic, group, run, INITIAL_RECORDS)
        elapsed = time.monotonic() - started
        result = {
            "mode": mode, "points": INITIAL_RECORDS,
            "batch_size": 20 if mode == "batch" else 1,
            "elapsed_seconds": round(elapsed, 3),
            "publish_seconds": round(publishing_seconds, 3),
            "records_per_second": round(INITIAL_RECORDS / elapsed, 2),
            "stored_points": INITIAL_RECORDS,
            "committed_offset": INITIAL_RECORDS,
        }
        results.append(result)
        print("M3_E2E_BENCHMARK " + json.dumps(result, sort_keys=True), flush=True)
    print("M3_E2E_BENCHMARK_SUMMARY " + json.dumps({
        "legacy_rate": results[0]["records_per_second"],
        "batch_rate": results[1]["records_per_second"],
        "indicative_rate_ratio": round(
            results[1]["records_per_second"] / results[0]["records_per_second"], 3
        ) if results[0]["records_per_second"] else None,
        "important": "Short sequential-run CI measurement, not production capacity",
    }, sort_keys=True), flush=True)
    print("PASS: both isolated Kafka consumers persisted 120 points and committed offset 120", flush=True)


def phase_during_outage(client: InfluxDBClient) -> None:
    topic, group, run = CASES["batch"]
    assert stored_count(client, run) == INITIAL_RECORDS
    assert committed_offset(topic, group) == INITIAL_RECORDS
    publish(topic, run, INITIAL_RECORDS + 1, INITIAL_RECORDS + RESTART_RECORDS)
    # CI must SIGKILL the batch service *before* invoking this phase.
    time.sleep(2)
    assert stored_count(client, run) == INITIAL_RECORDS, "Stopped worker unexpectedly wrote"
    assert committed_offset(topic, group) == INITIAL_RECORDS, "Stopped worker advanced offsets"
    print("PASS: 20 messages queued during worker SIGKILL, no writes or commits", flush=True)


def phase_after_restart(client: InfluxDBClient) -> None:
    topic, group, run = CASES["batch"]
    target = INITIAL_RECORDS + RESTART_RECORDS
    wait_for(client, topic, group, run, target)
    # Ensure the legacy worker kept its own topic and group position.
    legacy_topic, legacy_group, legacy_run = CASES["legacy"]
    assert committed_offset(legacy_topic, legacy_group) == INITIAL_RECORDS
    assert stored_count(client, legacy_run) == INITIAL_RECORDS
    print("PASS: restarted batch worker recovered exactly 20 queued messages", flush=True)
    print("PASS: source offsets advanced 120 -> 140; legacy group remained 120", flush=True)
    print("MODULE 03 REAL E2E AND RESTART: PASS", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("benchmark", "during-outage", "after-restart"))
    args = parser.parse_args()
    with InfluxDBClient(
        url=GUARD["INFLUXDB_URL"], token=GUARD["INFLUXDB_TOKEN"],
        org=GUARD["INFLUXDB_ORG"], timeout=10000,
    ) as client:
        {
            "benchmark": phase_benchmark,
            "during-outage": phase_during_outage,
            "after-restart": phase_after_restart,
        }[args.phase](client)


if __name__ == "__main__":
    main()
