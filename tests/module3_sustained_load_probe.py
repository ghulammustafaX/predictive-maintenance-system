"""Publisher and verifier for the isolated Module 03 sustained-load benchmark."""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from influxdb_client import InfluxDBClient
from kafka import KafkaProducer

EXPECTED = {
    "MODULE3_LOAD_PROBE_GUARD": "m3-isolated-sustained-load-probe-only",
    "KAFKA_BROKER": "kafka:9092",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
}
for key, value in EXPECTED.items():
    if os.environ.get(key) != value:
        raise SystemExit(f"REFUSED: invalid sustained-load probe setting {key}")

PARTITIONS = 4
BURST_RECORDS = 80
RECORDS_PER_MODE = int(os.environ.get("MODULE3_LOAD_RECORDS", "3000"))
TARGET_RATE = float(os.environ.get("MODULE3_LOAD_RATE_PER_SECOND", "200"))
RESULTS_DIR = Path(os.environ.get("MODULE3_LOAD_RESULTS_DIR", "/results"))
TOPICS = {
    "legacy": "m3-load-legacy-telemetry",
    "batch": "m3-load-batch-telemetry",
}
RUN_IDS = {
    "legacy": "m3-sustained-legacy-run",
    "batch": "m3-sustained-batch-run",
}


def payload(mode: str, sequence: int, timestamp: str) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema_version": "1.0.0",
        "event_id": f"m3-load-{mode}-{sequence}",
        "run_id": RUN_IDS[mode],
        "dataset_id": "FD001",
        "asset_id": "engine_1",
        "unit_id": 1,
        "time_cycles": sequence,
        "sequence": sequence,
        "source_type": "synthetic",
        "timestamp": timestamp,
        "operational_setting_1": 0.1,
        "operational_setting_2": 0.2,
        "operational_setting_3": 100.0,
    }
    for sensor in range(1, 22):
        value[f"sensor_{sensor}"] = float(sequence + sensor / 100)
    return value


def publish() -> None:
    if RECORDS_PER_MODE < 100 or TARGET_RATE <= 0:
        raise SystemExit("REFUSED: sustained-load workload must be >=100 records and positive rate")
    producer = KafkaProducer(
        bootstrap_servers=EXPECTED["KAFKA_BROKER"],
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )
    started = time.monotonic()
    try:
        for sequence in range(1, RECORDS_PER_MODE + 1):
            timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            for mode, topic in TOPICS.items():
                producer.send(
                    topic,
                    key=f"engine-1-{sequence}",
                    value=payload(mode, sequence, timestamp),
                    partition=(sequence - 1) % PARTITIONS,
                ).get(timeout=15)
            # Pace bounded bursts at the same long-run rate. Evenly spaced
            # individual sends can let a fast consumer flush single points,
            # failing to exercise batching despite the intended workload.
            if sequence % BURST_RECORDS == 0 or sequence == RECORDS_PER_MODE:
                target = started + sequence / TARGET_RATE
                remaining = target - time.monotonic()
                if remaining > 0:
                    time.sleep(remaining)
            if sequence % 500 == 0:
                print(f"M3_LOAD_PUBLISH_PROGRESS records_per_topic={sequence}", flush=True)
        producer.flush(timeout=30)
    finally:
        producer.close(timeout=30)

    elapsed = time.monotonic() - started
    summary = {
        "records_per_mode": RECORDS_PER_MODE,
        "target_rate_per_topic": TARGET_RATE,
        "actual_rate_per_topic": round(RECORDS_PER_MODE / max(elapsed, 0.001), 3),
        "elapsed_seconds": round(elapsed, 3),
        "partitions_per_topic": PARTITIONS,
        "burst_records_per_mode": BURST_RECORDS,
        "topics": TOPICS,
        "run_ids": RUN_IDS,
    }
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "publisher.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )
    print("M3_LOAD_PUBLISH_RESULT " + json.dumps(summary, sort_keys=True), flush=True)
    print("PASS: sustained-load publisher delivered equal workloads to both topics", flush=True)


def persisted_count(client: InfluxDBClient, run_id: str) -> int:
    query = f'''from(bucket: "{EXPECTED["INFLUXDB_BUCKET"]}")
  |> range(start: -1d)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry"
      and r._field == "time_cycles" and r.run_id == "{run_id}")
  |> group(columns: [])
  |> count()
'''
    records = [
        record
        for table in client.query_api().query(query, org=EXPECTED["INFLUXDB_ORG"])
        for record in table.records
    ]
    return int(records[0].get_value()) if records else 0


def verify() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    publisher_path = RESULTS_DIR / "publisher.json"
    if not publisher_path.exists():
        raise AssertionError("missing publisher evidence")
    publisher = json.loads(publisher_path.read_text(encoding="utf-8"))
    if publisher["records_per_mode"] != RECORDS_PER_MODE:
        raise AssertionError(f"unexpected publisher record count: {publisher}")

    results: dict[str, Any] = {}
    for mode in ("legacy", "batch"):
        path = RESULTS_DIR / f"{mode}.json"
        if not path.exists():
            raise AssertionError(f"missing {mode} worker evidence")
        worker = json.loads(path.read_text(encoding="utf-8"))
        if worker["processed_records"] != RECORDS_PER_MODE:
            raise AssertionError(f"{mode} processed count mismatch: {worker}")
        if worker["offsets"]["total_lag"] != 0:
            raise AssertionError(f"{mode} consumer lag is not zero: {worker['offsets']}")
        if len(worker["offsets"]["committed_offsets"]) != PARTITIONS:
            raise AssertionError(f"{mode} did not own all expected partitions")
        if worker["end_to_end_latency_ms"]["samples"] != RECORDS_PER_MODE:
            raise AssertionError(f"{mode} end-to-end latency sample count mismatch")
        if worker["write_latency_ms"]["calls"] < 1 or worker["commit_latency_ms"]["calls"] < 1:
            raise AssertionError(f"{mode} missing write/commit timing samples")
        if mode == "legacy" and worker["points_per_write"]["average"] != 1:
            raise AssertionError(f"legacy consumer must write exactly one point per call: {worker['points_per_write']}")
        if mode == "batch" and worker["points_per_write"]["average"] < 2:
            raise AssertionError(
                "batch workload did not exercise multi-point writes sufficiently: "
                f"{worker['points_per_write']}"
            )
        results[mode] = worker

    with InfluxDBClient(
        url=EXPECTED["INFLUXDB_URL"],
        token=EXPECTED["INFLUXDB_TOKEN"],
        org=EXPECTED["INFLUXDB_ORG"],
        timeout=15000,
    ) as client:
        for mode in ("legacy", "batch"):
            count = persisted_count(client, RUN_IDS[mode])
            if count != RECORDS_PER_MODE:
                raise AssertionError(
                    f"{mode} expected {RECORDS_PER_MODE} persisted points, found {count}"
                )
            results[mode]["persisted_points"] = count

    report = {
        "workload": publisher,
        "results": results,
        "comparison": {
            "batch_over_legacy_throughput_ratio": round(
                results["batch"]["records_per_second"]
                / results["legacy"]["records_per_second"], 3
            ) if results["legacy"]["records_per_second"] else None,
            "performance_claim": "not asserted; one isolated CI run is indicative only",
        },
    }
    (RESULTS_DIR / "sustained-load-report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
    )
    print("M3_SUSTAINED_LOAD_ACCEPTANCE " + json.dumps(report, sort_keys=True), flush=True)
    print(
        "PASS: both consumers persisted every source record, committed all partitions, "
        "and recovered consumer lag to zero",
        flush=True,
    )


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in {"publish", "verify"}:
        raise SystemExit("usage: module3_sustained_load_probe.py publish|verify")
    publish() if sys.argv[1] == "publish" else verify()
