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
from module3_evidence import evidence, observed_identity, published_identity, write_json
from module3_load_evidence import IdentityMismatch, aggregate, read_jsonl, reconcile, write_jsonl

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
    pending = []
    acknowledged = {'legacy': [], 'batch': []}
    try:
        for sequence in range(1, RECORDS_PER_MODE + 1):
            timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            for mode, topic in TOPICS.items():
                item = payload(mode, sequence, timestamp)
                pending.append((producer.send(
                    topic,
                    key=f"engine-1-{sequence}",
                    value=item,
                    partition=(sequence - 1) % PARTITIONS,
                ), mode, item))
            # Pace bounded bursts at the same long-run rate. Evenly spaced
            # individual sends can let a fast consumer flush single points,
            # failing to exercise batching despite the intended workload.
            if sequence % BURST_RECORDS == 0 or sequence == RECORDS_PER_MODE:
                for delivery, mode, item in pending:
                    metadata = delivery.get(timeout=15)
                    acknowledged[mode].append({'topic': metadata.topic, 'partition': metadata.partition,
                                               'offset': metadata.offset, 'event_id': item['event_id'],
                                               'identity': published_identity(item)})
                pending.clear()
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
    for mode in ('legacy', 'batch'):
        path = RESULTS_DIR / f'sustained_load_{mode}_published.jsonl'
        write_jsonl(path, acknowledged[mode])
        read_jsonl(path, RECORDS_PER_MODE)
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


def persisted_cycles(client: InfluxDBClient, run_id: str) -> list[int]:
    query = f'''from(bucket: "{EXPECTED["INFLUXDB_BUCKET"]}")
  |> range(start: -1d)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry"
      and r._field == "time_cycles" and r.run_id == "{run_id}")
'''
    return sorted(int(record.get_value())
                  for table in client.query_api().query(query, org=EXPECTED['INFLUXDB_ORG'])
                  for record in table.records)


def persisted_identities(client: InfluxDBClient, run_id: str) -> list[dict]:
    query = f'''from(bucket: "{EXPECTED["INFLUXDB_BUCKET"]}")
  |> range(start: -1d)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry"
      and r._field == "time_cycles" and r.run_id == "{run_id}")
'''
    return [observed_identity(record.values)
            for table in client.query_api().query(query, org=EXPECTED['INFLUXDB_ORG'])
            for record in table.records]


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
            observed = persisted_cycles(client, RUN_IDS[mode])
            expected = list(range(1, RECORDS_PER_MODE + 1))
            published_path = RESULTS_DIR / f'sustained_load_{mode}_published.jsonl'
            observed_path = RESULTS_DIR / f'sustained_load_{mode}_observed.jsonl'
            published = read_jsonl(published_path, RECORDS_PER_MODE)
            stored_identities = persisted_identities(client, RUN_IDS[mode])
            write_jsonl(observed_path, stored_identities)
            stored_identities = read_jsonl(observed_path, len(stored_identities))
            mismatch = None
            try:
                identity_result = reconcile(published, stored_identities, topic=TOPICS[mode],
                                            run_id=RUN_IDS[mode], offsets=results[mode]['offsets'],
                                            expected_count=RECORDS_PER_MODE, partitions=PARTITIONS)
            except IdentityMismatch as exc:
                identity_result = exc.result
                mismatch = exc
            raw_exit = os.environ.get(f'M3_LOAD_{mode.upper()}_EXIT')
            exit_status = int(raw_exit) if raw_exit is not None else None
            count_mismatch = count != RECORDS_PER_MODE
            cycle_mismatch = observed != expected
            record = evidence('sustained_load', run_id=RUN_IDS[mode], topic=TOPICS[mode],
                              consumer_group=results[mode]['consumer_group'], partition=None,
                              worker_exit_status=exit_status,
                              scenario_status='failed' if mismatch or count_mismatch or cycle_mismatch or (exit_status is not None and exit_status != 0) else ('passed' if exit_status == 0 else 'incomplete'),
                              artifact_root=RESULTS_DIR, expected_count=RECORDS_PER_MODE,
                              compose_project=os.environ.get('M3_PROJECT'), git_commit=os.environ.get('GITHUB_SHA'),
                              expected_identity_artifact=published_path.name, observed_identity_artifact=observed_path.name,
                              identity_reconciliation=identity_result, partition_offsets=results[mode]['offsets'],
                              throughput_records_per_second=results[mode]['records_per_second'],
                              successful_application_write_calls=results[mode]['write_latency_ms']['calls'],
                              total_http_write_attempts=None,
                              repeated_writes_of_identical_points=None)
            write_json(RESULTS_DIR / f'sustained_load_{mode}.json', record)
            if mismatch:
                raise mismatch
            if count_mismatch:
                raise AssertionError(
                    f"{mode} expected {RECORDS_PER_MODE} persisted points, found {count}"
                )
            if cycle_mismatch:
                raise AssertionError(f"{mode} point cycle identities do not match published identities")
            results[mode]["persisted_points"] = count
            results[mode]["identity_reconciliation"] = {
                "run_id": RUN_IDS[mode], "identity_scope": "measurement, all tags and timestamp",
                "expected_cycles": expected, "observed_cycles": observed,
                "missing_cycles": sorted(set(expected) - set(observed)),
                "unexpected_cycles": sorted(set(observed) - set(expected)),
                "duplicate_observed_cycles": len(observed) - len(set(observed)),
                "full_point_identity_comparison": identity_result,
            }

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
    aggregate_record = aggregate(RESULTS_DIR, project=os.environ.get('M3_PROJECT'),
                                 commit=os.environ.get('GITHUB_SHA'), expected_count=3000)
    write_json(RESULTS_DIR / 'sustained_load.json', aggregate_record)
    if aggregate_record['scenario_status'] != 'passed':
        raise AssertionError(f'Sustained-load aggregate is {aggregate_record["scenario_status"]}')
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
    if sys.argv[1] == 'publish':
        publish()
    else:
        try:
            verify()
        except Exception as exc:
            for mode in ('legacy', 'batch'):
                path = RESULTS_DIR / f'sustained_load_{mode}.json'
                if not path.exists():
                    write_json(path, evidence('sustained_load', run_id=RUN_IDS[mode], topic=TOPICS[mode],
                                              scenario_status='incomplete', error_type=type(exc).__name__,
                                              error=str(exc), compose_project=os.environ.get('M3_PROJECT'),
                                              git_commit=os.environ.get('GITHUB_SHA')))
            write_json(RESULTS_DIR / 'sustained_load.json',
                       aggregate(RESULTS_DIR, project=os.environ.get('M3_PROJECT'),
                                 commit=os.environ.get('GITHUB_SHA'), expected_count=3000))
            raise
