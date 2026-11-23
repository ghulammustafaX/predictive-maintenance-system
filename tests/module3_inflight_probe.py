"""Guarded Kafka and InfluxDB assertions for a real post-write, pre-commit SIGKILL.

Dedicated CI Kafka topic/group and deterministic InfluxDB identities only.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from influxdb_client import InfluxDBClient
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition
from module3_evidence import evidence, observed_identity, published_identity, write_json

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
EVIDENCE_DIR = os.environ.get('M3_EVIDENCE_DIR')


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
        published = []
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
            futures.append((producer.send(TOPIC, key=RUN.encode(), value=event), event))
        for future, event in futures:
            metadata = future.get(timeout=20)
            published.append({'topic': metadata.topic, 'partition': metadata.partition,
                              'offset': metadata.offset, 'event_id': event['event_id'],
                              'identity': published_identity(event)})
        producer.flush(timeout=20)
        if EVIDENCE_DIR:
            write_json(Path(EVIDENCE_DIR) / 'inflight-published.json',
                       {'run_id': RUN, 'records': published})
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


def capture_stage(client, stage):
    """Capture broker and storage observations; never infer an unobserved exit."""
    if not EVIDENCE_DIR:
        raise ValueError('M3_EVIDENCE_DIR is required for evidence capture')
    published = json.loads((Path(EVIDENCE_DIR) / 'inflight-published.json').read_text(encoding='utf-8'))
    records = published['records']
    if (published.get('run_id') != RUN or len(records) != COUNT or
            [(r['topic'], r['partition'], r['offset']) for r in records] !=
            [(TOPIC, 0, n) for n in range(COUNT)] or
            any(r['identity']['tags']['run_id'] != RUN for r in records)):
        raise ValueError('Published evidence does not identify the isolated source run')
    observer = KafkaConsumer(bootstrap_servers=REQUIRED['KAFKA_BROKER'], group_id=GROUP,
                             enable_auto_commit=False, request_timeout_ms=10000)
    tp = TopicPartition(TOPIC, 0)
    try:
        observer.assign([tp])
        beginning = observer.beginning_offsets([tp])[tp]
        end = observer.end_offsets([tp])[tp]
        committed = observer.committed(tp)
    finally:
        observer.close()
    query = f'''from(bucket: "{REQUIRED["INFLUXDB_BUCKET"]}") |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "sensor_telemetry" and r._field == "time_cycles" and r.run_id == "{RUN}")'''
    points = [observed_identity(r.values) for table in client.query_api().query(query, org=REQUIRED['INFLUXDB_ORG']) for r in table.records]
    raw_exit = os.environ.get('M3_WORKER_EXIT_STATUS')
    exit_status = int(raw_exit) if raw_exit is not None else None
    scenario = 'crash_after_ack' if stage == 'record-crash' else 'restart_replay'
    record = evidence(scenario, run_id=RUN, topic=TOPIC, consumer_group=GROUP,
                      partition=0, beginning_offset=beginning, end_offset=end,
                      committed_offset=committed,
                      expected_point_identities=[r['identity'] for r in records],
                      observed_point_identities=points, worker_exit_status=exit_status,
                      scenario_status='incomplete', compose_project=os.environ.get('M3_PROJECT'),
                      git_commit=os.environ.get('GITHUB_SHA'), stage=stage,
                      published_source_records=records, http_write_attempt_count=None,
                      influx_write_ack_log_marker_verified=(True if os.environ.get('M3_ACK_MARKER_VERIFIED') == 'true' else None),
                      offset_observation='after process crash' if stage == 'record-crash' else 'after recovery worker exit')
    valid_offsets = beginning == 0 and end == COUNT and (
        committed in (None, 0) if stage == 'record-crash' else committed == COUNT)
    expected_exit = 137 if stage == 'record-crash' else 0
    mismatch = bool(record['missing_point_identities'] or record['unexpected_point_identities']
                    or record['duplicate_observed_identities'])
    if not valid_offsets or mismatch or (exit_status is not None and exit_status != expected_exit):
        record['scenario_status'] = 'failed'
    elif exit_status is not None and os.environ.get('M3_PROJECT') and os.environ.get('GITHUB_SHA'):
        from module3_inflight_aggregate import finalize
        try:
            record = finalize(record, os.environ['M3_PROJECT'], os.environ['GITHUB_SHA'], EVIDENCE_DIR)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            record['scenario_status'] = 'incomplete' if isinstance(exc, OSError) else 'failed'
            record['validation_error'] = str(exc)
            write_json(Path(EVIDENCE_DIR) / f'{scenario}.json', record)
            raise
    write_json(Path(EVIDENCE_DIR) / f'{scenario}.json', record)
    if record['scenario_status'] == 'failed':
        raise AssertionError(f'Crash/replay evidence did not reconcile: {scenario}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("publish", "before-restart", "after-recovery", "record-crash", "record-recovery"))
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
        elif phase == 'after-recovery':
            after_recovery(client)
        else:
            capture_stage(client, phase)


if __name__ == "__main__":
    main()
