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
from module3_evidence import evidence, observed_identity, published_identity, write_json

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


def partition_offsets():
    observer = KafkaConsumer(bootstrap_servers=EXPECTED['KAFKA_BROKER'],
                             group_id=EXPECTED['CONSUMER_GROUP'], enable_auto_commit=False,
                             request_timeout_ms=10000)
    try:
        partitions = [TP0, TP1]
        observer.assign(partitions)
        beginnings = observer.beginning_offsets(partitions)
        ends = observer.end_offsets(partitions)
        return {tp.partition: {'beginning': beginnings[tp], 'end': ends[tp],
                               'committed': observer.committed(tp)} for tp in partitions}
    finally:
        observer.close()


def stage_evidence(stage, published, offsets, points, failure_count, released, initial_offsets):
    if stage not in {'failure', 'recovery'}:
        raise ValueError('Unknown isolation stage')
    runs = {0: RUN_P0, 1: RUN_P1}
    partitions = []
    valid = failure_count >= 3 and (not released if stage == 'failure' else released)
    for partition in (0, 1):
        run = runs[partition]
        source = published.get(partition, [])
        if (len(source) != 3 or any(item['partition'] != partition or item['topic'] != EXPECTED['SENSOR_TOPIC']
                                    or item['identity']['tags']['run_id'] != run for item in source)
                or [item['offset'] for item in source] != [0, 1, 2]):
            raise ValueError('Published source records do not match partition and run')
        current = offsets[partition]
        observed = [observed_identity(record.values) for record in points[partition]]
        record = evidence('partition_isolation', run_id=run, topic=EXPECTED['SENSOR_TOPIC'],
                          consumer_group=EXPECTED['CONSUMER_GROUP'], partition=partition,
                          beginning_offset=current['beginning'], end_offset=current['end'],
                          committed_offset=current['committed'],
                          expected_point_identities=[item['identity'] for item in source],
                          observed_point_identities=observed, worker_exit_status=None,
                          scenario_status='incomplete', published_source_records=source,
                          initial_committed_offset=initial_offsets[partition],
                          injected_failure_count=failure_count if partition == 0 else 0,
                          release_marker_present=released)
        valid = valid and current['beginning'] == 0 and current['end'] == 3
        if stage == 'failure' and partition == 0:
            valid = valid and current['committed'] in (None, 0) and not observed
        else:
            valid = (valid and current['committed'] == 3 and not record['missing_point_identities']
                     and not record['unexpected_point_identities'] and not record['duplicate_observed_identities'])
        if stage == 'failure' and partition == 1:
            valid = valid and initial_offsets[partition] in (None, 0)
        partitions.append(record)
    # The two existing fault-injection run IDs are distinct. The shared identifier
    # describes the scenario; it is not substituted for either source run ID.
    root = evidence('partition_isolation', run_id=None, topic=EXPECTED['SENSOR_TOPIC'],
                    consumer_group=EXPECTED['CONSUMER_GROUP'], scenario_status='incomplete' if valid else 'failed',
                    compose_project=os.environ.get('M3_PROJECT'), git_commit=os.environ.get('GITHUB_SHA'),
                    scenario_run_id=f'{RUN_P0}|{RUN_P1}', stage=stage,
                    failure_snapshot_observed=stage == 'failure', partitions=partitions,
                    worker_status=None, worker_exit_status_observation=None,
                    http_write_attempt_count=None)
    return root, valid


def capture_evidence(stage, published, client, initial_offsets):
    output = os.environ.get('M3_EVIDENCE_DIR')
    if not output:
        return
    if stage == 'recovery':
        failure_path = Path(output) / 'partition_isolation_failure.json'
        if not failure_path.is_file():
            raise ValueError('Missing failure-stage snapshot; recovery evidence is incomplete')
        failure = json.loads(failure_path.read_text(encoding='utf-8'))
        if (failure.get('stage') != 'failure' or failure.get('scenario_status') == 'failed'
                or failure.get('scenario_run_id') != f'{RUN_P0}|{RUN_P1}'
                or failure.get('compose_project') != os.environ.get('M3_PROJECT')
                or failure.get('git_commit') != os.environ.get('GITHUB_SHA')):
            raise ValueError('Failure-stage snapshot does not match this scenario')
    count = int(FAILURE_COUNT.read_text(encoding='utf-8'))
    record, valid = stage_evidence(stage, published, partition_offsets(),
                                   {0: points_for(client, RUN_P0), 1: points_for(client, RUN_P1)},
                                   count, RELEASE_MARKER.exists(), initial_offsets)
    write_json(Path(output) / f'partition_isolation_{stage}.json', record)
    if not valid:
        raise AssertionError(f'Partition isolation {stage} evidence did not reconcile')


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
        initial_offsets = committed_offsets()
        published = {0: [], 1: []}
        for cycle in range(1, 4):
            for partition, run in ((0, RUN_P0), (1, RUN_P1)):
                payload = event(partition, cycle)
                metadata = producer.send(EXPECTED['SENSOR_TOPIC'], value=payload, key=f'{run}-{cycle}'.encode(), partition=partition).get(timeout=20)
                published[partition].append({'topic': metadata.topic, 'partition': metadata.partition,
                                             'offset': metadata.offset, 'event_id': payload['event_id'],
                                             'identity': published_identity(payload)})
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

        capture_evidence('failure', published, client, initial_offsets)
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
        capture_evidence('recovery', published, client, initial_offsets)
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
