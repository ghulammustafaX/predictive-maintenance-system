"""Real Kafka -> opt-in batch worker -> InfluxDB integration verification.

Never connects to the main stack. The consumer and this test use independent
Module 03 batch Kafka topic/group names.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

from influxdb_client import InfluxDBClient
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition
from module3_evidence import evidence, observed_identity, published_identity, write_json

EXPECTED = {
    "MODULE3_BATCH_TEST_GUARD": "m3-isolated-batch-only",
    "KAFKA_BROKER": "kafka:9092",
    "SENSOR_TOPIC": "m3-batch-telemetry",
    "DEAD_LETTER_TOPIC": "m3-batch-dead-letter",
    "CONSUMER_GROUP": "m3-batch-writer-group",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
}
for key, value in EXPECTED.items():
    if os.environ.get(key) != value:
        raise SystemExit(f"REFUSED: unexpected batch test setting {key}")

RUN = "m3-batch-" + uuid.uuid4().hex
STAMP = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=2)


def event(offset: int):
    cycle = offset + 1
    return {
        "schema_version": "1.0.0", "event_id": f"{RUN}-{cycle}",
        "run_id": RUN, "dataset_id": "FD003", "asset_id": "engine_9",
        "unit_id": 9, "time_cycles": cycle, "sequence": cycle,
        "source_type": "cmapss",
        "timestamp": (STAMP + timedelta(seconds=cycle)).isoformat(),
        "operational_setting_1": 0.1, "operational_setting_2": 0.2,
        "operational_setting_3": 100.0, "sensor_21": float(cycle * 5),
    }


def offset_observed() -> int | None:
    observer = KafkaConsumer(
        bootstrap_servers=EXPECTED["KAFKA_BROKER"],
        group_id=EXPECTED["CONSUMER_GROUP"],
        enable_auto_commit=False, request_timeout_ms=10000,
    )
    try:
        tp = TopicPartition(EXPECTED["SENSOR_TOPIC"], 0)
        observer.assign([tp])
        return observer.committed(tp)
    finally:
        observer.close()


def dlq_observed(expected_source_offset: int) -> bool:
    observer = KafkaConsumer(
        bootstrap_servers=EXPECTED["KAFKA_BROKER"], enable_auto_commit=False,
        request_timeout_ms=10000,
    )
    try:
        tp = TopicPartition(EXPECTED["DEAD_LETTER_TOPIC"], 0)
        observer.assign([tp])
        observer.seek_to_beginning(tp)
        for m in observer.poll(timeout_ms=800, max_records=50).get(tp, []):
            payload = json.loads(m.value)
            if payload.get("source_offset") == expected_source_offset:
                return True
        return False
    finally:
        observer.close()


def source_offsets():
    observer = KafkaConsumer(bootstrap_servers=EXPECTED['KAFKA_BROKER'],
                             group_id=EXPECTED['CONSUMER_GROUP'], enable_auto_commit=False,
                             request_timeout_ms=10000)
    try:
        tp = TopicPartition(EXPECTED['SENSOR_TOPIC'], 0)
        observer.assign([tp])
        return observer.beginning_offsets([tp])[tp], observer.end_offsets([tp])[tp], observer.committed(tp)
    finally:
        observer.close()


def matching_dlq_records(source_offset, invalid_payload, committed):
    observer = KafkaConsumer(bootstrap_servers=EXPECTED['KAFKA_BROKER'], enable_auto_commit=False,
                             request_timeout_ms=10000)
    try:
        tp = TopicPartition(EXPECTED['DEAD_LETTER_TOPIC'], 0)
        observer.assign([tp])
        beginning = observer.beginning_offsets([tp])[tp]
        end = observer.end_offsets([tp])[tp]
        observer.seek(tp, beginning)
        matches = []
        deadline = time.monotonic() + 15
        while observer.position(tp) < end and time.monotonic() < deadline:
            for message in observer.poll(timeout_ms=800, max_records=50).get(tp, []):
                payload = json.loads(message.value)
                raw = payload.get('value')
                try:
                    original = json.loads(raw) if isinstance(raw, str) else raw
                except (TypeError, ValueError):
                    continue
                if (payload.get('source_topic') == EXPECTED['SENSOR_TOPIC']
                        and payload.get('source_partition') == 0
                        and payload.get('source_offset') == source_offset
                        and original == invalid_payload):
                    matches.append({'source_topic': payload['source_topic'], 'source_partition': 0,
                                    'source_offset': source_offset, 'dlq_topic': message.topic,
                                    'dlq_partition': message.partition, 'dlq_offset': message.offset,
                                    'source_committed_offset': committed,
                                    'source_value': original})
        if observer.position(tp) < end:
            raise TimeoutError('Did not inspect the complete DLQ offset range')
        return matches
    finally:
        observer.close()


def stored_points(client):
    q = f'''from(bucket: "{EXPECTED["INFLUXDB_BUCKET"]}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry" and r._field == "time_cycles")
  |> filter(fn: (r) => r.run_id == "{RUN}")
'''
    return [rec for table in client.query_api().query(q, org=EXPECTED["INFLUXDB_ORG"])
            for rec in table.records]


def main() -> None:
    expected_points = None
    observed_points = None
    correlations = None
    published_source_records = []
    beginning = end = committed = None
    result = 'incomplete'
    completed = False
    producer = client = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=EXPECTED["KAFKA_BROKER"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            acks="all", retries=3,
        )
        client = InfluxDBClient(
            url=EXPECTED["INFLUXDB_URL"], org=EXPECTED["INFLUXDB_ORG"],
            token=EXPECTED["INFLUXDB_TOKEN"], timeout=10000,
        )
        payloads = [event(0), event(1), event(2), {"invalid": True},
                    event(3), event(4), event(5), event(5)]
        expected_points = []
        delivered = []
        for index, payload in enumerate(payloads):
            metadata = producer.send(EXPECTED["SENSOR_TOPIC"], value=payload, key=RUN.encode()).get(timeout=20)
            delivered.append((metadata.partition, metadata.offset))
            published_source_records.append({'topic': metadata.topic, 'partition': metadata.partition,
                                             'offset': metadata.offset, 'event_id': payload.get('event_id')})
            if index != 3:
                identity = published_identity(payload)
                if identity not in expected_points:
                    expected_points.append(identity)
        producer.flush()
        assert delivered == [(0, offset) for offset in range(8)], delivered
        deadline = time.monotonic() + 90
        last = None
        while time.monotonic() < deadline:
            points = stored_points(client)
            committed = offset_observed()
            dlq = dlq_observed(3)
            last = (len(points), committed, dlq)
            if last == (6, 8, True):
                break
            time.sleep(2)
        else:
            raise AssertionError(f"Isolated batch worker did not converge: {last}")
        identities = {(p.values["dataset_id"], p.values["unit_id"],
                       p.values["run_id"], p.values["cycle"]) for p in points}
        assert identities == {("FD003", "9", RUN, str(n)) for n in range(1, 7)}, identities
        observed_points = [observed_identity(point.values) for point in points]
        beginning, end, committed = source_offsets()
        correlations = matching_dlq_records(delivered[3][1], payloads[3], committed)
        assert beginning == 0 and end == 8 and committed == 8, (beginning, end, committed)
        assert correlations, 'DLQ did not contain the published invalid source record at offset 3'
        reconciliation = evidence('batch_persistence', run_id=RUN, topic=EXPECTED['SENSOR_TOPIC'],
                                  consumer_group=EXPECTED['CONSUMER_GROUP'], partition=0,
                                  beginning_offset=beginning, end_offset=end, committed_offset=committed,
                                  expected_point_identities=expected_points, observed_point_identities=observed_points,
                                  dlq_source_correlations=correlations)
        assert not reconciliation['missing_point_identities'] and not reconciliation['unexpected_point_identities']
        assert not reconciliation['duplicate_observed_identities']
        # The continuously running worker has no observed exit status here.
        result = 'incomplete'
        completed = True
        print("PASS: 6 unique raw points persisted from 7 valid messages (one replay)", flush=True)
        print("PASS: invalid record sent to DLQ before committing offset 8", flush=True)
        print("PASS: isolated batch worker preserved dataset, engine, run and cycle identity", flush=True)
        print("MODULE 03 REAL BATCH INTEGRATION: PASS", flush=True)
    except Exception:
        result = 'failed'
        raise
    finally:
        try:
            evidence_dir = os.environ.get('M3_EVIDENCE_DIR')
            if evidence_dir:
                record = evidence('batch_persistence', run_id=RUN, topic=EXPECTED['SENSOR_TOPIC'],
                                  consumer_group=EXPECTED['CONSUMER_GROUP'], partition=0,
                                  beginning_offset=beginning, end_offset=end, committed_offset=committed,
                                  expected_point_identities=expected_points, observed_point_identities=observed_points,
                                  dlq_source_correlations=correlations, worker_exit_status=None,
                                  scenario_status=result, compose_project=os.environ.get('M3_PROJECT'),
                                  git_commit=os.environ.get('GITHUB_SHA'),
                                  published_source_records=published_source_records,
                                  dlq_ack_before_source_commit_observed=None,
                                  note='DLQ record and final commit observed; their ordering is not independently timestamped.')
                if completed and os.environ.get('M3_PROJECT') and os.environ.get('GITHUB_SHA'):
                    from module3_batch_aggregate import finalize
                    try:
                        record = finalize(record, os.environ['M3_PROJECT'], os.environ['GITHUB_SHA'])
                    except ValueError as exc:
                        record['scenario_status'] = 'failed'
                        record['validation_error'] = str(exc)
                        write_json(os.path.join(evidence_dir, 'batch_persistence.json'), record)
                        raise
                write_json(os.path.join(evidence_dir, 'batch_persistence.json'), record)
                if completed:
                    dlq_record = dict(record)
                    dlq_record['scenario_id'] = 'dlq'
                    if record['scenario_status'] == 'passed':
                        dlq_record = finalize(dlq_record, os.environ['M3_PROJECT'], os.environ['GITHUB_SHA'])
                    write_json(os.path.join(evidence_dir, 'dlq.json'), dlq_record)
        finally:
            try:
                if producer is not None:
                    producer.close()
            finally:
                if client is not None:
                    client.close()


if __name__ == "__main__":
    main()
