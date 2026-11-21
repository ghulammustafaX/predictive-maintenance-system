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


def stored_points(client):
    q = f'''from(bucket: "{EXPECTED["INFLUXDB_BUCKET"]}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry" and r._field == "time_cycles")
  |> filter(fn: (r) => r.run_id == "{RUN}")
'''
    return [rec for table in client.query_api().query(q, org=EXPECTED["INFLUXDB_ORG"])
            for rec in table.records]


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=EXPECTED["KAFKA_BROKER"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        acks="all", retries=3,
    )
    client = InfluxDBClient(
        url=EXPECTED["INFLUXDB_URL"], org=EXPECTED["INFLUXDB_ORG"],
        token=EXPECTED["INFLUXDB_TOKEN"], timeout=10000,
    )
    try:
        payloads = [event(0), event(1), event(2), {"invalid": True},
                    event(3), event(4), event(5), event(5)]
        for payload in payloads:
            producer.send(EXPECTED["SENSOR_TOPIC"], value=payload, key=RUN.encode()).get(timeout=20)
        producer.flush()
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
        print("PASS: 6 unique raw points persisted from 7 valid messages (one replay)", flush=True)
        print("PASS: invalid record sent to DLQ before committing offset 8", flush=True)
        print("PASS: isolated batch worker preserved dataset, engine, run and cycle identity", flush=True)
        print("MODULE 03 REAL BATCH INTEGRATION: PASS", flush=True)
    finally:
        producer.close()
        client.close()


if __name__ == "__main__":
    main()
