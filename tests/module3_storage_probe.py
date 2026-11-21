"""Module 03: guarded, isolated REAL Kafka -> accepted consumer -> InfluxDB probe.

Run only within docker-compose.module3-verification.yml's isolated verifier service.
No production broker, bucket, network, volume, or model artifacts are used.
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
    "MODULE3_TEST_GUARD": "m3-isolated-only",
    "KAFKA_BROKER": "kafka:9092",
    "SENSOR_TOPIC": "m3-isolated-telemetry",
    "DEAD_LETTER_TOPIC": "m3-isolated-dead-letter",
    "CONSUMER_GROUP": "m3-isolated-consumer",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
    "INFLUXDB_TOKEN": "m3-isolated-token",
}
for name, expected in EXPECTED.items():
    if os.environ.get(name) != expected:
        raise SystemExit(f"REFUSED: {name} does not match the isolated test configuration")

BROKER = EXPECTED["KAFKA_BROKER"]
TOPIC = EXPECTED["SENSOR_TOPIC"]
DLQ = EXPECTED["DEAD_LETTER_TOPIC"]
GROUP = EXPECTED["CONSUMER_GROUP"]
ORG = EXPECTED["INFLUXDB_ORG"]
BUCKET = EXPECTED["INFLUXDB_BUCKET"]
RUN = "m3-test-" + uuid.uuid4().hex
T0 = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=3)


def payload(dataset: str, cycle: int, offset: int) -> dict:
    unit = 1
    timestamp = (T0 + timedelta(seconds=offset)).isoformat()
    return {
        "schema_version": "1.0.0",
        "event_id": f"{RUN}:{dataset}:{unit}:{cycle}",
        "run_id": RUN,
        "dataset_id": dataset,
        "asset_id": "engine_1",
        "unit_id": unit,
        "time_cycles": cycle,
        "sequence": cycle,
        "source_type": "cmapss",
        "timestamp": timestamp,
        "operational_setting_1": 0.1,
        "operational_setting_2": 0.2,
        "operational_setting_3": 100.0,
        **{f"sensor_{i}": float(i * 10 + cycle) for i in range(1, 22)},
    }


def query_rows(client: InfluxDBClient, field: str) -> list:
    # All selectors are internal constants or a generated hexadecimal run ID.
    flux = f'''
from(bucket: "{BUCKET}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry")
  |> filter(fn: (r) => r._field == "{field}" and r.run_id == "{RUN}")
'''
    return [record for table in client.query_api().query(flux, org=ORG) for record in table.records]


def committed_offset() -> int | None:
    # Explicit partition assignment does not join/rebalance the writer's group.
    # kafka-python KafkaConsumer.committed() queries the group's broker offset.
    observer = KafkaConsumer(
        bootstrap_servers=BROKER,
        group_id=GROUP,
        enable_auto_commit=False,
        request_timeout_ms=10000,
    )
    try:
        partition = TopicPartition(TOPIC, 0)
        observer.assign([partition])
        return observer.committed(partition)
    finally:
        observer.close()


def dlq_received() -> bool:
    # Use manual assignment, not the writer's consumer group.
    consumer = KafkaConsumer(bootstrap_servers=BROKER, enable_auto_commit=False,
                             request_timeout_ms=10000)
    try:
        tp = TopicPartition(DLQ, 0)
        consumer.assign([tp])
        consumer.seek_to_beginning(tp)
        for message in consumer.poll(timeout_ms=500, max_records=20).get(tp, []):
            record = json.loads(message.value)
            if record.get("source_topic") == TOPIC and record.get("source_offset") == 4:
                return True
        return False
    finally:
        consumer.close()


def main() -> None:
    first = payload("FD001", 1, 1)
    second = payload("FD001", 2, 2)
    # Explicit source_timestamp must take precedence over timestamp.
    second["source_timestamp"] = second["timestamp"]
    second["timestamp"] = (T0 + timedelta(seconds=120)).isoformat()
    other_dataset = payload("FD002", 1, 3)
    messages = [first, second, other_dataset, first, {"invalid_payload": True}]

    client = InfluxDBClient(
        url=EXPECTED["INFLUXDB_URL"], token=EXPECTED["INFLUXDB_TOKEN"],
        org=ORG, timeout=10000,
    )
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda obj: json.dumps(obj).encode("utf-8"),
        acks="all", retries=3,
    )
    try:
        for message in messages:
            producer.send(TOPIC, value=message, key=RUN.encode("utf-8")).get(timeout=20)
        producer.flush()
        print("Published 3 unique points, 1 exact replay, 1 malformed record")

        deadline = time.monotonic() + 120
        last_state = None
        while time.monotonic() < deadline:
            rows = query_rows(client, "time_cycles")
            sensor_rows = query_rows(client, "sensor_21")
            try:
                offset = committed_offset()
            except Exception as exc:
                print("Waiting for committed offset:", repr(exc), flush=True)
                offset = None
            try:
                dlq_ok = dlq_received()
            except Exception as exc:
                print("Waiting for dead-letter record:", repr(exc), flush=True)
                dlq_ok = False
            last_state = (len(rows), len(sensor_rows), offset, dlq_ok)
            if len(rows) == 3 and len(sensor_rows) == 3 and offset == 5 and dlq_ok:
                break
            time.sleep(3)
        else:
            raise AssertionError(f"Persistence/offset/DLQ did not converge: {last_state}")

        identity = {(r.values["dataset_id"], r.values["unit_id"],
                     r.values["run_id"], r.values["cycle"]) for r in rows}
        assert identity == {
            ("FD001", "1", RUN, "1"),
            ("FD001", "1", RUN, "2"),
            ("FD002", "1", RUN, "1"),
        }, identity
        timestamps = {(r.values["dataset_id"], r.values["cycle"]): r.get_time()
                      for r in rows}
        assert timestamps[("FD001", "2")] == T0 + timedelta(seconds=2), timestamps
        assert all(r.get_value() == 21 * 10 + int(r.values["cycle"])
                   for r in sensor_rows), "sensor_21 values changed"
        assert all(r.get_time() is not None for r in rows)
        print("PASS: Real Kafka -> accepted consumer -> isolated InfluxDB")
        print("PASS: dataset/run/cycle identities, all written timestamp identities")
        print("PASS: source_timestamp precedence, sensor field, replay idempotency")
        print("PASS: malformed payload dead-letter and Kafka offset 5 after persistence")
    finally:
        producer.close()
        client.close()


if __name__ == "__main__":
    main()
