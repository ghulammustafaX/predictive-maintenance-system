"""
Module 02 Isolated E2E Test
=============================
Publishes real C-MAPSS records to the isolated test Kafka topic,
waits for the ingestion consumer to write them to the isolated InfluxDB,
then queries InfluxDB to verify storage.

Runs INSIDE the pms-ingestion-reliability-test Docker network.
Execute as:
  docker run --rm --network pms-ingestion-reliability-test-network \\
    -e MODULE2_TEST_GUARD=isolated-ingestion-reliability-test \\
    -v <host_project_dir>:/app -w /app \\
    pms-ingestion-reliability-test-consumer python scripts/run_ingestion_e2e_test.py

Environment assumed (matches docker-compose.ingestion-reliability-test.yml):
  KAFKA_BROKER     = kafka:9092
  TOPIC            = pms-ingestion-reliability-test-sensor
  DLT_TOPIC        = pms-ingestion-reliability-test-sensor-dead-letter
  INFLUXDB_URL     = http://influxdb:8086
  INFLUXDB_TOKEN   = ingestion-reliability-test-token
  INFLUXDB_ORG     = pms-ingestion-reliability-test-org
  INFLUXDB_BUCKET  = pms-ingestion-reliability-test-bucket

This historical probe assumes fresh, empty test topics and a fresh test group.
The guard is an explicit opt-in, not proof of network isolation; inspect the
project network and endpoints before running it.
"""
from __future__ import annotations
import json, os, sys, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from influxdb_client import InfluxDBClient

if os.getenv("MODULE2_TEST_GUARD") != "isolated-ingestion-reliability-test":
    raise RuntimeError("Refusing to run outside the isolated ingestion reliability test")

KAFKA_BROKER  = "kafka:9092"
SENSOR_TOPIC  = "pms-ingestion-reliability-test-sensor"
DLT_TOPIC     = "pms-ingestion-reliability-test-sensor-dead-letter"
INFLUX_URL    = "http://influxdb:8086"
INFLUX_TOKEN  = "ingestion-reliability-test-token"
INFLUX_ORG    = "pms-ingestion-reliability-test-org"
INFLUX_BUCKET = "pms-ingestion-reliability-test-bucket"

PASS = []
FAIL = []

def check(label, condition, detail=""):
    if condition:
        PASS.append(label)
        print(f"  PASS  {label}")
    else:
        FAIL.append(label)
        print(f"  FAIL  {label}  {detail}")


print("=" * 60)
print("MODULE 02 ISOLATED END-TO-END TEST")
print("=" * 60)
print()

# ------------------------------------------------------------------
# Step 1: Build 10 valid C-MAPSS payloads
# ------------------------------------------------------------------
print("Step 1: Build 10 valid C-MAPSS payloads")
run_id = uuid.uuid4().hex
payloads = []
for cycle in range(1, 11):
    ts = datetime.now(timezone.utc).isoformat()
    p = {
        "schema_version": "1.0.0",
        "event_id": str(uuid.uuid5(uuid.UUID(int=0), f"{run_id}:engine_1:{cycle}")),
        "run_id": run_id,
        "dataset_id": "FD001",
        "asset_id": "engine_1",
        "unit_id": 1,
        "time_cycles": cycle,
        "operational_setting_1": -0.0007,
        "operational_setting_2": -0.0004,
        "operational_setting_3": 100.0,
        "timestamp": ts,
        "sequence": cycle,
        "source_type": "cmapss",
        "sensor_1": 518.67,
    }
    for i in range(2, 22):
        p[f"sensor_{i}"] = float(i) * 10.5
    payloads.append(p)
print(f"  Built {len(payloads)} payloads with run_id={run_id[:8]}...")

# ------------------------------------------------------------------
# Step 2: Publish valid payloads to Kafka
# ------------------------------------------------------------------
print()
print("Step 2: Publish 10 valid + 2 invalid payloads to Kafka")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=3,
)
for p in payloads:
    key = f"FD001:1:{run_id}".encode("utf-8")
    producer.send(SENSOR_TOPIC, value=p, key=key)

# Publish 2 invalid messages (missing required fields -> should go to DLT)
invalid1 = {"garbage": True, "run_id": run_id, "event_id": "bad-1"}
invalid2 = {"unit_id": "not-a-number", "run_id": run_id, "event_id": "bad-2"}
producer.send(SENSOR_TOPIC, value=invalid1)
producer.send(SENSOR_TOPIC, value=invalid2)
producer.flush()
producer.close()
print(f"  Published 10 valid + 2 invalid messages")

# ------------------------------------------------------------------
# Step 3: Wait for consumer to process
# ------------------------------------------------------------------
print()
print("Step 3: Waiting 35s for consumer to process and write to InfluxDB...")
time.sleep(35)

# ------------------------------------------------------------------
# Step 4: Verify Kafka offsets (use a unique throw-away group so we
#         never join the ingestion consumer's group and cause a rebalance)
# ------------------------------------------------------------------
print()
print("Step 4: Verify Kafka received messages (>=12 cumulative)")
c = KafkaConsumer(
    bootstrap_servers=KAFKA_BROKER,
    group_id=f"e2e-verifier-offsets-{uuid.uuid4().hex[:8]}",
    request_timeout_ms=5000,
)
offsets = {}
for part in [0, 1]:
    tp = TopicPartition(SENSOR_TOPIC, part)
    c.assign([tp])
    c.seek_to_end(tp)
    offsets[part] = c.position(tp)
c.close()
total_sensor = sum(offsets.values())
check("sensor topic received messages (>=12 cumulative)", total_sensor >= 12,
      f"got {total_sensor}")

# ------------------------------------------------------------------
# Step 5: Query InfluxDB for stored records
# ------------------------------------------------------------------
print()
print("Step 5: Query InfluxDB for stored records")
influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN,
                        org=INFLUX_ORG, timeout=15000)
q_api = influx.query_api()

# 5a: Count records
q_count = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry")
  |> filter(fn: (r) => r.run_id == "{run_id}")
  |> filter(fn: (r) => r._field == "time_cycles")
  |> count()
'''
tables = q_api.query(q_count, org=INFLUX_ORG)
stored_count = sum(r.get_value() for t in tables for r in t.records)
check("10 valid records stored in InfluxDB", stored_count == 10,
      f"got {stored_count}")

# 5b: Verify measurement name
q_meas = f'''
import "influxdata/influxdb/schema"
schema.measurements(bucket: "{INFLUX_BUCKET}")
'''
measurements = [r.get_value() for t in q_api.query(q_meas, org=INFLUX_ORG) for r in t.records]
check("measurement name is sensor_telemetry", "sensor_telemetry" in measurements,
      f"got {measurements}")

# 5c: Verify dataset_id tag
q_first = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry")
  |> filter(fn: (r) => r.run_id == "{run_id}")
  |> filter(fn: (r) => r._field == "sensor_1")
  |> first()
'''
records = [r for t in q_api.query(q_first, org=INFLUX_ORG) for r in t.records]
if records:
    r0 = records[0]
    check("dataset_id tag = FD001", r0.values.get("dataset_id") == "FD001",
          r0.values.get("dataset_id"))
    check("unit_id tag = 1", r0.values.get("unit_id") == "1",
          r0.values.get("unit_id"))
    check("run_id tag matches published run_id", r0.values.get("run_id") == run_id)
    check("sensor_1 value = 518.67",
          abs(r0.get_value() - 518.67) < 0.001,
          r0.get_value())
    check("timestamp is set", r0.get_time() is not None)
else:
    for lbl in ["dataset_id tag = FD001", "unit_id tag = 1",
                "run_id tag matches", "sensor_1 value", "timestamp"]:
        FAIL.append(lbl); print(f"  FAIL  {lbl}  no records returned")

# 5d: Verify all 21 sensors stored
q_pivot = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry")
  |> filter(fn: (r) => r.run_id == "{run_id}")
  |> filter(fn: (r) => r.unit_id == "1")
  |> filter(fn: (r) => r.cycle == "1")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
piv = [r for t in q_api.query(q_pivot, org=INFLUX_ORG) for r in t.records]
if piv:
    sensor_count = sum(1 for i in range(1, 22) if f"sensor_{i}" in piv[0].values)
    op_count = sum(1 for i in range(1, 4) if f"operational_setting_{i}" in piv[0].values)
    check(f"all 21 sensor fields stored", sensor_count == 21, f"got {sensor_count}")
    check(f"all 3 op settings stored", op_count == 3, f"got {op_count}")
else:
    FAIL.append("pivot query returned no records")
    print("  FAIL  pivot query returned no records")

influx.close()

# ------------------------------------------------------------------
# Step 6: Verify DLT received invalid messages
# ------------------------------------------------------------------
print()
print("Step 6: Verify dead-letter topic received invalid messages")
c2 = KafkaConsumer(
    bootstrap_servers=KAFKA_BROKER,
    group_id=f"e2e-verifier-dlt-{uuid.uuid4().hex[:8]}",
    request_timeout_ms=5000,
)
dlt_offsets = {}
for part in [0, 1]:
    tp2 = TopicPartition(DLT_TOPIC, part)
    c2.assign([tp2])
    c2.seek_to_end(tp2)
    dlt_offsets[part] = c2.position(tp2)
c2.close()
total_dlt = sum(dlt_offsets.values())
check("invalid messages routed to dead-letter topic (>=2)", total_dlt >= 2,
      f"got {total_dlt}")

# ------------------------------------------------------------------
# Step 7: Verify consumer group committed offset via AdminClient
# ------------------------------------------------------------------
print()
print("Step 7: Verify consumer group committed offset")
from kafka import KafkaAdminClient
from kafka.structs import TopicPartition as TP
try:
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, request_timeout_ms=10000)
    offsets_resp = admin.list_consumer_group_offsets(
        "pms-ingestion-reliability-test-group"
    )
    admin.close()
    total_committed = sum(
        meta.offset for tp, meta in offsets_resp.items()
        if tp.topic == SENSOR_TOPIC
    )
    check("consumer committed all 12 message offsets",
          total_committed == 12, f"committed {total_committed}")
except Exception as e:
    check("consumer committed all 12 message offsets", False,
          f"offset query failed: {e}")

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
print()
print("=" * 60)
print(f"PASS: {len(PASS)}  FAIL: {len(FAIL)}")
if FAIL:
    print("Failed checks:")
    for f in FAIL:
        print(f"  - {f}")
print()
if not FAIL:
    print("MODULE 02 E2E TEST: PASS")
    sys.exit(0)
else:
    print("MODULE 02 E2E TEST: FAIL")
    sys.exit(1)
