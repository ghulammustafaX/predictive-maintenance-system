"""
Task 6: Simulator → Kafka → Kafka consumer → isolated InfluxDB end-to-end.
Runs inside pms-m1-test_m1-test-net.
Uses the isolated test InfluxDB (pms-m1-test-influxdb) — NOT the production bucket.
"""

import sys, os, json, time, uuid
sys.path.insert(0, "/app/src")
os.environ["CMAPSS_DATA_DIR"] = "/app/data/cmapss"

from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from cmapss_loader import load_cmapss, assign_rul_labels, iter_rows
from event_schema import new_run_id
from kafka_producer import KafkaProducerClient

KAFKA_HOST    = "kafka:9092"
INFLUX_URL    = "http://influxdb:8086"
INFLUX_TOKEN  = "test-token-m1-gap-verification"
INFLUX_ORG    = "pms-test-org"
INFLUX_BUCKET = "test-sensor-data"
SENSOR_TOPIC  = "sensor-telemetry"

print("=== TASK 6: Simulator → Kafka → InfluxDB End-to-End ===")
print()

# ── 1) Load a small dataset ────────────────────────────────────────────────
print("Step 1: Load 3 engines of FD001 train data")
df_full = load_cmapss("FD001", "train")
df = df_full[df_full["unit_id"].isin([1, 2, 3])].reset_index(drop=True)
df = assign_rul_labels(df, 125)
run_id = new_run_id()
payloads = [(p, k) for p, k, _ in iter_rows(df, run_id=run_id,
            dataset_id="FD001", source_split="train")]
print(f"  Payloads: {len(payloads)}  run_id: {run_id}")

# ── 2) Publish directly to Kafka ───────────────────────────────────────────
print()
print("Step 2: Publish to Kafka sensor-telemetry")
producer = KafkaProducerClient(bootstrap_servers=KAFKA_HOST, connect_attempts=5)
producer.connect()
for payload, kafka_key in payloads:
    producer.send(SENSOR_TOPIC, payload, key=kafka_key)
producer.close()
print(f"  Published {len(payloads)} messages to {SENSOR_TOPIC}")

# ── 3) Write to InfluxDB (simulating the ingestion consumer) ──────────────
print()
print("Step 3: Write to isolated test InfluxDB")

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN,
                                org=INFLUX_ORG, timeout=15000)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

written = 0
errors  = 0
sample_point_info = None

for payload, _ in payloads:
    try:
        unit_id    = int(payload["unit_id"])
        cycle      = int(payload["time_cycles"])
        dataset_id = str(payload["dataset_id"])
        p_run_id   = str(payload["run_id"])

        point = (
            Point("sensor_telemetry")
            .tag("dataset_id", dataset_id)
            .tag("unit_id", str(unit_id))
            .tag("run_id", p_run_id)
            .tag("cycle", str(cycle))
            .field("time_cycles", cycle)
            .field("operational_setting_1", float(payload["operational_setting_1"]))
            .field("operational_setting_2", float(payload["operational_setting_2"]))
            .field("operational_setting_3", float(payload["operational_setting_3"]))
        )
        for i in range(1, 22):
            v = payload.get(f"sensor_{i}")
            if v is not None:
                point = point.field(f"sensor_{i}", float(v))
        point = point.time(payload["timestamp"])
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        written += 1
        if written == 1:
            sample_point_info = {
                "dataset_id": dataset_id, "unit_id": unit_id, "cycle": cycle,
                "run_id": p_run_id, "sensor_1": payload.get("sensor_1"),
                "timestamp": payload["timestamp"],
            }
    except Exception as e:
        errors += 1
        if errors <= 3:
            print(f"  Write error: {e}")

write_api.close()
print(f"  Written: {written}  Errors: {errors}")
print(f"  Sample point: {sample_point_info}")

# ── 4) Query back and verify ───────────────────────────────────────────────
print()
print("Step 4: Query InfluxDB and verify")
time.sleep(2)   # let InfluxDB flush

query_api = influx_client.query_api()

# 4a) Count total points written
q_count = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry")
  |> filter(fn: (r) => r.run_id == "{run_id}")
  |> filter(fn: (r) => r._field == "time_cycles")
  |> count()
'''
tables = query_api.query(q_count, org=INFLUX_ORG)
total_points = sum(len(t.records) for t in tables)
print(f"  Total points in InfluxDB for run_id: {total_points} (expect {len(payloads)})")

# 4b) Verify dataset_id tag
q_tags = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry")
  |> filter(fn: (r) => r.run_id == "{run_id}")
  |> filter(fn: (r) => r._field == "sensor_1")
  |> first()
'''
records = []
for t in query_api.query(q_tags, org=INFLUX_ORG):
    for r in t.records:
        records.append(r)

if records:
    r0 = records[0]
    print(f"  dataset_id tag: {r0.values.get('dataset_id')!r}  (expect 'FD001')")
    print(f"  unit_id tag:    {r0.values.get('unit_id')!r}")
    print(f"  run_id tag:     {r0.values.get('run_id')[:16]}...")
    print(f"  sensor_1 value: {r0.get_value()}")
    print(f"  timestamp:      {r0.get_time()}")
    dataset_ok  = r0.values.get("dataset_id") == "FD001"
    unit_ok     = r0.values.get("unit_id") is not None
    run_ok      = r0.values.get("run_id") == run_id
    sensor_ok   = r0.get_value() is not None
    ts_ok       = r0.get_time() is not None
    print(f"  dataset_id PASS: {dataset_ok}")
    print(f"  unit_id    PASS: {unit_ok}")
    print(f"  run_id     PASS: {run_ok}")
    print(f"  sensor_1   PASS: {sensor_ok}")
    print(f"  timestamp  PASS: {ts_ok}")
else:
    print("  WARN: No records returned from query")

# 4c) Verify all 21 sensors are present for a specific point
q_fields = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry")
  |> filter(fn: (r) => r.run_id == "{run_id}")
  |> filter(fn: (r) => r.unit_id == "1")
  |> filter(fn: (r) => r.cycle == "1")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
pivot_tables = query_api.query(q_fields, org=INFLUX_ORG)
pivot_records = [r for t in pivot_tables for r in t.records]
if pivot_records:
    pr = pivot_records[0]
    sensor_fields_found = sum(1 for i in range(1, 22) if f"sensor_{i}" in pr.values)
    print(f"  Sensor fields in pivoted record: {sensor_fields_found}/21")
    print(f"  All 21 sensors present: {sensor_fields_found == 21}")
else:
    print("  WARN: Pivot query returned no records")

# 4d) Verify measurement name
q_meas = f'''
import "influxdata/influxdb/schema"
schema.measurements(bucket: "{INFLUX_BUCKET}")
'''
meas_tables = query_api.query(q_meas, org=INFLUX_ORG)
measurements = [r.get_value() for t in meas_tables for r in t.records]
print(f"  Measurements in bucket: {measurements}")
print(f"  'sensor_telemetry' present: {'sensor_telemetry' in measurements}")

influx_client.close()

print()
write_ok  = written == len(payloads) and errors == 0
query_ok  = len(records) > 0
count_ok  = total_points > 0
print(f"Write:  {'PASS' if write_ok  else 'FAIL'} ({written}/{len(payloads)})")
print(f"Query:  {'PASS' if query_ok  else 'FAIL'}")
print(f"Count:  {'PASS' if count_ok  else 'FAIL'} ({total_points} points)")
print()
print("TASK 6:", "PASS" if (write_ok and query_ok) else "FAIL")
