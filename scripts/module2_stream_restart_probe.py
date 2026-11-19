"""Publish and reconcile a unique run in the isolated Module 02 test network."""
import json
import os
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient
from kafka import KafkaConsumer

if os.getenv("MODULE2_TEST_GUARD") != "isolated-module2-verification":
    raise RuntimeError("Refusing to run without the isolated Module 02 guard")

run_id, mode, total_arg = sys.argv[1:4]
total = int(total_arg)
if mode == "publish":
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect("mosquitto", 1883)
    client.loop_start()
    for i in range(1, total + 1):
        payload = {"schema_version": "1.0.0", "event_id": f"{run_id}-{i}",
                   "run_id": run_id, "dataset_id": "FD001", "asset_id": "engine_1",
                   "unit_id": 1, "time_cycles": i, "sequence": i, "source_type": "cmapss",
                   "timestamp": datetime.now(timezone.utc).isoformat(),
                   "operational_setting_1": -0.0007, "operational_setting_2": -0.0004,
                   "operational_setting_3": 100.0,
                   **{f"sensor_{n}": (518.67 + i if n == 1 else float(n)) for n in range(1, 22)}}
        info = client.publish("pms/telemetry/cmapss", json.dumps(payload), qos=1)
        info.wait_for_publish(timeout=10)
        if not info.is_published():
            raise RuntimeError(f"MQTT publication not acknowledged: {i}")
        print(f"PUBLISHED sequence={i} mid={info.mid}", flush=True)
        time.sleep(0.5)
    client.disconnect()
    client.loop_stop()
    print(f"COMPLETE published={total}", flush=True)
elif mode == "verify":
    c = KafkaConsumer("m2-test-sensor", bootstrap_servers="kafka:9092", group_id=None,
                      auto_offset_reset="earliest", consumer_timeout_ms=6000)
    records = [json.loads(m.value) for m in c if json.loads(m.value).get("run_id") == run_id]
    c.close()
    expected = {f"{run_id}-{i}" for i in range(1, total + 1)}
    ids = [r["event_id"] for r in records]
    if set(ids) != expected:
        raise RuntimeError(f"Kafka identity mismatch: distinct={len(set(ids))}, expected={total}")
    with InfluxDBClient(url="http://influxdb:8086", token="module2-isolated-test-token",
                        org="module2-test") as db:
        query = ('from(bucket: "module2-test") |> range(start: 0) '
                 '|> filter(fn: (r) => r._measurement == "sensor_telemetry" '
                 f'and r.run_id == "{run_id}") '
                 '|> filter(fn: (r) => r._field =~ /^sensor_[0-9]+$/)')
        rows = [r for table in db.query_api().query(query) for r in table.records]
    values = {(int(r.values["cycle"]), r.get_field()): r.get_value() for r in rows}
    expected_fields = {(i, f"sensor_{n}") for i in range(1, total + 1) for n in range(1, 22)}
    if len(rows) != total * 21 or set(values) != expected_fields:
        raise RuntimeError(f"Influx field mismatch: rows={len(rows)} distinct={len(values)}")
    if any(abs(value - (518.67 + cycle if field == "sensor_1" else float(field[7:]))) > 0.00001
           for (cycle, field), value in values.items()):
        raise RuntimeError("Influx sensor values differ from the published values")
    print(f"VERIFIED published={total} kafka_records={len(records)} kafka_unique_ids={len(set(ids))} "
          f"duplicate_kafka_records={len(records)-len(set(ids))} influx_points={total} sensor_values_match={len(values)}")
else:
    raise ValueError(mode)
