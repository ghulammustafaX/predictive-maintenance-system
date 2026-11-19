"""Read/write probe for the dedicated docker-compose.module2-verification project only."""
import json
import os
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient
from kafka import KafkaConsumer

if os.getenv("MODULE2_TEST_GUARD") != "isolated-module2-verification":
    raise RuntimeError("Refusing test writes without the isolated Module 02 guard")

run_id, mode = sys.argv[1:3]
event_id = f"probe-{run_id}"
if mode in ("publish", "duplicate"):
    payload = {"schema_version": "1.0.0", "event_id": event_id,
               "run_id": run_id, "dataset_id": "FD001", "asset_id": "engine_1",
               "unit_id": 1, "time_cycles": 1, "sequence": 1, "source_type": "cmapss",
               "timestamp": datetime.now(timezone.utc).isoformat(),
               "operational_setting_1": -0.0007, "operational_setting_2": -0.0004,
               "operational_setting_3": 100.0}
    payload.update({f"sensor_{i}": 518.67 if i == 1 else float(i) for i in range(1, 22)})
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect("mosquitto", 1883)
    client.loop_start()
    for _ in range(2 if mode == "duplicate" else 1):
        info = client.publish("pms/telemetry/cmapss", json.dumps(payload), qos=1)
        info.wait_for_publish(timeout=10)
        if not info.is_published():
            raise RuntimeError("MQTT publisher was not acknowledged")
        print(f"PUBLISHED event_id={event_id} mid={info.mid} qos=1")
    client.disconnect()
    client.loop_stop()
elif mode == "verify":
    c = KafkaConsumer("m2-test-sensor", bootstrap_servers="kafka:9092",
                      group_id=None, auto_offset_reset="earliest", consumer_timeout_ms=6000)
    found = [json.loads(m.value) for m in c if json.loads(m.value).get("event_id") == event_id]
    c.close()
    if not found:
        raise RuntimeError(f"event {event_id} missing from Kafka")
    with InfluxDBClient(url="http://influxdb:8086", token="module2-isolated-test-token",
                        org="module2-test") as db:
        query = ('from(bucket: "module2-test") |> range(start: 0) '
                 '|> filter(fn: (r) => r._measurement == "sensor_telemetry" '
                 f'and r.run_id == "{run_id}" and r._field == "sensor_1")')
        rows = [r for table in db.query_api().query(query) for r in table.records]
    if len(rows) != 1 or abs(rows[0].get_value() - 518.67) > 0.00001:
        raise RuntimeError(f"unexpected InfluxDB rows: {[(r.get_value()) for r in rows]}")
    print(f"VERIFIED event_id={event_id} kafka_records={len(found)} influx_points={len(rows)} sensor_1={rows[0].get_value()}")
elif mode in ("bad-json", "bad-utf8", "bad-schema"):
    if mode == "bad-json":
        wire = ("{bad-json-" + run_id).encode()
    elif mode == "bad-utf8":
        wire = b"\xff" + run_id.encode()
    else:
        wire = json.dumps({"schema_version": "99.0", "event_id": event_id,
                           "run_id": run_id, "dataset_id": "FD001", "asset_id": "engine_1",
                           "unit_id": 1, "time_cycles": 1, "sequence": 1,
                           "source_type": "cmapss", "timestamp": datetime.now(timezone.utc).isoformat(),
                           "operational_setting_1": 0, "operational_setting_2": 0,
                           "operational_setting_3": 0, "sensor_1": 518.67}).encode()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect("mosquitto", 1883)
    client.loop_start()
    info = client.publish("pms/telemetry/cmapss", wire, qos=1)
    info.wait_for_publish(timeout=10)
    if not info.is_published():
        raise RuntimeError("MQTT publish not acknowledged")
    print(f"PUBLISHED {mode} run_id={run_id} mid={info.mid} qos=1")
    client.disconnect()
    client.loop_stop()
elif mode == "verify-dlq":
    c = KafkaConsumer("m2-test-dead-letter", bootstrap_servers="kafka:9092",
                      group_id=None, auto_offset_reset="earliest", consumer_timeout_ms=6000)
    found = [m.value.decode("utf-8") for m in c if run_id in m.value.decode("utf-8")]
    c.close()
    if not found:
        raise RuntimeError(f"dead-letter record for {run_id} missing")
    print(f"VERIFIED dead-letter run_id={run_id} kafka_records={len(found)}")
else:
    raise ValueError(mode)
