"""
Task 2: IMS MQTT Integration Test
Runs inside pms-m1-test_m1-test-net with:
  KAFKA_HOST=kafka:9092, MQTT_HOST=mosquitto, MQTT_PORT=1883
"""

import json, sys, time, uuid
sys.path.insert(0, "/app/src")

from pathlib import Path
from kafka import KafkaConsumer
from event_schema import new_run_id
from ims_loader import (
    list_snapshots, extract_snapshot, extract_all_features,
    build_ims_payload, ARCHIVE_CONFIGS
)

KAFKA_HOST   = "kafka:9092"
MQTT_HOST    = "mosquitto"
MQTT_PORT    = 1883
BEARING_TOPIC = "bearing-telemetry"

data_dir  = Path("/app/data/ims")
test_name = "2nd_test"
cfg       = ARCHIVE_CONFIGS[test_name]
rar       = data_dir / cfg["archive"]

print("=== TASK 2: IMS MQTT Integration ===")
print()

# ── Step 1: Load a real IMS recording ──────────────────────────────────────
print("Step 1: Load real IMS recording")
snaps = list_snapshots(test_name, data_dir)
snap_path = snaps[0]
snap = extract_snapshot(rar, snap_path, n_channels=4, chunk_size=20480)
print(f"  Extracted {snap_path}  shape={snap.shape}  dtype={snap.dtype}")
assert snap.shape == (20480, 4), "unexpected shape"
assert not __import__("numpy").isnan(snap).any(), "NaN in snapshot"

bearing_names  = list(cfg["bearings"].keys())
all_feats      = extract_all_features(snap, bearing_names)
bearing_to_use = bearing_names[0]      # bearing_1
features       = all_feats[bearing_to_use]
print(f"  Using bearing: {bearing_to_use}")
print(f"  rms={features['rms']:.5f}  kurtosis={features['kurtosis']:.4f}")
del snap

# ── Step 2: Build the telemetry event ──────────────────────────────────────
print()
print("Step 2: Build telemetry event")
run_id   = new_run_id()
sequence = 1
payload  = build_ims_payload(
    test_name=test_name,
    snapshot_path=snap_path,
    snapshot=__import__("numpy").zeros((1, 4), dtype="float32"),
    run_id=run_id,
    sequence=sequence,
    bearing_name=bearing_to_use,
    channel_data=__import__("numpy").zeros(1, dtype="float32"),
    features=features,
    sampling_hz=cfg["sampling_hz"],
    chunk_size=20480,
)
print(f"  event_id  : {payload['event_id']}")
print(f"  run_id    : {payload['run_id']}")
print(f"  asset_id  : {payload['asset_id']}")
print(f"  dataset_id: {payload['dataset_id']}")
print(f"  sequence  : {payload['sequence']}")
print(f"  source_type: {payload['source_type']}")
print(f"  timestamp : {payload['timestamp']}")
print(f"  features  : rms={payload['features']['rms']:.5f}")

# ── Step 3: Subscribe to bearing-telemetry BEFORE publishing ───────────────
print()
print("Step 3: Subscribe to bearing-telemetry")
group    = f"task2-{uuid.uuid4().hex[:8]}"
consumer = KafkaConsumer(
    BEARING_TOPIC,
    bootstrap_servers=KAFKA_HOST,
    group_id=group,
    auto_offset_reset="latest",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    consumer_timeout_ms=20000,
)
# Wait for partition assignment
deadline = time.time() + 15
while time.time() < deadline:
    consumer.poll(timeout_ms=300)
    if consumer.assignment():
        break
assert consumer.assignment(), "Consumer never got partition assignment"
print(f"  Partition assigned: {consumer.assignment()}")

# ── Step 4: Publish via MQTT ───────────────────────────────────────────────
print()
print("Step 4: Publish via MQTT → bridge → Kafka")
import paho.mqtt.client as mqtt
try:
    from paho.mqtt.enums import CallbackAPIVersion
    pub = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION1,
                      client_id=f"task2-pub-{uuid.uuid4().hex[:8]}")
except ImportError:
    pub = mqtt.Client(client_id=f"task2-pub-{uuid.uuid4().hex[:8]}")

pub.connect(MQTT_HOST, MQTT_PORT, keepalive=10)
pub.loop_start()
time.sleep(1.0)

result = pub.publish(
    "pms/telemetry/ims",
    json.dumps(payload).encode("utf-8"),
    qos=1,
)
result.wait_for_publish(timeout=10)
print(f"  MQTT publish confirmed (qos=1): {result.is_published()}")
time.sleep(2.0)
pub.loop_stop()
pub.disconnect()

# ── Step 5: Consume from bearing-telemetry ────────────────────────────────
print()
print("Step 5: Consume from bearing-telemetry")
received = []
for msg in consumer:
    if msg.value.get("run_id") == run_id:
        received.append(msg.value)
        break
consumer.close()

assert len(received) == 1, f"Expected 1 message, got {len(received)}"
m = received[0]
print(f"  Message received at offset: {msg.offset}")

# ── Step 6: Field-by-field comparison ─────────────────────────────────────
print()
print("Step 6: Compare original event vs received")
checks = [
    ("event_id",    payload["event_id"],    m.get("event_id")),
    ("run_id",      payload["run_id"],       m.get("run_id")),
    ("asset_id",    payload["asset_id"],     m.get("asset_id")),
    ("dataset_id",  payload["dataset_id"],   m.get("dataset_id")),
    ("sequence",    payload["sequence"],     m.get("sequence")),
    ("source_type", payload["source_type"],  m.get("source_type")),
    ("bearing_name",payload["bearing_name"], m.get("bearing_name")),
]
all_ok = True
for field, expected, actual in checks:
    ok = expected == actual
    all_ok = all_ok and ok
    status = "PASS" if ok else "FAIL"
    print(f"  {status}  {field}: expected={expected!r}  got={actual!r}")

# timestamp is re-serialised so just verify it's present and parseable
from datetime import datetime
ts = m.get("timestamp", "")
try:
    datetime.fromisoformat(ts.replace("Z", "+00:00"))
    print(f"  PASS  timestamp parseable: {ts}")
except Exception as e:
    print(f"  FAIL  timestamp: {e}")
    all_ok = False

# features dict preserved
feat_ok = abs(m["features"]["rms"] - features["rms"]) < 1e-5
print(f"  {'PASS' if feat_ok else 'FAIL'}  features.rms preserved: {m['features']['rms']:.5f}")
all_ok = all_ok and feat_ok

print()
print("TASK 2:", "PASS" if all_ok else "FAIL")
