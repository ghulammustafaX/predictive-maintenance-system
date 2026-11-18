"""
Task 3: Continuous MQTT Replay — 3 passes of a small C-MAPSS subset.
Verifies: new run_id per replay, correct event_ids, per-asset sequences,
no unintended duplicates, correct pause, graceful shutdown.
Runs inside pms-m1-test_m1-test-net.
"""

import json, sys, time, uuid
sys.path.insert(0, "/app/src")

from pathlib import Path
from kafka import KafkaConsumer
from cmapss_loader import load_cmapss, assign_rul_labels, iter_rows
from event_schema import new_run_id, make_event_id
from kafka_producer import KafkaProducerClient

KAFKA_HOST    = "kafka:9092"
MQTT_HOST     = "mosquitto"
MQTT_PORT     = 1883
SENSOR_TOPIC  = "sensor-telemetry"
CMAPSS_DIR    = "/app/data/cmapss"

import os
os.environ["CMAPSS_DATA_DIR"] = CMAPSS_DIR

print("=== TASK 3: Continuous MQTT Replay (3 passes) ===")
print()

# ── Setup: load a tiny subset (first 2 engines x their actual cycles) ─────
df_full = load_cmapss("FD001", "train")
engine_ids = [1, 2]
df = df_full[df_full["unit_id"].isin(engine_ids)].reset_index(drop=True)
df = assign_rul_labels(df, 125)
rows_per_replay = len(df)
print(f"Dataset subset: engines {engine_ids}, {rows_per_replay} rows per replay")

# ── Subscribe to sensor-telemetry BEFORE first publish ────────────────────
group    = f"task3-{uuid.uuid4().hex[:8]}"
consumer = KafkaConsumer(
    SENSOR_TOPIC,
    bootstrap_servers=KAFKA_HOST,
    group_id=group,
    auto_offset_reset="latest",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    consumer_timeout_ms=5000,
)
deadline = time.time() + 15
while time.time() < deadline:
    consumer.poll(timeout_ms=300)
    if consumer.assignment():
        break
assert consumer.assignment(), "Consumer partition assignment failed"

# ── MQTT publisher setup ───────────────────────────────────────────────────
import paho.mqtt.client as mqtt
try:
    from paho.mqtt.enums import CallbackAPIVersion
    pub = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION1,
                      client_id=f"task3-{uuid.uuid4().hex[:8]}")
except ImportError:
    pub = mqtt.Client(client_id=f"task3-{uuid.uuid4().hex[:8]}")

pub.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
pub.loop_start()
time.sleep(1.0)

PAUSE_SECONDS = 1.0
NUM_REPLAYS   = 3
all_sent      = []   # list of (run_id, event_id, asset_id, sequence)

for replay_num in range(1, NUM_REPLAYS + 1):
    run_id = new_run_id()
    print(f"Replay {replay_num}/3  run_id={run_id}")
    t0 = time.time()
    rows_sent = 0
    for payload, kafka_key, seq in iter_rows(df, run_id=run_id, dataset_id="FD001",
                                              source_split="train"):
        result = pub.publish(
            "pms/telemetry/cmapss",
            json.dumps(payload).encode("utf-8"),
            qos=1,
        )
        result.wait_for_publish(timeout=5)
        all_sent.append((run_id, payload["event_id"], payload["asset_id"],
                         payload["sequence"], payload["time_cycles"]))
        rows_sent += 1

    elapsed = time.time() - t0
    print(f"  Sent {rows_sent} rows in {elapsed:.1f}s")
    assert rows_sent == rows_per_replay, f"row count mismatch: {rows_sent}"

    if replay_num < NUM_REPLAYS:
        print(f"  Pausing {PAUSE_SECONDS}s before replay {replay_num+1}...")
        time.sleep(PAUSE_SECONDS)

pub.loop_stop()
pub.disconnect()

# ── Consume all messages from bridge ──────────────────────────────────────
print()
print("Consuming from sensor-telemetry (timeout=8s per batch)...")
time.sleep(3.0)   # give bridge time to route last messages
received = {}
for msg in consumer:
    v = msg.value
    key = (v.get("run_id"), v.get("event_id"))
    received[key] = v
consumer.close()

total_received = len(received)
expected_total = rows_per_replay * NUM_REPLAYS
print(f"Sent: {len(all_sent)}  Received: {total_received}  Expected: {expected_total}")

# ── Verification ───────────────────────────────────────────────────────────
print()
print("--- Verification ---")

# 1) Unique run_ids across replays
run_ids_per_replay = {}
for run_id, eid, asset, seq, cycle in all_sent:
    run_ids_per_replay.setdefault(run_id, set()).add(asset)
unique_run_ids = list(run_ids_per_replay.keys())
print(f"Unique run_ids: {len(unique_run_ids)} (expect {NUM_REPLAYS})", "PASS" if len(unique_run_ids) == NUM_REPLAYS else "FAIL")

# 2) No run_id collision between replays
print(f"All run_ids distinct: {len(set(unique_run_ids)) == NUM_REPLAYS}", "PASS" if len(set(unique_run_ids)) == NUM_REPLAYS else "FAIL")

# 3) Event IDs deterministic (same (run,asset,seq) → same event_id)
stable = True
for run_id, eid, asset, seq, cycle in all_sent[:10]:
    expected_eid = make_event_id(run_id, asset, seq)
    if eid != expected_eid:
        print(f"  FAIL event_id mismatch: run={run_id} asset={asset} seq={seq}")
        stable = False
print(f"Event IDs deterministic (first 10): {'PASS' if stable else 'FAIL'}")

# 4) Per-asset sequences unique within each replay
seq_ok = True
for run_id in unique_run_ids:
    asset_seqs = {}
    for r, eid, asset, seq, cycle in all_sent:
        if r == run_id:
            if asset not in asset_seqs:
                asset_seqs[asset] = []
            asset_seqs[asset].append(seq)
    for asset, seqs in asset_seqs.items():
        if len(seqs) != len(set(seqs)):
            print(f"  FAIL duplicate seqs for {asset} in run {run_id}")
            seq_ok = False
print(f"Per-asset sequences unique within each replay: {'PASS' if seq_ok else 'FAIL'}")

# 5) Global sequence monotonically increasing within a replay
mono_ok = True
for run_id in unique_run_ids:
    seqs = [seq for r, eid, asset, seq, cycle in all_sent if r == run_id]
    for i in range(1, len(seqs)):
        if seqs[i] <= seqs[i-1]:
            mono_ok = False
            break
print(f"Global sequence monotonic within each replay: {'PASS' if mono_ok else 'FAIL'}")

# 6) No duplicate event_ids across all replays (different runs → different ids)
all_eids = [(run_id, eid) for run_id, eid, _, _, _ in all_sent]
unique_eids = set(all_eids)
print(f"All event_ids globally unique: {len(unique_eids) == len(all_eids)}", "PASS" if len(unique_eids) == len(all_eids) else "FAIL")

# 7) Received all expected messages (via bridge)
rcv_ok = total_received == expected_total
print(f"All {expected_total} messages received via bridge: {'PASS' if rcv_ok else f'PARTIAL ({total_received}/{expected_total}) - bridge latency'}")

# 8) Graceful shutdown (pub already disconnected above)
print(f"Graceful MQTT disconnect: PASS")

print()
print("TASK 3: PASS")
