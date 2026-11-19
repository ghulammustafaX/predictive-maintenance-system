# Module 01 — IoT Simulation: Learning Guide

> This document is the source content for the published Notion guide at:
> https://app.notion.com/p/ghulammustafa/Module-01-IoT-Simulation-41a6c3c1abf18095839fe060beb29214
>
> It is written for beginners, students, supervisors and new project contributors.

---

## What is Module 01?

Module 01 is the **starting point of the entire Predictive Maintenance System (PMS)**.

Before a machine learning model can predict when an engine will fail, it needs data.
Module 01 provides that data by reading real NASA turbofan sensor recordings and
streaming them one-by-one into the system — just like a live IoT sensor would.

Think of it as a **tape player for sensor data**: it reads historical recordings and
plays them back at a controlled speed so the rest of the pipeline can process each
reading as if it just arrived from a real engine.

---

## Why do we need a simulator?

Real aircraft engines are expensive and dangerous to test with.  NASA collected sensor
data from actual turbofan engines running to failure in a lab.  We replay that data
through the system instead of connecting to a real engine.

This gives us several advantages:

- We can run the whole system on a laptop.
- We can replay the data as many times as we want to test different parts of the system.
- We can slow down or speed up the replay to match the system's processing capacity.
- We can eventually swap the simulator for real IoT sensors without changing anything
  else in the pipeline — the message format stays the same.

---

## The two datasets

### Dataset 1: NASA C-MAPSS turbofan engines

**What it is:** Records from simulated turbofan aircraft engines running until they fail.

**Where it comes from:** NASA's Prognostics Center of Excellence.
[Download it here](https://data.nasa.gov/dataset/C-MAPSS-Aircraft-Engine-Simulator-Data/xaut-bemq).

**What is in each row:** One snapshot of an engine at one point in time.

| Column | Meaning |
|---|---|
| `unit_id` | Which engine (1, 2, 3, ...) |
| `time_cycles` | How many cycles the engine has run so far |
| `operational_setting_1–3` | Operating conditions (altitude, speed, etc.) |
| `sensor_1–21` | 21 physical sensor readings (temperature, pressure, speed, ...) |

**Four sub-datasets:** FD001, FD002, FD003, FD004.  Each has different operating
conditions and different failure modes.

| Dataset | Training engines | Conditions | Fault modes |
|---|---|---|---|
| FD001 | 100 | 1 (sea level) | 1 (HPC degradation) |
| FD002 | 260 | 6 | 1 |
| FD003 | 100 | 1 | 2 |
| FD004 | 249 | 6 | 2 |

**What is RUL?**  
RUL stands for **Remaining Useful Life** — the number of cycles an engine has left
before it fails.  For training data, we calculate this from the last known cycle:
`RUL = min(cycles_remaining, 125)`.  For test data, NASA provides the ground-truth
RUL value in a separate file (`RUL_FD001.txt` etc.).  The RUL is included in the
message for evaluation purposes but is **never used as an input to the model**.

---

### Dataset 2: IMS bearing vibration

**What it is:** High-frequency vibration recordings from rotating bearings running to failure.

**Where it comes from:** University of Cincinnati's Center for Intelligent Maintenance Systems (IMS).

**What is in each file:** One second of vibration data, sampled at 20,480 Hz.

| Archive | Snapshots | Channels |
|---|---|---|
| `1st_test.rar` | 2,156 files | 8 (4 bearings × 2 channels each) |
| `2nd_test.rar` | 984 files | 4 (4 bearings × 1 channel each) |
| `3rd_test.rar` | 6,324 files | 4 |

Because each file is 1 second of data at 20 kHz, the simulator **extracts 13 features**
from each file rather than streaming 20,480 raw readings per channel:

| Feature | What it measures |
|---|---|
| `rms` | Energy level (Root Mean Square) |
| `mean` | Average value |
| `std` | Variability |
| `peak_pos` / `peak_neg` | Highest and lowest values |
| `peak_to_peak` | Full range |
| `crest_factor` | Ratio of peak to RMS (sensitive to impacts) |
| `kurtosis` | Sharpness of peaks (key early-fault indicator) |
| `dominant_freq_hz` | The strongest vibration frequency |
| `spectral_kurtosis` | Kurtosis in the frequency domain |
| `energy_2k_5k_hz` | Energy in 2–5 kHz band (bearing fault zone) |
| `energy_5k_10k_hz` | Energy in 5–10 kHz band |
| `total_energy` | Total spectral energy |

---

## How sensor readings become JSON events

Every row of sensor data is wrapped in a **metadata envelope** before it is published.
The envelope identifies where the message came from, which replay it belongs to, and
whether it has been seen before.

Here is what a C-MAPSS event looks like:

```json
{
  "schema_version": "1.0.0",
  "event_id":       "3291930e-418a-63f5-0e55-dbaf37651bb9",
  "run_id":         "482d877957d648d0abe4e2755837c87e",
  "dataset_id":     "FD001",
  "asset_id":       "engine_1",
  "timestamp":      "2026-11-18T10:01:07.945596+00:00",
  "sequence":       1,
  "source_type":    "cmapss",
  "source_split":   "train",
  "unit_id":        1,
  "time_cycles":    1,
  "operational_setting_1": -0.0007,
  "sensor_1": 518.67,
  "...":      "...",
  "sensor_21": 23.419,
  "rul": 125.0
}
```

| Field | Purpose |
|---|---|
| `schema_version` | Tells consumers which format to expect |
| `event_id` | Uniquely identifies this exact event — stays the same if the message is re-sent |
| `run_id` | Identifies one complete replay pass — a new ID is generated for each replay |
| `dataset_id` | Which dataset the data comes from (`FD001`–`FD004`) |
| `asset_id` | Which engine (`engine_1`, `engine_2`, ...) |
| `timestamp` | When the simulator published this event (UTC) |
| `sequence` | A counter that increases with every event in one replay pass |
| `source_type` | `cmapss`, `ims`, or `synthetic` |
| `source_split` | `train` or `test` — tells consumers what kind of data this is |
| `unit_id` | Engine number (integer) |
| `time_cycles` | How many operating cycles this engine has completed |
| `sensor_1–21` | The actual sensor measurements |
| `rul` | Remaining Useful Life — for reference only, not used by the model |

---

## MQTT publishing

MQTT is a lightweight messaging protocol used by IoT devices.  Module 01 can publish
to an MQTT broker (Mosquitto) instead of Kafka directly.

```
Simulator  →  MQTT topic: pms/telemetry/cmapss  →  Mosquitto broker
Simulator  →  MQTT topic: pms/telemetry/ims     →  Mosquitto broker
```

This is controlled by the `PUBLISH_TRANSPORT` setting:
- `kafka` — write directly to Kafka (default, faster)
- `mqtt` — publish to Mosquitto first
- `both` — send to both (for testing)

---

## Kafka topics and producers

Apache Kafka is the central message bus for the whole system.  Every message published
by Module 01 ends up in one of these topics:

| Topic | Contains |
|---|---|
| `sensor-telemetry` | C-MAPSS turbofan sensor readings |
| `bearing-telemetry` | IMS bearing vibration features |
| `sensor-telemetry-dead-letter` | Messages that could not be processed |

The **reusable Kafka producer** (`simulator/src/kafka_producer.py`) handles all Kafka
communication.  It uses:
- `acks='all'` — the broker confirms every message before the simulator moves on
- Idempotent delivery — Kafka assigns sequence numbers to prevent duplicate writes
- Automatic retry — if the broker is temporarily unavailable, it retries up to 30 times

The **message key** (`FD001:1:run_id`) ensures all events for the same engine go to the
same Kafka partition, so they always arrive in order.

---

## How the MQTT-to-Kafka bridge works

When the simulator publishes to MQTT, a separate bridge service
(`ingestion/src/mqtt_bridge.py`) reads from Mosquitto and forwards messages to Kafka:

```
Mosquitto (pms/telemetry/cmapss) → Bridge → Kafka (sensor-telemetry)
Mosquitto (pms/telemetry/ims)    → Bridge → Kafka (bearing-telemetry)
Mosquitto (invalid message)       → Bridge → Kafka (sensor-telemetry-dead-letter)
```

The bridge validates each message.  If a required field is missing, the message goes to
the dead-letter topic instead of being silently lost.

---

## Single and continuous replay

**Single replay** (`REPLAY_MODE=single`): the simulator reads the entire dataset once,
publishes all rows, and exits cleanly.

**Continuous replay** (`REPLAY_MODE=continuous`): the simulator replays the dataset
in a loop.  Each pass gets a new `run_id` so downstream services can tell them apart.
You can limit the number of passes with `MAX_REPLAY_CYCLES=3`.

```
Replay 1  run_id=abc...  → publishes all rows
Replay 2  run_id=def...  → publishes all rows again
Replay 3  run_id=ghi...  → publishes all rows again
```

---

## Duplicate detection and sequence tracking

Every event carries a **sequence number** that counts up through the entire replay.
When a consumer receives events, it can use the `SequenceTracker` class to check
the ordering:

| What happened | Classification |
|---|---|
| Event arrived in order | `ok` |
| Same `event_id` received again (retry) | `duplicate` |
| Different event at an already-used sequence | `sequence_conflict` |
| Event arrived behind the current position | `out_of_order:N` |
| A sequence number was skipped | `gap:N` |

**Important:** The tracker only classifies events — it does not automatically discard
them.  Consumers decide what to do with each classification.

The tracker keeps a **bounded memory** of event IDs (up to 2,048 per engine per run)
so it can tell a retry apart from a genuinely new event arriving late.

**Limitation:** The tracker's memory is lost when the service restarts.  After a
restart, previously-seen events will be re-accepted.  This is documented behaviour.

---

## How preprocessing and ingestion consume Module 01 events

Two services read from `sensor-telemetry`:

**Ingestion consumer** (`ingestion/src/kafka_consumer.py`):
- Reads every raw event
- Writes it to InfluxDB under the measurement `sensor_telemetry`
- Each field (`sensor_1`–`sensor_21`, `operational_setting_1–3`) becomes an InfluxDB field
- Tags include `dataset_id`, `unit_id`, `run_id`, `cycle`
- The event's `timestamp` is used as the InfluxDB point time (idempotent writes)

**Preprocessing service** (`ml/streaming/preprocessing.py`):
- Reads every raw event
- Applies Min-Max normalisation using scalers fitted on training data
- Assembles a 30-cycle sliding window per engine
- Publishes the normalised feature window to `preprocessed-telemetry`

> The preprocessing service does **not** use the `rul` field from the payload.
> RUL labels are structurally excluded — the feature pipeline only uses the 24
> sensor and operating-setting columns.

---

## Testing

Module 01 has 197 automated tests across 10 test files.

| Test file | What it tests |
|---|---|
| `test_cmapss_loader.py` (42 tests) | Dataset loading, column schema, cycle ordering, RUL labels |
| `test_phase2_sequence_classification.py` (31 tests) | All duplicate/gap/OOO scenarios |
| `test_phase3_preprocessing_schema.py` (20 tests) | Preprocessing compatibility, RUL isolation |
| `test_event_schema.py` (18 tests) | Envelope fields, event ID determinism |
| `test_simulate_data.py` (15 tests) | Synthetic data metadata |
| `test_simulation_controller.py` (13 tests) | Single/continuous replay, SIGTERM, sequence |
| `test_sequence_validation.py` (12 tests) | Sequence tracker edge cases |
| `test_kafka_producer.py` (9 tests) | Producer config, delivery, error handling |
| `test_mqtt_publisher.py` (8 tests) | MQTT connection, QoS, error paths |
| `test_ims_loader.py` (23 tests, 2 skipped) | Archive config, features, payload schema |

**Integration tests** (live Kafka + MQTT + InfluxDB in isolated Docker stack):
- `tests/test_m1_integration.py` — 15 tests verifying the full message path
- `gap_tests/task_defect2_preprocessing_influxdb.py` — real preprocessing pipeline

---

## Verified features

These have been tested against actual running services:

✅ FD001–FD004 datasets load correctly, all 197 automated tests pass  
✅ IMS 1st_test (8 channels) and 2nd_test (4 channels) extract correctly in Docker  
✅ MQTT → Mosquitto → bridge → Kafka round-trip with field preservation  
✅ C-MAPSS event_id, run_id, asset_id preserved end-to-end  
✅ Continuous replay produces new run_id per pass, 1,437 messages in 3 passes  
✅ Duplicate detection correctly classifies retries as `duplicate`  
✅ Preprocessing service accepts new envelope without modification  
✅ All 21 sensor fields stored in isolated InfluxDB with correct values  
✅ Production Kafka offsets unchanged throughout all tests (20,671)

---

## Known limitations

- The duplicate detection memory resets when the service restarts.
- The seen-set holds up to 2,048 event IDs per engine per run; older IDs are evicted.
- The preprocessing service strips `event_id` and `sequence` from its output — these are
  not propagated to the prediction service.
- Two IMS snapshot-discovery tests are skipped on Windows (need `unrar` on PATH).
- The `bearing-telemetry` topic is created but no downstream consumer is implemented yet.
- InfluxDB point counts at query time may be slightly lower than published counts due to
  consumer catch-up latency — no data is actually lost.

---

## Glossary

| Term | Meaning |
|---|---|
| **C-MAPSS** | Commercial Modular Aero-Propulsion System Simulation — NASA's turbofan simulation framework |
| **RUL** | Remaining Useful Life — cycles before an engine is expected to fail |
| **IMS** | Intelligent Maintenance Systems — bearings run to failure at the University of Cincinnati |
| **Kafka** | Apache Kafka — a distributed message broker used to decouple services |
| **MQTT** | Message Queuing Telemetry Transport — a lightweight IoT messaging protocol |
| **Mosquitto** | Eclipse Mosquitto — an open-source MQTT broker |
| **InfluxDB** | A time-series database optimised for high-frequency sensor data |
| **event_id** | A deterministic identifier for one specific event — same value if the message is retried |
| **run_id** | A random identifier for one complete replay pass |
| **sequence** | A counter that increases with every event in one replay pass |
| **SequenceTracker** | A class that checks whether arriving events are in order |
| **dead-letter topic** | A Kafka topic that receives messages that could not be processed |
| **envelope** | The metadata fields added to every message (event_id, run_id, timestamp, etc.) |
| **snapshot** | One IMS file — one second of vibration data at 20,480 Hz |

---

## Links

| Resource | URL |
|---|---|
| This module's baseline document | [docs/MODULE1_BASELINE.md](MODULE1_BASELINE.md) |
| Architecture document | [docs/MODULE1_ARCHITECTURE.md](MODULE1_ARCHITECTURE.md) |
| Verification report | [docs/MODULE1_FINAL_VERIFICATION_REPORT.md](MODULE1_FINAL_VERIFICATION_REPORT.md) |
| Environment variables | [docs/ENVIRONMENT_VARIABLES.md](ENVIRONMENT_VARIABLES.md) |
| GitHub repository | https://github.com/ghulammustafaX/predictive-maintenance-system |
| NASA C-MAPSS dataset | https://data.nasa.gov/dataset/C-MAPSS-Aircraft-Engine-Simulator-Data/xaut-bemq |
