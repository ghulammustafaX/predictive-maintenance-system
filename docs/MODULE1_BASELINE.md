# Module 01 — Verified Baseline

**Baseline version:** 1.0  
**Date:** 2026-11-18  
**Verified commit:** `256faefd914aadef7dd6e07077287a8ffd3cb06c` (`256faef`)  
**Branch:** `main`  
**Test result:** 197 passed · 2 skipped · 0 failed (run against HEAD `256faef`)

---

## 1. What this document is

This document freezes the verified behaviour of Module 01 (IoT Simulation) as an
integration baseline for the rest of the PMS pipeline.  Downstream modules (02–12)
may rely on the interfaces described here.  Changes to Module 01 must follow the
[change-control policy](#8-change-control-policy) at the bottom of this file.

---

## 2. Supported features

### C-MAPSS turbofan simulation

| Feature | Status |
|---|---|
| Load `train_FD001–4.txt` (training sequences) | ✅ Verified |
| Load `test_FD001–4.txt` (truncated test sequences) | ✅ Verified |
| Load `RUL_FD001–4.txt` (ground-truth RUL per engine) | ✅ Verified |
| Piecewise-linear RUL labels on training data (`RUL_CAP=125`) | ✅ Verified |
| RUL reference attached to test payloads (evaluation-only, not leaked to inference) | ✅ Verified |
| 100 engines in FD001/FD003, 260 in FD002, 249 in FD004 | ✅ Verified |
| All 21 sensor columns + 3 operational settings per row | ✅ Verified |
| Zero NaN values in any of the four datasets | ✅ Verified |
| Cycles start at 1 and are strictly sequential per engine | ✅ Verified |

### IMS bearing simulation

| Feature | Status |
|---|---|
| `2nd_test.rar` — 984 snapshots, 4 channels (1 per bearing) | ✅ Verified |
| `3rd_test.rar` — 6,324 snapshots, 4 channels | ✅ Verified |
| `1st_test.rar` — 2,156 snapshots, 8 channels (2 per bearing) | ✅ Verified in Docker |
| Acquisition timestamps reconstructed from filenames | ✅ Verified |
| 20,480 rows × 4 or 8 columns per snapshot | ✅ Verified |
| 13 features extracted per bearing channel | ✅ Verified |
| Extraction uses `chdir(tmpdir)` workaround for `unrar-free` | ✅ Verified |

**Extracted features (13 per channel):**  
`rms`, `mean`, `std`, `peak_pos`, `peak_neg`, `peak_to_peak`,
`crest_factor`, `kurtosis`, `dominant_freq_hz`, `spectral_kurtosis`,
`energy_2k_5k_hz`, `energy_5k_10k_hz`, `total_energy`

### Simulation control

| Feature | Status |
|---|---|
| Single replay (publish once, exit cleanly) | ✅ Verified |
| Continuous replay with configurable pass count (`MAX_REPLAY_CYCLES`) | ✅ Verified |
| New `run_id` generated per replay pass | ✅ Verified |
| Configurable stream interval (`STREAM_INTERVAL`) | ✅ Verified |
| Configurable pause between replays (`REPLAY_PAUSE_SECONDS`) | ✅ Verified |
| Graceful `SIGTERM` and `KeyboardInterrupt` shutdown | ✅ Verified |

### Publishing transports

| Feature | Status |
|---|---|
| Direct Kafka via `KafkaProducerClient` | ✅ Verified |
| MQTT publish via `MQTTPublisher` (paho-mqtt 1.x/2.x compatible) | ✅ Verified |
| MQTT-to-Kafka bridge (`ingestion/src/mqtt_bridge.py`) | ✅ Verified |
| C-MAPSS events routed to `sensor-telemetry` | ✅ Verified |
| IMS events routed to `bearing-telemetry` | ✅ Verified |
| Malformed messages routed to `sensor-telemetry-dead-letter` | ✅ Verified |

### Event identity and ordering

| Feature | Status |
|---|---|
| Deterministic `event_id` = `SHA-256(run_id:asset_id:sequence)` | ✅ Verified |
| `event_id` stable across delivery retries | ✅ Verified |
| Global monotonic `sequence` counter per replay | ✅ Verified |
| `SequenceTracker` — detects `ok`, `gap:N`, `duplicate`, `out_of_order:N`, `sequence_conflict` | ✅ Verified |
| `event_id`-based deduplication (bounded LRU seen-set, 2,048 entries) | ✅ Verified |
| Independent tracking per `(run_id, asset_id)` | ✅ Verified |

### Kafka producer

| Feature | Status |
|---|---|
| `acks='all'` | ✅ Verified |
| `enable_idempotence=True`, `max_in_flight=1` | ✅ Verified |
| Synchronous delivery confirmation per message | ✅ Verified |
| Exponential-capped retry on broker connection (up to 30 attempts) | ✅ Verified |
| Graceful `flush()` + `close()` on shutdown | ✅ Verified |

### Downstream integration

| Feature | Status |
|---|---|
| Preprocessing service (`ml/streaming/preprocessing.py`) accepts new envelope | ✅ Verified |
| `dataset_id`, `unit_id`, `run_id`, `time_cycles` correctly parsed | ✅ Verified |
| All 21 sensor fields + 3 op settings stored in isolated InfluxDB | ✅ Verified |
| `sensor_1` value for FD001 engine_1 cycle_1 = `518.67` | ✅ Verified |
| Measurement name `sensor_telemetry` in InfluxDB | ✅ Verified |
| `preprocessed-telemetry` produced (10,603 events from 20,631 inputs) | ✅ Verified |

---

## 3. Public interfaces

### 3.1 C-MAPSS event schema

Every C-MAPSS message on `sensor-telemetry` carries:

```json
{
  "schema_version":        "1.0.0",
  "event_id":              "3291930e-418a-63f5-0e55-dbaf37651bb9",
  "run_id":                "482d877957d648d0abe4e2755837c87e",
  "dataset_id":            "FD001",
  "asset_id":              "engine_1",
  "timestamp":             "2026-11-18T10:01:07.945596+00:00",
  "sequence":              1,
  "source_type":           "cmapss",
  "source_split":          "train",
  "unit_id":               1,
  "time_cycles":           1,
  "operational_setting_1": -0.0007,
  "operational_setting_2": -0.0004,
  "operational_setting_3": 100.0,
  "sensor_1":              518.67,
  "...":                   "...",
  "sensor_21":             23.419,
  "rul":                   125.0
}
```

> `rul` is **reference-only** on training/test payloads. It is never consumed by the
> inference pipeline. The `source_split` field (`train`/`test`) distinguishes the two.

### 3.2 IMS bearing event schema

Every IMS message on `bearing-telemetry` carries:

```json
{
  "schema_version":        "1.0.0",
  "event_id":              "d2232936-b42b-f93f-67bf-fa93caa04daf",
  "run_id":                "fe07451dd52d44f79d7e26e1596113d5",
  "dataset_id":            "IMS_2nd_test",
  "asset_id":              "2nd_test_bearing_1",
  "timestamp":             "2026-11-18T07:06:30.123904+00:00",
  "sequence":              1,
  "source_type":           "ims",
  "source_split":          "test",
  "test_name":             "2nd_test",
  "snapshot_file":         "2004.02.12.10.32.39",
  "bearing_name":          "bearing_1",
  "acquisition_timestamp": "2004-02-12T10:32:39+00:00",
  "timestamp_source":      "filename",
  "sampling_hz":           20480,
  "samples_per_snapshot":  20480,
  "features": {
    "rms": 0.07418, "kurtosis": 3.629, "dominant_freq_hz": 1009.0,
    "...": "..."
  },
  "n_samples":             20480
}
```

### 3.3 Kafka topics

| Topic | Producer | Consumers | Key format |
|---|---|---|---|
| `sensor-telemetry` | Simulator or MQTT bridge | Preprocessing, Ingestion consumer | `{dataset_id}:{unit_id}:{run_id}` |
| `bearing-telemetry` | Simulator or MQTT bridge | (future anomaly detection) | `{dataset_id}:{bearing_name}:{run_id}` |
| `sensor-telemetry-dead-letter` | MQTT bridge (on invalid messages) | Manual inspection | None |
| `preprocessed-telemetry` | Preprocessing service | Prediction service | — |

### 3.4 MQTT topics

| Topic | Dataset | QoS |
|---|---|---|
| `pms/telemetry/cmapss` | C-MAPSS turbofan data | 1 |
| `pms/telemetry/ims` | IMS bearing data | 1 |

### 3.5 Event identity rules

1. `event_id` = `SHA-256(run_id + ":" + asset_id + ":" + sequence)` — always deterministic.
2. Same `(run_id, asset_id, sequence)` triple → same `event_id` → safe to retry delivery.
3. New replay pass → new `run_id` → new `event_id` for every event in that pass.
4. `sequence` increments globally across all engines in a single replay pass.
5. Kafka message key = `{dataset_id}:{unit_id}:{run_id}` — same engine always goes to same partition.

### 3.6 Timestamp semantics

| Field | Source | Description |
|---|---|---|
| `timestamp` | Publication wall-clock (UTC) | When the simulator published the event |
| `acquisition_timestamp` | IMS filename only | When the vibration measurement was recorded (reconstructed) |
| `timestamp_source` | `"filename"` | Marks reconstructed timestamps |

### 3.7 SequenceTracker classification

```
check(run_id, asset_id, received_seq, event_id=None)
  → (True,  "ok")                    normal forward delivery
  → (False, "duplicate")             same event_id seen before, or seq == last (no eid)
  → (False, "sequence_conflict")     different event_id at already-used sequence
  → (False, "out_of_order:N")        event not seen before, arrived N behind last
  → (False, "gap:N")                 N sequences were skipped above last
```

**Classification is diagnostic only.** The tracker returns a tuple; it never discards
an event or raises.

---

## 4. Dependencies and configuration

### Docker services required

| Service | Image | Purpose |
|---|---|---|
| `pms-kafka` | `confluentinc/cp-kafka:7.4.0` | Message broker |
| `pms-zookeeper` | `confluentinc/cp-zookeeper:7.4.0` | Kafka coordination |
| `pms-mosquitto` | `eclipse-mosquitto:2.0` | MQTT broker (MQTT transport only) |

### Python dependencies (`simulator/requirements.txt`)

```
kafka-python>=2.0.2
pandas>=2.2.3
numpy>=2.1.0
python-dotenv>=1.0.0
paho-mqtt>=1.6.1
rarfile>=4.0
scipy>=1.13.0
```

### Key environment variables

See [`docs/ENVIRONMENT_VARIABLES.md`](ENVIRONMENT_VARIABLES.md) for the complete
reference.  Most-used variables:

| Variable | Default | Meaning |
|---|---|---|
| `DATASET_ID` | `FD001` | Which C-MAPSS sub-dataset to replay |
| `STREAM_INTERVAL` | `0.01` | Seconds between rows (≈100 rows/sec) |
| `REPLAY_MODE` | `single` | `single` or `continuous` |
| `PUBLISH_TRANSPORT` | `kafka` | `kafka`, `mqtt`, or `both` |

---

## 5. Automated verification evidence

```
Command:
  python -m pytest tests/test_event_schema.py tests/test_sequence_validation.py
    tests/test_kafka_producer.py tests/test_simulate_data.py
    tests/test_simulation_controller.py tests/test_cmapss_loader.py
    tests/test_ims_loader.py tests/test_mqtt_publisher.py
    tests/test_phase2_sequence_classification.py
    tests/test_phase3_preprocessing_schema.py
    -v --tb=short --no-header

Result (HEAD 256faef, 2026-11-18):
  197 passed, 2 skipped, 1 warning in 82.87s

Skipped (2): IMS archive snapshot-discovery tests require `unrar` on the
  host PATH. Both pass inside Docker where unrar-free is installed.
```

| Test file | PASS | SKIP | FAIL |
|---|---|---|---|
| test_event_schema.py | 18 | 0 | 0 |
| test_sequence_validation.py | 12 | 0 | 0 |
| test_kafka_producer.py | 9 | 0 | 0 |
| test_simulate_data.py | 15 | 0 | 0 |
| test_simulation_controller.py | 13 | 0 | 0 |
| test_cmapss_loader.py | 42 | 0 | 0 |
| test_ims_loader.py | 23 | **2** | 0 |
| test_mqtt_publisher.py | 8 | 0 | 0 |
| test_phase2_sequence_classification.py | 31 | 0 | 0 |
| test_phase3_preprocessing_schema.py | 20 | 0 | 0 |
| **TOTAL** | **191** | **2** | **0** |

Full verification evidence: [`docs/MODULE1_FINAL_VERIFICATION_REPORT.md`](MODULE1_FINAL_VERIFICATION_REPORT.md)

---

## 6. Known limitations

| ID | Limitation | Severity |
|---|---|---|
| L-1 | `SequenceTracker` seen-set is in-memory — resets on service restart | Medium |
| L-2 | seen-set is bounded at 2,048 entries per `(run_id, asset_id)` — oldest evicted when full | Low |
| L-3 | Preprocessing service strips `event_id`/`sequence` from its output — prediction cannot correlate back to the original event | Low |
| L-4 | IMS archive snapshot tests skip on Windows host (no `unrar` on PATH) | Low |
| L-5 | kafka-python 2.3.1 validates `enable_idempotence` config lazily at first broker contact | Info |
| L-6 | paho-mqtt 2.x `CallbackAPIVersion` DeprecationWarning in `mqtt_bridge.py` | Low |
| L-7 | `bearing-telemetry` downstream consumer not yet implemented (Modules 7+) | Planned |
| L-8 | InfluxDB point count at query time (20,218) was lower than published count (20,631) due to consumer catch-up latency — no data loss | Info |

---

## 7. Module 02 compatibility expectations

The preprocessing service (`pms-preprocessing`) and ingestion consumer (`pms-consumer`)
consume `sensor-telemetry` and require these fields in every message:

| Required field | Type | Notes |
|---|---|---|
| `dataset_id` | string | `FD001`–`FD004` |
| `unit_id` | int | Engine identifier |
| `run_id` | string | Replay pass identifier |
| `time_cycles` | int | Cycle number |
| `operational_setting_1–3` | float | Operating conditions |
| `sensor_1–21` | float | Sensor measurements |
| `timestamp` | ISO-8601 string | Used as InfluxDB point time |

The Module 01 envelope is a **strict superset** of this — all required fields are
present and no existing field names or types have changed.

New fields added by Module 01 (`event_id`, `asset_id`, `sequence`,
`schema_version`, `source_type`, `source_split`) are silently ignored by the
existing preprocessing and ingestion services.

---

## 8. Change-control policy

Module 01 is a **stable baseline** as of commit `256faef`.

Future changes should:

1. Be justified by a documented defect (`FAIL` in a test) or an approved requirement.
2. Preserve the event schema defined in §3 — adding fields is allowed; removing or
   renaming existing fields is a breaking change.
3. Include regression tests that cover the changed behaviour.
4. Update this document and `docs/MODULE1_ARCHITECTURE.md` when interfaces change.
5. Be reviewed for downstream impact on Modules 02–12 before merging.

Non-breaking additions (new features, new env vars, new test cases) do not require
a full re-verification cycle, but must not reduce the passing test count.
