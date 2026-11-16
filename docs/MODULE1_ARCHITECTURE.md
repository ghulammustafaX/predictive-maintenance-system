# Module 1 — IoT Simulation: Architecture

## Overview

Module 1 reads sensor datasets from disk and publishes each record as a
structured telemetry event.  It is the head of the entire PMS data pipeline.
No downstream module runs without it.

Two dataset families are supported:

| Family | Files | Purpose |
|---|---|---|
| NASA C-MAPSS | `train_FD001-4.txt`, `test_FD001-4.txt`, `RUL_FD001-4.txt` | Turbofan run-to-failure sequences, used for RUL prediction |
| IMS Bearing | `1st_test.rar`, `2nd_test.rar`, `3rd_test.rar` | Vibration snapshots, used for bearing anomaly detection |

---

## Source files

```
simulator/src/
  cmapss_loader.py     # C-MAPSS loader + SimulationController (entry point)
  ims_loader.py        # IMS archive reader + feature extractor
  event_schema.py      # Canonical envelope, event IDs, SequenceTracker
  kafka_producer.py    # Reusable KafkaProducerClient
  mqtt_publisher.py    # MQTTPublisher for paho-mqtt 1.x/2.x
  simulate_data.py     # Synthetic generator (dev/test aid)
```

---

## Message schema

Every event carries a full metadata envelope regardless of dataset:

| Field | Type | Description |
|---|---|---|
| `schema_version` | string | Currently `"1.0.0"` |
| `event_id` | string | Deterministic SHA-256 derived UUID — stable across retries |
| `run_id` | string | Random hex ID, fresh for each replay pass |
| `dataset_id` | string | `FD001`–`FD004` or `IMS_2nd_test`, `SYNTHETIC` |
| `asset_id` | string | `engine_N` or `2nd_test_bearing_N` |
| `timestamp` | ISO-8601 | UTC wall-clock at time of publication |
| `sequence` | int | Monotonically increasing global counter across all assets in one replay |
| `source_type` | string | `cmapss` \| `ims` \| `synthetic` |
| `source_split` | string | `train` \| `test` \| `live` |

**C-MAPSS events** additionally carry: `unit_id`, `time_cycles`,
`operational_setting_1–3`, `sensor_1–21`, `rul` (training labels only).

**IMS events** additionally carry: `bearing_name`, `acquisition_timestamp`,
`sampling_hz`, `features` (dict of time/freq domain statistics),
`test_name`, `snapshot_file`.

---

## Publish transports

`PUBLISH_TRANSPORT` selects the delivery path:

| Value | Behaviour |
|---|---|
| `kafka` (default) | Direct to Kafka via `KafkaProducerClient` |
| `mqtt` | Publish to Mosquitto; `mqtt-bridge` forwards to Kafka |
| `both` | Dual-publish (testing only) |

### Direct Kafka path
```
Simulator ──→ Kafka: sensor-telemetry  ──→ pms-preprocessing
                                        └─→ pms-consumer (InfluxDB writer)
         ──→ Kafka: bearing-telemetry  ──→ (anomaly detection, future)
```

### MQTT path (full architecture)
```
Simulator ──MQTT──→ Mosquitto ──→ mqtt-bridge ──→ Kafka: sensor-telemetry
                                               └──→ Kafka: bearing-telemetry
```

---

## Replay modes

| Mode | Behaviour |
|---|---|
| `single` | Publish dataset once, exit |
| `continuous` | Repeat indefinitely; new `run_id` per pass |

`MAX_REPLAY_CYCLES=N` caps continuous mode at N passes.
`REPLAY_PAUSE_SECONDS` sleeps between passes (default 1 s).

---

## Event identity and ordering

- **Deterministic `event_id`**: `SHA-256(run_id:asset_id:sequence)` — identical on retry, different on new replay.
- **Global `sequence`**: monotonically increases across all engines in one run, never resets mid-replay.
- **Kafka key**: `{dataset_id}:{unit_id}:{run_id}` — routes all messages for the same engine to the same partition, preserving order within the partition.
- **`SequenceTracker`**: detects gaps, duplicates, and out-of-order arrivals. Available to consumers via `event_schema.SequenceTracker`.

---

## RUL handling

**Training data**: `assign_rul_labels()` applies piecewise-linear targets:
`RUL = min(cycles_to_failure, RUL_CAP)`.  First cycles of long-lived engines
are capped at `RUL_CAP=125`.  Last cycle of every engine is 0.

**Test data**: `attach_test_rul()` reconstructs RUL at every test cycle using
the ground-truth file: `RUL[cycle] = RUL_ground_truth + (last_cycle - cycle)`.
The `rul` field in test payloads is for **offline evaluation only** — it is
not passed to the LSTM as an input feature.

---

## IMS feature extraction

Each IMS snapshot (1 second of vibration at 20 480 Hz) produces 13 features
per bearing channel:

`rms`, `mean`, `std`, `peak_pos`, `peak_neg`, `peak_to_peak`,
`crest_factor`, `kurtosis`, `dominant_freq_hz`, `spectral_kurtosis`,
`energy_2k_5k_hz`, `energy_5k_10k_hz`, `total_energy`

These features are designed for bearing degradation detection, not for the
LSTM RUL model (which expects C-MAPSS normalised sensor windows).

---

## Connection to Module 2

The preprocessing service (`pms-preprocessing`) consumes `sensor-telemetry`
and expects every message to carry `run_id`, `dataset_id`, `unit_id`,
`time_cycles`, and 21 sensor fields.  The updated Module 1 schema is a
strict superset of this — all existing downstream consumers continue to work.

The `mqtt-bridge` service in `ingestion/src/mqtt_bridge.py` handles the
MQTT → Kafka routing when `PUBLISH_TRANSPORT=mqtt`.
