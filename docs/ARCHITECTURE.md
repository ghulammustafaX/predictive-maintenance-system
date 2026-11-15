# Architecture

This document describes the components of the Predictive Maintenance System, how data flows between them, and the reasoning behind key design choices.

---

## Components

### Simulator

**Location:** `simulator/`  
**Container:** `pms-simulator`

Reads the NASA C-MAPSS dataset files and streams rows to Kafka at a configurable interval (default 10ms, giving roughly 100 rows/second). Each row represents one sensor reading cycle for one engine. The simulator cycles through all engines in the dataset continuously, assigning a `run_id` to each pass so the prediction service can track which run a cycle belongs to.

The dataset contains four sub-datasets (FD001–FD004) with different operating conditions and fault modes. Each row includes 21 sensor readings, 3 operational settings, the engine unit number, and the cycle count.

---

### Kafka

**Container:** `pms-kafka` + `pms-zookeeper`  
**Topics:**

| Topic | Producer | Consumer |
|---|---|---|
| `sensor-telemetry` | Simulator | Preprocessing service, Consumer |
| `preprocessed-telemetry` | Preprocessing service | Prediction service |
| `rul-predictions` | Prediction service | (dashboard reads from InfluxDB) |
| `sensor-telemetry-dead-letter` | Consumer (on error) | Manual inspection |

Kafka decouples the simulator from the rest of the pipeline. If the prediction service or consumer falls behind, Kafka buffers the backlog without data loss. The dead-letter topic captures any messages the consumer could not parse or write, with a 7-day retention.

---

### Preprocessing Service

**Location:** `ml/streaming/`  
**Container:** `pms-preprocessing`

Consumes raw `sensor-telemetry` messages and applies:

1. **Per-sensor normalisation** using scalers fitted on training data (artefacts stored in `ml/saved_v2_a1/scalers/`). This avoids data leakage — the scaler parameters are fixed at training time, never updated on live data.
2. **Windowing** — assembles a fixed-length sliding window of recent cycles for each engine so the LSTM receives properly shaped input (sequence of N timesteps × 21 sensors).

Publishes normalised windows to `preprocessed-telemetry`.

---

### Prediction Service

**Location:** `ml/prediction/`  
**Container:** `pms-prediction`  
**Port:** 8001

Consumes `preprocessed-telemetry`. For each window it:

1. Runs the LSTM model (`.pt` checkpoint) to produce a RUL estimate in cycles.
2. Publishes the prediction to `rul-predictions`.
3. Writes the prediction directly to InfluxDB under the `rul_predictions` measurement so the dashboard can query it without consuming from Kafka.

Also exposes a REST endpoint at `:8001` for direct inference on demand.

The LSTM is a stacked two-layer model trained on the C-MAPSS training split. The target is piece-wise linear RUL — flat at the maximum value for healthy engines, then linearly decreasing as the engine approaches failure. This matches observed degradation behaviour better than a pure linear target.

---

### Consumer (Ingestion Service)

**Location:** `ingestion/`  
**Container:** `pms-consumer`

A second Kafka consumer on `sensor-telemetry` that writes each raw telemetry point to InfluxDB. This runs in parallel with preprocessing and stores the original unnormalised sensor values so the dashboard can display what the actual sensors reported.

Writes are batched for efficiency. On write failure the message is forwarded to the dead-letter topic.

---

### InfluxDB

**Container:** `pms-influxdb`  
**Port:** 8086  
**Measurements:**

| Measurement | Written by | Contains |
|---|---|---|
| `sensor_telemetry` | Consumer | Raw sensor readings per engine per cycle |
| `rul_predictions` | Prediction service | RUL estimate, cycle, run_id, model version |

Tags on `sensor_telemetry`: `dataset_id`, `unit_id`, `run_id`, `cycle`.

InfluxDB is used for time-series data because it handles high-frequency append-only writes efficiently, compresses sensor data well (~10× vs a row store), and its Flux query language makes windowed aggregation and downsampling straightforward.

---

### Backend API

**Location:** `backend/`  
**Container:** `pms-backend`  
**Port:** 8000

A FastAPI application with two concerns:

**Telemetry query layer** — reads from InfluxDB and returns fleet status, per-engine telemetry, and RUL history to the dashboard. Flux queries are kept narrow (single field, `keep()` before grouping) to stay within InfluxDB's query budget even with millions of stored points.

**Application layer** — user accounts, authentication, alerts, and anomaly records backed by PostgreSQL. All state changes (login, logout, alert acknowledgement) are written to an audit log table.

Routers:

| Router | Endpoints | Purpose |
|---|---|---|
| `auth` | `/api/auth/*` | Register, login, refresh, logout, Google OAuth, password reset |
| `fleet` | `/api/fleet/status`, `/api/engines/:id/telemetry` | Engine list and bounded telemetry |
| `rul` | `/api/engines/:id/rul/latest`, `/api/engines/:id/rul/history` | RUL predictions |
| `anomalies` | `/api/anomalies`, `/api/anomalies/detect` | Anomaly records and on-demand detection |
| `alerts` | `/api/alerts`, `/api/alerts/:id/acknowledge`, `/api/alerts/:id/resolve` | Alert lifecycle |

---

### PostgreSQL

**Container:** `pms-postgres`  
**Port:** 5433

Stores structured application data:

| Table | Purpose |
|---|---|
| `users` | Accounts, roles, auth provider, verification state |
| `email_verification_tokens` | One-time tokens for email confirmation |
| `password_reset_tokens` | One-time tokens for password recovery |
| `revoked_tokens` | JTI blacklist for logout/token revocation |
| `audit_logs` | Immutable record of every auth event |
| `anomaly_records` | Persisted anomaly detection events |
| `alerts` | Alert records with full lifecycle state |
| `alert_audit` | Immutable audit trail for every alert state change |

---

### Frontend

**Location:** `frontend/`  
**Served by:** Python `http.server` on port 5500 (static files, no build step)

Four pages:

| Page | Purpose |
|---|---|
| `authentication/login.html` | Email/password and Google Sign-In |
| `authentication/register.html` | New account creation with password strength meter |
| `dashboard/index.html` | Live operations — fleet status, sensor telemetry, RUL panel |
| `dashboard/alerts.html` | Alert centre — history, acknowledge, resolve |
| `dashboard/anomalies.html` | Anomaly events and on-demand detection trigger |
| `dashboard/users.html` | User management (admin only) |

`auth-shared.js` provides token storage, `authedFetch` with silent token refresh, and the `Auth.requireAuth()` route guard used by every protected page. The dashboard polls every 5 seconds. Health and fleet requests run on separate `AbortController` instances so a slow health probe cannot cancel the fleet request.

---

## Data flow end-to-end

```
1. Simulator reads one C-MAPSS row
2. Publishes to Kafka: sensor-telemetry
        ↓
3. Consumer writes raw row to InfluxDB: sensor_telemetry measurement
        ↓
4. Preprocessing service reads same row, normalises, builds window
5. Publishes to Kafka: preprocessed-telemetry
        ↓
6. Prediction service receives window, runs LSTM
7. Publishes RUL to Kafka: rul-predictions
8. Writes RUL to InfluxDB: rul_predictions measurement
        ↓
9. Browser polls FastAPI every 5 s
10. FastAPI queries InfluxDB, returns fleet + telemetry + RUL
11. Dashboard updates engine selector, sensor chart, RUL panel
```

---

## Design decisions

**Why two separate Kafka consumers for the same topic?**  
The consumer and the preprocessing service both read `sensor-telemetry` but serve different purposes. The consumer stores raw data for display; the preprocessor produces normalised windows for ML inference. Keeping them independent means the ingestion pipeline and the ML pipeline can be deployed, scaled, and restarted separately without coupling.

**Why InfluxDB for telemetry and PostgreSQL for application data?**  
InfluxDB is optimised for time-ordered append-only writes and time-range queries — exactly the access pattern for sensor data. PostgreSQL handles the relational concerns (user accounts, alert state machine, foreign keys, transactions) where a schema and ACID guarantees matter.

**Why not React or another frontend framework?**  
Vanilla JavaScript keeps the frontend self-contained with no build toolchain. For an FYP demonstrating backend and ML depth, the simpler frontend lets the interesting parts stay in focus.

**Why piece-wise linear RUL targets?**  
On the C-MAPSS dataset, treating every early cycle as "max RUL" (flat region) and only starting the linear decrease once the engine enters observable degradation gives better model accuracy than a pure linear target. It also avoids penalising the model for predicting a healthy engine will last a long time.
