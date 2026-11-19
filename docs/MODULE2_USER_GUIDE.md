# Module 02 — Data Ingestion: User Guide

**Audience:** Beginners and developers new to the PMS project.  
**Updated:** 2026-11-19

---

## What is Module 02?

Module 02 is the **data ingestion** layer. It takes sensor readings that
the simulator (Module 01) has published to Apache Kafka and stores them
in a time-series database called InfluxDB, where dashboards and
prediction models can read them.

Think of it as a **mailroom** between the sensor data factory (Module 01)
and the storage warehouse (InfluxDB). Without it, sensor readings would
pile up in Kafka but never reach long-term storage.

---

## Why is it needed?

Kafka is a message broker — it temporarily holds messages in order. It is
great for passing data between services quickly, but it is not designed for
long-term storage or flexible querying.

InfluxDB is a **time-series database** — it is built specifically for
storing and querying sensor readings over time, like "show me sensor 1
readings for engine 5 over the last 24 hours."

Module 02 bridges the two: it reads every sensor message from Kafka the
moment it arrives and durably writes it to InfluxDB.

---

## How it connects to Module 01

Module 01 (the simulator) publishes sensor readings in two possible ways:

| Transport | Path |
|---|---|
| **Direct Kafka** (default) | Simulator → Kafka `sensor-telemetry` → Module 02 consumer → InfluxDB |
| **MQTT** | Simulator → Mosquitto → MQTT bridge → Kafka → Module 02 consumer → InfluxDB |

Both paths end at the same place: InfluxDB's `sensor_telemetry` measurement.

---

## The two components

### 1. Kafka Consumer (`kafka_consumer.py`)

This is the main worker. It:

1. Connects to Kafka and listens to the `sensor-telemetry` topic.
2. For each message: validates it has the required fields.
3. Builds an InfluxDB data point with the sensor values, engine ID, dataset, and timestamp.
4. Writes the point to InfluxDB.
5. **Only then** tells Kafka "I have processed this message" (commits the offset).
6. If validation fails, sends the bad message to the **dead-letter topic** (`sensor-telemetry-dead-letter`) instead.

This order matters. If the write to InfluxDB fails (e.g. InfluxDB is briefly down),
the offset is not committed and the message will be re-processed when the consumer
restarts. No data is lost.

### 2. MQTT Bridge (`mqtt_bridge.py`)

This component runs alongside the consumer when you use MQTT transport.

It:

1. Connects to Mosquitto (the MQTT broker).
2. Subscribes to `pms/telemetry/cmapss` and `pms/telemetry/ims`.
3. Validates each incoming message.
4. Forwards valid C-MAPSS messages to Kafka's `sensor-telemetry` topic.
5. Forwards valid IMS bearing messages to Kafka's `bearing-telemetry` topic.
6. Sends invalid messages to the dead-letter topic.

---

## Example JSON payloads

### A valid C-MAPSS sensor reading

```json
{
  "schema_version": "1.0.0",
  "event_id": "3291930e-418a-63f5-0e55-dbaf37651bb9",
  "run_id": "482d877957d648d0abe4e2755837c87e",
  "dataset_id": "FD001",
  "asset_id": "engine_1",
  "unit_id": 1,
  "time_cycles": 1,
  "operational_setting_1": -0.0007,
  "operational_setting_2": -0.0004,
  "operational_setting_3": 100.0,
  "sensor_1": 518.67,
  "sensor_2": 641.82,
  "sensor_21": 23.419,
  "timestamp": "2026-11-19T10:00:00+00:00",
  "sequence": 1,
  "source_type": "cmapss"
}
```

The consumer requires: `unit_id`, `time_cycles`,
`operational_setting_1`, `operational_setting_2`, `operational_setting_3`,
and at least one of `timestamp` or `source_timestamp`.

### A valid IMS bearing reading (MQTT bridge only)

```json
{
  "schema_version": "1.0.0",
  "event_id": "d2232936-b42b-f93f-67bf-fa93caa04daf",
  "run_id": "fe07451dd52d44f79d7e26e1596113d5",
  "dataset_id": "IMS_2nd_test",
  "asset_id": "2nd_test_bearing_1",
  "bearing_name": "bearing_1",
  "timestamp": "2026-11-19T10:00:00+00:00",
  "sequence": 1,
  "source_type": "ims",
  "features": {
    "rms": 0.07418,
    "kurtosis": 3.629,
    "dominant_freq_hz": 1009.0
  }
}
```

---

## How is data stored in InfluxDB?

Each sensor reading becomes a **data point** in InfluxDB with this structure:

| Part | Value | Used for |
|---|---|---|
| Measurement | `sensor_telemetry` | Identifying the data type |
| Tag: `dataset_id` | `FD001` | Filtering by C-MAPSS sub-dataset |
| Tag: `unit_id` | `1` | Filtering by engine number |
| Tag: `run_id` | `abc123...` | Separating different replay passes |
| Tag: `cycle` | `42` | Filtering by time cycle |
| Field: `sensor_1` to `sensor_21` | floats | The actual sensor measurements |
| Field: `operational_setting_1-3` | floats | Operating conditions |
| Field: `time_cycles` | integer | Cycle number as a field (for aggregation) |
| Timestamp | ISO-8601 | When the reading was published |

---

## Setup instructions

### Prerequisites

- Docker Desktop installed and running
- The PMS project cloned locally
- `.env` file with `INFLUXDB_TOKEN` set

### Starting Module 02 services

The consumer and MQTT bridge are defined in `docker-compose.yml` and start
as part of the full stack:

```bash
# Start the full PMS stack (including Module 02)
docker compose up -d

# Start only the ingestion services
docker compose up -d consumer mqtt-bridge
```

### Checking the consumer is running

```bash
docker logs pms-consumer --follow
```

You should see lines like:
```
INFO:pms.ingestion:Kafka consumer connected
INFO:pms.ingestion:Connecting to InfluxDB url=http://influxdb:8086
INFO:pms.ingestion:Processed messages=100
```

### Running the simulator to generate data

```bash
docker compose run --rm simulator
```

After the simulator finishes, run the Module 01 test suite to verify data reached InfluxDB:

```bash
python -m pytest tests/ --ignore=tests/test_m1_integration.py -q
```

### Querying InfluxDB directly

Open the InfluxDB UI at http://localhost:8086 and log in with the credentials
in your `.env` file. Use the **Data Explorer** to run a Flux query:

```flux
from(bucket: "sensor-data")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry")
  |> filter(fn: (r) => r._field == "sensor_1")
  |> filter(fn: (r) => r.unit_id == "1")
```

---

## Running the tests

```bash
# Unit tests for Module 02 (no broker needed)
python -m pytest tests/test_ingestion_consumer.py tests/test_ingestion_mqtt_bridge.py -v

# Full regression suite (Module 01 + Module 02 unit tests)
python -m pytest tests/ --ignore=tests/test_m1_integration.py -q
```

Expected result: **298 passed, 2 skipped**

---

## Common errors and solutions

### Consumer exits with "NoBrokersAvailable"

```
kafka.errors.NoBrokersAvailable
```

**Cause:** Kafka has not started yet.  
**Fix:** Wait for `pms-kafka` to be fully healthy before starting the consumer.
The consumer retries 30 times with 2-second delays automatically.

### "INFLUXDB_TOKEN not set"

```
ValueError: INFLUXDB_TOKEN not set
```

**Cause:** The InfluxDB auth token is missing.  
**Fix:** Add `INFLUXDB_TOKEN=your-token` to your `.env` file.

### No data appears in InfluxDB

**Common causes:**
1. The simulator has not run yet — start it with `docker compose run --rm simulator`.
2. The consumer is not running — check `docker logs pms-consumer`.
3. The wrong bucket name is configured — check `INFLUXDB_BUCKET` in `.env`.

### Dead-letter messages appearing

Check the dead-letter topic for details:
```bash
docker exec pms-kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic sensor-telemetry-dead-letter \
  --from-beginning \
  --max-messages 5
```

---

## Architecture diagram

```
┌───────────────────┐     Kafka key:                   ┌──────────────────────┐
│  Module 01        │     dataset_id:unit_id:run_id     │  Kafka               │
│  (Simulator)      │──────────────────────────────────▶│  sensor-telemetry    │
└───────────────────┘                                   └──────────┬───────────┘
                                                                   │
                                                                   │ manual poll
                                                                   ▼
                                                       ┌──────────────────────┐
                                                       │  kafka_consumer.py   │
                                                       │  ● validate fields   │
                                                       │  ● build InfluxDB    │
                                                       │    point             │
                                                       │  ● write (3 retries) │
                                                       │  ● commit offset     │
                                                       └──────┬───────────────┘
                                                              │ SYNCHRONOUS write
                                                              ▼
                                                       ┌──────────────────────┐
                                                       │  InfluxDB            │
                                                       │  measurement:        │
                                                       │  sensor_telemetry    │
                                                       └──────────────────────┘
```

---

## Limitations and future improvements

| Limitation | Plan |
|---|---|
| IMS bearing data is not written to InfluxDB | Add a bearing ingestion consumer in Modules 7+ |
| `bearing-telemetry` Kafka topic must be created manually in production | Add a startup script or health-check to auto-create it |
| No consumer-side duplicate detection (relies on InfluxDB overwrite) | Add an `event_id` LRU seen-set, similar to Module 01's `SequenceTracker` |
| Sequence gaps and out-of-order arrivals are not logged | Integrate `SequenceTracker` into the consumer for observability |
