# Environment Variables Reference

All variables can be set in `.env` (host) or the `environment:` block in
`docker-compose.yml` (container).  Container values take precedence.

---

## Simulator (Module 1)

| Variable | Default | Description |
|---|---|---|
| `DATASET_ID` | `FD001` | C-MAPSS sub-dataset: `FD001`–`FD004` |
| `SOURCE_SPLIT` | `train` | Dataset split: `train` or `test` |
| `STREAM_INTERVAL` | `0.01` | Seconds between rows (0.01 ≈ 100 rows/sec) |
| `REPLAY_MODE` | `single` | `single` or `continuous` |
| `MAX_REPLAY_CYCLES` | `0` | Max replays in continuous mode (0 = unlimited) |
| `REPLAY_PAUSE_SECONDS` | `1.0` | Pause between replays (continuous mode) |
| `PUBLISH_TRANSPORT` | `kafka` | `kafka` \| `mqtt` \| `both` |
| `RUN_ID` | auto | Fixed run ID (auto-generated from UUID4 if unset) |
| `RUL_CAP` | `125` | Piecewise-linear RUL ceiling for training labels |
| `CMAPSS_DATA_DIR` | `/app/data/cmapss` | Path to C-MAPSS `.txt` files |
| `IMS_DATA_DIR` | `/app/data/ims` | Path to IMS `.rar` archives |
| `UNRAR_TOOL` | `unrar` | Path/name of UnRAR executable |
| `IMS_CHUNK_SIZE` | `20480` | Rows per IMS snapshot to read |
| `IMS_STREAM_INTERVAL` | `0.0` | Seconds between IMS snapshot events |
| `IMS_TEST_NAME` | `2nd_test` | IMS archive: `1st_test`, `2nd_test`, `3rd_test` |
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap server |
| `SENSOR_TOPIC` | `sensor-telemetry` | Kafka topic for C-MAPSS events |
| `BEARING_TOPIC` | `bearing-telemetry` | Kafka topic for IMS events |
| `MQTT_BROKER` | `mosquitto` | MQTT broker hostname |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_TOPIC_CMAPSS` | `pms/telemetry/cmapss` | MQTT topic for C-MAPSS events |
| `MQTT_TOPIC_IMS` | `pms/telemetry/ims` | MQTT topic for IMS events |
| `MQTT_QOS` | `1` | MQTT QoS level (0, 1, or 2) |
| `MQTT_CLIENT_ID` | auto | MQTT client ID (auto-generated if unset) |
| `KAFKA_PRODUCER_RETRIES` | `5` | Kafka producer retry count |
| `KAFKA_CONNECT_ATTEMPTS` | `30` | Broker connection attempts before failure |

## .env convenience overrides (host-only)

These are translated by docker-compose substitution into the container env
vars above.  Set them in `.env` to override defaults without editing compose.

| `.env` key | Maps to container var |
|---|---|
| `SIMULATOR_DATASET_ID` | `DATASET_ID` |
| `SIMULATOR_SOURCE_SPLIT` | `SOURCE_SPLIT` |
| `SIMULATOR_STREAM_INTERVAL` | `STREAM_INTERVAL` |
| `SIMULATOR_REPLAY_MODE` | `REPLAY_MODE` |
| `SIMULATOR_MAX_REPLAY_CYCLES` | `MAX_REPLAY_CYCLES` |
| `SIMULATOR_REPLAY_PAUSE_SECONDS` | `REPLAY_PAUSE_SECONDS` |
| `SIMULATOR_PUBLISH_TRANSPORT` | `PUBLISH_TRANSPORT` |

---

## Ingestion / MQTT Bridge (Module 2)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap server |
| `SENSOR_TOPIC` | `sensor-telemetry` | Target Kafka topic for C-MAPSS |
| `BEARING_TOPIC` | `bearing-telemetry` | Target Kafka topic for IMS |
| `DEAD_LETTER_TOPIC` | `sensor-telemetry-dead-letter` | Invalid message destination |
| `MQTT_BROKER` | `mosquitto` | MQTT broker hostname |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_TOPIC_CMAPSS` | `pms/telemetry/cmapss` | Subscribed MQTT topic (C-MAPSS) |
| `MQTT_TOPIC_IMS` | `pms/telemetry/ims` | Subscribed MQTT topic (IMS) |
| `MQTT_QOS` | `1` | Subscription QoS |

---

## Kafka Consumer → InfluxDB (Module 2 existing)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap server |
| `SENSOR_TOPIC` | `sensor-telemetry` | Topic to consume |
| `CONSUMER_GROUP` | `influxdb-writer-group` | Kafka consumer group |
| `INFLUXDB_URL` | `http://influxdb:8086` | InfluxDB endpoint |
| `INFLUXDB_TOKEN` | — | InfluxDB auth token |
| `INFLUXDB_ORG` | `pms-org` | InfluxDB organisation |
| `INFLUXDB_BUCKET` | `sensor-data` | InfluxDB bucket |

---

## Kafka Topics

| Topic | Producer | Consumer | Retention |
|---|---|---|---|
| `sensor-telemetry` | Simulator (C-MAPSS) or MQTT bridge | Preprocessing, Consumer | Kafka default (7 days) |
| `bearing-telemetry` | Simulator (IMS) or MQTT bridge | (future anomaly detection) | Kafka default |
| `preprocessed-telemetry` | Preprocessing service | Prediction service | Kafka default |
| `rul-predictions` | Prediction service | (dashboard reads InfluxDB) | Kafka default |
| `sensor-telemetry-dead-letter` | Consumer, MQTT bridge | Manual inspection | 7 days |
