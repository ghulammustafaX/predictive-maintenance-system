# Module 02 — Verified and Frozen Baseline

## 1. Module identification

| Item | Baseline |
|---|---|
| Module | 02 — Data Ingestion |
| Status | Accepted and Frozen |
| Acceptance scope | NASA C-MAPSS ingestion |
| Verified source commit | `af65ac69e273e84eabe0dc1de964dd17b7b4ea71` |
| Freeze verification date | 20 November 2026 |
| Branch | `main` |
| Module 01 verified baseline | `256faefd914aadef7dd6e07077287a8ffd3cb06c` |

The accepted integration and regression evidence was recorded on 19 November 2026; the source baseline and documentation were checked for this freeze on 20 November. This document freezes the observed engineering baseline, not a Git-level prohibition on future edits.

## 2. Verified functionality

- C-MAPSS telemetry can arrive directly in Kafka or through Mosquitto and the MQTT-to-Kafka bridge. The independent preprocessing service also consumes the Kafka telemetry topic.
- The bridge checks the MQTT payload and routes C-MAPSS messages to Kafka. It waits for a Kafka delivery confirmation before manually acknowledging an MQTT QoS 1 delivery. Invalid MQTT messages are sent to the configured dead-letter topic before acknowledgment.
- The Kafka consumer validates telemetry, writes valid points synchronously to InfluxDB, and manually commits offsets only after a successful write or confirmed dead-letter publication. Storage failures propagate without committing the source offset.
- Isolated checks exercised Kafka and InfluxDB outages, dead-letter recovery, a consumer restart while publishing continued, a consumer-group rebalance, and a graceful Mosquitto restart with a pending message. A client-side injected commit failure also exercised replay after a successful write.
- Replaying an identical point overwrites its existing InfluxDB point identity. This is not general event-ID deduplication.

The evidence and its limits are in the [verification report](MODULE2_VERIFICATION_REPORT.md). The checks do not establish recovery from every possible failure.

## 3. Frozen interfaces

### Topics and consumers

| Interface | Default | Behavior |
|---|---|---|
| C-MAPSS MQTT | `pms/telemetry/cmapss` | Bridge subscribes with `MQTT_QOS`, default `1`. |
| IMS MQTT | `pms/telemetry/ims` | Bridge routes IMS messages to Kafka; no IMS storage consumer is implemented. |
| C-MAPSS Kafka | `sensor-telemetry` | Ingestion consumer and preprocessing use separate consumer groups. |
| IMS Kafka | `bearing-telemetry` | Bridge and simulator destination; downstream InfluxDB ingestion is outside this baseline. |
| Dead-letter Kafka | `sensor-telemetry-dead-letter` | Invalid records; configurable. The consumer default is derived from `SENSOR_TOPIC`, while the bridge has the named default shown. |
| Ingestion group | `influxdb-writer-group` | `CONSUMER_GROUP` is configurable; auto-commit is disabled and `auto_offset_reset` is `earliest` when no committed offset exists. |
| MQTT client | `pms-mqtt-bridge` | `MQTT_CLIENT_ID` is configurable and must remain stable and unique per bridge instance; MQTT 3.1.1 uses `clean_session=False` and manual acknowledgment. |

Default topic and group names can be overridden by their environment variables. The consumer defaults to a maximum of five records per poll, a 600,000 ms maximum poll interval and a 10,000 ms commit timeout. Kafka ordering is per partition, not global across partitions.

### C-MAPSS event and validation

The Module 01 versioned envelope uses `schema_version` `1.0.0`, `event_id`, `run_id`, `dataset_id`, `asset_id`, `timestamp`, `sequence` and `source_type`. The bridge checks the required C-MAPSS envelope fields plus `unit_id` and `time_cycles`; its validation is less restrictive than the consumer's. The consumer requires `unit_id`, `time_cycles`, `operational_setting_1`, `operational_setting_2`, `operational_setting_3`, and either `source_timestamp` or `timestamp`. Positive integer-convertible unit and cycle values are required; boolean and fractional float values are rejected.

When `schema_version` is present, the consumer accepts only `1.0.0`, requires nonempty string identity fields, requires a positive integer `sequence`, accepts `source_type` `cmapss` or `synthetic`, and requires `asset_id` to match `engine_<unit_id>`. Legacy messages without `schema_version` retain the earlier optional identity behavior; missing `dataset_id` and `run_id` default to `unknown` and `default`. Sensor fields `sensor_1` through `sensor_21` are optional; null sensors are omitted, and present non-null values and operational settings must be finite numeric-convertible values and cannot be boolean. The selected timestamp is parsed during point construction; an explicit timezone is not required by the implementation.

### InfluxDB point and acknowledgment rules

| Item | Implemented interface |
|---|---|
| Measurement | `sensor_telemetry` in configurable bucket `INFLUXDB_BUCKET`, default `sensor-data` |
| Tags | `dataset_id`, `unit_id`, `run_id`, `cycle` |
| Fields | Integer `time_cycles`, three float operational settings, and each present non-null `sensor_1`–`sensor_21` as a float |
| Timestamp | `source_timestamp` takes precedence over `timestamp` |
| Event metadata | `event_id`, `asset_id`, `sequence` and `source_type` are not stored as point fields |
| Point identity | Measurement, complete tag set and timestamp; an identical point identity is overwritten |

The bridge waits for Kafka confirmation before acknowledging an MQTT QoS 1 delivery; a Kafka or dead-letter publication failure stops the bridge without acknowledging that delivery. Recovery depends on the persistent MQTT session and broker availability. QoS 0 has no acknowledgment or redelivery guarantee.

For Kafka records, the consumer commits the next offset for the source partition only after the synchronous InfluxDB write succeeds, or after the dead-letter producer receives its Kafka acknowledgment for an invalid record. Invalid JSON, invalid UTF-8 and failed validation follow the dead-letter path. InfluxDB writes default to three attempts with exponential backoff; authentication failures are raised immediately. An exhausted write, dead-letter failure or commit failure does not count as successful processing and may lead to replay. This is at-least-once delivery under the documented operational conditions, not an exactly-once transaction.

## 4. Verification evidence

The [Module 02 verification report](MODULE2_VERIFICATION_REPORT.md) records the following accepted results:

| Check | Recorded result |
|---|---|
| Full regression with isolated Kafka and Mosquitto | 344 passed, 0 failed, 0 skipped; 239 warnings |
| Active-ingestion restart | 160 of 160 unique events reconciled; final Kafka lag zero |
| Sensor integrity in that run | 3,360 of 3,360 sensor values matched |
| Kafka outage and recovery | Verified in the documented isolated scenario |
| InfluxDB outage and recovery | Verified, including an uncommitted offset while storage was unavailable |
| Dead-letter failure and recovery | Verified in the documented isolated scenario |
| Consumer rebalance | Verified with two isolated partitions and consumers; see report for timing and source-version context |
| Graceful Mosquitto restart | Pending QoS 1 event recovered in the documented isolated scenario; see report for source-version context |

The report distinguishes the client-side commit-failure injection from a broker-induced failure and records the exact test scope.

## 5. Known limitations

- At-least-once delivery permits duplicate Kafka deliveries; it is not exactly-once processing.
- InfluxDB overwrites an identical point identity, not every event with the same `event_id`. A different timestamp or tag set creates a different point.
- Abrupt broker termination, host failure, power loss and persistence across broker container recreation were not verified. The documented Mosquitto restart was graceful.
- The commit-failure test injected an exception in the client after a real write; it did not induce a Kafka broker commit failure.
- IMS-to-InfluxDB ingestion is not implemented. The IMS bridge and Kafka routing do not establish downstream storage.
- Production safety checks compared sampled container states, topics, offsets, counts and volumes; they did not compare all data byte for byte.
- Dependency deprecation warnings and scikit-learn scaler-version warnings remain documented in the verification report. No ML artifact was changed for this baseline.
- The consumer does not track sequence gaps or out-of-order arrivals and does not store `event_id` as a point field. Unexpected infrastructure failures can trigger repeated service restarts and require operator attention.

## 6. Dependencies

| Dependency | Baseline information |
|---|---|
| Module 01 | Versioned event envelope and C-MAPSS schema; see [Module 01 baseline](MODULE1_BASELINE.md). |
| Kafka | Compose image `confluentinc/cp-kafka:7.4.0`; independent consumer groups support ingestion and preprocessing. |
| Mosquitto | Compose image `eclipse-mosquitto:2.0`; the isolated verification reported Mosquitto 2.0.22 and used persistence. |
| InfluxDB | Compose image `influxdb:2.7`; synchronous Python client writes. |
| Python | Ingestion Dockerfile uses `python:3.11-slim`. |
| Python packages | Requirements specify `kafka-python>=2.0.2`, `influxdb-client>=1.38.0`, `python-dotenv>=1.0.0` and `paho-mqtt>=2.0,<3`; these ranges are not fully pinned. Isolated tests inspected kafka-python 3.0.13 and Paho MQTT 2.2.0. |
| Docker Compose | Defines the services and restart configuration; the recorded verification used isolated Compose projects. |

## 7. Change-control policy

1. Treat the verified Module 02 behavior at the source commit above as the reference implementation.
2. Do not change frozen schemas without a compatibility review.
3. Do not change MQTT acknowledgment semantics without reliability testing.
4. Do not change Kafka offset management without replay and recovery tests.
5. Do not change InfluxDB point identity without duplicate-handling tests.
6. Do not break existing Module 01 compatibility.
7. Future changes must pass the relevant regression and integration tests.
8. Maintain a clear record of approved changes and their reasons.

The freeze is a documented engineering baseline. Future changes remain possible when they are explicitly reviewed and approved under these rules.

Related documents: [architecture](MODULE2_ARCHITECTURE.md), [verification report](MODULE2_VERIFICATION_REPORT.md), and [user guide](MODULE2_USER_GUIDE.md).
