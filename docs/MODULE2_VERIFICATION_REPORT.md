# Module 02 — Verification Report

> **Current status: ACCEPTED for the documented C-MAPSS ingestion scope.** The checks and limitations below define the evidence; they do not establish exactly-once delivery or IMS-to-InfluxDB ingestion.

## Final release verification (2026-11-19)

The four previously skipped Module 01 Kafka and MQTT integration tests passed against a separate Kafka/Mosquitto project using `KAFKA_HOST=kafka:9092`, `MQTT_HOST=mosquitto` and `MQTT_PORT=1883`. An override routed the bridge to their expected topic names inside that isolated broker. Result: **4 passed, 0 failed, 0 skipped** (11 other tests deselected).

In a separate isolated full pipeline, a QoS 1 publisher sent 160 distinct C-MAPSS events while the actual consumer was stopped and restarted. Before stopping it the consumer had committed offset 21; after stop, one observation showed committed 47, end 52; while stopped a later observation showed committed 47, end 104. Publishing continued during the outage and after restart. The publisher completed all 160 acknowledgments. After recovery the writer group reached committed offset 160, end offset 160, lag 0. Independent reconciliation found **160 Kafka records, 160 distinct event IDs, zero duplicate Kafka records, 160 InfluxDB points and 3,360 matching sensor values**. InfluxDB does not store `event_id`; reconciliation pairs the Kafka IDs with the run and cycle identity stored in InfluxDB. The earlier database-outage and injected-commit-failure checks provide separate evidence of commit ordering.

A fresh full regression connected the Module 01 integration tests to isolated Kafka and Mosquitto: **344 passed, 0 failed, 0 skipped, 239 warnings** in 118.27 seconds. Of the warnings, 218 are importlib-resource deprecations from kafka-python, seven are kafka-python serializer/deserializer deprecations, two are Paho callback API deprecations, and 12 are scikit-learn warnings about loading existing 1.9.0 scaler artifacts with 1.9.1 in the disposable test environment. The last category is an unrelated ML artifact compatibility warning, not evidence that the versions are interchangeable; no artifact was changed. The deprecated APIs work in this run but will need attention before dependency upgrades remove them.

Production topic names and offsets, InfluxDB aggregate field counts and bucket names, PostgreSQL row counts, observed container states and listed named volumes matched the saved baseline. This comparison is sampled and does not prove byte-for-byte data equality. Abrupt broker or host failure, MQTT QoS 0 redelivery, and every possible network or database failure are not established. IMS is routed to Kafka but has no verified downstream InfluxDB consumer.

## Real-service remediation observations (2026-11-19)

### Additional isolated reliability checks

In an independent test project, InfluxDB was stopped before a QoS 1 event was published. Kafka's end offset became 1 while the writer group had no committed offset. The consumer exhausted three write retries and logged that offset 0 remained uncommitted. After InfluxDB restarted, the event was query-visible with `sensor_1=518.67` and the committed offset reached 1. Two identical events then produced two Kafka records and one InfluxDB point with the original value; the writer offset advanced to 3. This is point overwriting for identical measurement, tags and timestamp, not event-ID deduplication or exactly-once delivery.

The test topic was expanded to two partitions and two consumers were observed with one partition each. After stopping one consumer, an initial query found the new event still pending during rebalance; a later query found it stored, and the remaining consumer acquired both partitions. This does not establish a maximum recovery time. For a controlled commit failure, a test-only wrapper raised at `commit_message` after the real InfluxDB write. Kafka showed committed offset 4, end offset 5; after normal consumer restart the offset reached 5 and one matching point remained. The failure was injected in the client, not by a Kafka broker.

The controlled commit-failure scenario was repeated against the final consumer source in a fresh isolated project. The runner logged an injected failure at offset 0 after the point was written; the group had no committed offset while the topic end offset was 1. After normal consumer restart the group committed offset 1, and the same event still had one stored point with `sensor_1=518.67`.

With isolated Kafka stopped, the bridge logged an unacknowledged QoS 1 message, ID 6. Isolated Mosquitto was gracefully restarted and logged saving its persistence database. After Kafka and the bridge recovered, the same event ID was present once in Kafka and once in InfluxDB. The first check preceded bridge reconnection and failed; the later check passed. Abrupt broker termination, power loss and persistence across container recreation were not tested.

### Consumer validation audit

| Field | Current handling |
|---|---|
| `schema_version` | Optional for older payloads; when supplied, only `1.0.0` is accepted. |
| `event_id`, `asset_id`, `sequence`, `source_type` | Required for versioned messages; nonempty strings, positive integer sequence, supported source type and engine/unit consistency are checked. These fields are not stored as point fields. |
| `run_id`, `dataset_id` | Required nonempty strings for versioned messages. Legacy messages without schema version can omit them and receive `default` and `unknown`. |
| `unit_id`, `time_cycles` | Required positive integer-convertible values; booleans and fractional floats are rejected. |
| Timestamp | `source_timestamp` or `timestamp` required; parsed when constructing the point, with no explicit timezone requirement. |
| Operational settings | Required and checked for finite numeric conversion; boolean values are rejected. |
| `sensor_1`–`sensor_21` | Optional; null values are skipped for Module 01 compatibility. Present non-null values must convert to finite numbers and cannot be boolean. |

The bridge has separate, less restrictive validation; the consumer enforces its additional checks before writing. The broker-restart and rebalance scenarios above ran before the final envelope validation and storage error-handling changes. The InfluxDB outage was repeated against the final source: before the event there was no committed offset and the end offset was 0; during the outage the end offset was 1 with no committed offset, and retry exhaustion left offset 0 uncommitted. After recovery, one point with `sensor_1=518.67` was query-visible and the committed offset was 1. Delivery from Kafka to InfluxDB is at least once when the service restarts and replays an uncommitted record, subject to Kafka retention and operational availability. MQTT QoS 1 recovery relies on the persistent broker session and a stable unique client ID. No exactly-once guarantee is established.

An isolated Compose project with separate network, Kafka, Mosquitto and InfluxDB was used; no host ports were published. The bridge and consumer ran the current source. A QoS 1 event `probe-m2happy20261119` reached Kafka once and produced one InfluxDB `sensor_1` value of 518.67. With the isolated Kafka broker stopped, `probe-m2recover20261119` was published to Mosquitto; the bridge logged a Kafka timeout and `MQTT delivery not acknowledged mid=2 qos=1`. After Kafka restarted, that event was observed once in Kafka and once in InfluxDB with the same sensor value. Mosquitto logged the same client reconnecting with a persistent session (`c0`). This is evidence of recovery in this scenario, not a guarantee against broker data loss or all restart conditions.

Invalid JSON, invalid UTF-8, and a declared unsupported schema each produced one matching dead-letter record. For a further invalid JSON message with Kafka stopped, the bridge logged an unacknowledged QoS 1 message (`mid=6`); after Kafka restarted, a matching dead-letter record was observed. The unsupported schema was routed to Kafka by the bridge and dead-lettered by the consumer. The isolated writer group's offset was 3 with zero lag: two valid events and the unsupported-schema event. Event IDs are preserved in Kafka, but are not stored as InfluxDB fields by the current consumer.

The original baseline and observed production comparison agree for Kafka topic names and group offsets, InfluxDB measurement field counts and bucket names, PostgreSQL table counts, production service states and the listed named volumes. Counts are not content hashes. The additional checks above cover selected recovery cases, not all possible failures. IMS-to-InfluxDB remains unimplemented.

The combined `tests/` run in a disposable Python container completed with **320 passed, 4 skipped, 0 failed** in 106.51 seconds. It emitted dependency deprecation warnings and scaler version warnings (artifact 1.9.0, installed scikit-learn 1.9.1). The run used a read-only repository mount; network-dependent tests were not connected to the real-service test project and skips should not be treated as integration evidence. The isolated Compose containers and network were removed without deleting volumes.

After the final validation and storage-error changes, a fresh combined `tests/` run completed with **340 passed, 4 skipped, 0 failed, 230 warnings** in 105.96 seconds. The warnings comprise 218 kafka-python/importlib deprecations and 12 scaler-version warnings. A follow-up run of the Module 01 integration and IMS tests reported the four skips at `test_m1_integration.py` lines 203, 250, 293 and 344 because Kafka was not reachable at `localhost:39092`; these are not passing integration checks. That follow-up had 35 passed and 4 skipped. A focused consumer/bridge run completed with **130 passed, 0 failed, 0 skipped**. All ran in disposable containers with a read-only project mount.

**Repository:** `ghulammustafaX/predictive-maintenance-system`  
**Report date:** 2026-11-19  
**Git HEAD at start of verification:** `b41ef9b`  
**Branch:** `main`

---

## 1. Production Safety

Before and after all tests — no production changes.

| Resource | Before | After |
|---|---|---|
| `influxdb-writer-group` offset | 20,671 | 20,671 ✅ |
| `preprocessing-service-group` offset | 20,671 | 20,671 ✅ |
| `rul-prediction-service-group` offset | 20,671 | 20,671 ✅ |
| Production containers | Running unchanged | Running unchanged ✅ |
| Production volumes | Intact | Intact ✅ |
| Production ML artifacts | Intact | Intact ✅ |

All write tests used the isolated `pms-ingestion-reliability-test` Docker Compose project
with separate network, volumes, Kafka topics, consumer groups, and InfluxDB bucket.

---

## 2. Module 01 Regression

The Module 01 baseline of 197 tests remained intact throughout all Module 02 work.

```
Command:
  python -m pytest tests/ --ignore=tests/test_m1_integration.py --tb=short -q

Result (HEAD b41ef9b + Module 02 additions, 2026-11-19):
  298 passed, 2 skipped, 0 failed in 46.01s
```

The 2 skips are pre-existing IMS snapshot-discovery tests that require `unrar` on the host PATH.

---

## 3. Unit Test Results

### 3.1 Module 02 — kafka_consumer.py

**File:** `tests/test_ingestion_consumer.py`

| Test class | Tests | Result |
|---|---|---|
| TestDecodeMessage | 18 | ✅ PASS |
| TestBuildPoint | 15 | ✅ PASS |
| TestWriteToInfluxDB | 7 | ✅ PASS |
| TestDeadLetter | 6 | ✅ PASS |
| TestCommitMessage | 3 | ✅ PASS |
| TestProcessMessage | 7 | ✅ PASS |
| TestDuplicateHandling | 2 | ✅ PASS |
| TestSchemaVersionBackwardCompat | 3 | ✅ PASS |
| TestDatasetIdentityFields | 5 | ✅ PASS |
| TestOutOfOrderMessages | 2 | ✅ PASS |
| **TOTAL** | **70** | **✅ 70/70** |

### 3.2 Module 02 — mqtt_bridge.py

**File:** `tests/test_ingestion_mqtt_bridge.py`

| Test class | Tests | Result |
|---|---|---|
| TestValidateCmapss | 9 | ✅ PASS |
| TestValidateIms | 7 | ✅ PASS |
| TestOnMessageRouting | 15 | ✅ PASS |
| **TOTAL** | **31** | **✅ 31/31** |

---

## 4. Isolated End-to-End Test

**Environment:** `pms-ingestion-reliability-test` Docker Compose project

| Service | Image |
|---|---|
| Kafka | `confluentinc/cp-kafka:7.4.0` |
| Zookeeper | `confluentinc/cp-zookeeper:7.4.0` |
| InfluxDB | `influxdb:2.7` |
| Ingestion consumer | Built from `ingestion/Dockerfile` |

**Run:** `python /tmp/e2e_test.py` (inside `pms-ingestion-reliability-test-consumer`)

| Check | Result |
|---|---|
| sensor topic received messages (>=12) | ✅ PASS |
| 10 valid records stored in InfluxDB | ✅ PASS |
| measurement name is `sensor_telemetry` | ✅ PASS |
| `dataset_id` tag = `FD001` | ✅ PASS |
| `unit_id` tag = `1` | ✅ PASS |
| `run_id` tag matches published run_id | ✅ PASS |
| `sensor_1` value = `518.67` | ✅ PASS |
| `timestamp` is set | ✅ PASS |
| all 21 sensor fields stored | ✅ PASS |
| all 3 operational setting fields stored | ✅ PASS |
| invalid messages routed to dead-letter topic | ✅ PASS |
| consumer committed all offsets (lag=0 confirmed) | ✅ PASS |

**Result: 12/12 — PASS**

---

## 5. Full Regression Summary

| Test category | Passed | Failed | Skipped |
|---|---:|---:|---:|
| Module 01 Unit (pre-existing) | 197 | 0 | 2 |
| Module 02 Unit — consumer | 70 | 0 | 0 |
| Module 02 Unit — mqtt bridge | 31 | 0 | 0 |
| **Total automated** | **298** | **0** | **2** |
| Module 02 E2E (isolated) | 12 | 0 | 0 |

---

## 6. Validation Evidence

### 6.1 Field-level value verification (InfluxDB)

Queried from isolated test InfluxDB bucket during E2E test run:

| Field | Expected | Stored | Match |
|---|---|---|---|
| `_measurement` | `sensor_telemetry` | `sensor_telemetry` | ✅ |
| `dataset_id` tag | `FD001` | `FD001` | ✅ |
| `unit_id` tag | `1` | `1` | ✅ |
| `run_id` tag | `5fd00c19...` | `5fd00c19...` | ✅ |
| `sensor_1` field | `518.67` | `518.67` | ✅ |
| All 21 sensor fields present | Yes | Yes | ✅ |
| All 3 op-setting fields present | Yes | Yes | ✅ |
| `timestamp` | ISO-8601 | ISO-8601 | ✅ |

### 6.2 Invalid message handling

Two intentionally invalid payloads were published:
- `{"garbage": True, "run_id": ..., "event_id": "bad-1"}` — missing 5 required fields
- `{"unit_id": "not-a-number", ...}` — non-numeric unit_id

Both were routed to `sensor-telemetry-dead-letter` and **not** written to InfluxDB. ✅

### 6.3 Pre-existing production verification (from Module 01 baseline)

- Production `sensor_telemetry` measurement: **41,302 points** (FD001)
- `sensor_1` for FD001 engine_1 cycle_1: **518.67** ✅
- Consumer group `influxdb-writer-group` offset: **20,671**, lag: **0**

---

## 7. Defects Found During Audit

| ID | Defect | Severity | Resolution |
|---|---|---|---|
| D-1 | `bearing-telemetry` topic absent in production Kafka (not created at current stack startup) | Medium | Documented; topic is created by `kafka-topic-init` on fresh stack start; no code change required |
| D-2 | E2E test verifier initially joined the ingestion consumer's Kafka group, causing a rebalance | Medium (test script only) | Fixed in `scripts/run_ingestion_e2e_test.py` — verifiers now use unique throw-away group IDs |
| D-3 | Zero dedicated unit tests for `kafka_consumer.py` and `mqtt_bridge.py` prior to this module | High | Resolved — 101 new tests added |

---

## 8. Acceptance Decision

**ACCEPTED for the documented C-MAPSS ingestion scope**, subject to the limitations and test evidence at the top of this report. The IMS downstream consumer (`bearing-telemetry` → InfluxDB) remains planned and outside that acceptance.
