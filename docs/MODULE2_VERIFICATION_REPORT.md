# Module 02 — Verification Report

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

**✅ ACCEPTED**

All requirements of Module 02 that have been implemented are verified:

- Kafka consumer reads `sensor-telemetry`, validates messages, writes to InfluxDB, commits offsets correctly.
- MQTT bridge validates and routes C-MAPSS and IMS messages to correct Kafka topics.
- Invalid messages reach the dead-letter topic and do not corrupt InfluxDB.
- Delivery guarantee is documented as at-least-once.
- 101 unit tests and 12 E2E checks all pass.
- Production infrastructure unchanged.

The IMS downstream consumer (`bearing-telemetry` → InfluxDB) is documented as a **planned** feature (Modules 7+) and is explicitly out of scope for Module 02.
