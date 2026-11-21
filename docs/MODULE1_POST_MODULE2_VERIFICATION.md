# Module 01 — Post-Module 02 Verification

**Date:** 20 November 2026

**Recommendation:** CONDITIONAL ACCEPTANCE

**Initial branch and HEAD:** `main`, `59d37fbcf3175e248bd6ea1ac8d9b4af582dc11c`

**Module 01 historical source baseline:** `256faefd914aadef7dd6e07077287a8ffd3cb06c`

**Module 02 frozen source baseline:** `af65ac69e273e84eabe0dc1de964dd17b7b4ea71`
**Module 02 documentation freeze:** `59d37fbcf3175e248bd6ea1ac8d9b4af582dc11c`

The initial working tree was clean. During this verification an IMS crest-factor defect was reproduced and the user explicitly approved a targeted source correction and regression test. At the time of this verification the correction was **uncommitted working-tree code**, not part of the historical Module 01 baseline. Module 02 source was not modified. Conditional acceptance reflected the need to review and checkpoint the approved Module 01 change and investigate the production consumer-group state described below.

## C-MAPSS dataset verification

Fresh reads of the actual dataset files gave the following counts:

| Dataset | Training rows | Training units | Test rows | Test units |
|---|---:|---:|---:|---:|
| FD001 | 20,631 | 100 | 13,096 | 100 |
| FD002 | 53,759 | 260 | 33,991 | 259 |
| FD003 | 24,720 | 100 | 16,596 | 100 |
| FD004 | 61,249 | 249 | 41,214 | 248 |

The fresh Module 01 unit suite exercised column mapping, missing-file and invalid-identifier handling, NaN checks, unit separation, cycle ordering and RUL label handling. The preprocessing compatibility tests checked that the actual 24-column feature allowlist excludes `rul` and that RUL-bearing payloads do not put `rul` in the tested preprocessing output. This is evidence for the tested preprocessing path, not a proof about every possible future model input path.

## IMS archive and feature verification

Real archive discovery returned 2,156 snapshots for `1st_test`, 984 for `2nd_test` and 6,324 for `3rd_test`. The first snapshots from the first two archives were actually extracted: shapes were 20,480 × 8 and 20,480 × 4 respectively, and their filenames were parsed into UTC timestamps. The third archive was discovered and listed, but a snapshot from it was not extracted in this run. Temporary extraction directories were empty after each checked extraction. Processing loads one bounded snapshot array at a time; this check is not a whole-process memory bound benchmark.

Independent calculations on all 12 channels of the extracted snapshots confirmed dominant frequencies and, after correction, crest factors. Before correction, the implementation divided the positive peak by RMS instead of the maximum absolute peak. For `1st_test` channel 1 it reported approximately 3.1136 while the absolute-peak reference was approximately 5.7779. The user approved the one-line correction. The new three-case regression first produced **1 failed, 2 passed** on the old implementation, then the focused IMS suite produced **27 passed, 0 failed, 0 skipped** with the correction. Real-snapshot RMS calculations also matched independent NumPy calculations.

A real `2nd_test` bearing event travelled through isolated MQTT, the actual bridge and Kafka. Before and after the approved fix, the event identity, bearing identity, timestamp and RMS were preserved. After the fix, a negative-dominant channel's corrected crest factor also matched the independent reference and survived the route. No IMS-to-InfluxDB capability is claimed.

## Event identity, sequence and replay

The fresh tests exercised deterministic event IDs, unique run IDs, duplicate, out-of-order, gap and conflict classification, and the Kafka partition key. The in-memory sequence tracker bounds remembered event IDs **per key**; it does not impose a global bound on the number of keys. Its state is lost on process restart, so it is not durable deduplication.

Using the full actual FD001 training file and a transport test double, the actual replay controller sent 20,631 rows in single mode and 41,262 rows across two continuous passes. It generated one run ID per pass, kept each pass's ID stable, produced a monotonic global sequence and called shutdown. This count test did not use a real broker.

A separate actual MQTT replay used ten real FD001 rows for each of two passes: 20 distinct event IDs were confirmed by the publisher and observed in isolated Kafka, with two run IDs, a measured 0.213-second inter-pass interval for a 0.2-second configured pause, and publisher shutdown. The source file contained 20,631 rows; only the ten-row subset was sent in this real-broker replay. Graceful-stop unit coverage simulates the stop flag; an operating-system signal during a full real-broker replay was not separately tested.

## Kafka, MQTT and Module 02 compatibility

The full regression included four broker-backed Module 01 integration tests using isolated Kafka and Mosquitto. An additional real publisher reconnection check restarted only isolated Mosquitto: disconnect and reconnect were observed, both QoS 1 publishes were confirmed, and both event IDs reached isolated Kafka. Kafka producer retry and flush behavior also have mock-based unit coverage; this run did not induce every possible Kafka producer failure.

For the most important cross-module check, the actual Module 01 C-MAPSS loader, event iterator and MQTT publisher sent eight training records from each of FD001–FD004 through isolated Mosquitto, the frozen Module 02 bridge, Kafka and the frozen ingestion consumer to isolated InfluxDB. Results:

| Observation | Fresh result |
|---|---:|
| Module 01 events published / distinct Kafka event IDs | 32 / 32 |
| InfluxDB points visible | 32 |
| Sensor values compared to source rows | 672 / 672 |
| Operational settings compared to source rows | 96 / 96 |
| Total checked stored fields, including cycles | 800 / 800 |

Kafka messages retained the expected event IDs, run IDs, dataset IDs, unit IDs, asset IDs and partition keys. InfluxDB tags preserved dataset, run, unit and cycle; `asset_id` and `event_id` are not stored as point fields. The isolated ingestion group was at offset 34 with end offset 34 and lag zero at the observation; the topic also contained records from concurrently executed integration tests. No `rul` field appeared in the checked InfluxDB points. This run observed final offsets after storage; the separately documented Module 02 outage and commit-failure tests provide the earlier failure-ordering evidence.

## Fresh automated results

These categories partition the 347-test `tests/` regression; they are not additional counts to add to the total. The Module 01 integration-file category contains four tests connected to real isolated Kafka/MQTT and eleven tests using local or mocked behavior.

| Category | Passed | Failed | Skipped |
|---|---:|---:|---:|
| Focused Module 01 unit and dataset tests | 182 | 0 | 0 |
| Module 02 consumer and bridge unit tests | 130 | 0 | 0 |
| Preprocessing compatibility tests | 20 | 0 | 0 |
| Module 01 integration test file | 15 | 0 | 0 |
| **Full `tests/` regression** | **347** | **0** | **0** |

The full post-correction run produced 239 warnings: 218 importlib-resource deprecations, seven kafka-python serializer/deserializer deprecations, two Paho callback API deprecations, and 12 scikit-learn warnings about loading existing 1.9.0 scaler artifacts with 1.9.1 in the disposable test environment. The scaler warning is a compatibility limitation, not proof that the versions are equivalent. No model or artifact was changed. The separate real-data probes above are observations, not additional pytest counts. The manually invoked IMS MQTT check is identified separately; other standalone `gap_tests/` scripts were not all rerun and are not included in the `tests/` total.

## Production safety comparison

Read-only checks at the start and after the tests showed the same Kafka topics and `sensor-telemetry` end offset 20,671; the same PostgreSQL table counts (24, 14, 20, 48, 6, 0, 3, 3 in the baseline table order); and the same InfluxDB aggregate field counts (`sensor_telemetry` 1,032,550; `rul_predictions` 88,710). The observed production container states and the five recorded production named volumes matched. These are sampled comparisons, not a complete content comparison. Only isolated test containers and their network were removed; volumes were retained.

**Operational finding:** at the start of this verification the production Kafka consumer-group list was empty, and describing `influxdb-writer-group` reported that it did not exist. The earlier Module 02 baseline had recorded that group and two others at offset 20,671. The group list remained empty in the post-test comparison. The cause was not established and no offsets were reset. The ingestion consumer is configured to use `earliest` when no committed offset exists, so a future production restart may replay retained telemetry; investigate the group state before restarting production consumers.

## Decision and limitations

**CONDITIONAL ACCEPTANCE AT THE TIME OF THIS VERIFICATION.** The fresh regression and the tested C-MAPSS Module 01-to-Module 02 path passed, and the approved IMS correction passed its targeted and real-data checks. At that time the correction and its regression test were uncommitted and had not been incorporated into a new Module 01 frozen baseline. The missing production consumer groups needed operator investigation. Abrupt host or broker failures, a real OS-signal interruption of a full replay, full real-broker replay of every dataset row, and a third-archive snapshot extraction were not verified here. IMS-to-InfluxDB remains out of scope and unimplemented.
