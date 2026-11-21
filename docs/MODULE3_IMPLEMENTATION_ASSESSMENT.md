# Module 03 implementation assessment

Assessment date: 21 November 2026  
Scope: `PMS-Scope.txt`, Module 3, Time-Series Storage Module (InfluxDB).  
Reviewed repository baseline: `ef2a247e7b2f6ef589fd0b08cce112ebc3e9aba6` (post-PR #15 `main`).  
Working branch: `feature/module-03-influxdb`.

**Status: preliminary static source audit, not a completed real-service verification.** No production services, topics, offsets, data, volumes, or artifacts were changed. The frozen Module 02 source was not edited.

## Evidence and submodule matrix

| Scope submodule | Assessment | Repository evidence and remaining work |
| --- | --- | --- |
| 3.1 Kafka Consumer | **Implemented, needs real InfluxDB regression verification** | `ingestion/src/kafka_consumer.py` polls `sensor-telemetry`, validates payloads, builds `sensor_telemetry` points, retries synchronous Influx writes, confirms invalid-record DLQ publication, then manually commits each source offset. This is **at-least-once**, not exactly-once. `tests/test_ingestion_consumer.py` covers consumer behavior with mocks; current CI's isolated broker job does not establish end-to-end InfluxDB persistence. |
| 3.2 Bucket & Retention Policy Management | **Partial** | `docker-compose.yml` initializes InfluxDB 2.7 with an environment-selected initial bucket and a named persistent data volume. `storage/influx_init/SCHEMA.md` claims 90-day retention, but no tracked implementation of retention management was verified. `storage/influx_init/setup.sh` is empty. Actual existing bucket settings must be inspected only in an isolated environment before implementing any provisioning. |
| 3.3 Tag and Field Schema | **Implemented, documentation outdated** | `build_point()` sets measurement `sensor_telemetry`, tags `dataset_id`, `unit_id`, `run_id`, `cycle`, and adds cycle, operating settings and present sensor fields. Point time comes from `source_timestamp` or `timestamp`. `SCHEMA.md` incorrectly documents only `unit_id` as a tag and auto-generated timestamps. Review the `cycle` tag's indexing/cardinality tradeoff before any schema migration. |
| 3.4 Batch Write Optimization | **Not implemented for the raw Kafka-to-InfluxDB consumer** | `create_influxdb_client()` uses `SYNCHRONOUS` writing; `write_to_influxdb()` submits one point per message and `consume_and_write()` commits after each message. `KAFKA_MAX_POLL_RECORDS=5` controls polling, not Influx write batches. Any batching must preserve confirmed durability, DLQ semantics, order and offset safety, including partial-failure recovery. |
| 3.5 Query API | **Partially implemented** | `backend/app/routers/fleet.py` provides JWT-protected fleet status and per-engine telemetry via Flux, bounded time windows, selected sensor channels, and query-time `aggregateWindow`. `backend/app/routers/rul.py` reads prediction measurements and supports per-run history. `docs/API.md` documents routes. A cohesive Module 03 query contract, cross-run semantics, safety/performance limits and real InfluxDB integration coverage still need verification. |
| 3.6 Data Downsampling | **Not verified as automated persisted downsampling** | The telemetry endpoint uses Flux `aggregateWindow` at read time. This is **query-time result reduction**, not an InfluxDB scheduled task storing historical aggregates. No tracked automated downsampling task was verified in the reviewed storage setup and source files. |

## Additional findings

1. `storage/influx_init/setup.sh` is a zero-byte placeholder. Do not assume its provisioning steps currently run.
2. `SCHEMA.md` must be reconciled with the live writer before a new schema is adopted. Influx point identity includes measurement, tag set and timestamp; retries may rewrite/merge field values for identical identities, but no cross-system atomic exactly-once guarantee exists.
3. The Compose InfluxDB named volume is persistent. Avoid editing live bucket retention or adding destructive setup to the existing deployment.
4. The frozen ingestion source must not be changed silently to implement batching. Seek explicit compatibility approval and demonstrate failure/recovery behavior first.
5. The recent CI passes 251 offline and 4 isolated Kafka/MQTT tests, but it does not prove isolated Kafka-to-InfluxDB full-path persistence or scheduled retention/downsampling.

## Proposed implementation sequence (separate reviewed PRs)

1. **Documentation and tests first:** reconcile `SCHEMA.md`; define bucket/retention policy, measurement contracts, timestamp handling, tags, query semantics and the expected behavior of duplicate input. Add isolated InfluxDB integration test scaffolding that does not touch production.
2. **Query layer:** verify/filter bounded Flux queries on isolated InfluxDB using data that separates datasets, units, runs and cycles; add query tests and documentation. Preserve the existing authenticated endpoints.
3. **Provisioning and retention:** add idempotent, explicit bucket/task provisioning for isolated or new installs, with configuration validation and non-destructive behavior for existing volumes. Require approval for any existing bucket changes.
4. **Persistent downsampling:** implement and test scheduled aggregate tasks into distinct buckets/measurements as specified by an agreed retention plan. Prevent duplication and preserve raw data until retention approval.
5. **Optional batching optimization:** benchmark the current synchronous write baseline; change ingestion only if measurements justify it and compatibility/recovery tests are approved. Prove no early Kafka commit, correct per-partition handling and bounded retries/backpressure.

## Entry and acceptance gates

- Open PRs from `feature/module-03-influxdb` or smaller branches based on updated `main`; required GitHub Actions checks must pass.
- Use unique isolated Kafka topics/groups and isolated InfluxDB org/buckets/volumes, and verify Compose isolation before starting containers.
- For storage-affecting code, run actual Kafka + InfluxDB success/failure/replay tests; MQTT is additionally required if its path changes.
- Verify bucket provisioning, retention, chronological/range queries, tag and field filters, dataset/unit/run isolation, query bounds, duplicate handling, aggregation timestamps and downsample correctness.
- Preserve current production data and checkpoints; no offset resets, bucket deletion, destructive migrations or deployment without separate authorization.
- Keep the immutable `pms-reliability-baseline-v1.0` as the accepted recovery reference.

## Unresolved questions for implementation design

- The required retention periods for raw and aggregate buckets, and whether old data may ever be automatically expired.
- Target downsampling intervals (for example minute/hour) and target accuracy/aggregations.
- Whether ingestion batching is necessary given measured message throughput, and whether it can be implemented without altering the frozen Module 02 behavior.
- Whether per-run cycle tags should stay in the indexed schema or move to fields for future datasets; no unapproved schema change.
