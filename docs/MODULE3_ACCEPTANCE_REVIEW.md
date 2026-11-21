# Module 03 acceptance review, InfluxDB Time-Series Storage

**Review date:** 2026-11-21  
**Repository:** `ghulammustafaX/predictive-maintenance-system`  
**Verified base commit:** `3412a6b56c4505b14a439e12c8e001c571f330c7`  
**Verified post-merge CI run:** https://github.com/ghulammustafaX/predictive-maintenance-system/actions/runs/44902884465  
**Assessment:** **Development acceptance for the six Module 03 submodules, subject to the deployment/operational gates below. Production activation is NOT approved by CI alone.**

## Submodule acceptance

| Scope | Status | Verified evidence | Open limitation |
| --- | --- | --- | --- |
| 3.1 Kafka Consumer | Implemented, isolated integration verified | Real Kafka -> existing consumer -> InfluxDB persistence; manual offsets after persistence or confirmed DLQ, replay checks | Crash, rebalances, long-duration soak, multi-partition load still need targeted acceptance |
| 3.2 Bucket and Retention Policy Management | Implemented as manual, create-only tool; isolated verified | Existing bucket checked without modification, explicit confirmation required to create new buckets, existing retention mismatches refused | No automatic production retention policy; intentional to prevent data expiry without review |
| 3.3 Tag and Field Schema | Documented and isolated verified | `sensor_telemetry` schema, dataset/unit/run/cycle identities, sensor fields, timestamps | `cycle` stored as tag introduces cardinality risk; schema migration not authorized |
| 3.4 Batch Write Optimization | Implemented as **opt-in candidate**, isolated verified | Bounded synchronous batch writer with per-partition commit-after-ack, invalid-message DLQ barriers, offline tests and real broker test | Not default; no comparative throughput benchmark, soak/load/crash/rebalance or partial-write failure matrix |
| 3.5 Flux Query API | Implemented, isolated verified | Run filtering, channel selection, dataset/engine filtering, bounded query-time aggregation and invalid input rejection | Query-time aggregation is not persisted downsampling; performance under real cardinality still needs measurement |
| 3.6 Automated Downsampling | Implemented as **manual, create-only scheduled-task provisioning**, isolated verified | Real InfluxDB scheduled Flux task wrote expected mean to separate bucket, dropped cycle tag and preserved raw points | Not enabled in production; late data/backfill policy, task health, target retention and restoration plan not yet approved |

## Latest CI evidence

The post-merge run `44902884465` on `main` at `3412a6b56c4505b14a439e12c8e001c571f330c7` completed successfully:

- `Offline Python regression`: **258 passed**, 4 intentionally deselected (run in broker job); five subtests also passed. The report contained warnings, which were not treated as errors.
- `Isolated Kafka and MQTT integration`: **4 passed**, no failures, errors or skips.
- `Isolated Kafka and InfluxDB persistence`: passed including existing storage, Flux query, bucket policy, automated downsampling and real batch writer checks.

### Resolved CI incidents

- Run `44901363549`, post-downsampling merge, originally failed because a Kafka/MQTT test did not receive expected telemetry. A targeted rerun subsequently passed; the timing root cause was **not conclusively established**.
- Run `44901870967`, post-batch-writer merge, had an initially cancelled attempt. Attempt 2 failed **one offline test** because the assertion imposed an exact ordering across independent Kafka partitions despite valid ordered commits within each partition. PR #22 changed **only the test** to check per-partition order and healthy progress. The correction merged in commit `3412a6b56c4505b14a439e12c8e001c571f330c7`; the next post-merge CI run `44902884465` passed.

Passing the final run resolves the known red pipeline, but does not establish that occasional timing-sensitive delivery failures cannot recur.

## Preserved safety boundaries

- Production `docker-compose.yml` remains unchanged by the Module 03 feature PRs, and the original Module 02 `ingestion/src/kafka_consumer.py` remains the default entrypoint.
- The new batch worker `ingestion/src/kafka_batch_consumer.py` is **not** activated in production.
- The standalone `storage/influx_init/bucket_policy.py` and `storage/influx_init/downsample_task.py` are **not** called automatically on production startup and never patch/delete existing resources.
- Isolated CI uses its own Compose project and InfluxDB; it is not a live production validation. No production offset rewind, volume deletion, bucket expiry, or model replacement is part of acceptance.
- At-least-once processing remains the reliability model. Acknowledged writes and Kafka commits are not one atomic transaction; replay can occur.

## Required gates before enabling new storage behavior in production

1. **Performance baseline:** On the same isolated setup and workload, compare current single-point consumer with the batch candidate. Record throughput, write latency percentiles, CPU/memory, errors, retries, and backlog/consumer lag; do not claim acceleration without measurements.
2. **Failure/recovery matrix:** Test crash between InfluxDB write and Kafka commit; ambiguous timeout/partial batch acceptance; unavailable InfluxDB; DLQ publish failure; consumer group rebalance; multi-partition interleaving and restarts. Verify durable handling before commit and replay identity.
3. **Storage policy:** Approve raw and aggregate bucket names, finite versus infinite retention and data archival. Be aware retention uses source timestamps and expiry can remove historical data.
4. **Downsampling operations:** Approve the aggregate interval, acceptable lateness, backfill approach, observability of task runs, alarms for task failures, and rollback/disable procedure. Verify aggregate counts for multiple engine runs.
5. **Production deployment change request:** Explicitly authorize any worker cutover or task creation, including verified group/topic identity, backup and rollback. Never run conflicting active workers or reset committed offsets as a shortcut.
6. **Acceptance evidence:** Attach a repeatable benchmark report, fault-injection logs, source/aggregate sample queries, the final deployment checklist and human sign-off.

## Decision

**Accept Module 03 for repository-level development/testing scope.** All six planned submodules have code or opt-in provisioning implementations, and the full CI suite is passing on the verified base commit. **Do not classify the optional batch writer, retention expiry or downsampling task as production-enabled or operationally validated.** Keep the accepted single-point consumer as production default until the above evidence and approval gates are met.

**Suggested next milestone:** Module 03 performance and fault-injection verification on a separate isolated branch, followed by signed operational acceptance, before progressing to the next PMS module.
