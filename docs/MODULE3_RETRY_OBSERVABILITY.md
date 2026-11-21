# Module 03, retry observability and operator alerts

## Scope

The **optional**, partition-isolating Kafka batch writer now exposes in-memory retry health and logs actionable warnings for persistent partition failures. It is **not** a production deployment, metrics server, UI alert, email, or external pager integration. The frozen legacy Module 02 consumer, production Compose configuration, Kafka source topics, consumer offsets, InfluxDB buckets and model artifacts are unchanged.

## Operator signals

| Log marker or metric | Meaning |
| --- | --- |
| M3_PARTITION_ALERT (WARNING) | One alert per unresolved partition failure incident when either retry count or elapsed failure duration meets a threshold |
| M3_PARTITION_RECOVERED (INFO) | Failed partition finished its pending operation and recovered, including retry count and downtime |
| M3_PARTITION_METRICS (INFO) | Periodic JSON snapshot of in-memory partition state (disabled if interval is 0) |
| total_failures_by_stage | Cumulative observed exceptions in write, commit_valid, dlq, or commit_dlq |
| alerts_emitted, recoveries | Cumulative counts for the lifetime of this worker process |
| failing_partitions | Number of currently assigned partitions paused by a failed operation |
| queued_records | Number of records currently queued in memory across assigned partitions |
| revocations_with_pending_work | Reassignment events that discarded queued or pending uncommitted work for safe Kafka replay |
| Per partition retry_attempt, failure_age_seconds, retry_wait_seconds, stage | Active failure details, remaining backoff time, and pending operation stage |
| Per partition failure_paused, backpressure_paused, alert_emitted | Separate failure and backpressure state, plus alert deduplication |

metrics_snapshot() returns local in-memory state only, and does not make network requests. **Queued records are not Kafka lag.** The broker's committed and end offsets must be measured independently to report Kafka lag.

## Configuration

All settings affect only the opt-in batch writer:

| Environment variable | Default | Behavior |
| --- | --- | --- |
| PMS_PARTITION_ALERT_RETRY_THRESHOLD | 3 | Emit one operator warning after this many failed attempts on an unresolved partition |
| PMS_PARTITION_ALERT_AGE_SECONDS | 60 | Emit one warning when a failure has persisted for at least this many seconds, even during backoff |
| PMS_PARTITION_METRICS_LOG_SECONDS | 60 | Log JSON snapshots at this interval, 0 disables periodic snapshots |

Retry and age thresholds must be positive. Logging interval must be nonnegative. Existing PMS_BATCH_WORKER_CONFIRM=opt-in-isolated-batch-worker and other safe configuration gates remain mandatory.

### Example log events

    M3_PARTITION_ALERT topic=m3-partition-fault-telemetry partition=0 stage=write retries=3 failure_age_seconds=2.000 queued_records=3
    M3_PARTITION_RECOVERED topic=m3-partition-fault-telemetry partition=0 retries=3 downtime_seconds=3.000 alert_emitted=True

These markers are intended for filtering in container logs or a future external monitoring service. This change **does not** automatically create entries in the PMS application's existing Alerts module.

## Alert lifecycle and safety

- Only one alert per ongoing failure incident per partition, with retries continuing at the existing bounded exponential backoff.
- Once the partition completes its pending operation successfully, log recovery and reset that partition's active incident state.
- Alerts are counted separately from recoveries, and failures are counted by the exact pending stage. Alerts do not commit or skip offsets and do not alter persistence/DLQ behavior.
- Kafka partition revocation discards local state for safe replay and counts it when work was outstanding. A new owner begins with fresh local incident state.
- Backpressure and local queue occupancy are explicitly distinguished from output failures.
- These process-local counts reset on worker restart. They are not durable histories, on-call pages, per-engine alert notifications, or globally aggregated metrics.
- Existing synchronous I/O may temporarily delay polls while a network call blocks.

## Acceptance tests

The offline test suite validates deduplicated alert emission at the retry threshold, age-based alert during a retry wait, recovery and cleared active state, stage-specific commit failures, healthy partition progress, rebalance cleanup, safe configuration and failure count preservation. Existing real Kafka/InfluxDB integration tests continue to inject repeated partition-local write failures and validate recovery.

Production batch-worker activation remains blocked pending graceful-shutdown/rollback checks and final operational readiness review. External alert integrations, if needed, require separate design and authorization.