# Module 03, real in-flight write/commit crash-replay evidence

**Scope:** isolated CI only. This milestone builds on the real idle-worker SIGKILL restart test from PR #25.

## Fault point and recovery contract

A test-only Python worker wrapper, `tests/module3_inflight_kill_worker.py`, invokes the unchanged opt-in `kafka_batch_consumer.py` and replaces its `persist_points` callable **in the test worker process only**. It first calls the original synchronous InfluxDB write function. **Immediately after that call returns successfully**, it emits a marker and sends SIGKILL to its own process, preventing `handle_partition` from reaching the Kafka offset commit.

The dedicated isolated source topic has one partition and six valid records, with unique identities matching the accepted telemetry schema. A separate read-only test process verifies:

1. InfluxDB contains all **6** raw points from the source records after the fault.
2. The Kafka committed next-offset remains **absent or 0**, because the write-confirmation/commit gap was deliberately interrupted.
3. The killed worker's container exit code is 137, and its log contains the post-Inlux-acknowledgment crash marker.
4. A **new, unmodified batch-worker process**, started with the same isolated topic/group, replays the six messages and commits next-offset **6**.
5. The source measurements still contain exactly six unique points, confirming deterministic replay identity rather than six additional independent points.

The fault worker and recovery worker are never started concurrently. No production Compose or ingestion source files are modified.

## Boundary and limitations

This specifically covers a real SIGKILL at the **after-synchronous-write-return, before-Kafka-commit** boundary. It validates at-least-once replay behavior and prevents an unhandled message from being acknowledged. It does **not** prove cross-system exactly-once processing, nor does it cover killing the process while the InfluxDB API request is in flight, server-side partial batch acceptance with timeout, broker rebalances across multiple consumers, arbitrary power loss, or multi-partition failure behavior.

All test resources are confined to the isolated `pms-ci-m3` Compose project with its own topic, consumer group and InfluxDB. No existing production Kafka offsets, volumes, tasks, buckets, retention policies, or model artifacts are touched.

**Production gate remains open:** repeated large-load comparisons, real Kafka group rebalance under multiple partitions, in-flight network partition and partial write response injection, task monitoring, rollback and operator approval.
