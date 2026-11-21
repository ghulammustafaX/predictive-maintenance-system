# Module 03, partition-specific failure isolation

**Status: implementation and acceptance tests added; GitHub Actions verification pending.**

## Goal

A persistence, offset-commit, or dead-letter failure in one Kafka partition must not terminate the entire opt-in batch worker or prevent healthy assigned partitions from progressing. The accepted legacy Module 02 consumer remains frozen and remains the production default.

## Runtime design

### Single-threaded Kafka ownership

One thread owns all `KafkaConsumer` operations: `poll`, `pause`, `resume`, assignment inspection and offset commits. The batch worker subscribes with rebalance callbacks. This avoids calling a non-thread-safe Kafka consumer from background retry threads.

### Partition-local queues and ordering

- Each assigned partition has its own FIFO queue and pending operation.
- A poll result is enqueued by partition. A partition is paused for backpressure when its local queue reaches the configured batch size.
- The worker advances at most one bounded batch operation per partition at a time, using round-robin selection.
- It never processes a later record in a partition past an earlier uncommitted record.
- The queue is bounded by Kafka's configured `KAFKA_MAX_POLL_RECORDS` per poll; paused partitions stop receiving additional fetches while their local queue is drained or retried.

### Failure and retry stages

A pending partition prefix records its exact stage:

1. **Influx write:** one synchronous write attempt. On failure, only that partition is paused and the same deterministic point batch is retried after bounded exponential backoff.
2. **Kafka commit:** after InfluxDB confirms the write, the worker commits the next offset for that prefix. If the commit fails or its response is ambiguous, the worker retries the commit stage only, without re-writing the confirmed batch while it retains ownership.
3. **Dead-letter publication:** malformed records remain an ordering barrier. A valid prefix is written and committed first, then the invalid record is published to the DLQ and committed only after broker acknowledgement. DLQ and commit retries are isolated to that partition.

The retry delay starts at `PMS_PARTITION_RETRY_BASE_SECONDS` (default 1 second) and doubles up to `PMS_PARTITION_RETRY_MAX_SECONDS` (default 30 seconds). The runtime makes one storage/commit attempt per scheduling turn, and polls between attempts so healthy partitions and group heartbeats continue to be serviced.

### Reassignment and replay

On partition revocation, the worker discards that partition's uncommitted in-memory queue and pending stage. It does not attempt a stale commit from the revocation callback. The new owner starts from Kafka's committed offset and replays the uncommitted prefix. Deterministic InfluxDB measurement/tag/timestamp identities make replay converge to the same point identity.

This remains **at-least-once**. A crash or revocation after an InfluxDB write but before a confirmed Kafka commit can cause a duplicate write attempt. DLQ publication can also duplicate if its acknowledgement is ambiguous. Kafka offsets and InfluxDB writes are not one atomic transaction.

## Configuration

The new retry settings are optional and have safe defaults:

| Setting | Default | Meaning |
|---|---:|---|
| `PMS_PARTITION_RETRY_BASE_SECONDS` | `1` | First retry delay for a failed partition operation |
| `PMS_PARTITION_RETRY_MAX_SECONDS` | `30` | Maximum exponential backoff delay |

Existing opt-in requirements remain: `PMS_BATCH_WORKER_CONFIRM=opt-in-isolated-batch-worker`, batch size 2 through 200, and `KAFKA_MAX_POLL_RECORDS >= PMS_INFLUX_BATCH_SIZE`.

## Verification added

### Offline fault tests

- Partition 0 write failure pauses only partition 0 while partition 1 commits.
- Partition 0 commit failure retries only the commit stage, without repeating the already-confirmed InfluxDB batch.
- DLQ acknowledgement failure pauses only the invalid-record partition while a healthy partition commits.
- Revocation discards local uncommitted work so a new owner replays from the broker's committed offset.

### Real-service CI acceptance

A dedicated isolated two-partition Kafka topic, consumer group, InfluxDB bucket and coordination volume are added to the Module 03 Compose test stack. The test injects a failure before partition 0's InfluxDB write, verifies that partition 0 remains uncommitted and has no stored points while partition 1 reaches offset 3 with three stored points, then releases partition 0 and verifies both partitions reach offset 3 with exactly three deterministic points each.

The fault wrapper is test-only. It does not alter the production consumer, main Compose configuration, production topics, offsets, databases, volumes or model artifacts.

## Limits and release gates

- The worker uses synchronous writes on one consumer thread. A single slow client call delays the next poll until the configured client timeout returns; polling occurs between operations, not during a blocked network call.
- A persistently failing partition remains paused and retries indefinitely at the capped interval. Operator alerting and an explicit terminal/quarantine policy remain future work.
- The isolated real-service scenario injects a deterministic pre-write failure. It does not by itself establish behavior for every InfluxDB partial-write response, multi-broker outage, network partition, long-duration load or every Kafka assignor.
- Existing crash/replay tests continue to cover ambiguous write acknowledgements and at-least-once duplicate behavior.
- Production batch-writer cutover remains prohibited until the full CI suite passes, repeated failure/rebalance runs pass, sustained-load behavior is measured, monitoring and alerting are defined, and rollback is reviewed separately.

No production deployment, source-group handoff, offset reset, database migration or checkpoint change is authorized by this work.
