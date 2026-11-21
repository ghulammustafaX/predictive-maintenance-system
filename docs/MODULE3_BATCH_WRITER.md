# Module 03.4: Opt-in Kafka-to-InfluxDB batch writing

The existing `ingestion/src/kafka_consumer.py` is the **accepted Module 02 baseline** and remains the production default. This milestone adds a separate **experimental, opt-in** entrypoint: `ingestion/src/kafka_batch_consumer.py`. The main production `docker-compose.yml` is not modified. Merely merging this code will not deploy the new worker.

## Batch semantics

- Uses the existing Kafka payload validation, InfluxDB `build_point` schema and confirmed Kafka DLQ publication logic.
- A bounded Kafka poll reads at most `KAFKA_MAX_POLL_RECORDS`. Within **each Kafka partition**, a synchronous InfluxDB call writes no more than `PMS_INFLUX_BATCH_SIZE` points at once (default 20, supported 2 through 200).
- **No Kafka offset is committed until the entire valid batch has been acknowledged by InfluxDB**. Each partition's commit advances only to the offset following its last fully handled record.
- A malformed input is a barrier: write and commit any earlier valid batch, send the malformed record to the DLQ with confirmed broker acknowledgement, then commit that record. Processing later messages cannot skip over an unhandled record.
- A failed/ambiguous Influx write or failed offset commit stops processing rather than acknowledging unwritten data. Replay can produce at-least-once duplicate attempts; deterministic Influx point identity gives same-point overwrite/merge behavior, not a cross-system exactly-once transaction.
- Synchronous bounded retries use the accepted consumer's retry count/backoff. 401/403 are not retried.
- The opt-in worker requires `PMS_BATCH_WORKER_CONFIRM=opt-in-isolated-batch-worker` and `KAFKA_MAX_POLL_RECORDS >= PMS_INFLUX_BATCH_SIZE`.

**Important:** The batch worker must NEVER run concurrently with the legacy worker in a different consumer group on the same production topic. Switching active consumers, offsets, consumer-group identity, live credentials, or deployment requires separate operational approval and a recovery plan. The string guard is an explicit opt-in guard, not a production authorization mechanism.

## What is verified

- Offline tests cover bounded batch sizes, offset commits after successful writes, invalid-message DLQ boundaries, failure without commit, no processing after commit error, 401 behavior, and opt-in startup.
- Real isolated CI uses its own `m3-batch-telemetry` topic, `m3-batch-dead-letter` topic and `m3-batch-writer-group` alongside the already-tested legacy consumer. It publishes valid records, one invalid record and an exact replay, then checks Influx persisted identities, DLQ publication and committed offset.
- CI runs the existing 251-test regression and 4 MQTT/Kafka integration tests plus the existing Module 03 storage, query and bucket-policy probes.

## Limits and remaining gates

This feature is **not automatically enabled in production** and has not established a real throughput improvement. Before considering rollout, benchmark an identical workload using the same isolated hardware, measure records/second, write latency, retry rates and resource use, and repeat crash/rebalance/partial-write failure recovery tests. Validate behavior under multiple partitions and different message ordering. Require operator approval and a plan for safe group handoff. No production data, volumes, checkpoints or offsets should be reset or touched by these experiments.

## Rollback

Leave the existing production consumer and its compose configuration unchanged. For an explicitly approved future deployment, reverting to the legacy entrypoint must preserve source topic and consumer-group committed offsets and account for at-least-once replay. Never rewrite Kafka offsets to force a rollback.
