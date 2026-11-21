# Module 03, multi-partition rebalance and durable-prefix recovery

**Status: CI acceptance pending.** The first probe intentionally attempted to commit while the group was in `PreparingRebalance`. Kafka accepted that commit. This is expected for a worker that has not yet polled to process the revocation and whose synchronous InfluxDB write has already completed. The test was corrected to verify the safe ordering rather than incorrectly expecting a broker rejection at that point.

## Scope

Exercise the opt-in batch consumer with a real two-partition Kafka topic and isolated InfluxDB. The first worker processes partition 0. After its first three-record synchronous InfluxDB write is acknowledged, it pauses. A second worker joins the same Kafka consumer group, and the probe waits for the broker to report two members in `PreparingRebalance`.

The probe then publishes six records to partition 1 and releases the first worker. The worker verifies that the broker is still in the observed rebalance state, commits the already-persisted partition-0 prefix, processes the rest of its polled prefix, and calls `poll` again. That poll completes reassignment. The two active workers then process the remaining assigned records.

## Acceptance conditions

1. The topic is created with exactly two partitions in the isolated CI Compose project.
2. The first worker stores the first three records from partition 0 in InfluxDB, without committing before the write acknowledgment.
3. A second batch worker joins the same group while the first worker is paused. The Kafka Admin API reports `PreparingRebalance` and at least two group members.
4. The test publishes six records to partition 1 while the group is rebalancing.
5. The first worker confirms the group is still in `PreparingRebalance` and commits the already-acknowledged partition-0 prefix. This commit is expected to succeed before the worker's next poll processes revocation.
6. The group completes reassignment and stabilizes with two active members.
7. Both partitions converge to committed offset 6 and end offset 6.
8. InfluxDB contains exactly six deterministic points for each partition's unique run ID. No records are lost, and the replay-safe point identities remain stable.

## What the initial probe established

The first probe observed a successful offset commit while the broker reported `PreparingRebalance`. It did **not** establish stale-member fencing, because the old worker had not yet called `poll` and therefore had not processed its revocation. The corrected probe now asserts the actual safe ordering: persist, commit the durable prefix, then poll and complete reassignment. This distinction avoids claiming that Kafka rejects a commit merely because a rebalance has started.

## Isolation

The test uses only `m3-rebalance-telemetry`, `m3-rebalance-group`, the disposable `m3-isolated-bucket`, and a dedicated Compose coordination volume under project `pms-ci-m3`. The fault wrapper and probe are test-only. The batch worker source, production Compose, production Kafka offsets, databases, retention policies and model artifacts are not changed.

## Delivery semantics and limits

The expected recovery behavior is **at least once**. The deterministic InfluxDB point identity makes a replay of an already acknowledged batch converge to the same stored points. This does not create an atomic transaction between Kafka and InfluxDB and does not prove exactly-once delivery.

This test covers a broker-observed group rebalance while a synchronous InfluxDB write is in progress, and verifies that the acknowledged prefix is committed before the worker polls to complete reassignment. It does not cover output failure isolation between partitions, all cooperative assignor strategies, multi-broker failure, network partitions during an InfluxDB request, server-side partial batch acceptance, or sustained production load.

Production batch-writer cutover remains prohibited pending broader sustained-load tests, per-partition failure isolation, repeated rebalance/failure runs, operator monitoring and rollback validation.
