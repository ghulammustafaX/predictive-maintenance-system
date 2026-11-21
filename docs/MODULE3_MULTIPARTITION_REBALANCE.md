# Module 03, multi-partition rebalance and stale-commit recovery

**Status: CI acceptance pending.** This document describes the isolated test added on the dedicated branch. It must not be treated as passed until the linked GitHub Actions run completes successfully.

## Scope

Exercise the opt-in batch consumer with a real two-partition Kafka topic and isolated InfluxDB. The scenario pauses a worker after its first synchronous InfluxDB batch has been acknowledged, starts a second worker in the same Kafka consumer group, and waits until the broker reports two members in `PreparingRebalance`.

While the original worker is still holding the first partition's batch in memory, the test releases it to attempt the corresponding source offset commit. The broker must reject the old generation's commit. The original worker then exits, and the peer worker takes over, replays the uncommitted first partition, and processes the second partition.

## Acceptance conditions

1. The topic is created with exactly two partitions in the isolated CI Compose project.
2. The first worker stores the first three records from partition 0 in InfluxDB, but has not committed the source offset.
3. A second batch worker joins the same group while the first worker is paused. The Kafka Admin API must report `PreparingRebalance` and at least two group members.
4. The test publishes six records to partition 1 while the group is rebalancing.
5. The original worker's attempted commit must be rejected with a recognized Kafka generation/rebalance error. An accepted commit is a test failure.
6. After the old worker exits, the peer worker must process both partitions.
7. Both partitions must converge to committed offset 6 and end offset 6.
8. InfluxDB must contain exactly six deterministic points for each partition's unique run ID. The replay of partition 0 must not create additional point identities.

## Isolation

The test uses only `m3-rebalance-telemetry`, `m3-rebalance-group`, the disposable `m3-isolated-bucket`, and a dedicated Compose coordination volume under project `pms-ci-m3`. The fault wrapper and probe are test-only. The batch worker source, production Compose, production Kafka offsets, databases, retention policies and model artifacts are not changed by this test.

## Delivery semantics and limits

The expected recovery behavior is **at least once**. The deterministic InfluxDB point identity makes a replay of an already acknowledged batch converge to the same stored points. This does not create an atomic transaction between Kafka and InfluxDB and does not prove exactly-once delivery.

The test covers a broker-observed group rebalance while an InfluxDB write is already acknowledged and the source offset is still pending. It does not cover every cooperative assignor, multi-broker failure, network partitions during an InfluxDB request, server-side partial batch acceptance, or sustained production load.

Production batch-writer cutover remains prohibited pending broader sustained-load tests, repeated rebalance/failure runs, operator monitoring and rollback validation.
