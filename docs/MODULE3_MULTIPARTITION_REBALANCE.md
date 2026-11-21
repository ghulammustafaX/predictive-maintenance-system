# Module 03, multi-partition rebalance and durable-prefix recovery

<badge>Verified in isolated GitHub Actions</badge>

**Status: PASSED.** The final workflow run was [44917967431](https://github.com/ghulammustaX/predictive-maintenance-system/actions/runs/44917967431), on branch commit `a25b4c37919d878b4ba96904d04360a70677a835`. All three workflow jobs ultimately passed. The isolated Kafka/MQTT job had one transient no-message failure on its first attempt; rerunning only that failed job passed all four tests. The offline regression and Module 03 persistence job passed on their initial attempt.

## Scope and observed Kafka behavior

The opt-in batch consumer was exercised with a real two-partition Kafka topic and isolated InfluxDB. The first worker processed partition 0. After its first three-record synchronous InfluxDB write was acknowledged, it paused. A second worker joined the same Kafka consumer group, and the broker reported two members in `PreparingRebalance`.

The probe then published six records to partition 1 and released the first worker. It verified that the broker was still in the observed rebalance state, then committed the already-persisted partition-0 prefix. The worker processed the rest of its polled prefix and called `poll` again, completing reassignment. The two active workers then processed their assigned partitions.

## Verified evidence

- **Offline Python regression:** 264 passed, 4 broker-dependent tests deselected, 218 warnings, 5 subtests passed.
- **Isolated Kafka and MQTT integration:** 4 passed, 0 failed, 0 skipped, 227 warnings. This job passed on its isolated rerun after an initial transient no-message failure.
- **Module 03 isolated Kafka and InfluxDB job:** passed, including existing batch, benchmark, restart, and in-flight crash checks.
- The broker reported `PreparingRebalance` with two group members while the first worker was paused after an acknowledged InfluxDB write.
- The first worker committed partition 0's durable prefix at next-offset **3** before its next poll processed reassignment.
- Partition 0 then completed at committed/end offset **6/6**, with exactly **6** unique InfluxDB points for its run ID.
- Partition 1 completed at committed/end offset **6/6**, with exactly **6** unique InfluxDB points for its run ID.
- The group returned to `Stable` with two members. Both partitions had zero lag.
- The isolated Compose services were torn down by the workflow. No production services, offsets, databases, volumes, retention policies or model artifacts were touched.

## Important interpretation

The first probe attempted to force a broker rejection while the group reported `PreparingRebalance`. Kafka accepted the commit. This is expected when the old worker has not yet polled to process revocation and the synchronous InfluxDB write has already completed. It did **not** establish stale-member fencing. The corrected test asserts the safe ordering: persist, commit the durable prefix, then poll and complete reassignment. The test and report do not claim that a rebalance-start notification alone fences a commit.

## Delivery semantics and limits

The expected recovery behavior is **at least once**. Deterministic InfluxDB point identity makes replay of an already acknowledged batch converge to the same stored points. This does not create an atomic transaction between Kafka and InfluxDB and does not prove exactly-once delivery.

This test covers a broker-observed group rebalance while a synchronous InfluxDB write is in progress, and verifies that the acknowledged prefix is committed before the worker polls to complete reassignment. It does not cover output-failure isolation between partitions, all cooperative assignor strategies, multi-broker failure, network partitions during an InfluxDB request, server-side partial batch acceptance, or sustained production load.

**Production batch-writer cutover remains prohibited.** Remaining gates include per-partition failure isolation, repeated rebalance/failure runs, sustained-load testing, operator monitoring and rollback validation.
