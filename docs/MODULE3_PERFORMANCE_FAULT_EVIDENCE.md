# Module 03, performance and fault-injection evidence (isolated)

This change adds **tests and measurements only**. The frozen Module 02 consumer, optional Module 03 batch writer, production Compose, Kafka topic/group offsets, InfluxDB retention, and checkpoint/model artifacts remain unchanged.

## Batch fault-injection tests

`tests/test_module3_batch_faults.py` uses deterministic, mocked API failures to verify:

1. An ambiguous write acknowledgment (data accepted, network response lost) retries with the **identical point identities**. Kafka does not commit before a successful acknowledgment.
2. An Influx write that never confirms cannot advance a Kafka offset.
3. A failure after a confirmed Influx write but during Kafka commit stops processing; reconstruction of replayed points yields the same identities.
4. A failed DLQ acknowledgement blocks committing the malformed record and all later records, while preserving earlier durably handled prefix commits.
5. Commits for independent partitions use their own partition IDs and offsets.
6. Legacy single-point and opt-in batch writers produce the same measurement, tag, field, and source timestamp schema.

These are offline fault-injection tests, **not** a complete real-service crash, broker rebalance, partial-write or partition-outage test matrix.

## Real InfluxDB synchronous write microbenchmark

`tests/module3_write_benchmark.py` compares the real InfluxDB client synchronous **write API only**, using 120 point payloads per mode per round, two rounds with alternated ordering, and batch size 20. It runs against isolated CI InfluxDB and uses unique run IDs and the same measurement/tag/field schema. It verifies that every round persisted exactly 120 points. It reports, for each mode/round:

- Number of write requests, total time, records per second, mean and maximum write-call duration.
- Median record rates and their ratio across the two short rounds.

These measurements **do not include** Kafka producer, consumer poll, Kafka commit, downstream processing, retry costs, or network/hardware typical of production. They are not an end-to-end throughput comparison, not an SLA, not a statistically powered performance study, and do **not** justify a performance claim or production cutover.

## CI

The existing Module 03 isolated real-broker/persistence/query/retention/downsampling/batch checks still run. The offline regression suite additionally executes the fault-injection tests. The isolated InfluxDB job additionally executes the real write microbenchmark. CI fails if persistence counts are wrong, **not** if batching happens to be slower.

## Outstanding operational acceptance

Before enabling batch writing outside isolation, benchmark the legacy and batch **Kafka consumers** end to end on representative loads, with p50/p95 latency, records/second, consumer lag, CPU/RAM, retries, failure recovery and storage volume. Inject real process termination between write and commit, broker/group rebalances, multi-partition traffic, timeouts and partial-batch responses, then reconcile source offsets and stored point identities. Establish sign-off and a rollback plan. Never reset production offsets or enable bucket expiration without independent approval.
