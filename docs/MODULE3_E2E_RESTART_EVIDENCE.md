# Module 03, isolated end-to-end benchmark and real process restart

This follow-up to PR #24 uses **two actual Kafka consumers** and real InfluxDB inside the isolated Module 03 CI Compose project. It does **not** deploy the optional batch writer into production, modify the original Module 02 consumer, change live offsets, or use production network/credentials/volumes.

## Benchmark protocol

The isolated test sets up **two additional Kafka topics and consumer groups**:

| Worker | Topic | Consumer group | Batch size |
| --- | --- | --- | --- |
| Accepted legacy worker | `m3-e2e-legacy-telemetry` | `m3-e2e-legacy-group` | 1, synchronous |
| Optional batch worker | `m3-e2e-batch-telemetry` | `m3-e2e-batch-group` | 20, synchronous |

The test publishes 120 telemetry messages with the same schema for each worker, sequentially, and measures elapsed time from the start of source publication to **both** confirmed InfluxDB storage of 120 points and a Kafka committed next-offset of 120. It reports individual mode throughput and an **indicative** batch/single-point rate ratio to CI logs.

This is a short functional performance sample using GitHub-hosted CI and a small input workload. It includes Kafka publication, consumer execution, InfluxDB storage, and Kafka offset commits, but it does not measure representative sustained load, p95 latency, backlog under contention, resource use, or statistically repeatable production throughput. **A faster result is not a required test condition**. Correct persisted identities and offsets are required.

## Real SIGKILL and restart test

After the comparative phase:

1. CI sends SIGKILL to **only** the isolated batch worker service, without stopping Kafka, InfluxDB, or the legacy worker.
2. It publishes 20 additional records to the batch topic while its worker is stopped.
3. It checks that the batch group's committed offset stays at 120 and there are still only 120 stored points during that outage.
4. CI restarts that batch worker without resetting its group or offsets, then waits for exactly 140 stored points and committed offset 140.
5. It verifies the independent legacy topic/group still has 120 stored points and a committed offset of 120.

This validates recovery after a process was killed while **idle after confirmed commits**. It **does not** establish crash safety for the narrow interval between InfluxDB accepting a batch and Kafka confirming its commit. That risk is covered with mocked ambiguous-ack/commit fault tests, but a real-service in-flight kill test is still outstanding. It does not simulate consumer rebalances across multiple active workers or partial writes.

## Safety

- Test-only services are defined exclusively in `docker-compose.module3-verification.yml` and start only within its CI job with Compose project `pms-ci-m3`.
- No production Compose edits. No production worker activation, offset reset, bucket retention changes, data deletion, artifact changes, or production secrets.
- The existing isolated CI job still runs raw storage, Flux query, retention, downsampling, batch, and synchronous InfluxDB write microbenchmark probes.
- Never extrapolate the measured rate ratio as guaranteed production performance.

## Remaining production acceptance gates

Repeat the end-to-end comparison under sustained representative load on comparable hardware and evaluate p50/p95/p99 latency, throughput, memory/CPU, Kafka lag, retry rates, storage load and Influx cardinality. Run real-service fault injection during active Influx writes and pending commits, plus broker rebalances, DLQ outages, and partial-batch acceptance. Capture reconciliation and rollback evidence, then obtain separate explicit operational approval.
