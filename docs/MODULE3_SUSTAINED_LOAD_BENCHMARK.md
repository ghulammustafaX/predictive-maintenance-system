# Module 03, sustained-load benchmark (isolated)

## Purpose

Compare the accepted single-point Kafka consumer with the opt-in partition-isolating batch consumer under the same controlled telemetry workload. This is a test-only acceptance harness. It does not enable the batch worker in production and does not modify the frozen Module 02 consumer.

## Isolation and safety

- The test runs only in `docker-compose.module3-verification.yml` under the dedicated Compose project `pms-ci-m3`.
- It uses the isolated Kafka broker and InfluxDB service already provisioned by the Module 03 CI job.
- It creates two dedicated four-partition topics and two independent consumer groups.
- The legacy and batch workers use separate run IDs in the same isolated InfluxDB bucket so their point counts can be reconciled independently.
- Test scripts refuse to start unless the expected isolated broker, InfluxDB URL, token, organization, bucket, and guard values are present.
- Production `docker-compose.yml`, production topics, consumer groups, offsets, volumes, credentials, buckets, and retention policies are not touched.

## Workload

| Parameter | Value |
| --- | ---: |
| Records per consumer | 3,000 |
| Target publication rate | 200 records/second per topic |
| Nominal publication duration | 15 seconds |
| Partitions per topic | 4 |
| Telemetry schema | Existing version 1.0.0 schema, including 21 sensor fields |
| Legacy consumer | Existing `ingestion/src/kafka_consumer.py` processing and commit functions |
| Batch consumer | Existing `PartitionIsolatingBatchWorker`, batch size 20 |
| Persistence | Synchronous writes to the same isolated InfluxDB instance |

The publisher sends paired records with the same sensor values and source timestamp to the two topics. The only intentional identity difference is the run/event identity needed to keep their InfluxDB points separate.

## Measurements

The test-only worker records:

- End-to-end latency from Kafka record timestamp to successful Kafka offset commit, p50, p95 and p99.
- Synchronous InfluxDB write-call latency, p50, p95 and p99.
- Effective write batch utilization, average, p50, p95 and maximum points per InfluxDB write call.
- Kafka commit-call latency, p50, p95 and p99.
- Records per second from first observed record to final successful commit.
- Process CPU time and peak resident memory.
- Per-partition committed offsets, end offsets, and final consumer lag.
- Exact persisted point count for each run ID.

## Acceptance checks

The benchmark passes only when both consumer modes:

1. Process exactly 3,000 records.
2. Persist exactly 3,000 unique source points in InfluxDB.
3. Produce one end-to-end latency sample per committed record.
4. Finish with all four partitions assigned and zero consumer lag.
5. Produce write and commit latency measurements.
6. Confirm the legacy consumer writes exactly one point per call and the batch candidate averages at least two points per write call, proving the workload actually exercises multi-point writes.
7. Exit successfully within the bounded worker timeout.

The initial 50-record/second run completed successfully but produced only 1.04 points per batch write on average. That did not sufficiently exercise batching and was not a fair throughput comparison. The benchmark rate is therefore raised to 200 records/second per topic and now asserts measured multi-point write utilization. The test still does not assert that batching must be faster. A single CI comparison is indicative, not a statistically powered performance study, SLA, production capacity claim, or 24-hour soak test. Shared CI hardware and concurrent workers can affect measured rates.

## Remaining reliability work

This benchmark does not replace the next repeated-failure acceptance matrix. That work must separately exercise repeated partition-local write failures, ambiguous commit acknowledgements, DLQ acknowledgement failures, and recovery while healthy partitions continue to make progress.

Production batch-writer cutover remains prohibited until repeated failure/rebalance testing, longer-duration load testing, monitoring/alerting, storage policy review, rollback validation, and explicit operational approval are complete.
