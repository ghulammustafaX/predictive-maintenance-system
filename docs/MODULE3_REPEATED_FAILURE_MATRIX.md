# Module 03, repeated-failure acceptance matrix

## Scope

This matrix extends the isolated Module 03 acceptance tests. It exercises repeated failures and recovery without enabling the batch worker in production.

## Real Kafka + InfluxDB partition-local outage

The dedicated partition-fault worker injects three consecutive synchronous InfluxDB write failures for partition 0. The probe waits until all three failures are observed and verifies:

- Partition 0 remains paused, its committed offset does not advance, and no points from the affected batch appear in InfluxDB.
- Healthy partition 1 persists all three points and commits its next offset while partition 0 is repeatedly failing.
- After the explicit release marker, partition 0 retries, persists its three deterministic points, and commits.
- Both partitions converge to exactly three points and their expected committed offsets, with no skipped cycles.

The failure counter and release marker live only on the disposable Module 03 coordination volume. The probe requires exact isolated Kafka and InfluxDB settings.

## Offline repeated-failure matrix

| Failure scenario | Injected behavior | Required invariant |
| --- | --- | --- |
| Repeated InfluxDB write failure | Partition 0 write fails three times, then recovers | Only partition 0 pauses; healthy partition 1 commits; failed source offsets remain unchanged until persistence succeeds |
| Repeated ambiguous Kafka commit acknowledgement | Partition 0 commit fails three times after a confirmed InfluxDB write | Confirmed points are not rewritten during commit retries; no offset advances until commit succeeds; healthy partition progresses |
| Repeated dead-letter acknowledgement failure | Invalid partition-0 record receives three failed DLQ acknowledgements, then succeeds | Invalid source offset is not committed until DLQ publication is acknowledged; healthy partition progresses |
| Existing crash after write, before commit | Worker is killed after durable InfluxDB write but before Kafka commit | Restart replays the batch, deterministic point identity prevents duplicates, and the source offset advances only after successful recovery |
| Existing consumer-group rebalance during pending work | Second member joins while the first partition has a durable but uncommitted batch | Ownership transfers safely, replay is idempotent, both partitions converge, and group lag returns to zero |

## Interpretation

Passing these tests demonstrates recovery behavior for the injected failure sequences in the isolated test environment. It does not establish production availability, eliminate every possible network failure mode, or approve a production worker cutover.

Production activation remains blocked until sustained-load results, failure/rebalance evidence, operational monitoring, storage policy, rollback, and explicit deployment approval are reviewed together.
