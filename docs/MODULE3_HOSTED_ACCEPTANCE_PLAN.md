# Module 03 hosted acceptance preparation

The Kafka/InfluxDB job is intended for an ephemeral GitHub-hosted Ubuntu runner.
It checks for at least 4 GiB available memory, 10 GiB free disk and two CPUs
before starting Docker. These thresholds are a starting requirement, not proof
that a particular runner will perform reliably. The Compose project includes
the GitHub run ID and attempt number. The verification Compose file provides
separate services and network without production host-port bindings. Topic and
consumer group names used by older probes are fixed, but exist on that fresh
broker only; the two additional position cases also include the run identity.

The job uploads the preflight result, Compose logs, topic and consumer-group
descriptions, position probe output and any available sustained-load report.
It removes only its run-specific Compose project and its separately labelled
position-test containers and volumes. Runner disk usage and runtime still need
to be verified in GitHub Actions. Do not execute this workflow on a self-hosted
runner or against production.

| Scenario | Hosted status |
| --- | --- |
| Batch persistence and existing DLQ verifier | Existing CI steps |
| Crash after write, restart and replay | Existing CI steps |
| Two-partition rebalance and partition failure isolation | Existing CI steps |
| Sustained load, 3,000 records per consumer | Existing CI steps; JSON report collected |
| Same-owner and different-owner position recovery | New hosted orchestration; not yet exercised on GitHub |
| Graceful shutdown and exhausted-drain recovery | Hosted orchestration added; **not yet exercised on GitHub** |
| Per-scenario machine-readable offsets, Influx identities and DLQ correlation | **Incomplete**; existing probes emit a mixture of logs and assertions |

The shutdown helper waits for partition assignment and queued input before
SIGTERM. It captures committed/end offsets and observed cycle identities; the
exhausted case requires an incomplete nonzero shutdown and then replays the
uncommitted records. It does not count write attempts. Its synchronization,
cleanup behavior and results remain unverified on a real hosted runner.

The shared evidence helper records missing observations as null and generates
an acceptance summary that cannot pass when any required scenario is missing or
incomplete. Hosted shutdown snapshots now build identities from published
records and observed measurement, dataset_id, unit_id, run_id, cycle and UTC
timestamp; this remains unverified against a hosted service. Source duplicate
counts are not yet captured. The sustained-load verifier now records the full
identities of broker-acknowledged source records and compares them with Flux
identities in separate JSONL artifacts; hosted verification is still pending.
Batch, DLQ, crash and partition probes prepare shared-schema records; their
real-service output and DLQ source-to-record correlations remain unverified.
DLQ delivery is at least once: a crash after broker acknowledgement can yield
duplicate DLQ records on replay.

For sustained-load correctness, full point-identity equality, source partition
offsets and zero final lag are required; throughput is an observed measurement,
not a guaranteed improvement. A particular instantaneous local consumer
position is diagnostic and is not required to establish the final result. The
worker exit status and measured successful application write calls are recorded
when available; retries inside a call are not counted as separate HTTP attempts.
Final stored points cannot reveal how many identical writes were overwritten.
Each mode's shared-schema record validates its JSONL artifacts, all four source
partitions and a successful worker exit before it may pass. The combined
sustained-load summary independently re-reads both mode records, worker
reports and identity artifacts. It remains incomplete if either mode is
incomplete, and fails on a verified mismatch. The overall acceptance summary
revalidates a passing aggregate against the 3,000-record workload; the hosted
workflow has not yet exercised this path.

The hosted job must not be presented as final acceptance until its real-service
evidence and all required checks pass on the runner.
The previously observed intermittent direct Kafka delivery failure has not been
explained or fixed. Previous local evidence is 53 focused tests and 308 offline
tests passed, four Kafka/MQTT tests passed, and 3,000 records persisted per
consumer in sustained load. These results are not hosted-run evidence.

## Acceptance evidence and final gate

Mandatory correctness evidence is the scenario/project/commit and source-run
association, acknowledged source records, actual broker offsets, reconciled
full InfluxDB point identities, and the scenario's required fault or recovery
observations. Controlled process exit is mandatory when a scenario explicitly
tests shutdown or crash; it is not applicable to a batch worker deliberately
left running. Partition isolation requires a failure-window snapshot showing
the healthy partition advancing before release, followed by reconciled recovery.
Reassignment requires observed discarded source offsets, ownership transition,
final offsets and identities and controlled worker exits.

Instantaneous broker membership, local position and explicit seek measurements
are diagnostics when the broker offsets, observed transition and complete final
identity reconciliation establish correctness. A null diagnostic is never
reported as an observed value. DLQ delivery remains at least once; a replay can
duplicate DLQ records. Final InfluxDB identities do not expose repeated writes.
Shutdown is cooperative: its deadline cannot interrupt a synchronous operation,
and a returned consumer close does not independently prove broker departure.

Container-written evidence is owned by the hosted runner UID/GID when exported;
runner-side changes use atomic replacement in the runner-owned evidence
directory. The workflow checks that the runner can read and replace an artifact
after the batch verifier. A final dependent job independently revalidates the
downloaded scenario evidence and uses the actual regression, Kafka/MQTT and
Kafka/InfluxDB job results. A failure or cancellation fails the final result;
skipped, unavailable or incomplete required evidence cannot pass. A hard
workflow cancellation may prevent final artifact upload, but cannot yield a
passing gate. None of this is evidence of a hosted run until executed.

The guarded position runner and the hosted position and shutdown scripts and
probe must all be tracked before a branch push. Local test helpers use a fixed
Docker project and must not be executed unchanged on the hosted runner.
