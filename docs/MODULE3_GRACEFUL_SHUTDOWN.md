# Optional batch worker: shutdown limits

The optional Module 03 worker pauses its assigned partitions after SIGTERM or
SIGINT, polls while draining queued records, and commits only after a confirmed
InfluxDB write or dead-letter acknowledgement. It uses
`PMS_BATCH_SHUTDOWN_DRAIN_SECONDS` (default 30) as a best-effort deadline and
logs the topic, partition, head offset and processing stage for unfinished work.
Uncommitted records are replayed after restart or reassignment. InfluxDB point
identities are deterministic, but a lost acknowledgement can cause repeat
writes and DLQ publication can be duplicated; this is not exactly-once delivery.

The deadline is checked between operations and cannot interrupt an in-progress
synchronous operation. InfluxDB uses `INFLUX_TIMEOUT_MS` (default 10000), and
Kafka commits use `KAFKA_COMMIT_TIMEOUT_MS` (default 10000). The batch DLQ
producer uses `PMS_BATCH_DLQ_MAX_BLOCK_MS` (default 10000) for Kafka send and
waits up to 10 seconds for its delivery future. These are operation timeouts,
not a strict upper bound on process lifetime. After draining, producer flush and
close and Kafka consumer close share a separate `PMS_BATCH_CLEANUP_SECONDS`
budget (default 5 seconds); Kafka consumer close
explicitly disables auto-commit. The InfluxDB client's close method exposes no
timeout. Library internals, connection setup, a blocking call, signal-handler
delivery and operating-system scheduling can also exceed these limits.

`M3_SHUTDOWN_STATUS` reports the drain and each cleanup operation separately.
Incomplete drain, cleanup failure, exhausted cleanup budget, or Kafka errors
observed during cleanup yield a nonzero exit unless an original application
exception is already propagating. Kafka may log and suppress LeaveGroup errors;
a successful close return does not prove the broker accepted departure, which
is reported as unverified. An already blocked synchronous call or InfluxDB
close cannot be interrupted by this budget, so it is not a hard process deadline.

The frozen legacy consumer catches KeyboardInterrupt but has no SIGTERM handler.
During the isolated rollback it ran as PID 1 and Docker sent SIGKILL after its
15-second stop grace, producing exit 137. A later idle test with a 30-second
grace also ended with exit 137. The evidence does not establish an I/O blockage.
A longer grace period alone cannot guarantee termination. Keep the
batch worker stopped and verify legacy exit and group ownership before restart.

If a write or DLQ acknowledgement is uncertain, the source offset remains
uncommitted unless an already-running commit was accepted by the broker. A
commit timeout is ambiguous: the broker may have committed even when its reply
was lost. The commit is issued only after the required output was confirmed.
Do not rely on a forced container stop as a graceful stop; allow additional
termination time for the configured operation timeouts and verify broker offsets.

In an isolated Kafka/InfluxDB test, SIGTERM with an expired drain left offsets
15–17 uncommitted: the committed offset stayed at 15, the end offset was 18,
and no points for that run were present. The worker exited 1 and reported all
four cleanup operations completed, group departure unverified, and drain
incomplete. Kafka independently logged a successful LeaveGroup response. A
fresh worker replayed the records, reached committed/end offset 18/18, and
stored three distinct cycle identities; it exited 0 on a later SIGTERM. This
is a single controlled test, not proof of every shutdown failure mode, and does
not authorize production deployment.
