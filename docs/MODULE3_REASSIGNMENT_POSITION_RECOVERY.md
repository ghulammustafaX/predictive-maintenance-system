# Module 03 partition reassignment position recovery

The optional batch worker can receive an eager revocation callback for a
partition that the same consumer later retains. With the tested kafka-python
client, the local fetch position and pause state survive that reassignment.
Discarding the application queue without reconciling the position can therefore
leave fetched but uncommitted records beyond the committed offset.

On revocation the worker marks partitions with discarded work. The assignment
callback records ownership only; it does not make blocking Kafka calls on the
client's network event loop. On the polling thread, the worker pauses marked
partitions, fetches the broker's current committed offset and local position,
and seeks to that offset when the local position differs. This also avoids
processing behind a newer confirmed broker commit. If no offset is committed,
it requests the configured earliest/latest reset policy; an unsupported policy
fails closed. Work returned by a poll before reconciliation is discarded and
retrieved again after the seek. Failed lookups leave the partition paused for
retry while other partitions remain eligible to progress. A progress identity
and current assignment check protects against acting on an already replaced
partition state. A write completed during revocation is not followed by a
stale owner's input commit. No broker offset is changed by the seek.

In a guarded real Kafka reproduction the same consumer's public position was 6
and committed offset 3 with queued offsets 3, 4 and 5. Its revocation callback
discarded that queue; the reassignment callback's local state retained position
6. The next public position remained 6 with committed offset 3. Callback
snapshots use nonblocking client-local state: blocking public Kafka APIs inside
the callback would block the client's network event loop. After the fix, the
same-owner reproduction logged position 6 and committed offset 3, sought to 3,
then re-fetched and committed through 6.

Two further guarded, independent Kafka/InfluxDB runs forced six partition-0
records into the queue before the first write, committed the first three, and
rebalanced while the remaining three were queued. In `same4` the original
consumer retained partition 0, discarded three queued records, and logged
`M3_POSITION_RECOVERED ... position=6 committed=3`. In `transfer4` the original
owner discarded three queued records and the peer received partition 0. Each
case published six records to partition 1; both partitions ended at committed
and end offsets 6/6, with cycles 1–6 and six distinct stored points each.
These point counts cannot measure overwritten repeated writes. The preserved
original failed workers and their topic/group were not restarted or altered.
The final `same5` and `transfer5` reruns exercised the same guarded boundary
after the ownership and shutdown-status checks were added, again reaching 6/6
and six distinct point identities on each partition. During shutdown, a
partition still awaiting position reconciliation remains incomplete and is left
uncommitted for a later owner to replay.

After adding the check that a local position behind a newer committed offset
seeks forward rather than committing backwards, fresh `same6` and `transfer6`
runs again discarded three queued partition-zero records at revocation and
recovered both partitions to 6/6 with cycles 1–6 and zero duplicate cycle
identities. The full offline suite reported 308 passed and four broker-dependent
tests deselected. The separate Kafka/MQTT rerun had one intermittent direct
Kafka delivery failure and three passes; it is not a clean overall regression.

This remains at-least-once processing, not exactly-once delivery. A committed
offset is a broker snapshot, and a failed lookup blocks only that partition
until retry. Long synchronous broker or storage operations can delay polling;
the cooperative shutdown limitations in `MODULE3_GRACEFUL_SHUTDOWN.md` still
apply. Recovery after retention deletes the desired input offset follows the
underlying consumer's configured reset behavior; it cannot reconstruct data
that Kafka no longer retains.
