"""Run the existing rebalance acceptance with guarded, unique identifiers."""
import os
import re
import sys
import json
import time
from pathlib import Path

case = os.environ.get("M3_POSITION_CASE")
if os.environ.get("M3_POSITION_ACCEPT_GUARD") != "m3-position-acceptance-only" or not case or not re.fullmatch(r"(?:same|transfer)[0-9]{0,24}", case):
    raise SystemExit("Refusing unexpected acceptance guard or case")
topic = f"m3-position-accept-{case}-20261122"
group = f"m3-position-accept-{case}-group-20261122"
if os.environ.get("KAFKA_BROKER") != "kafka:9092" or os.environ.get("INFLUXDB_URL") != "http://influxdb:8086":
    raise SystemExit("Refusing unexpected service endpoint")
if os.environ.get("SENSOR_TOPIC") != topic or os.environ.get("CONSUMER_GROUP") != group:
    raise SystemExit("Refusing unexpected topic/group")

role = sys.argv[1]
sys.path.insert(0, "/app/src")
import kafka_batch_consumer as batch
import kafka_consumer as legacy

if role in {"fault", "peer"}:
    # Prefix controls range-assignor member ordering; all other consumer
    # settings match the optional production-disabled batch worker.
    owner_first = case.startswith("same")
    first = (role == "fault") == owner_first
    client_id = "a-position-owner" if first else "z-position-peer"

    def emit(kind, tp, **fields):
        # Test-only tracing must never interrupt the worker or alter its state.
        try:
            event = {'kind': kind, 'topic': tp.topic, 'partition': tp.partition,
                     'role': role, 'epoch_ns': time.time_ns(), **fields}
            with open(f'/coordination/events-{role}.jsonl', 'a', encoding='utf-8') as stream:
                stream.write(json.dumps(event, sort_keys=True) + '\n')
        except Exception as exc:
            print(f'M3_POSITION_TRACE_ERROR {type(exc).__name__}: {exc}', flush=True)

    def member_id(worker):
        coordinator = getattr(worker.consumer, '_coordinator', None)
        value = getattr(coordinator, 'member_id', None)
        return value if isinstance(value, str) and value else None

    original_revoked = batch.PartitionIsolatingBatchWorker.on_partitions_revoked
    original_assigned = batch.PartitionIsolatingBatchWorker.on_partitions_assigned
    def trace_revoked(self, partitions):
        try:
            for tp in partitions:
                progress = self.partitions.get(tp)
                offsets = [message.offset for message in progress.records] if progress else []
                emit('revoked', tp, member_id=member_id(self), queued_offsets=offsets,
                     assigned=[{'topic': item.topic, 'partition': item.partition} for item in self.consumer.assignment()])
        except Exception as exc:
            print(f'M3_POSITION_TRACE_ERROR {type(exc).__name__}: {exc}', flush=True)
        return original_revoked(self, partitions)
    def trace_assigned(self, partitions):
        result = original_assigned(self, partitions)
        try:
            for tp in partitions:
                emit('assigned', tp, member_id=member_id(self),
                     assigned=[{'topic': item.topic, 'partition': item.partition} for item in self.consumer.assignment()])
        except Exception as exc:
            print(f'M3_POSITION_TRACE_ERROR {type(exc).__name__}: {exc}', flush=True)
        return result
    batch.PartitionIsolatingBatchWorker.on_partitions_revoked = trace_revoked
    batch.PartitionIsolatingBatchWorker.on_partitions_assigned = trace_assigned

    def create_consumer():
        consumer = legacy.KafkaConsumer(
            bootstrap_servers=legacy.KAFKA_BROKER, group_id=legacy.CONSUMER_GROUP,
            client_id=client_id, value_deserializer=lambda value: value,
            auto_offset_reset="earliest", enable_auto_commit=False,
            max_poll_records=legacy.MAX_POLL_RECORDS,
            max_poll_interval_ms=legacy.MAX_POLL_INTERVAL_MS,
        )
        for name in ('committed', 'position', 'seek'):
            original = getattr(consumer, name)
            def traced(tp, *args, _name=name, _original=original, **kwargs):
                result = _original(tp, *args, **kwargs)
                value = args[0] if _name == 'seek' and args else result
                if value is None or isinstance(value, int):
                    emit(_name, tp, value=value)
                return result
            setattr(consumer, name, traced)
        return consumer

    batch.create_batch_consumer = create_consumer

if role == "fault":
    # Hold processing and its backpressure pause until all six partition-zero
    # records are fetched; then the first three are written while the last
    # three remain in the real worker queue throughout the rebalance.
    original_process = batch.PartitionIsolatingBatchWorker.process_ready
    original_sync = batch.PartitionIsolatingBatchWorker._sync_pause
    ready = [False]

    def wait_until_six(self, max_partitions=None):
        if not ready[0]:
            progress = next((p for tp, p in self.partitions.items() if tp.topic == topic and tp.partition == 0), None)
            if progress is None or len(progress.records) < 6:
                return 0
            ready[0] = True
            print("M3_ACCEPT_SIX_QUEUED_BEFORE_WRITE", flush=True)
        if (Path("/coordination/write_then_commit_confirmed").exists()
                and self.revocations_with_pending_work == 0):
            return 0
        return original_process(self, max_partitions=max_partitions)

    def pause_after_six(self, tp, progress):
        if not ready[0] and tp.topic == topic and tp.partition == 0 and len(progress.records) < 6:
            return
        return original_sync(self, tp, progress)

    batch.PartitionIsolatingBatchWorker.process_ready = wait_until_six
    batch.PartitionIsolatingBatchWorker._sync_pause = pause_after_six
    source = Path("/tests/module3_rebalance_fault_worker.py").read_text(encoding="utf-8")
    source = source.replace('"m3-rebalance-telemetry"', repr(topic))
    source = source.replace('"m3-rebalance-group"', repr(group))
    exec(compile(source, "/tests/module3_rebalance_fault_worker.py", "exec"), {"__name__": "__main__"})
elif role == "peer":
    batch.run()
elif role == "probe":
    if len(sys.argv) > 2 and sys.argv[2] == "inspect-identities":
        from influxdb_client import InfluxDBClient
        from kafka import KafkaConsumer, TopicPartition
        from module3_evidence import evidence, observed_identity, write_json
        from module3_reassignment_evidence import build_record
        run_ids = [f"m3-position-accept-{case}-p{partition}-20261122" for partition in (0, 1)]
        observer = KafkaConsumer(bootstrap_servers='kafka:9092', group_id=group,
                                 enable_auto_commit=False, request_timeout_ms=10000)
        tps = [TopicPartition(topic, partition) for partition in (0, 1)]
        try:
            observer.assign(tps)
            beginnings = observer.beginning_offsets(tps)
            ends = observer.end_offsets(tps)
            committed = {tp.partition: observer.committed(tp) for tp in tps}
        finally:
            observer.close()
        partition_records = []
        initial = None
        if os.environ.get('M3_EVIDENCE_DIR'):
            initial = json.loads(Path('/coordination/initial-offsets.json').read_text(encoding='utf-8'))
        with InfluxDBClient(url="http://influxdb:8086", token="m3-isolated-token",
                            org="m3-isolated-org", timeout=10000) as client:
            for partition in (0, 1):
                run = f"m3-position-accept-{case}-p{partition}-20261122"
                query = f'''from(bucket: "m3-isolated-bucket") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "sensor_telemetry" and r._field == "time_cycles" and r.run_id == "{run}")'''
                records = [r for table in client.query_api().query(query) for r in table.records]
                cycles = sorted(int(r.get_value()) for r in records)
                assert cycles == list(range(1, 7)), (partition, cycles)
                print(f"M3_ACCEPT_IDENTITIES case={case} partition={partition} cycles={cycles} points={len(cycles)} duplicates={len(cycles) - len(set(cycles))}", flush=True)
                if os.environ.get('M3_EVIDENCE_DIR'):
                    published = json.loads(Path(f'/coordination/published-p{partition}.json').read_text(encoding='utf-8'))
                    source = published['records']
                    if (published['run_id'] != run or len(source) != 6 or
                            [(item['topic'], item['partition'], item['offset']) for item in source] !=
                            [(topic, partition, offset) for offset in range(6)] or
                            any(item['identity']['tags']['run_id'] != run for item in source)):
                        raise ValueError('Published records do not match the isolated run and partition')
                    tp = tps[partition]
                    partition_records.append(evidence('same_owner' if case.startswith('same') else 'different_owner',
                        run_id=run, topic=topic, consumer_group=group, partition=partition,
                        beginning_offset=beginnings[tp], end_offset=ends[tp], committed_offset=committed[partition],
                        expected_point_identities=[item['identity'] for item in source],
                        observed_point_identities=[observed_identity(record.values) for record in records],
                        scenario_status='incomplete', published_source_records=source,
                        initial_end_offset=initial['end'][str(partition)],
                        initial_committed_offset=initial['committed'][str(partition)]))
        if os.environ.get('M3_EVIDENCE_DIR'):
            events = []
            for worker in ('fault', 'peer'):
                path = Path(f'/coordination/events-{worker}.jsonl')
                if path.exists():
                    events.extend(json.loads(line) for line in path.read_text(encoding='utf-8').splitlines())
            mode = 'same' if case.startswith('same') else 'transfer'
            result = build_record(mode, events=events, partitions=partition_records,
                                  project=os.environ.get('M3_PROJECT'), commit=os.environ.get('GITHUB_SHA'),
                                  topic=topic, group=group, run_ids=run_ids)
            filename = 'reassignment_same_owner.json' if mode == 'same' else 'reassignment_different_owner.json'
            write_json(Path(os.environ['M3_EVIDENCE_DIR']) / filename, result)
            if result['scenario_status'] == 'failed':
                raise AssertionError('Reassignment ownership or identities did not reconcile')
        raise SystemExit(0)
    source = Path("/tests/module3_rebalance_probe.py").read_text(encoding="utf-8")
    source = source.replace('"m3-rebalance-telemetry"', repr(topic))
    source = source.replace('"m3-rebalance-group"', repr(group))
    source = source.replace('"m3-rebalance-p0-20261121"', repr(f"m3-position-accept-{case}-p0-20261122"))
    source = source.replace('"m3-rebalance-p1-20261121"', repr(f"m3-position-accept-{case}-p1-20261122"))
    sys.argv = ["/tests/module3_rebalance_probe.py", *sys.argv[2:]]
    exec(compile(source, "/tests/module3_rebalance_probe.py", "exec"), {"__name__": "__main__"})
else:
    raise SystemExit("Unknown acceptance role")
