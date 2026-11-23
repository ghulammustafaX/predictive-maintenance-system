"""Guarded hosted-only shutdown worker and Kafka/InfluxDB evidence probe."""
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

project = os.environ.get('M3_PROJECT', '')
commit = os.environ.get('GITHUB_SHA', '')
scenario = os.environ.get('M3_HOSTED_SCENARIO', '')
if (os.environ.get('M3_HOSTED_GUARD') != 'github-hosted-shutdown-only'
        or not re.fullmatch(r'pms-m3-[0-9]+-[0-9]+', project)
        or not re.fullmatch(r'[0-9a-f]{40}', commit)
        or scenario not in {'graceful', 'exhausted'}
        or os.environ.get('KAFKA_BROKER') != 'kafka:9092'
        or os.environ.get('INFLUXDB_URL') != 'http://influxdb:8086'):
    raise SystemExit('Refusing unexpected hosted shutdown environment')
topic = f'{project}-{scenario}'
group = f'{topic}-group'
run = f'{topic}-run'
if os.environ.get('SENSOR_TOPIC') != topic or os.environ.get('CONSUMER_GROUP') != group:
    raise SystemExit('Refusing unexpected topic or group')

role = sys.argv[1]
if role in {'worker', 'recovery'}:
    sys.path.insert(0, '/app/src')
    import kafka_batch_consumer as batch
    if role == 'worker':
        original = batch.PartitionIsolatingBatchWorker.process_ready
        def hold_queued(self, max_partitions=None):
            if any(progress.records for progress in self.partitions.values()):
                print('M3_HOSTED_QUEUED_READY', flush=True)
                deadline = time.monotonic() + 90
                while not Path('/coordination/release').exists():
                    if time.monotonic() >= deadline:
                        raise RuntimeError('Timed out waiting for test coordinator')
                    time.sleep(0.05)
            return original(self, max_partitions=max_partitions)
        batch.PartitionIsolatingBatchWorker.process_ready = hold_queued
    batch.run()
elif role == 'publish':
    from kafka import KafkaProducer
    from module3_evidence import published_identity, write_json
    producer = KafkaProducer(bootstrap_servers='kafka:9092', acks='all',
                             value_serializer=lambda value: json.dumps(value).encode())
    try:
        timestamp = datetime.now(timezone.utc) - timedelta(minutes=1)
        identities = []
        source_records = []
        for cycle in range(1, 4):
            event = {'schema_version': '1.0.0', 'event_id': f'{run}-{cycle}',
                     'run_id': run, 'dataset_id': 'FD001', 'asset_id': 'engine_9',
                     'unit_id': 9, 'time_cycles': cycle, 'sequence': cycle,
                     'source_type': 'cmapss', 'timestamp': (timestamp + timedelta(seconds=cycle)).isoformat(),
                     'operational_setting_1': 0.1, 'operational_setting_2': 0.2,
                     'operational_setting_3': 100.0, 'sensor_21': float(cycle)}
            result = producer.send(topic, value=event, partition=0).get(timeout=15)
            identity = published_identity(event)
            identities.append(identity)
            source_records.append({'topic': result.topic, 'partition': result.partition, 'offset': result.offset,
                                   'event_id': event['event_id'], 'identity': identity})
            print(json.dumps({'partition': result.partition, 'offset': result.offset}), flush=True)
        write_json('/coordination/published.json', {'run_id': run, 'identities': identities,
                                                    'source_records': source_records})
    finally:
        producer.close(timeout=15)
elif role == 'snapshot':
    from kafka import KafkaConsumer, TopicPartition
    from influxdb_client import InfluxDBClient
    consumer = KafkaConsumer(bootstrap_servers='kafka:9092', group_id=group,
                             enable_auto_commit=False)
    partition = TopicPartition(topic, 0)
    consumer.assign([partition])
    try:
        beginning = consumer.beginning_offsets([partition])[partition]
        end = consumer.end_offsets([partition])[partition]
        committed = consumer.committed(partition)
    finally:
        consumer.close()
    with InfluxDBClient(url='http://influxdb:8086', token='m3-isolated-token',
                        org='m3-isolated-org', timeout=10000) as client:
        query = f'''from(bucket: "m3-isolated-bucket") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "sensor_telemetry" and r._field == "time_cycles" and r.run_id == "{run}")'''
        records = [record for table in client.query_api().query(query) for record in table.records]
        observed = sorted(int(record.get_value()) for record in records)
    expected = [1, 2, 3]
    from module3_evidence import evidence, observed_identity
    published = json.loads(Path('/coordination/published.json').read_text(encoding='utf-8'))
    if published.get('run_id') != run:
        raise ValueError('Published identity run does not match snapshot run')
    record = evidence('graceful_shutdown' if scenario == 'graceful' else 'exhausted_drain_replay',
                      run_id=run, topic=topic, consumer_group=group, partition=0,
                      beginning_offset=beginning, end_offset=end, committed_offset=committed,
                      expected_point_identities=published['identities'],
                      observed_point_identities=[observed_identity(record.values) for record in records],
                      scenario_status='incomplete',
                      compose_project=project, git_commit=commit,
                      published_source_records=published.get('source_records'),
                      identity_scope='measurement, all four tags and UTC timestamp in nanoseconds',
                      write_attempt_count=None, observed_cycle_identities=observed,
                      duplicate_cycle_identities=len(observed) - len(set(observed)))
    print(json.dumps(record))
else:
    raise SystemExit('Unknown hosted shutdown role')
