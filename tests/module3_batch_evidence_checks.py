"""Offline regression for batch DLQ correlation and identity reconciliation."""
import importlib
import json
import sys
from types import SimpleNamespace

import pytest
from module3_batch_aggregate import finalize


@pytest.fixture
def probe(monkeypatch):
    settings = {'MODULE3_BATCH_TEST_GUARD': 'm3-isolated-batch-only',
                'KAFKA_BROKER': 'kafka:9092', 'SENSOR_TOPIC': 'm3-batch-telemetry',
                'DEAD_LETTER_TOPIC': 'm3-batch-dead-letter', 'CONSUMER_GROUP': 'm3-batch-writer-group',
                'INFLUXDB_URL': 'http://influxdb:8086', 'INFLUXDB_TOKEN': 'm3-isolated-token',
                'INFLUXDB_ORG': 'm3-isolated-org', 'INFLUXDB_BUCKET': 'm3-isolated-bucket'}
    for key, value in settings.items():
        monkeypatch.setenv(key, value)
    sys.modules.pop('module3_batch_probe', None)
    return importlib.import_module('module3_batch_probe')


def test_dlq_correlation_allows_replay_but_rejects_wrong_source(probe, monkeypatch):
    source = {'source_topic': 'm3-batch-telemetry', 'source_partition': 0,
              'source_offset': 3, 'value': json.dumps({'invalid': True})}
    wrong = {**source, 'source_topic': 'other'}
    class Consumer:
        def __init__(self, **kwargs): self.offset = 0
        def assign(self, partitions): self.tp = partitions[0]
        def beginning_offsets(self, partitions): return {self.tp: 0}
        def end_offsets(self, partitions): return {self.tp: 3}
        def seek(self, tp, offset): self.offset = offset
        def position(self, tp): return self.offset
        def poll(self, **kwargs):
            self.offset = 3
            return {self.tp: [SimpleNamespace(value=json.dumps(p).encode(), topic=self.tp.topic,
                                              partition=0, offset=i)
                              for i, p in enumerate([source, wrong, source])]}
        def close(self): pass
    monkeypatch.setattr(probe, 'KafkaConsumer', Consumer)
    found = probe.matching_dlq_records(3, {'invalid': True}, 8)
    assert [item['dlq_offset'] for item in found] == [0, 2]
    assert all(item['source_offset'] == 3 and item['source_committed_offset'] == 8 for item in found)


def test_published_duplicate_maps_to_same_full_point_identity(probe):
    first = probe.published_identity(probe.event(5))
    replay = probe.published_identity(probe.event(5))
    assert first == replay
    assert first['tags']['run_id'] == probe.RUN
    assert set(first['tags']) == {'dataset_id', 'unit_id', 'run_id', 'cycle'}


@pytest.mark.parametrize('wrong_timestamp', [False, True])
def test_probe_writes_evidence_and_fails_closed_on_identity_mismatch(probe, monkeypatch, tmp_path, wrong_timestamp):
    class Producer:
        def __init__(self, **kwargs): self.offset = 0
        def send(self, *args, **kwargs):
            result = SimpleNamespace(topic='m3-batch-telemetry', partition=0, offset=self.offset)
            self.offset += 1
            return SimpleNamespace(get=lambda timeout: result)
        def flush(self): pass
        def close(self): pass
    class Client:
        def __init__(self, **kwargs): pass
        def close(self): pass
    points = []
    for cycle in range(6):
        payload = probe.event(cycle)
        identity = probe.published_identity(payload)
        timestamp = payload['timestamp']
        if wrong_timestamp and cycle == 0:
            timestamp = '2020-01-01T00:00:00Z'
        points.append(SimpleNamespace(values={'_measurement': 'sensor_telemetry',
                                              '_time': timestamp, **identity['tags']}))
    monkeypatch.setenv('M3_EVIDENCE_DIR', str(tmp_path))
    monkeypatch.setenv('M3_PROJECT', 'pms-m3-1-1')
    monkeypatch.setenv('GITHUB_SHA', 'test-sha')
    monkeypatch.setattr(probe, 'KafkaProducer', Producer)
    monkeypatch.setattr(probe, 'InfluxDBClient', Client)
    monkeypatch.setattr(probe, 'stored_points', lambda client: points)
    monkeypatch.setattr(probe, 'offset_observed', lambda: 8)
    monkeypatch.setattr(probe, 'dlq_observed', lambda offset: True)
    monkeypatch.setattr(probe, 'source_offsets', lambda: (0, 8, 8))
    monkeypatch.setattr(probe, 'matching_dlq_records', lambda *args: [
        {'source_topic': probe.EXPECTED['SENSOR_TOPIC'], 'source_partition': 0, 'source_offset': 3,
         'dlq_topic': probe.EXPECTED['DEAD_LETTER_TOPIC'], 'dlq_partition': 0, 'dlq_offset': 0,
         'source_committed_offset': 8, 'source_value': {'invalid': True}}])
    if wrong_timestamp:
        with pytest.raises(AssertionError):
            probe.main()
    else:
        probe.main()
    result = json.loads((tmp_path / 'batch_persistence.json').read_text())
    assert result['run_id'] == probe.RUN
    assert result['worker_exit_status'] is None
    assert result['scenario_status'] == ('failed' if wrong_timestamp else 'passed')
    assert bool(result['missing_point_identities']) == wrong_timestamp
    if not wrong_timestamp:
        assert json.loads((tmp_path / 'dlq.json').read_text())['scenario_status'] == 'passed'


def test_batch_aggregate_accepts_duplicate_dlq_and_rejects_wrong_source(probe):
    points = [probe.published_identity(probe.event(i)) for i in range(6)]
    source_ids = [f'{probe.RUN}-{i}' for i in (1, 2, 3)] + [None] + [f'{probe.RUN}-{i}' for i in (4, 5, 6, 6)]
    source = [{'topic': probe.EXPECTED['SENSOR_TOPIC'], 'partition': 0, 'offset': i, 'event_id': event_id}
              for i, event_id in enumerate(source_ids)]
    correlation = {'source_topic': probe.EXPECTED['SENSOR_TOPIC'], 'source_partition': 0, 'source_offset': 3,
                   'dlq_topic': probe.EXPECTED['DEAD_LETTER_TOPIC'], 'dlq_partition': 0, 'dlq_offset': 0,
                   'source_committed_offset': 8, 'source_value': {'invalid': True}}
    record = {'scenario_id': 'dlq', 'compose_project': 'project', 'git_commit': 'sha',
              'topic': probe.EXPECTED['SENSOR_TOPIC'], 'consumer_group': probe.EXPECTED['CONSUMER_GROUP'],
              'partition': 0, 'run_id': probe.RUN, 'beginning_offset': 0, 'end_offset': 8,
              'committed_offset': 8, 'published_source_records': source,
              'expected_point_identities': points, 'observed_point_identities': points,
              'dlq_source_correlations': [correlation, {**correlation, 'dlq_offset': 1}]}
    assert finalize(record, 'project', 'sha')['scenario_status'] == 'passed'
    record['dlq_source_correlations'][0]['source_offset'] = 2
    with pytest.raises(ValueError): finalize(record, 'project', 'sha')
