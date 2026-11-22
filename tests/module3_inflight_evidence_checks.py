"""Offline checks of crash/replay evidence without a broker or database."""
import importlib
import json
import sys
from types import SimpleNamespace

import pytest


@pytest.fixture
def setup_probe(monkeypatch, tmp_path):
    settings = {'MODULE3_INFLIGHT_PROBE_GUARD': 'm3-isolated-inflight-probe-only',
                'KAFKA_BROKER': 'kafka:9092', 'INFLUXDB_URL': 'http://influxdb:8086',
                'INFLUXDB_TOKEN': 'm3-isolated-token', 'INFLUXDB_ORG': 'm3-isolated-org',
                'INFLUXDB_BUCKET': 'm3-isolated-bucket', 'M3_EVIDENCE_DIR': str(tmp_path)}
    for key, value in settings.items(): monkeypatch.setenv(key, value)
    sys.modules.pop('module3_inflight_probe', None)
    probe = importlib.import_module('module3_inflight_probe')
    records = []
    points = []
    for i in range(1, 7):
        payload = {'run_id': probe.RUN, 'dataset_id': 'FD002', 'unit_id': 903,
                   'time_cycles': i, 'timestamp': f'2026-01-01T00:00:{i:02d}Z'}
        identity = probe.published_identity(payload)
        records.append({'topic': probe.TOPIC, 'partition': 0, 'offset': i-1,
                        'event_id': f'{probe.RUN}-{i}', 'identity': identity})
        points.append(SimpleNamespace(values={'_measurement': 'sensor_telemetry',
                                             '_time': payload['timestamp'], **identity['tags']}))
    (tmp_path / 'inflight-published.json').write_text(json.dumps({'run_id': probe.RUN, 'records': records}))
    state = {'committed': None, 'points': points}
    class Consumer:
        def __init__(self, **kwargs): pass
        def assign(self, partitions): self.tp = partitions[0]
        def beginning_offsets(self, partitions): return {self.tp: 0}
        def end_offsets(self, partitions): return {self.tp: 6}
        def committed(self, tp): return state['committed']
        def close(self): pass
    monkeypatch.setattr(probe, 'KafkaConsumer', Consumer)
    client = SimpleNamespace(query_api=lambda: SimpleNamespace(query=lambda *args, **kwargs: [SimpleNamespace(records=state['points'])]))
    return probe, client, state, tmp_path


def test_crash_and_recovery_offsets_and_identities(setup_probe, monkeypatch):
    probe, client, state, path = setup_probe
    monkeypatch.setenv('M3_WORKER_EXIT_STATUS', '137')
    probe.capture_stage(client, 'record-crash')
    crash = json.loads((path / 'crash_after_ack.json').read_text())
    assert crash['committed_offset'] is None and crash['end_offset'] == 6
    assert crash['worker_exit_status'] == 137 and crash['missing_point_identities'] == []
    assert crash['scenario_status'] == 'incomplete'
    state['committed'] = 6
    monkeypatch.setenv('M3_WORKER_EXIT_STATUS', '0')
    probe.capture_stage(client, 'record-recovery')
    recovery = json.loads((path / 'restart_replay.json').read_text())
    assert recovery['committed_offset'] == recovery['end_offset'] == 6
    assert recovery['run_id'] == crash['run_id'] and recovery['worker_exit_status'] == 0


def test_crash_record_rejects_premature_commit(setup_probe, monkeypatch):
    probe, client, state, path = setup_probe
    state['committed'] = 1
    monkeypatch.setenv('M3_WORKER_EXIT_STATUS', '137')
    with pytest.raises(AssertionError):
        probe.capture_stage(client, 'record-crash')
    record = json.loads((path / 'crash_after_ack.json').read_text())
    assert record['committed_offset'] == 1 and record['scenario_status'] == 'failed'


def test_confirmed_crash_and_matching_recovery_can_pass(setup_probe, monkeypatch):
    probe, client, state, path = setup_probe
    monkeypatch.setenv('M3_PROJECT', 'project')
    monkeypatch.setenv('GITHUB_SHA', 'sha')
    monkeypatch.setenv('M3_ACK_MARKER_VERIFIED', 'true')
    monkeypatch.setenv('M3_WORKER_EXIT_STATUS', '137')
    probe.capture_stage(client, 'record-crash')
    assert json.loads((path / 'crash_after_ack.json').read_text())['scenario_status'] == 'passed'
    state['committed'] = 6
    monkeypatch.setenv('M3_WORKER_EXIT_STATUS', '0')
    probe.capture_stage(client, 'record-recovery')
    assert json.loads((path / 'restart_replay.json').read_text())['scenario_status'] == 'passed'


@pytest.mark.parametrize('change', ['missing', 'unexpected', 'wrong_commit', 'wrong_run', 'missing_exit'])
def test_rejects_mismatch_or_preserves_missing_exit(setup_probe, monkeypatch, change):
    probe, client, state, path = setup_probe
    state['committed'] = 6
    monkeypatch.setenv('M3_WORKER_EXIT_STATUS', '0')
    if change == 'missing': state['points'] = state['points'][:-1]
    if change == 'unexpected': state['points'][0].values['_time'] = '2020-01-01T00:00:00Z'
    if change == 'wrong_commit': state['committed'] = 5
    if change == 'wrong_run':
        path.joinpath('inflight-published.json').write_text(json.dumps({'run_id': 'wrong', 'records': []}))
    if change == 'missing_exit': monkeypatch.delenv('M3_WORKER_EXIT_STATUS')
    if change in {'missing', 'unexpected', 'wrong_commit'}:
        with pytest.raises(AssertionError): probe.capture_stage(client, 'record-recovery')
        assert json.loads((path / 'restart_replay.json').read_text())['scenario_status'] == 'failed'
    elif change == 'wrong_run':
        with pytest.raises(ValueError): probe.capture_stage(client, 'record-recovery')
    else:
        probe.capture_stage(client, 'record-recovery')
        record = json.loads((path / 'restart_replay.json').read_text())
        assert record['worker_exit_status'] is None and record['scenario_status'] == 'incomplete'
