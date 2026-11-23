"""Offline checks for failure-window and recovery evidence."""
import importlib
import sys
from types import SimpleNamespace

import pytest
from module3_partition_aggregate import aggregate
from module3_evidence import summary, write_json


@pytest.fixture
def case(monkeypatch):
    settings = {'MODULE3_PARTITION_FAULT_PROBE_GUARD': 'm3-isolated-partition-fault-probe-only',
                'KAFKA_BROKER': 'kafka:9092', 'SENSOR_TOPIC': 'm3-partition-fault-telemetry',
                'CONSUMER_GROUP': 'm3-partition-fault-group', 'INFLUXDB_URL': 'http://influxdb:8086',
                'INFLUXDB_TOKEN': 'm3-isolated-token', 'INFLUXDB_ORG': 'm3-isolated-org',
                'INFLUXDB_BUCKET': 'm3-isolated-bucket'}
    for key, value in settings.items(): monkeypatch.setenv(key, value)
    sys.modules.pop('module3_partition_fault_probe', None)
    probe = importlib.import_module('module3_partition_fault_probe')
    published, points = {}, {}
    for partition in (0, 1):
        published[partition], points[partition] = [], []
        for cycle in (1, 2, 3):
            payload = probe.event(partition, cycle)
            identity = probe.published_identity(payload)
            published[partition].append({'topic': settings['SENSOR_TOPIC'], 'partition': partition,
                                         'offset': cycle - 1, 'identity': identity})
            points[partition].append(SimpleNamespace(values={'_measurement': 'sensor_telemetry',
                                                              '_time': payload['timestamp'], **identity['tags']}))
    offsets = {0: {'beginning': 0, 'end': 3, 'committed': None},
               1: {'beginning': 0, 'end': 3, 'committed': 3}}
    return probe, published, points, offsets


def test_failure_window_and_recovery(case):
    probe, published, points, offsets = case
    failure, valid = probe.stage_evidence('failure', published, offsets, {0: [], 1: points[1]}, 3, False, {0: None, 1: None})
    assert valid and failure['scenario_status'] == 'incomplete'
    assert failure['partitions'][0]['committed_offset'] is None
    assert failure['partitions'][1]['committed_offset'] == 3
    assert failure['partitions'][1]['missing_point_identities'] == []
    offsets[0]['committed'] = 3
    recovery, valid = probe.stage_evidence('recovery', published, offsets, points, 3, True, {0: None, 1: None})
    assert valid and recovery['scenario_run_id'] == failure['scenario_run_id']
    assert all(not item['missing_point_identities'] for item in recovery['partitions'])


@pytest.mark.parametrize('defect', ['healthy_not_advanced', 'failed_advanced', 'wrong_end', 'missing', 'unexpected'])
def test_detects_invalid_progress_or_identity(case, defect):
    probe, published, points, offsets = case
    observed = {0: [], 1: points[1]}
    if defect == 'healthy_not_advanced': offsets[1]['committed'] = 0
    if defect == 'failed_advanced': offsets[0]['committed'] = 1
    if defect == 'wrong_end': offsets[1]['end'] = 4
    if defect == 'missing': observed[1] = observed[1][:-1]
    if defect == 'unexpected': observed[1][0].values['_time'] = '2020-01-01T00:00:00Z'
    record, valid = probe.stage_evidence('failure', published, offsets, observed, 3, False, {0: None, 1: None})
    assert not valid and record['scenario_status'] == 'failed'


@pytest.mark.parametrize('defect', ['partition', 'run'])
def test_rejects_incorrect_published_correlation(case, defect):
    probe, published, points, offsets = case
    if defect == 'partition': published[0][0]['partition'] = 1
    else: published[0][0]['identity']['tags']['run_id'] = 'other'
    with pytest.raises(ValueError):
        probe.stage_evidence('failure', published, offsets, {0: [], 1: points[1]}, 3, False, {0: None, 1: None})


def test_recovery_requires_prior_failure_snapshot(case, monkeypatch, tmp_path):
    probe, published, _, _ = case
    monkeypatch.setenv('M3_EVIDENCE_DIR', str(tmp_path))
    with pytest.raises(ValueError, match='Missing failure-stage'):
        probe.capture_evidence('recovery', published, None, {0: None, 1: None})


def make_stages(case, monkeypatch, tmp_path):
    probe, published, points, offsets = case
    monkeypatch.setenv('M3_PROJECT', 'project')
    monkeypatch.setenv('GITHUB_SHA', 'commit')
    failure, _ = probe.stage_evidence('failure', published, offsets, {0: [], 1: points[1]}, 3, False, {0: None, 1: None})
    offsets[0]['committed'] = 3
    recovery, _ = probe.stage_evidence('recovery', published, offsets, points, 3, True, {0: None, 1: None})
    for stage, value in (('failure', failure), ('recovery', recovery)):
        write_json(tmp_path / f'partition_isolation_{stage}.json', value)
    return failure, recovery


def test_aggregate_rechecks_healthy_progress_and_recovery(case, monkeypatch, tmp_path):
    make_stages(case, monkeypatch, tmp_path)
    result = aggregate(tmp_path, 'project', 'commit')
    assert result['scenario_status'] == 'passed', result['reason']
    path = tmp_path / 'partition_isolation.json'
    write_json(path, result)
    checked = summary('project', 'commit', {'status': 'passed'}, {'partition_isolation': path}, 'success')
    assert checked['scenarios']['partition_isolation']['status'] == 'passed'
    assert checked['overall_acceptance_status'] == 'incomplete'


@pytest.mark.parametrize('defect,expected', [('missing', 'incomplete'), ('project', 'incomplete'),
    ('commit', 'incomplete'), ('run', 'incomplete'), ('stage_failed', 'failed'),
    ('healthy_offset', 'failed'), ('early_commit', 'failed'), ('missing_point', 'failed'),
    ('wrong_recovery', 'failed'), ('missing_offset', 'incomplete')])
def test_aggregate_rejects_missing_failed_and_inconsistent_stages(case, monkeypatch, tmp_path, defect, expected):
    failure, recovery = make_stages(case, monkeypatch, tmp_path)
    if defect == 'missing': (tmp_path / 'partition_isolation_failure.json').unlink()
    else:
        if defect == 'project': recovery['compose_project'] = 'other'
        if defect == 'commit': recovery['git_commit'] = 'other'
        if defect == 'run': recovery['scenario_run_id'] = 'other'
        if defect == 'stage_failed': failure['scenario_status'] = 'failed'
        if defect == 'healthy_offset': failure['partitions'][1]['committed_offset'] = 0
        if defect == 'early_commit': failure['partitions'][0]['committed_offset'] = 1
        if defect == 'missing_point': recovery['partitions'][0]['observed_point_identities'].pop()
        if defect == 'wrong_recovery': recovery['partitions'][0]['committed_offset'] = 2
        if defect == 'missing_offset': del failure['partitions'][0]['committed_offset']
        write_json(tmp_path / 'partition_isolation_failure.json', failure)
        write_json(tmp_path / 'partition_isolation_recovery.json', recovery)
    assert aggregate(tmp_path, 'project', 'commit')['scenario_status'] == expected
