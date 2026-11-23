import json
import pytest
from module3_evidence import evidence, summary, write_json, published_identity, observed_identity


def valid(**updates):
    first = published_identity({'run_id': 'r', 'unit_id': 1, 'time_cycles': 1,
                                'timestamp': '2026-01-01T00:00:01Z'})
    second = published_identity({'run_id': 'r', 'unit_id': 1, 'time_cycles': 2,
                                 'timestamp': '2026-01-01T00:00:02Z'})
    values = dict(run_id='r', topic='t', consumer_group='g', partition=0,
                  beginning_offset=0, end_offset=2, committed_offset=2,
                  expected_point_identities=[first, second],
                  observed_point_identities=[second, first], worker_exit_status=0,
                  scenario_status='passed', compose_project='project', git_commit='abc')
    values.update(updates)
    return evidence('batch_persistence', **values)


def test_set_reconciliation_and_missing_evidence():
    record = evidence('batch_persistence', expected_point_identities=['a', 'b'],
                      observed_point_identities=['b', 'c', 'c'])
    assert record['missing_point_identities'] == ['a']
    assert record['unexpected_point_identities'] == ['c']
    assert record['duplicate_observed_identities'] == ['c']
    assert evidence('batch_persistence')['missing_point_identities'] is None


def test_pass_rejects_unknown_or_mismatched_observation():
    with pytest.raises(ValueError):
        valid(committed_offset=None)
    with pytest.raises(ValueError):
        valid(observed_point_identities=['r-1'])


def test_summary_rejects_missing_and_wrong_scenario(tmp_path):
    path = tmp_path / 'one.json'
    points = [published_identity({'run_id': 'r', 'unit_id': 9, 'time_cycles': i,
                                  'timestamp': f'2026-01-01T00:00:0{i}Z'}) for i in range(1, 7)]
    ids = ['r-1', 'r-2', 'r-3', None, 'r-4', 'r-5', 'r-6', 'r-6']
    source = [{'topic': 'm3-batch-telemetry', 'partition': 0, 'offset': i, 'event_id': event_id}
              for i, event_id in enumerate(ids)]
    correlation = {'source_topic': 'm3-batch-telemetry', 'source_partition': 0, 'source_offset': 3,
                   'dlq_topic': 'm3-batch-dead-letter', 'dlq_partition': 0, 'dlq_offset': 0,
                   'source_committed_offset': 8, 'source_value': {'invalid': True}}
    record = evidence('batch_persistence', run_id='r', topic='m3-batch-telemetry',
                      consumer_group='m3-batch-writer-group', partition=0, beginning_offset=0,
                      end_offset=8, committed_offset=8, expected_point_identities=points,
                      observed_point_identities=points, worker_exit_status=0, scenario_status='passed',
                      published_source_records=source, dlq_source_correlations=[correlation],
                      compose_project='project', git_commit='abc')
    write_json(path, record)
    report = summary('project', 'abc', {}, {'batch_persistence': path, 'dlq': path})
    assert report['overall_acceptance_status'] == 'incomplete'
    assert report['scenarios']['batch_persistence']['status'] == 'passed'
    assert report['scenarios']['dlq']['status'] == 'incomplete'
    assert summary('other-project', 'abc', {}, {'batch_persistence': path})['scenarios']['batch_persistence']['status'] == 'incomplete'
    assert json.loads(path.read_text())['schema_version'] == '1.0'
    assert summary('project', 'abc', {}, {}, 'failure')['overall_acceptance_status'] == 'failed'


def test_complete_point_identity_and_missing_tags():
    payload = {'dataset_id': 'FD001', 'unit_id': 9, 'run_id': 'r',
               'time_cycles': 2, 'timestamp': '2026-01-01T01:00:00.123456+01:00'}
    point = published_identity(payload)
    assert point == observed_identity({'_measurement': 'sensor_telemetry',
        '_time': '2026-01-01T00:00:00.123456Z', **point['tags']})
    assert point['timestamp_ns'] % 1000 == 0
    with pytest.raises(ValueError):
        observed_identity({'_measurement': 'sensor_telemetry', '_time': payload['timestamp']})
    with pytest.raises(ValueError):
        published_identity({**payload, 'timestamp': '2026-01-01T00:00:00'})


def test_export_ownership_applies_only_inside_evidence_directory(tmp_path, monkeypatch):
    import module3_evidence
    root = tmp_path / 'evidence'
    root.mkdir()
    calls = []
    monkeypatch.setenv('M3_EVIDENCE_DIR', str(root))
    monkeypatch.setenv('M3_EVIDENCE_UID', '123')
    monkeypatch.setenv('M3_EVIDENCE_GID', '456')
    monkeypatch.setattr(module3_evidence.os, 'chown', lambda path, uid, gid: calls.append((path, uid, gid)))
    write_json(root / 'result.json', {'ok': True})
    write_json(tmp_path / 'other.json', {'ok': True})
    assert calls == [(root / 'result.json', 123, 456)]
