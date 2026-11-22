"""Offline sustained-load identity, source-offset and artifact checks."""
import copy
import importlib.util
import json
from pathlib import Path
import pytest
from module3_evidence import evidence, published_identity, summary, write_json
from module3_load_evidence import IdentityMismatch, aggregate, read_jsonl, reconcile, write_jsonl


def sample():
    records = []
    for cycle in range(1, 5):
        identity = published_identity({'run_id': 'run', 'dataset_id': 'FD001', 'unit_id': 1,
                                       'time_cycles': cycle, 'timestamp': f'2026-01-01T00:00:0{cycle}Z'})
        records.append({'topic': 'topic', 'partition': cycle - 1, 'offset': 0,
                        'event_id': f'e{cycle}', 'identity': identity})
    offsets = {'committed_offsets': {f'topic:{p}': 1 for p in range(4)},
               'end_offsets': {f'topic:{p}': 1 for p in range(4)}, 'total_lag': 0}
    return records, [copy.deepcopy(item['identity']) for item in records], offsets


def check(records, observed, offsets):
    return reconcile(records, observed, topic='topic', run_id='run', offsets=offsets,
                     expected_count=4, partitions=4)


def test_complete_identity_match_and_artifact_roundtrip(tmp_path):
    records, observed, offsets = sample()
    path = tmp_path / 'observed.jsonl'
    write_jsonl(path, observed)
    result = check(records, read_jsonl(path, 4), offsets)
    assert result['expected_count'] == result['observed_count'] == 4
    assert not result['missing_point_identities'] and not result['unexpected_point_identities']


@pytest.mark.parametrize('change', ['missing', 'unexpected', 'timestamp', 'missing_timestamp', 'missing_tag', 'wrong_run', 'cross_run', 'offset', 'partition'])
def test_rejects_identity_and_offset_defects(change):
    records, observed, offsets = sample()
    if change == 'missing': observed.pop()
    if change == 'unexpected': observed[0]['tags']['unit_id'] = '2'
    if change == 'timestamp': observed[0]['timestamp_ns'] += 1000
    if change == 'missing_timestamp': del observed[0]['timestamp_ns']
    if change == 'missing_tag': del observed[0]['tags']['dataset_id']
    if change == 'wrong_run': records[0]['identity']['tags']['run_id'] = 'old'
    if change == 'cross_run': observed[0]['tags']['run_id'] = 'old'
    if change == 'offset': offsets['committed_offsets']['topic:0'] = 0
    if change == 'partition': records[0]['partition'] = 1
    with pytest.raises((ValueError, IdentityMismatch)):
        check(records, observed, offsets)


def test_missing_or_wrong_sized_artifact_fails(tmp_path):
    with pytest.raises(ValueError): read_jsonl(tmp_path / 'missing.jsonl', 1)
    path = tmp_path / 'one.jsonl'
    write_jsonl(path, [{'x': 1}])
    with pytest.raises(ValueError): read_jsonl(path, 2)


def test_shared_schema_requires_reconciled_artifacts_but_not_local_position(tmp_path):
    records, observed, offsets = sample()
    write_jsonl(tmp_path / 'published.jsonl', records)
    write_jsonl(tmp_path / 'observed.jsonl', observed)
    kwargs = dict(run_id='run', topic='topic', consumer_group='g', worker_exit_status=0,
                  scenario_status='passed', artifact_root=tmp_path, expected_count=4,
                  expected_identity_artifact='published.jsonl', observed_identity_artifact='observed.jsonl',
                  partition_offsets=offsets, local_consumer_position=None)
    assert evidence('sustained_load', **kwargs)['scenario_status'] == 'passed'
    (tmp_path / 'observed.jsonl').unlink()
    with pytest.raises(ValueError): evidence('sustained_load', **kwargs)


def test_verifier_emits_two_reconciled_artifact_records(monkeypatch, tmp_path):
    settings = {'MODULE3_LOAD_PROBE_GUARD': 'm3-isolated-sustained-load-probe-only',
                'KAFKA_BROKER': 'kafka:9092', 'INFLUXDB_URL': 'http://influxdb:8086',
                'INFLUXDB_TOKEN': 'm3-isolated-token', 'INFLUXDB_ORG': 'm3-isolated-org',
                'INFLUXDB_BUCKET': 'm3-isolated-bucket', 'MODULE3_LOAD_RECORDS': '4',
                'MODULE3_LOAD_RESULTS_DIR': str(tmp_path), 'M3_LOAD_LEGACY_EXIT': '0',
                'M3_LOAD_BATCH_EXIT': '0', 'M3_PROJECT': 'pms-m3-1-1', 'GITHUB_SHA': 'abc'}
    for key, value in settings.items(): monkeypatch.setenv(key, value)
    spec = importlib.util.spec_from_file_location('load_probe_evidence_test', Path(__file__).with_name('module3_sustained_load_probe.py'))
    probe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)
    (tmp_path / 'publisher.json').write_text(json.dumps({'records_per_mode': 4,
                                                          'topics': probe.TOPICS, 'run_ids': probe.RUN_IDS}))
    by_run = {}
    for mode, topic in probe.TOPICS.items():
        source = []
        for cycle in range(1, 5):
            item = probe.payload(mode, cycle, f'2026-01-01T00:00:0{cycle}Z')
            source.append({'topic': topic, 'partition': cycle - 1, 'offset': 0,
                           'event_id': item['event_id'], 'identity': published_identity(item)})
        by_run[probe.RUN_IDS[mode]] = [row['identity'] for row in source]
        write_jsonl(tmp_path / f'sustained_load_{mode}_published.jsonl', source)
        offsets = {'committed_offsets': {f'{topic}:{p}': 1 for p in range(4)},
                   'end_offsets': {f'{topic}:{p}': 1 for p in range(4)}, 'total_lag': 0}
        worker = {'mode': mode, 'topic': topic, 'expected_records': 4, 'processed_records': 4, 'offsets': offsets, 'consumer_group': f'{mode}-g',
                  'end_to_end_latency_ms': {'samples': 4}, 'write_latency_ms': {'calls': 1},
                  'commit_latency_ms': {'calls': 1}, 'points_per_write': {'average': 1 if mode == 'legacy' else 2},
                  'records_per_second': 1.0}
        (tmp_path / f'{mode}.json').write_text(json.dumps(worker))
    class Client:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
    monkeypatch.setattr(probe, 'InfluxDBClient', Client)
    monkeypatch.setattr(probe, 'persisted_count', lambda client, run: 4)
    monkeypatch.setattr(probe, 'persisted_cycles', lambda client, run: [1, 2, 3, 4])
    monkeypatch.setattr(probe, 'persisted_identities', lambda client, run: by_run[run])
    # The production acceptance path requires 3,000 records; this unit test
    # exercises the same validator with a compact, explicit fixture size.
    monkeypatch.setattr(probe, 'aggregate', lambda root, **kwargs:
                        aggregate(root, project=kwargs['project'], commit=kwargs['commit'], expected_count=4))
    probe.verify()
    for mode in ('legacy', 'batch'):
        record = json.loads((tmp_path / f'sustained_load_{mode}.json').read_text())
        assert record['scenario_status'] == 'passed'
        assert record['identity_reconciliation']['expected_count'] == 4
        assert len(read_jsonl(tmp_path / record['observed_identity_artifact'], 4)) == 4
    combined_record = json.loads((tmp_path / 'sustained_load.json').read_text())
    assert combined_record['scenario_status'] == 'passed'


def aggregate_fixture(root, count=4):
    topics = {mode: f'{mode}-topic' for mode in ('legacy', 'batch')}
    runs = {mode: f'{mode}-run' for mode in ('legacy', 'batch')}
    (root / 'publisher.json').write_text(json.dumps({'records_per_mode': count, 'topics': topics, 'run_ids': runs}))
    for mode in ('legacy', 'batch'):
        records = []
        for cycle in range(1, count + 1):
            identity = published_identity({'run_id': runs[mode], 'dataset_id': 'FD001', 'unit_id': 1,
                                           'time_cycles': cycle, 'timestamp': '2026-01-01T00:00:00Z'})
            records.append({'topic': topics[mode], 'partition': (cycle - 1) % 4, 'offset': (cycle - 1) // 4,
                            'event_id': f'{mode}-{cycle}', 'identity': identity})
        observed = [copy.deepcopy(item['identity']) for item in records]
        offsets = {'committed_offsets': {f'{topics[mode]}:{p}': (count + 3 - p) // 4 for p in range(4)},
                   'end_offsets': {f'{topics[mode]}:{p}': (count + 3 - p) // 4 for p in range(4)}, 'total_lag': 0}
        expected_file, observed_file = f'sustained_load_{mode}_published.jsonl', f'sustained_load_{mode}_observed.jsonl'
        write_jsonl(root / expected_file, records)
        write_jsonl(root / observed_file, observed)
        worker = {'mode': mode, 'topic': topics[mode], 'consumer_group': f'{mode}-group',
                  'expected_records': count, 'processed_records': count, 'offsets': offsets,
                  'records_per_second': 10.0, 'write_latency_ms': {'calls': 4},
                  'commit_latency_ms': {'calls': 4}, 'end_to_end_latency_ms': {'samples': count},
                  'points_per_write': {'average': 1 if mode == 'legacy' else 2}}
        (root / f'{mode}.json').write_text(json.dumps(worker))
        result = reconcile(records, observed, topic=topics[mode], run_id=runs[mode], offsets=offsets,
                           expected_count=count, partitions=4)
        record = evidence('sustained_load', run_id=runs[mode], topic=topics[mode],
                          consumer_group=worker['consumer_group'], worker_exit_status=0,
                          scenario_status='passed', artifact_root=root, expected_count=count,
                          expected_identity_artifact=expected_file, observed_identity_artifact=observed_file,
                          partition_offsets=offsets, identity_reconciliation=result,
                          throughput_records_per_second=10.0, successful_application_write_calls=4,
                          compose_project='project', git_commit='sha')
        (root / f'sustained_load_{mode}.json').write_text(json.dumps(record))


def test_aggregate_revalidates_both_modes(tmp_path):
    aggregate_fixture(tmp_path)
    result = aggregate(tmp_path, project='project', commit='sha', expected_count=4)
    assert result['scenario_status'] == 'passed'
    assert result['schema_version'] == '1.0' and result['evidence_timestamp']
    assert {item['status'] for item in result['mode_evidence'].values()} == {'passed'}
    assert aggregate(tmp_path, project='project', commit='sha')['scenario_status'] == 'incomplete'


@pytest.mark.parametrize('defect,expected', [
    ('missing_record', 'incomplete'), ('incomplete', 'incomplete'), ('failed', 'failed'),
    ('missing_artifact', 'incomplete'), ('timestamp', 'failed'), ('duplicate', 'failed'),
    ('offset', 'failed'), ('exit', 'failed'), ('missing_exit', 'incomplete'),
    ('project', 'incomplete'), ('commit', 'incomplete'), ('scenario', 'incomplete'),
    ('cross_run', 'failed'), ('reported_count', 'failed')])
def test_aggregate_rejects_invalid_mode_evidence(tmp_path, defect, expected):
    aggregate_fixture(tmp_path)
    path = tmp_path / 'sustained_load_batch.json'
    record = json.loads(path.read_text())
    if defect == 'missing_record': path.unlink()
    elif defect == 'missing_artifact': (tmp_path / 'sustained_load_batch_observed.jsonl').unlink()
    elif defect in {'timestamp', 'duplicate', 'cross_run'}:
        artifact = tmp_path / 'sustained_load_batch_observed.jsonl'
        rows = read_jsonl(artifact, 4)
        if defect == 'timestamp': rows[0]['timestamp_ns'] += 1000
        if defect == 'duplicate': rows[0] = copy.deepcopy(rows[1])
        if defect == 'cross_run': rows[0]['tags']['run_id'] = 'other'
        write_jsonl(artifact, rows)
    else:
        if defect == 'incomplete': record['scenario_status'] = 'incomplete'
        if defect == 'failed': record['scenario_status'] = 'failed'
        if defect == 'offset': record['partition_offsets']['committed_offsets']['batch-topic:0'] = 0
        if defect == 'exit': record['worker_exit_status'] = 1
        if defect == 'missing_exit': record['worker_exit_status'] = None
        if defect == 'project': record['compose_project'] = 'other'
        if defect == 'commit': record['git_commit'] = 'other'
        if defect == 'scenario': record['scenario_id'] = 'dlq'
        if defect == 'reported_count': record['identity_reconciliation']['observed_count'] = 3
        path.write_text(json.dumps(record))
    result = aggregate(tmp_path, project='project', commit='sha', expected_count=4)
    assert result['scenario_status'] == expected


def test_overall_summary_rechecks_aggregate_and_rejects_undersized_run(tmp_path):
    aggregate_fixture(tmp_path)
    aggregate_path = tmp_path / 'sustained_load.json'
    write_json(aggregate_path, aggregate(tmp_path, project='project', commit='sha', expected_count=4))
    result = summary('project', 'sha', {'status': 'passed'}, {'sustained_load': aggregate_path}, 'success')
    assert result['scenarios']['sustained_load']['status'] == 'incomplete'
    batch_path = tmp_path / 'sustained_load_batch.json'
    batch = json.loads(batch_path.read_text())
    batch['scenario_status'] = 'failed'
    batch_path.write_text(json.dumps(batch))
    result = summary('project', 'sha', {'status': 'passed'}, {'sustained_load': aggregate_path}, 'success')
    assert result['scenarios']['sustained_load']['status'] == 'incomplete'
    assert result['overall_acceptance_status'] == 'incomplete'


def test_verified_failure_takes_precedence_over_missing_peer(tmp_path):
    aggregate_fixture(tmp_path)
    (tmp_path / 'sustained_load_legacy.json').unlink()
    batch_path = tmp_path / 'sustained_load_batch.json'
    batch = json.loads(batch_path.read_text())
    batch['scenario_status'] = 'failed'
    batch_path.write_text(json.dumps(batch))
    result = aggregate(tmp_path, project='project', commit='sha', expected_count=4)
    assert result['mode_evidence']['legacy']['status'] == 'incomplete'
    assert result['mode_evidence']['batch']['status'] == 'failed'
    assert result['scenario_status'] == 'failed'


def test_summary_independently_rechecks_full_3000_record_modes(tmp_path):
    aggregate_fixture(tmp_path, count=3000)
    path = tmp_path / 'sustained_load.json'
    write_json(path, aggregate(tmp_path, project='project', commit='sha'))
    result = summary('project', 'sha', {'status': 'passed'}, {'sustained_load': path}, 'success')
    assert result['scenarios']['sustained_load']['status'] == 'passed'
    assert result['overall_acceptance_status'] == 'incomplete'
    batch_path = tmp_path / 'sustained_load_batch.json'
    batch = json.loads(batch_path.read_text())
    batch['scenario_status'] = 'failed'
    batch_path.write_text(json.dumps(batch))
    result = summary('project', 'sha', {'status': 'passed'}, {'sustained_load': path}, 'success')
    assert result['scenarios']['sustained_load']['status'] == 'failed'
    assert result['overall_acceptance_status'] == 'failed'
