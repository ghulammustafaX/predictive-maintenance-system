import copy
import pytest
from module3_evidence import published_identity, summary, write_json
from module3_shutdown_evidence import final_record


def snapshot(mode):
    project, commit = 'pms-m3-1-1', 'a' * 40
    topic = f'{project}-{mode}'
    run = f'{topic}-run'
    points = [published_identity({'run_id': run, 'unit_id': 9, 'time_cycles': i,
                                  'timestamp': f'2026-01-01T00:00:0{i}Z'}) for i in (1, 2, 3)]
    source = [{'topic': topic, 'partition': 0, 'offset': i, 'event_id': f'{run}-{i + 1}',
               'identity': point} for i, point in enumerate(points)]
    return {'schema_version': '1.0', 'scenario_id': 'graceful_shutdown' if mode == 'graceful' else 'exhausted_drain_replay',
            'compose_project': project, 'git_commit': commit, 'topic': topic,
            'consumer_group': f'{topic}-group', 'run_id': run, 'partition': 0,
            'beginning_offset': 0, 'end_offset': 3, 'committed_offset': 3,
            'expected_point_identities': points, 'observed_point_identities': copy.deepcopy(points),
            'published_source_records': source}


def cleanup(drained):
    return {'drained': drained, 'status': 'close_completed_group_unverified' if drained else 'incomplete',
            'cleanup': {key: 'completed' for key in ('producer flush', 'producer close',
                                                     'consumer close', 'influx close', 'group_departure')}}


@pytest.mark.parametrize('mode', ['graceful', 'exhausted'])
def test_correct_shutdown_evidence_and_summary(mode, tmp_path):
    value = snapshot(mode)
    before = None
    if mode == 'exhausted':
        before = copy.deepcopy(value)
        before['committed_offset'] = None
        before['observed_point_identities'] = []
    result = final_record(value, cleanup(mode == 'graceful'), value['compose_project'], value['git_commit'],
                          mode, 0 if mode == 'graceful' else 1, 0 if before else None, before)
    assert result['scenario_status'] == 'passed'
    path = tmp_path / 'shutdown.json'
    write_json(path, result)
    checked = summary(value['compose_project'], value['git_commit'], {'status': 'passed'},
                      {result['scenario_id']: path}, 'success')
    assert checked['scenarios'][result['scenario_id']]['status'] == 'passed'


@pytest.mark.parametrize('key,bad', [('compose_project', 'wrong'), ('git_commit', 'wrong'),
                                     ('run_id', 'wrong'), ('scenario_id', 'wrong'),
                                     ('committed_offset', 2), ('observed_point_identities', [])])
def test_shutdown_rejects_mismatched_metadata_or_observations(key, bad):
    value = snapshot('graceful')
    value[key] = bad
    with pytest.raises(ValueError):
        final_record(value, cleanup(True), 'pms-m3-1-1', 'a' * 40, 'graceful', 0)


def test_exhausted_shutdown_requires_uncommitted_replayable_input():
    value = snapshot('exhausted')
    before = copy.deepcopy(value)
    before['committed_offset'] = 1
    before['observed_point_identities'] = []
    with pytest.raises(ValueError):
        final_record(value, cleanup(False), value['compose_project'], value['git_commit'], 'exhausted', 1, 0, before)
