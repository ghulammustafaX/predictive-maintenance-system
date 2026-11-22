import pytest
from module3_reassignment_evidence import correlate, build_record, finalize
from module3_evidence import evidence, published_identity, summary, write_json


def events(owner='fault'):
    return [{'kind': 'revoked', 'partition': 0, 'role': 'fault', 'member_id': 'a', 'epoch_ns': 1, 'queued_offsets': [3, 4, 5]},
            {'kind': 'assigned', 'partition': 0, 'role': owner, 'member_id': 'b', 'epoch_ns': 2},
            {'kind': 'committed', 'partition': 0, 'role': owner, 'value': 3, 'epoch_ns': 3},
            {'kind': 'position', 'partition': 0, 'role': owner, 'value': 6, 'epoch_ns': 4},
            {'kind': 'seek', 'partition': 0, 'role': owner, 'value': 3, 'epoch_ns': 5}]


@pytest.mark.parametrize('mode,owner', [('same', 'fault'), ('transfer', 'peer')])
def test_boundary_correlation(mode, owner):
    boundary = correlate(mode, events(owner))
    assert boundary['owner_consistent'] is True
    assert boundary['discarded_offsets'] == [3, 4, 5]
    assert (boundary['local_position_before_recovery'], boundary['broker_committed_offset_during_recovery'], boundary['recovery_seek_target']) == (6, 3, 3)


def test_missing_seek_and_incorrect_owner_are_not_inferred():
    boundary = correlate('same', events('peer')[:-1])
    assert boundary['owner_consistent'] is False and boundary['recovery_seek_target'] is None
    assert correlate('same', [])['owner_consistent'] is None


def test_missing_identity_or_owner_does_not_pass():
    parts = [{'partition': 0, 'missing_point_identities': ['missing']}, {'partition': 1}]
    record = build_record('same', events=events(), partitions=parts, project='p', commit='c', topic='t', group='g', run_ids=['r0', 'r1'])
    assert record['scenario_status'] == 'failed'
    parts[0]['missing_point_identities'] = None
    assert build_record('same', events=[], partitions=parts, project=None, commit=None, topic='t', group='g', run_ids=[])['scenario_status'] == 'incomplete'


def test_unexpected_identity_or_wrong_offset_fails():
    parts = [{'partition': 0, 'unexpected_point_identities': ['extra']}, {'partition': 1}]
    assert build_record('same', events=events(), partitions=parts, project='p', commit='c', topic='t', group='g', run_ids=['r0','r1'])['scenario_status'] == 'failed'
    parts[0] = {'partition': 0, 'beginning_offset': 0, 'end_offset': 6, 'committed_offset': 5}
    assert build_record('same', events=events(), partitions=parts, project='p', commit='c', topic='t', group='g', run_ids=['r0','r1'])['scenario_status'] == 'failed'


def test_summary_never_passes_incomplete_or_foreign_evidence(tmp_path):
    parts = [{'partition': 0}, {'partition': 1}]
    record = build_record('same', events=events(), partitions=parts, project='old', commit='old', topic='t', group='g', run_ids=['r0','r1'])
    path = tmp_path / 'same.json'
    write_json(path, record)
    report = summary('new', 'new', {'status': 'passed'}, {'same_owner': path}, 'success')
    assert report['scenarios']['same_owner']['status'] == 'incomplete'
    assert report['overall_acceptance_status'] == 'incomplete'


@pytest.mark.parametrize('project,commit', [('other', 'sha'), ('project', 'other')])
def test_summary_rejects_foreign_project_or_commit(tmp_path, project, commit):
    identity = published_identity({'run_id': 'r', 'unit_id': 1, 'time_cycles': 1,
                                   'timestamp': '2026-01-01T00:00:01Z'})
    record = evidence('same_owner', run_id='r', topic='t', consumer_group='g', partition=0,
                      beginning_offset=0, end_offset=1, committed_offset=1,
                      expected_point_identities=[identity], observed_point_identities=[identity],
                      worker_exit_status=0, scenario_status='passed',
                      compose_project='project', git_commit='sha')
    path = tmp_path / 'same.json'
    write_json(path, record)
    report = summary(project, commit, {'status': 'passed'}, {'same_owner': path}, 'success')
    assert report['scenarios']['same_owner']['status'] == 'incomplete'
    assert report['overall_acceptance_status'] == 'incomplete'


@pytest.mark.parametrize('mode,owner', [('same', 'fault'), ('transfer', 'peer')])
def test_observable_reassignment_correctness_does_not_require_instantaneous_diagnostics(mode, owner):
    traced = events(owner)
    for event in traced: event['topic'] = 'topic'
    traced = [event for event in traced if event['kind'] not in {'position', 'seek'}]
    partitions = []
    runs = ['run0', 'run1']
    for partition in (0, 1):
        identities = [published_identity({'run_id': runs[partition], 'unit_id': partition + 1,
                                          'time_cycles': cycle, 'timestamp': '2026-01-01T00:00:00Z'})
                      for cycle in range(1, 7)]
        source = [{'topic': 'topic', 'partition': partition, 'offset': i, 'identity': identity}
                  for i, identity in enumerate(identities)]
        partitions.append({'partition': partition, 'run_id': runs[partition], 'topic': 'topic',
                           'consumer_group': 'group', 'beginning_offset': 0, 'end_offset': 6,
                           'committed_offset': 6, 'initial_end_offset': 0, 'initial_committed_offset': None,
                           'published_source_records': source, 'expected_point_identities': identities,
                           'observed_point_identities': list(reversed(identities))})
    record = build_record(mode, events=traced, partitions=partitions, project='project', commit='sha',
                          topic='topic', group='group', run_ids=runs, worker_exits={'fault': 0, 'peer': 0})
    assert finalize(record, 'project', 'sha')['scenario_status'] == 'passed'
    record['worker_exit_statuses']['peer'] = 137
    with pytest.raises(ValueError): finalize(record, 'project', 'sha')
