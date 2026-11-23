"""Conservative correlation of directly observed reassignment events."""
from module3_evidence import evidence


def correlate(mode, events):
    if mode not in {'same', 'transfer'}:
        raise ValueError('Unknown reassignment mode')
    events = sorted(events, key=lambda event: event['epoch_ns'])
    revocations = [e for e in events if e.get('kind') == 'revoked' and e.get('partition') == 0]
    revoked = next((event for event in revocations if event.get('queued_offsets')), revocations[0] if revocations else None)
    after = [e for e in events if revoked and e['epoch_ns'] > revoked['epoch_ns'] and e.get('partition') == 0]
    assigned = next((e for e in after if e.get('kind') == 'assigned'), None)
    owner = assigned.get('role') if assigned else None
    recovery = [e for e in after if e.get('role') == owner and assigned and e['epoch_ns'] >= assigned['epoch_ns']]
    def first(kind):
        return next((e for e in recovery if e.get('kind') == kind), None)
    committed, position, seek = first('committed'), first('position'), first('seek')
    expected_owner = revoked.get('role') if revoked and mode == 'same' else ('peer' if revoked and revoked.get('role') == 'fault' else None)
    observed_owner = assigned.get('role') if assigned else None
    owner_consistent = None if expected_owner is None or observed_owner is None else expected_owner == observed_owner
    return {'original_member_id': revoked.get('member_id') if revoked else None,
            'original_owner_role': revoked.get('role') if revoked else None,
            'revocation': revoked, 'new_member_id': assigned.get('member_id') if assigned else None,
            'new_owner_role': observed_owner, 'assignment': assigned,
            'owner_consistent': owner_consistent,
            'discarded_offsets': revoked.get('queued_offsets') if revoked else None,
            'broker_committed_offset_at_revocation': None,
            'local_position_before_recovery': position.get('value') if position else None,
            'broker_committed_offset_during_recovery': committed.get('value') if committed else None,
            'recovery_seek_target': seek.get('value') if seek else None,
            'resume_or_poll_state': None}


def build_record(mode, *, events, partitions, project, commit, topic, group, run_ids, worker_exits=None):
    if len(partitions) != 2 or {p.get('partition') for p in partitions} != {0, 1}:
        raise ValueError('Both source partitions must be represented')
    boundary = correlate(mode, events)
    scenario = 'same_owner' if mode == 'same' else 'different_owner'
    mismatch = boundary['owner_consistent'] is False or any(
        p.get('missing_point_identities') or p.get('unexpected_point_identities')
        or p.get('duplicate_observed_identities') or
        (p.get('beginning_offset') is not None and p['beginning_offset'] != 0) or
        (p.get('end_offset') is not None and p['end_offset'] != 6) or
        (p.get('committed_offset') is not None and p['committed_offset'] != 6)
        for p in partitions)
    return evidence(scenario, run_id=None, topic=topic, consumer_group=group,
                    scenario_status='failed' if mismatch else 'incomplete',
                    compose_project=project, git_commit=commit, run_ids=run_ids,
                    partitions=partitions, rebalance_boundary=boundary, raw_boundary_events=events,
                    worker_exit_status=None, worker_exit_statuses=worker_exits,
                    note='Incomplete until every mandatory boundary and lifecycle observation is verified.')


def finalize(record, project, commit):
    """Validate observable correctness; instantaneous position/seek remain diagnostics."""
    from module3_load_evidence import validate_identity
    scenario = record.get('scenario_id')
    if (scenario not in {'same_owner', 'different_owner'} or not project or not commit
            or record.get('compose_project') != project or record.get('git_commit') != commit
            or not record.get('topic') or not record.get('consumer_group')):
        raise ValueError('Reassignment metadata does not match this run')
    boundary = record.get('rebalance_boundary') or {}
    revoked, assigned = boundary.get('revocation') or {}, boundary.get('assignment') or {}
    expected_role = 'fault' if scenario == 'same_owner' else 'peer'
    if (boundary.get('owner_consistent') is not True or revoked.get('kind') != 'revoked'
            or revoked.get('role') != 'fault' or assigned.get('kind') != 'assigned'
            or assigned.get('role') != expected_role or revoked.get('partition') != 0
            or assigned.get('partition') != 0 or revoked.get('topic') != record['topic']
            or assigned.get('topic') != record['topic'] or boundary.get('discarded_offsets') != [3, 4, 5]
            or not isinstance(revoked.get('epoch_ns'), int) or not isinstance(assigned.get('epoch_ns'), int)
            or assigned['epoch_ns'] <= revoked['epoch_ns']):
        raise ValueError('Reassignment ownership or discarded-work observations do not reconcile')
    if record.get('worker_exit_statuses') != {'fault': 0, 'peer': 0}:
        raise ValueError('Controlled worker exit observations are incomplete')
    items, runs = record.get('partitions'), record.get('run_ids')
    if not isinstance(items, list) or len(items) != 2 or {p.get('partition') for p in items} != {0, 1} or not isinstance(runs, list) or len(runs) != 2 or len(set(runs)) != 2:
        raise ValueError('Both partition and run observations are required')
    for item in items:
        partition = item['partition']
        run = runs[partition]
        source = item.get('published_source_records')
        expected, observed = item.get('expected_point_identities'), item.get('observed_point_identities')
        if (item.get('topic') != record['topic'] or item.get('consumer_group') != record['consumer_group']
                or item.get('run_id') != run or item.get('beginning_offset') != 0
                or item.get('end_offset') != 6 or item.get('committed_offset') != 6
                or item.get('initial_end_offset') != 0 or 'initial_committed_offset' not in item
                or item['initial_committed_offset'] not in (None, 0)
                or not isinstance(source, list) or len(source) != 6
                or not isinstance(expected, list) or len(expected) != 6
                or not isinstance(observed, list) or len(observed) != 6):
            raise ValueError('Partition offsets, identity counts or run metadata do not reconcile')
        if ([(entry.get('topic'), entry.get('partition'), entry.get('offset')) for entry in source]
                != [(record['topic'], partition, offset) for offset in range(6)]
                or [entry.get('identity') for entry in source] != expected):
            raise ValueError('Published source records do not match expected identities')
        for identity in expected + observed:
            validate_identity(identity, run)
        checked = evidence(scenario, expected_point_identities=expected, observed_point_identities=observed)
        if checked['missing_point_identities'] or checked['unexpected_point_identities'] or checked['duplicate_observed_identities']:
            raise ValueError('Stored point identities do not reconcile')
    result = dict(record)
    result['scenario_status'] = 'passed'
    result['note'] = 'Offsets, identities, ownership and controlled exits verified; instantaneous position and seek are diagnostics.'
    return result
