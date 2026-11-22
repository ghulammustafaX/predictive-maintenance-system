"""Validate hosted shutdown evidence using observed offsets and point identities."""
import json
import os
from pathlib import Path
from module3_evidence import evidence
from module3_runner_evidence import atomic_update


def validate_snapshot(snapshot, project, commit, mode):
    scenario = 'graceful_shutdown' if mode == 'graceful' else 'exhausted_drain_replay'
    topic = f'{project}-{mode}'
    if (not project or not commit or snapshot.get('schema_version') != '1.0' or snapshot.get('scenario_id') != scenario
            or snapshot.get('compose_project') != project or snapshot.get('git_commit') != commit
            or snapshot.get('topic') != topic or snapshot.get('consumer_group') != f'{topic}-group'
            or snapshot.get('run_id') != f'{topic}-run' or snapshot.get('partition') != 0):
        raise ValueError('Shutdown scenario metadata does not match this run')


def final_record(snapshot, cleanup, project, commit, mode, initial_exit, recovery_exit=None, before=None):
    if mode not in {'graceful', 'exhausted'}:
        raise ValueError('Unknown shutdown mode')
    validate_snapshot(snapshot, project, commit, mode)
    if (snapshot.get('beginning_offset') != 0 or snapshot.get('end_offset') != 3
            or snapshot.get('committed_offset') != 3
            or len(snapshot.get('expected_point_identities') or []) != 3
            or len(snapshot.get('observed_point_identities') or []) != 3):
        raise ValueError('Shutdown final offsets or identities are incomplete')
    source = snapshot.get('published_source_records')
    if (not isinstance(source, list) or len(source) != 3
            or [(item.get('topic'), item.get('partition'), item.get('offset')) for item in source]
            != [(snapshot['topic'], 0, offset) for offset in range(3)]
            or [item.get('event_id') for item in source] != [f"{snapshot['run_id']}-{i}" for i in (1, 2, 3)]
            or [item.get('identity') for item in source] != snapshot['expected_point_identities']):
        raise ValueError('Shutdown published source identities do not reconcile')
    operations = cleanup.get('cleanup') if isinstance(cleanup, dict) else None
    if not isinstance(operations, dict) or any(key not in operations for key in
            ('producer flush', 'producer close', 'consumer close', 'influx close', 'group_departure')):
        raise ValueError('Shutdown cleanup observations are incomplete')
    if mode == 'graceful':
        if type(initial_exit) is not int or initial_exit != 0 or cleanup.get('drained') is not True or cleanup.get('status') != 'close_completed_group_unverified':
            raise ValueError('Graceful shutdown did not complete')
        exit_status = initial_exit
    else:
        if (type(initial_exit) is not int or initial_exit == 0 or type(recovery_exit) is not int
                or recovery_exit != 0 or cleanup.get('drained') is not False or cleanup.get('status') != 'incomplete'
                or not isinstance(before, dict)):
            raise ValueError('Exhausted-drain recovery observations are incomplete')
        validate_snapshot(before, project, commit, mode)
        if (before.get('beginning_offset') != 0 or before.get('end_offset') != 3
                or 'committed_offset' not in before or before['committed_offset'] not in (None, 0)
                or before.get('observed_point_identities') != []
                or before.get('expected_point_identities') != snapshot.get('expected_point_identities')
                or before.get('published_source_records') != source):
            raise ValueError('Pre-recovery input or identities do not reconcile')
        exit_status = recovery_exit
    return evidence(snapshot['scenario_id'], run_id=snapshot['run_id'], topic=snapshot['topic'],
                    consumer_group=snapshot['consumer_group'], partition=0,
                    beginning_offset=snapshot['beginning_offset'], end_offset=snapshot['end_offset'],
                    committed_offset=snapshot['committed_offset'],
                    expected_point_identities=snapshot['expected_point_identities'],
                    observed_point_identities=snapshot['observed_point_identities'],
                    worker_exit_status=exit_status, scenario_status='passed', compose_project=project,
                    git_commit=commit, shutdown_cleanup=cleanup, initial_worker_exit_status=initial_exit,
                    recovery_worker_exit_status=recovery_exit, pre_recovery_snapshot=before,
                    published_source_records=source,
                    http_write_attempt_count=None)


def validate_record(record, project, commit):
    mode = {'graceful_shutdown': 'graceful', 'exhausted_drain_replay': 'exhausted'}.get(record.get('scenario_id'))
    if mode is None:
        raise ValueError('Unknown shutdown scenario')
    return final_record(record, record.get('shutdown_cleanup'), project, commit, mode,
                        record.get('initial_worker_exit_status'), record.get('recovery_worker_exit_status'),
                        record.get('pre_recovery_snapshot'))


if __name__ == '__main__':
    import sys
    if len(sys.argv) not in (5, 7):
        raise SystemExit('usage: shutdown_evidence.py MODE SNAPSHOT CLEANUP INITIAL_EXIT [RECOVERY_EXIT BEFORE]')
    mode, snapshot_path, cleanup_path, initial = sys.argv[1:5]
    snapshot = json.loads(Path(snapshot_path).read_text(encoding='utf-8'))
    cleanup = json.loads(Path(cleanup_path).read_text(encoding='utf-8'))
    recovery = int(sys.argv[5]) if len(sys.argv) == 7 else None
    before = json.loads(Path(sys.argv[6]).read_text(encoding='utf-8')) if len(sys.argv) == 7 else None
    result = final_record(snapshot, cleanup, os.environ.get('M3_PROJECT'), os.environ.get('GITHUB_SHA'),
                          mode, int(initial), recovery, before)
    atomic_update(snapshot_path, lambda record: (record.clear(), record.update(result)))
