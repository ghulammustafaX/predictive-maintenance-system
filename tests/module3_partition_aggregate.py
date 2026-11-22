"""Reconcile the failure window and recovery without requiring a worker exit."""
import json
import os
from pathlib import Path
from module3_evidence import evidence, write_json


def aggregate(root, project, commit):
    root = Path(root)
    status, reason, stages = 'incomplete', None, {}
    try:
        if not project or not commit:
            raise ValueError('Project and commit are required')
        for stage in ('failure', 'recovery'):
            value = json.loads((root / f'partition_isolation_{stage}.json').read_text(encoding='utf-8'))
            if (value.get('schema_version') != '1.0' or value.get('scenario_id') != 'partition_isolation'
                    or value.get('stage') != stage or value.get('compose_project') != project
                    or value.get('git_commit') != commit):
                raise ValueError('Stage identity is missing or inconsistent')
            if value.get('scenario_status') == 'failed':
                status = 'failed'
                raise ValueError(f'{stage} stage reported failure')
            if value.get('scenario_status') not in {'incomplete', 'passed'}:
                raise ValueError('Stage has not supplied usable observations')
            stages[stage] = value
        failure, recovery = stages['failure'], stages['recovery']
        for field in ('topic', 'consumer_group', 'scenario_run_id'):
            if not failure.get(field) or failure.get(field) != recovery.get(field):
                raise ValueError(f'Stage {field} is missing or inconsistent')
        partition_sets = []
        for stage in ('failure', 'recovery'):
            items = stages[stage].get('partitions')
            if not isinstance(items, list) or len(items) != 2 or {p.get('partition') for p in items} != {0, 1}:
                raise ValueError('Two partition observations are required')
            partition_sets.append({p['partition']: p for p in items})
        before, after = partition_sets
        for partition in (0, 1):
            a, b = before[partition], after[partition]
            for item_stage, item in (('failure', a), ('recovery', b)):
                if (item.get('scenario_id') != 'partition_isolation' or item.get('topic') != failure['topic']
                        or item.get('consumer_group') != failure['consumer_group'] or not item.get('run_id')
                        or item.get('beginning_offset') != 0 or item.get('end_offset') != 3
                        or 'committed_offset' not in item or 'initial_committed_offset' not in item):
                    raise ValueError('Partition identity or source bounds are inconsistent')
                expected, observed = item.get('expected_point_identities'), item.get('observed_point_identities')
                source = item.get('published_source_records')
                if not isinstance(expected, list) or not isinstance(observed, list) or not isinstance(source, list):
                    raise ValueError('Point and published-record observations are required')
                if (len(expected) != 3 or len(source) != 3 or
                        [entry.get('offset') for entry in source] != [0, 1, 2] or
                        any(entry.get('topic') != failure['topic'] or entry.get('partition') != partition for entry in source) or
                        [entry.get('identity') for entry in source] != expected):
                    status = 'failed'
                    raise ValueError('Published source identities do not reconcile')
                for identity in expected + observed:
                    tags = identity.get('tags', {}) if isinstance(identity, dict) else {}
                    if (identity.get('measurement') != 'sensor_telemetry' or
                            set(tags) != {'dataset_id', 'unit_id', 'run_id', 'cycle'} or
                            tags.get('run_id') != item['run_id'] or type(identity.get('timestamp_ns')) is not int
                            or any(not isinstance(value, str) for value in tags.values())):
                        status = 'failed'
                        raise ValueError('Point identity is invalid or belongs to another run')
                if {identity['tags']['cycle'] for identity in expected} != {'1', '2', '3'}:
                    status = 'failed'
                    raise ValueError('Expected source cycles do not reconcile')
                checked = evidence('partition_isolation', run_id=item['run_id'],
                                   expected_point_identities=expected, observed_point_identities=observed)
                if (checked['unexpected_point_identities'] or checked['duplicate_observed_identities'] or
                        (item_stage == 'recovery' or partition == 1) and checked['missing_point_identities']):
                    status = 'failed'
                    raise ValueError('Observed point identities do not reconcile')
            if (a['run_id'] != b['run_id'] or a['expected_point_identities'] != b['expected_point_identities']
                    or a['published_source_records'] != b['published_source_records']):
                status = 'failed'
                raise ValueError('Failure and recovery source identities differ')
            if a.get('release_marker_present') is not False or b.get('release_marker_present') is not True:
                raise ValueError('Failure and recovery ordering is unavailable')
            if b.get('committed_offset') != 3:
                status = 'failed'
                raise ValueError('Partition did not reach the expected recovery offset')
        if (before[0]['run_id'] == before[1]['run_id'] or
                failure['scenario_run_id'] != f"{before[0]['run_id']}|{before[1]['run_id']}"):
            raise ValueError('Partition run identities do not match the scenario')
        if (before[0].get('committed_offset') not in (None, 0) or before[0]['observed_point_identities']
                or before[1].get('initial_committed_offset') not in (None, 0)
                or before[1].get('committed_offset') != 3 or len(before[1]['observed_point_identities']) != 3
                or type(before[0].get('injected_failure_count')) is not int
                or before[0]['injected_failure_count'] < 3
                or before[1].get('injected_failure_count') != 0
                or type(after[0].get('injected_failure_count')) is not int
                or after[0]['injected_failure_count'] < before[0]['injected_failure_count']
                or after[1].get('injected_failure_count') != 0):
            status = 'failed'
            raise ValueError('Failure-window isolation observations do not reconcile')
        status = 'passed'
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        reason = str(exc)
    record = evidence('partition_isolation', scenario_status='incomplete', compose_project=project,
                      git_commit=commit, stage_files=['partition_isolation_failure.json',
                                                     'partition_isolation_recovery.json'], reason=reason,
                      note='Worker exit is not required: broker offsets and persisted points establish final progress.')
    record['scenario_status'] = status
    return record


if __name__ == '__main__':
    directory = Path(os.environ.get('M3_EVIDENCE_DIR', 'm3-evidence'))
    write_json(directory / 'partition_isolation.json',
               aggregate(directory, os.environ.get('M3_PROJECT'), os.environ.get('GITHUB_SHA')))
