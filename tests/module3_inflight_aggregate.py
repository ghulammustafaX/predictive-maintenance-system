"""Validate genuine post-write crash and replay observations."""
import json
from pathlib import Path
from module3_evidence import evidence
from module3_load_evidence import validate_identity


def finalize(record, project, commit, root):
    scenario = record.get('scenario_id')
    if (scenario not in {'crash_after_ack', 'restart_replay'} or not project or not commit
            or record.get('compose_project') != project or record.get('git_commit') != commit
            or record.get('topic') != 'm3-inflight-telemetry' or record.get('consumer_group') != 'm3-inflight-group'
            or record.get('partition') != 0 or not record.get('run_id')):
        raise ValueError('Crash/replay metadata is missing or inconsistent')
    if record.get('scenario_status') == 'failed':
        raise ValueError('Crash/replay stage reported failure')
    expected, observed, source = (record.get('expected_point_identities'),
                                  record.get('observed_point_identities'), record.get('published_source_records'))
    if (record.get('beginning_offset') != 0 or record.get('end_offset') != 6
            or not isinstance(expected, list) or len(expected) != 6
            or not isinstance(observed, list) or len(observed) != 6
            or not isinstance(source, list) or len(source) != 6):
        raise ValueError('Crash/replay observations are incomplete')
    run = record['run_id']
    if ([(item.get('topic'), item.get('partition'), item.get('offset')) for item in source]
            != [(record['topic'], 0, i) for i in range(6)]
            or [item.get('event_id') for item in source] != [f'{run}-{i}' for i in range(1, 7)]
            or [item.get('identity') for item in source] != expected):
        raise ValueError('Published crash/replay source identities do not match')
    for identity in expected + observed:
        validate_identity(identity, run)
    checked = evidence(scenario, expected_point_identities=expected, observed_point_identities=observed)
    if checked['missing_point_identities'] or checked['unexpected_point_identities'] or checked['duplicate_observed_identities']:
        raise ValueError('Crash/replay point identities do not reconcile')
    if scenario == 'crash_after_ack':
        if (record.get('stage') != 'record-crash' or record.get('worker_exit_status') != 137
                or 'committed_offset' not in record or record['committed_offset'] not in (None, 0)
                or record.get('offset_observation') != 'after process crash'
                or record.get('influx_write_ack_log_marker_verified') is not True):
            raise ValueError('Confirmed-write crash observations are incomplete')
    else:
        if (record.get('stage') != 'record-recovery' or record.get('worker_exit_status') != 0
                or record.get('committed_offset') != 6
                or record.get('offset_observation') != 'after recovery worker exit'):
            raise ValueError('Recovery worker or committed offset did not reconcile')
        crash = json.loads((Path(root) / 'crash_after_ack.json').read_text(encoding='utf-8'))
        if crash.get('scenario_status') != 'passed':
            raise ValueError('Matching crash-stage evidence has not passed')
        finalize(crash, project, commit, root)
        if (crash['run_id'] != run or crash['published_source_records'] != source
                or crash['expected_point_identities'] != expected):
            raise ValueError('Recovery evidence does not match its crash stage')
    result = dict(record)
    result['scenario_status'] = 'passed'
    return result
