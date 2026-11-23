"""Validate final batch and at-least-once DLQ observations independently."""
from module3_evidence import evidence
from module3_load_evidence import validate_identity


def finalize(record, project, commit):
    scenario = record.get('scenario_id')
    if (scenario not in {'batch_persistence', 'dlq'} or not project or not commit
            or record.get('compose_project') != project or record.get('git_commit') != commit
            or record.get('topic') != 'm3-batch-telemetry'
            or record.get('consumer_group') != 'm3-batch-writer-group'
            or record.get('partition') != 0 or not record.get('run_id')):
        raise ValueError('Batch/DLQ metadata is missing or inconsistent')
    if (record.get('beginning_offset') != 0 or record.get('end_offset') != 8
            or record.get('committed_offset') != 8):
        raise ValueError('Batch source offsets do not reconcile')
    run = record['run_id']
    expected, observed = record.get('expected_point_identities'), record.get('observed_point_identities')
    source, correlations = record.get('published_source_records'), record.get('dlq_source_correlations')
    if (not isinstance(expected, list) or len(expected) != 6 or not isinstance(observed, list) or len(observed) != 6
            or not isinstance(source, list) or len(source) != 8 or not isinstance(correlations, list) or not correlations):
        raise ValueError('Batch/DLQ observations are incomplete')
    if ([(r.get('topic'), r.get('partition'), r.get('offset')) for r in source]
            != [(record['topic'], 0, offset) for offset in range(8)] or
            [r.get('event_id') for r in source] != [f'{run}-1', f'{run}-2', f'{run}-3', None,
                                                    f'{run}-4', f'{run}-5', f'{run}-6', f'{run}-6']):
        raise ValueError('Published source records do not match the benchmark run')
    for identity in expected + observed:
        validate_identity(identity, run)
    checked = evidence(scenario, expected_point_identities=expected, observed_point_identities=observed)
    if (checked['missing_point_identities'] or checked['unexpected_point_identities']
            or checked['duplicate_observed_identities'] or
            {identity['tags']['cycle'] for identity in expected} != {str(i) for i in range(1, 7)}):
        raise ValueError('Batch point identities do not reconcile')
    for correlation in correlations:
        if (correlation.get('source_topic') != record['topic'] or correlation.get('source_partition') != 0
                or correlation.get('source_offset') != 3 or correlation.get('dlq_topic') != 'm3-batch-dead-letter'
                or type(correlation.get('dlq_partition')) is not int or correlation['dlq_partition'] < 0
                or type(correlation.get('dlq_offset')) is not int or correlation['dlq_offset'] < 0
                or correlation.get('source_committed_offset') != 8
                or correlation.get('source_value') != {'invalid': True}):
            raise ValueError('DLQ record does not match the invalid source record')
    result = dict(record)
    result['scenario_status'] = 'passed'
    result['note'] = ('Final source offsets, identities and DLQ correlation verified; '
                      'DLQ acknowledgement timing and worker exit are not directly observed.')
    return result
