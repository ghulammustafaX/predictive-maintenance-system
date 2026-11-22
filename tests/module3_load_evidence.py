"""Offline-checkable sustained-load identity and offset reconciliation."""
from __future__ import annotations

import json
from pathlib import Path


class IdentityMismatch(ValueError):
    def __init__(self, result):
        super().__init__('Complete published and stored point identities differ')
        self.result = result


def key(identity):
    return json.dumps(identity, sort_keys=True, separators=(',', ':'))


def write_jsonl(path, records):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as stream:
        for record in records:
            stream.write(json.dumps(record, sort_keys=True, separators=(',', ':')) + '\n')


def read_jsonl(path, expected_count):
    path = Path(path)
    if not path.is_file():
        raise ValueError(f'Missing identity artifact: {path}')
    with path.open(encoding='utf-8') as stream:
        records = [json.loads(line) for line in stream if line.strip()]
    if len(records) != expected_count:
        raise ValueError(f'Identity artifact count mismatch: {path}')
    return records


def validate_identity(identity, run_id):
    if (not isinstance(identity, dict) or identity.get('measurement') != 'sensor_telemetry'
            or not isinstance(identity.get('timestamp_ns'), int) or isinstance(identity.get('timestamp_ns'), bool)
            or not isinstance(identity.get('tags'), dict)
            or set(identity['tags']) != {'dataset_id', 'unit_id', 'run_id', 'cycle'}
            or any(not isinstance(value, str) for value in identity['tags'].values())
            or identity['tags']['run_id'] != run_id):
        raise ValueError('Incomplete point identity or incorrect run')


def reconcile(published, observed, *, topic, run_id, offsets, expected_count, partitions):
    if len(published) != expected_count:
        raise ValueError('Published identity count differs from the workload')
    expected = []
    for record in published:
        identity = record['identity']
        validate_identity(identity, run_id)
        tags = identity['tags']
        cycle = int(tags['cycle'])
        if (record['topic'] != topic or tags['run_id'] != run_id or cycle < 1
                or cycle > expected_count or record['partition'] != (cycle - 1) % partitions
                or not isinstance(record.get('offset'), int) or record['offset'] < 0):
            raise ValueError('Published identity or source partition does not match the run')
        expected.append(identity)
    for identity in observed:
        validate_identity(identity, run_id)
    expected_by_key = {key(item): item for item in expected}
    observed_by_key = {key(item): item for item in observed}
    duplicate_expected = len(expected) - len(expected_by_key)
    duplicate_observed = len(observed) - len(observed_by_key)
    missing = [expected_by_key[k] for k in sorted(expected_by_key.keys() - observed_by_key.keys())]
    unexpected = [observed_by_key[k] for k in sorted(observed_by_key.keys() - expected_by_key.keys())]
    expected_keys = {f'{topic}:{partition}' for partition in range(partitions)}
    committed, ends = offsets['committed_offsets'], offsets['end_offsets']
    if set(committed) != expected_keys or set(ends) != expected_keys or offsets['total_lag'] != 0:
        raise ValueError('Missing partition offsets or nonzero lag')
    for partition in range(partitions):
        partition_key = f'{topic}:{partition}'
        source_offsets = [r['offset'] for r in published if r['partition'] == partition]
        if sorted(source_offsets) != list(range(len(source_offsets))) or committed[partition_key] != ends[partition_key] or ends[partition_key] != len(source_offsets):
            raise ValueError('Committed, end and published partition offsets do not reconcile')
    result = {'expected_count': len(expected), 'observed_count': len(observed),
              'missing_point_identities': missing, 'unexpected_point_identities': unexpected,
              'duplicate_published_identities': duplicate_expected,
              'duplicate_observed_identities': duplicate_observed}
    if len(observed) != expected_count or missing or unexpected or duplicate_expected or duplicate_observed:
        raise IdentityMismatch(result)
    return result


def aggregate(root, *, project, commit, expected_count=3000):
    """Re-read both mode records and artifacts; never trust their pass labels alone."""
    root = Path(root)
    modes = {}
    manifest = None
    try:
        manifest = json.loads((root / 'publisher.json').read_text(encoding='utf-8'))
        if (manifest['records_per_mode'] != expected_count
                or set(manifest['topics']) != {'legacy', 'batch'}
                or set(manifest['run_ids']) != {'legacy', 'batch'}
                or len(set(manifest['topics'].values())) != 2
                or len(set(manifest['run_ids'].values())) != 2):
            manifest = None
    except (OSError, ValueError, KeyError, TypeError):
        manifest = None
    for mode in ('legacy', 'batch'):
        record_path = root / f'sustained_load_{mode}.json'
        result = {'status': 'incomplete', 'path': record_path.name, 'reason': None}
        modes[mode] = result
        try:
            record = json.loads(record_path.read_text(encoding='utf-8'))
            if not project or not commit or record.get('compose_project') != project or record.get('git_commit') != commit:
                raise ValueError('Project or commit missing or inconsistent')
            if record.get('schema_version') != '1.0' or record.get('scenario_id') != 'sustained_load':
                raise ValueError('Unexpected scenario or schema')
            if manifest is None:
                raise ValueError('Publisher evidence is unavailable or outside the expected workload')
            if (record.get('topic') != manifest['topics'][mode]
                    or record.get('run_id') != manifest['run_ids'][mode]):
                raise ValueError('Mode record does not belong to the published benchmark run')
            if record.get('scenario_status') == 'failed':
                result['status'] = 'failed'
                result['reason'] = 'Mode reported a verified failure'
                continue
            if record.get('scenario_status') != 'passed':
                raise ValueError('Mode or publisher evidence is incomplete')
            worker = json.loads((root / f'{mode}.json').read_text(encoding='utf-8'))
            topic, run_id = manifest['topics'][mode], manifest['run_ids'][mode]
            record_fields = ('topic', 'run_id', 'expected_count', 'consumer_group',
                             'partition_offsets', 'identity_reconciliation', 'worker_exit_status',
                             'expected_identity_artifact', 'observed_identity_artifact',
                             'throughput_records_per_second', 'successful_application_write_calls')
            worker_fields = ('mode', 'topic', 'consumer_group', 'expected_records', 'processed_records',
                             'offsets', 'records_per_second', 'write_latency_ms', 'commit_latency_ms',
                             'end_to_end_latency_ms', 'points_per_write')
            if any(record.get(key) is None for key in record_fields) or any(worker.get(key) is None for key in worker_fields):
                raise ValueError('Required worker or mode observation is unavailable')
            if (record.get('topic') != topic or record.get('run_id') != run_id
                    or record.get('expected_count') != expected_count or worker.get('mode') != mode
                    or worker.get('topic') != topic or worker.get('consumer_group') != record.get('consumer_group')
                    or not record.get('consumer_group') or worker.get('expected_records') != expected_count
                    or worker.get('processed_records') != expected_count
                    or worker.get('offsets') != record.get('partition_offsets')
                    or worker.get('records_per_second') != record.get('throughput_records_per_second')
                    or worker['write_latency_ms']['calls'] != record.get('successful_application_write_calls')
                    or worker['write_latency_ms']['calls'] < 1 or worker['commit_latency_ms']['calls'] < 1
                    or worker['end_to_end_latency_ms']['samples'] != expected_count
                    or (mode == 'legacy' and worker['points_per_write']['average'] != 1)
                    or (mode == 'batch' and worker['points_per_write']['average'] < 2)):
                result['status'] = 'failed'
                raise ValueError('Mode, run, worker or offset metadata mismatch')
            if record.get('worker_exit_status') is None:
                raise ValueError('Worker exit status is unavailable')
            if type(record['worker_exit_status']) is not int or record['worker_exit_status'] != 0:
                result['status'] = 'failed'
                raise ValueError('Worker did not exit successfully')
            names = [f'sustained_load_{mode}_published.jsonl', f'sustained_load_{mode}_observed.jsonl']
            if [record.get('expected_identity_artifact'), record.get('observed_identity_artifact')] != names:
                result['status'] = 'failed'
                raise ValueError('Unexpected identity artifact references')
            if any(not (root / name).is_file() or (root / name).is_symlink()
                   or (root / name).resolve().parent != root.resolve() for name in names):
                raise ValueError('An identity artifact is unavailable')
            try:
                published = read_jsonl(root / names[0], expected_count)
                observed = read_jsonl(root / names[1], expected_count)
                actual = reconcile(published, observed, topic=topic, run_id=run_id,
                                   offsets=worker['offsets'], expected_count=expected_count, partitions=4)
                if record.get('identity_reconciliation') != actual:
                    raise ValueError('Reported identity reconciliation differs from artifacts')
                from module3_evidence import evidence
                evidence('sustained_load', run_id=run_id, topic=topic,
                         consumer_group=record['consumer_group'], worker_exit_status=record['worker_exit_status'],
                         scenario_status='passed', artifact_root=root, expected_count=expected_count,
                         expected_identity_artifact=names[0], observed_identity_artifact=names[1],
                         partition_offsets=worker['offsets'])
            except json.JSONDecodeError as exc:
                raise ValueError(f'Identity artifact is unreadable: {exc}') from exc
            except (ValueError, KeyError, TypeError) as exc:
                result['status'] = 'failed'
                raise ValueError(f'Artifact reconciliation failed: {exc}') from exc
            result['status'] = 'passed'
        except (OSError, ValueError, KeyError, TypeError) as exc:
            result['reason'] = str(exc)
    if all(modes[mode]['status'] == 'passed' for mode in ('legacy', 'batch')):
        legacy_worker = json.loads((root / 'legacy.json').read_text(encoding='utf-8'))
        batch_worker = json.loads((root / 'batch.json').read_text(encoding='utf-8'))
        if legacy_worker['consumer_group'] == batch_worker['consumer_group']:
            for result in modes.values():
                result['status'] = 'failed'
                result['reason'] = 'Benchmark consumers unexpectedly share a group'
    statuses = {entry['status'] for entry in modes.values()}
    overall = 'failed' if 'failed' in statuses else ('passed' if statuses == {'passed'} else 'incomplete')
    from module3_evidence import evidence
    result = evidence('sustained_load', scenario_status='incomplete', compose_project=project,
                      git_commit=commit, expected_count_per_mode=expected_count, mode_evidence=modes,
                      diagnostic_limitations=['Final InfluxDB points do not reveal repeated identical writes.',
                                              'Throughput is an observed measurement, not a performance guarantee.'])
    # The aggregate has two runs and partitions, so the scalar schema fields
    # remain null; the status derives only from the independently checked modes.
    result['scenario_status'] = overall
    return result
