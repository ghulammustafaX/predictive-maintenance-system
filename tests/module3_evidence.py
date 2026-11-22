"""Structured, fail-closed evidence for isolated Module 03 acceptance."""
from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = '1.0'
STATUSES = {'passed', 'failed', 'incomplete', 'not_run'}
SCENARIOS = ('batch_persistence', 'dlq', 'crash_after_ack', 'restart_replay',
             'partition_isolation', 'same_owner', 'different_owner',
             'graceful_shutdown', 'exhausted_drain_replay', 'sustained_load')


def _timestamp_ns(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('A timezone-aware timestamp is required')
    delta = value.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return ((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds) * 1000


def published_identity(payload):
    """Match build_point's measurement, tags and datetime-to-nanosecond precision."""
    timestamp = payload.get('source_timestamp') or payload.get('timestamp')
    if timestamp is None or payload.get('unit_id') is None or payload.get('time_cycles') is None:
        raise ValueError('Published point lacks a required identity component')
    return {'measurement': 'sensor_telemetry',
            'tags': {'dataset_id': str(payload.get('dataset_id', 'unknown')),
                     'unit_id': str(int(payload['unit_id'])),
                     'run_id': str(payload.get('run_id', 'default')),
                     'cycle': str(int(payload['time_cycles']))},
            'timestamp_ns': _timestamp_ns(timestamp)}


def observed_identity(values):
    """Build an identity from the actual Flux result without inventing tags."""
    tags = ('dataset_id', 'unit_id', 'run_id', 'cycle')
    if values.get('_measurement') != 'sensor_telemetry' or any(values.get(tag) is None for tag in tags):
        raise ValueError('Observed point lacks its measurement or a required tag')
    if values.get('_time') is None:
        raise ValueError('Observed point lacks its timestamp')
    return {'measurement': values['_measurement'],
            'tags': {tag: str(values[tag]) for tag in tags},
            'timestamp_ns': _timestamp_ns(values['_time'])}


def _key(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def evidence(scenario_id, *, run_id=None, topic=None, consumer_group=None,
             partition=None, beginning_offset=None, end_offset=None,
             committed_offset=None, expected_point_identities=None,
             observed_point_identities=None, dlq_source_correlations=None,
             worker_exit_status=None, scenario_status='incomplete', artifact_root=None, **extra):
    if scenario_id not in SCENARIOS or scenario_status not in STATUSES:
        raise ValueError('Unknown scenario or status')
    expected = expected_point_identities
    observed = observed_point_identities
    missing = unexpected = duplicates = None
    if expected is not None and observed is not None:
        expected_map = {_key(x): x for x in expected}
        observed_map = {_key(x): x for x in observed}
        missing = [expected_map[k] for k in sorted(expected_map.keys() - observed_map.keys())]
        unexpected = [observed_map[k] for k in sorted(observed_map.keys() - expected_map.keys())]
        counts = Counter(_key(x) for x in observed)
        duplicates = [observed_map[k] for k in sorted(counts) if counts[k] > 1]
    if scenario_status == 'passed' and scenario_id == 'sustained_load':
        from module3_load_evidence import read_jsonl, reconcile
        names = [extra.get('expected_identity_artifact'), extra.get('observed_identity_artifact')]
        count = extra.get('expected_count')
        offsets = extra.get('partition_offsets')
        if (not run_id or not topic or not consumer_group or worker_exit_status != 0
                or artifact_root is None or not isinstance(count, int) or count <= 0
                or not isinstance(offsets, dict) or any(not isinstance(name, str) or Path(name).name != name for name in names)):
            raise ValueError('Incomplete sustained-load correctness evidence')
        published = read_jsonl(Path(artifact_root) / names[0], count)
        observed = read_jsonl(Path(artifact_root) / names[1], count)
        reconcile(published, observed, topic=topic, run_id=run_id, offsets=offsets,
                  expected_count=count, partitions=4)
    elif scenario_status == 'passed':
        required = (run_id, topic, consumer_group, partition, beginning_offset,
                    end_offset, committed_offset, expected, observed, worker_exit_status)
        if any(value is None for value in required) or missing or unexpected or duplicates:
            raise ValueError('Passing evidence must contain reconciled observations')
        if scenario_id == 'dlq' and not dlq_source_correlations:
            raise ValueError('Passing DLQ evidence requires source correlations')
        if scenario_id == 'dlq':
            fields = ('source_topic', 'source_partition', 'source_offset', 'dlq_topic',
                      'dlq_partition', 'dlq_offset', 'source_committed_offset')
            if (not isinstance(dlq_source_correlations, list) or
                    any(not isinstance(item, dict) or any(item.get(field) is None for field in fields)
                        or item['source_topic'] != topic for item in dlq_source_correlations)):
                raise ValueError('Incomplete DLQ source-to-record correlation')
        for identity in [*expected, *observed]:
            if (not isinstance(identity, dict) or identity.get('measurement') != 'sensor_telemetry'
                    or not isinstance(identity.get('timestamp_ns'), int)
                    or not isinstance(identity.get('tags'), dict)
                    or set(identity['tags']) != {'dataset_id', 'unit_id', 'run_id', 'cycle'}
                    or any(not isinstance(v, str) for v in identity['tags'].values())
                    or identity['tags']['run_id'] != run_id):
                raise ValueError('Passing evidence requires full point identities for the same run')
    return {'schema_version': SCHEMA_VERSION, 'scenario_id': scenario_id,
            'run_id': run_id, 'topic': topic, 'consumer_group': consumer_group,
            'partition': partition, 'beginning_offset': beginning_offset,
            'end_offset': end_offset, 'committed_offset': committed_offset,
            'expected_point_identities': expected, 'observed_point_identities': observed,
            'missing_point_identities': missing, 'unexpected_point_identities': unexpected,
            'duplicate_observed_identities': duplicates,
            'dlq_source_correlations': dlq_source_correlations,
            'worker_exit_status': worker_exit_status, 'scenario_status': scenario_status,
            'evidence_timestamp': datetime.now(timezone.utc).isoformat(), **extra}


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    uid, gid, export = (os.environ.get('M3_EVIDENCE_UID'), os.environ.get('M3_EVIDENCE_GID'),
                        os.environ.get('M3_EVIDENCE_DIR'))
    if uid is not None or gid is not None:
        if not uid or not gid or not uid.isdigit() or not gid.isdigit() or not export:
            raise ValueError('Runner evidence ownership configuration is incomplete')
        if path.resolve().parent == Path(export).resolve():
            if not hasattr(os, 'chown'):
                raise OSError('Runner evidence ownership is unsupported')
            os.chown(path, int(uid), int(gid))


def summary(project, commit, preflight, scenario_files, job_status=None):
    statuses = {}
    for scenario in SCENARIOS:
        entry = scenario_files.get(scenario)
        if entry is None:
            # No evidence does not establish whether a CI step ran.
            statuses[scenario] = {'status': 'incomplete', 'path': None}
            continue
        path = Path(entry)
        if not path.is_file():
            statuses[scenario] = {'status': 'incomplete', 'path': str(path)}
            continue
        try:
            value = json.loads(path.read_text(encoding='utf-8'))
            status = value['scenario_status']
            if value.get('schema_version') != SCHEMA_VERSION or value.get('scenario_id') != scenario or status not in STATUSES:
                raise ValueError('Mismatched or invalid evidence')
            if scenario == 'sustained_load':
                from module3_load_evidence import aggregate
                if not project or not commit or value.get('compose_project') != project or value.get('git_commit') != commit:
                    raise ValueError('Aggregate belongs to a different project or commit')
                checked = aggregate(path.parent, project=project, commit=commit, expected_count=3000)
                if checked['scenario_status'] == 'failed':
                    status = 'failed'
                elif checked['scenario_status'] != 'passed' or status != 'passed':
                    status = 'incomplete'
            if scenario == 'partition_isolation':
                from module3_partition_aggregate import aggregate as partition_aggregate
                if not project or not commit or value.get('compose_project') != project or value.get('git_commit') != commit:
                    raise ValueError('Partition aggregate belongs to another project or commit')
                checked = partition_aggregate(path.parent, project, commit)
                if checked['scenario_status'] == 'failed':
                    status = 'failed'
                elif checked['scenario_status'] != 'passed' or status != 'passed':
                    status = 'incomplete'
            if status == 'passed':
                if not project or not commit or value.get('compose_project') != project or value.get('git_commit') != commit:
                    raise ValueError('Passing evidence must belong to this project and commit')
                if scenario in {'same_owner', 'different_owner'}:
                    from module3_reassignment_evidence import finalize
                    finalize(value, project, commit)
                if scenario in {'graceful_shutdown', 'exhausted_drain_replay'}:
                    from module3_shutdown_evidence import validate_record
                    validate_record(value, project, commit)
                if scenario in {'batch_persistence', 'dlq'}:
                    from module3_batch_aggregate import finalize as finalize_batch
                    finalize_batch(value, project, commit)
                if scenario in {'crash_after_ack', 'restart_replay'}:
                    from module3_inflight_aggregate import finalize as finalize_inflight
                    finalize_inflight(value, project, commit, path.parent)
                if scenario not in {'sustained_load', 'partition_isolation', 'same_owner', 'different_owner', 'batch_persistence', 'dlq', 'crash_after_ack', 'restart_replay'}:
                    evidence(scenario, **{key: value.get(key) for key in (
                        'run_id', 'topic', 'consumer_group', 'partition', 'beginning_offset',
                        'end_offset', 'committed_offset', 'expected_point_identities',
                        'observed_point_identities', 'worker_exit_status', 'dlq_source_correlations')}, scenario_status='passed')
        except (OSError, ValueError, KeyError, TypeError):
            status = 'incomplete'
        statuses[scenario] = {'status': status, 'path': str(path)}
    values = {entry['status'] for entry in statuses.values()}
    overall = ('failed' if 'failed' in values or job_status in {'failure', 'cancelled'} else
               ('passed' if values == {'passed'} and preflight.get('status') == 'passed'
                and job_status == 'success' else 'incomplete'))
    return {'schema_version': SCHEMA_VERSION, 'environment': 'github-hosted',
            'compose_project': project, 'git_commit': commit, 'resource_preflight': preflight,
            'overall_acceptance_status': overall, 'scenarios': statuses,
            'known_limitations': ['At-least-once delivery may create duplicate Kafka or DLQ records.',
                                  'Final InfluxDB point identities do not measure write attempts.']}


def read_preflight(root):
    root = Path(root)
    preflight_path = root / 'preflight.json'
    try:
        preflight = json.loads(preflight_path.read_text(encoding='utf-8'))
        preflight['status'] = ('passed' if preflight['available_memory_kib'] >= 4194304
                               and preflight['available_disk_kib'] >= 10485760
                               and preflight['cpus'] >= 2 else 'failed')
    except (OSError, ValueError, TypeError, KeyError):
        preflight = {'status': 'not_run'}
    return preflight


def scenario_entries(root):
    root = Path(root)
    aliases = {'same_owner': 'reassignment_same_owner.json',
               'different_owner': 'reassignment_different_owner.json'}
    return {name: root / aliases.get(name, f'{name}.json') for name in SCENARIOS
            if (root / aliases.get(name, f'{name}.json')).exists()}


if __name__ == '__main__':
    root = Path(os.environ.get('M3_EVIDENCE_DIR', 'm3-evidence'))
    write_json(root / 'module3-scenario-summary.json',
               summary(os.environ.get('M3_PROJECT'), os.environ.get('GITHUB_SHA'), read_preflight(root),
                       scenario_entries(root), os.environ.get('M3_JOB_STATUS')))
