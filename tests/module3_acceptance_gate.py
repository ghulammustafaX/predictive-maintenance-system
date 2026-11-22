"""Combine independent GitHub Actions job outcomes with scenario evidence."""
import json
import os
from pathlib import Path
from module3_evidence import SCENARIOS, read_preflight, scenario_entries, summary, write_json


REQUIRED_JOBS = ('regression', 'broker_integration', 'influxdb_integration')


def final_summary(source, job_results, project, commit, verified=None):
    jobs = {name: job_results.get(name) for name in REQUIRED_JOBS}
    statuses = set(jobs.values())
    if statuses & {'failure', 'cancelled'}:
        final = 'failed'
    elif statuses != {'success'}:
        final = 'incomplete'
    elif (not isinstance(source, dict) or not isinstance(verified, dict) or not project or not commit
          or source.get('compose_project') != project or source.get('git_commit') != commit
          or verified.get('compose_project') != project or verified.get('git_commit') != commit):
        final = 'incomplete'
    else:
        source_status = source.get('overall_acceptance_status')
        verified_status = verified.get('overall_acceptance_status')
        if 'failed' in (source_status, verified_status):
            final = 'failed'
        else:
            source_scenarios, checked_scenarios = source.get('scenarios'), verified.get('scenarios')
            complete = (isinstance(source_scenarios, dict) and isinstance(checked_scenarios, dict)
                        and set(source_scenarios) == set(SCENARIOS) == set(checked_scenarios)
                        and all(isinstance(source_scenarios[name], dict) and isinstance(checked_scenarios[name], dict)
                                and source_scenarios[name].get('status') == checked_scenarios[name].get('status') == 'passed'
                                for name in SCENARIOS))
            final = 'passed' if source_status == verified_status == 'passed' and complete else 'incomplete'
    return {'schema_version': '1.0', 'compose_project': project, 'git_commit': commit,
            'required_job_results': jobs, 'scenario_summary': source, 'independent_scenario_validation': verified,
            'overall_acceptance_status': final}


if __name__ == '__main__':
    source_path = Path('m3-downloaded/module3-scenario-summary.json')
    try:
        source = json.loads(source_path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        source = None
    results = {name: os.environ.get('M3_JOB_' + name.upper()) for name in REQUIRED_JOBS}
    root = source_path.parent
    verified = summary(os.environ.get('M3_PROJECT'), os.environ.get('GITHUB_SHA'), read_preflight(root),
                       scenario_entries(root), results['influxdb_integration']) if source is not None else None
    write_json('m3-final-evidence/module3-acceptance-summary.json',
               final_summary(source, results, os.environ.get('M3_PROJECT'), os.environ.get('GITHUB_SHA'), verified))
