import pytest
from module3_acceptance_gate import final_summary
from module3_evidence import SCENARIOS


def source(status='passed'):
    return {'compose_project': 'project', 'git_commit': 'sha', 'overall_acceptance_status': status,
            'scenarios': {name: {'status': 'passed'} for name in SCENARIOS}}


def jobs(**updates):
    result = {'regression': 'success', 'broker_integration': 'success', 'influxdb_integration': 'success'}
    result.update(updates)
    return result


def test_all_required_jobs_and_scenarios_must_pass():
    assert final_summary(source(), jobs(), 'project', 'sha', source())['overall_acceptance_status'] == 'passed'
    assert final_summary(source('incomplete'), jobs(), 'project', 'sha', source())['overall_acceptance_status'] == 'incomplete'
    assert final_summary(source('failed'), jobs(), 'project', 'sha', source())['overall_acceptance_status'] == 'failed'


@pytest.mark.parametrize('job', ['regression', 'broker_integration', 'influxdb_integration'])
@pytest.mark.parametrize('status,expected', [('failure', 'failed'), ('cancelled', 'failed'),
                                            ('skipped', 'incomplete'), (None, 'incomplete')])
def test_required_job_results_never_allow_false_pass(job, status, expected):
    assert final_summary(source(), jobs(**{job: status}), 'project', 'sha', source())['overall_acceptance_status'] == expected


@pytest.mark.parametrize('value,project,commit', [(None, 'project', 'sha'), (source(), 'wrong', 'sha'),
                                                  (source(), 'project', 'wrong'), (source(), None, 'sha')])
def test_missing_or_foreign_scenario_evidence_is_incomplete(value, project, commit):
    assert final_summary(value, jobs(), project, commit, source())['overall_acceptance_status'] == 'incomplete'


def test_gate_does_not_trust_a_pass_label_without_independent_scenario_validation():
    checked = source()
    checked['scenarios']['dlq']['status'] = 'incomplete'
    assert final_summary(source(), jobs(), 'project', 'sha', checked)['overall_acceptance_status'] == 'incomplete'
    assert final_summary(source(), jobs(), 'project', 'sha', None)['overall_acceptance_status'] == 'incomplete'
