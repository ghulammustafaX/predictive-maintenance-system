import json
import runpy
import sys
from pathlib import Path
import pytest

GUARD = Path(__file__).with_name('module04_compose_guard.py')


def config():
    project = 'pms-m4-123-1'
    group = 'm4-acceptance-123-1'
    return {'name': project, 'services': {
        'zookeeper': {}, 'kafka': {'environment': {
            'KAFKA_ADVERTISED_LISTENERS': 'PLAINTEXT://kafka:9092',
            'KAFKA_ZOOKEEPER_CONNECT': 'zookeeper:2181',
            'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'false'}}, 'topic-init': {},
        'worker': {'environment': {'KAFKA_BOOTSTRAP_SERVERS': 'kafka:9092',
            'PREPROCESSING_INPUT_TOPIC': 'm4-test-input', 'PREPROCESSING_OUTPUT_TOPIC': 'm4-test-output',
            'PREPROCESSING_CONSUMER_GROUP': group, 'PREPROCESSING_CHECKPOINT_PATH': '/checkpoint/state.sqlite'},
            'volumes': [{'type': 'volume', 'source': 'checkpoint', 'target': '/checkpoint'}]},
        'probe': {'environment': {'M4_GROUP': group}, 'volumes': [
            {'type': 'volume', 'source': 'checkpoint', 'target': '/checkpoint', 'read_only': True}]}},
        'volumes': {'checkpoint': {'name': project + '_checkpoint'}},
        'networks': {'default': {'name': project + '_default'}}}


def run_guard(tmp_path, monkeypatch, payload):
    path = tmp_path / 'compose.json'
    path.write_text(json.dumps(payload))
    monkeypatch.setenv('M4_PROJECT', 'pms-m4-123-1')
    monkeypatch.setenv('M4_GROUP', 'm4-acceptance-123-1')
    monkeypatch.setattr(sys, 'argv', [str(GUARD), str(path)])
    runpy.run_path(str(GUARD), run_name='__main__')


def test_guard_accepts_isolated_configuration(tmp_path, monkeypatch):
    run_guard(tmp_path, monkeypatch, config())


@pytest.mark.parametrize('mutation', ['host_port', 'external_volume', 'production_broker', 'bind_mount'])
def test_guard_rejects_unsafe_configuration(tmp_path, monkeypatch, mutation):
    payload = config()
    if mutation == 'host_port':
        payload['services']['kafka']['ports'] = ['9092:9092']
    elif mutation == 'external_volume':
        payload['volumes']['checkpoint']['external'] = True
    elif mutation == 'production_broker':
        payload['services']['worker']['environment']['KAFKA_BOOTSTRAP_SERVERS'] = 'pms-kafka:9092'
    else:
        payload['services']['worker']['volumes'][0]['type'] = 'bind'
    with pytest.raises(ValueError):
        run_guard(tmp_path, monkeypatch, payload)
