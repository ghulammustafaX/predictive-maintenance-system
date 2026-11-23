"""Fail closed on unsafe rendered Module 04 Compose configuration."""
import json
import os
import re
import sys
from pathlib import Path

project = os.environ['M4_PROJECT']
group = os.environ['M4_GROUP']
if not re.fullmatch(r'pms-m4-[0-9]+-[0-9]+', project) or group != project.replace('pms-m4-', 'm4-acceptance-', 1):
    raise ValueError('invalid isolated project or group')
config = json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))
if config.get('name') != project:
    raise ValueError('wrong rendered project')
services = config['services']
if set(services) != {'zookeeper', 'kafka', 'topic-init', 'worker', 'probe'}:
    raise ValueError('unexpected services')
for name, service in services.items():
    for key in ('container_name', 'ports', 'network_mode', 'privileged', 'devices', 'extra_hosts', 'external_links', 'env_file'):
        if service.get(key):
            raise ValueError(f'{name} uses forbidden {key}')
    if service.get('networks') and set(service['networks']) != {'default'}:
        raise ValueError(f'{name} uses unexpected network')
    for mount in service.get('volumes', []):
        if mount.get('type') != 'volume' or mount.get('source') != 'checkpoint' or mount.get('target') != '/checkpoint':
            raise ValueError(f'{name} uses unexpected mount')
        if name not in {'worker', 'probe'}:
            raise ValueError('unexpected checkpoint consumer')
worker = services['worker']
probe = services['probe']
broker_env = services['kafka'].get('environment', {})
if (broker_env.get('KAFKA_ADVERTISED_LISTENERS') != 'PLAINTEXT://kafka:9092'
        or broker_env.get('KAFKA_ZOOKEEPER_CONNECT') != 'zookeeper:2181'
        or broker_env.get('KAFKA_AUTO_CREATE_TOPICS_ENABLE') != 'false'):
    raise ValueError('Kafka listener or ZooKeeper is not isolated')
if len(worker.get('volumes', [])) != 1 or len(probe.get('volumes', [])) != 1 or not probe['volumes'][0].get('read_only'):
    raise ValueError('checkpoint mounts must be worker writable and probe read-only')
env = worker['environment']
expected = {'KAFKA_BOOTSTRAP_SERVERS': 'kafka:9092', 'PREPROCESSING_INPUT_TOPIC': 'm4-test-input',
            'PREPROCESSING_OUTPUT_TOPIC': 'm4-test-output', 'PREPROCESSING_CONSUMER_GROUP': group,
            'PREPROCESSING_CHECKPOINT_PATH': '/checkpoint/state.sqlite'}
if any(env.get(k) != v for k, v in expected.items()) or probe['environment'].get('M4_GROUP') != group:
    raise ValueError('unexpected broker, topic, group, or checkpoint configuration')
if set(config.get('volumes', {})) != {'checkpoint'}:
    raise ValueError('unexpected volumes')
for key, value in config['volumes'].items():
    if value.get('external') or value.get('name') != f'{project}_{key}' or value.get('driver_opts'):
        raise ValueError('volume is not project scoped')
for key, value in config.get('networks', {}).items():
    if key != 'default' or value.get('external') or value.get('name') != f'{project}_default':
        raise ValueError('network is not project scoped')
print('PASS: rendered Module 04 project isolation')
