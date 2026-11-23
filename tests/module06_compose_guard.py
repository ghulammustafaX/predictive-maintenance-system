"""Fail closed before starting the dedicated Module 06 test project.

Run with rendered Compose JSON and the same M6_PROJECT/M6_GROUP and approved
read-only artifact paths used to render it. This never starts Docker.
"""
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

CHECKPOINT_HASH = 'd1d0d1a329ecb9b2efffcdb7503cba076df4f68ac0d644a4f56a30d4f88948d0'
SCALERS = {'FD001': 'a93053fe543623d25061dc6ba6718c8a9f6aa5555db77bf92dfb6cadd8b5fddc',
           'FD002': 'a4c04f06e7f2938fc755d6c2464ab12a32e699c7fa9d5808911eeb8ec6c81cf7',
           'FD003': '102c5ed51ef093bc3e4f082e752a89f5bf70a757727b202a2439ad547c8ddd84',
           'FD004': 'd0a8534f765b9c79746196ee576ecee611f6c71f48deec5649a3f9185fe8586f'}
STATISTICS = {'FD001': '02b96a1e2cc1271e40b043c603bc8af54507afbd153217526a09c813d4ce4e81',
              'FD002': 'b1f9570cdefd17392f1524e843b52e725996acdecba0650daa3a07786aabb70f',
              'FD003': 'e55cd59fd7a1e6a5c8349d50fc0ad35e7b855cb5646ceda2c339eecf2b915768',
              'FD004': 'be72a7fc8463f929c84957398954140035baeb5a1494ecf26bafd8daf0f9d528'}


def fail(message):
    raise ValueError(message)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def require_unused_project(project):
    """Read Docker inventories before startup; reject ambiguity or a collision."""
    def names(*args):
        try:
            return subprocess.run(['docker', *args], check=True, capture_output=True,
                                  text=True, timeout=15).stdout.splitlines()
        except (OSError, subprocess.SubprocessError) as exc:
            raise ValueError('cannot verify unused Docker resources') from exc
    containers = names('ps', '-a', '--format', '{{.Names}}\t{{.Label "com.docker.compose.project"}}')
    for item in containers:
        fields = item.split('\t', 1)
        if len(fields) != 2:
            fail('invalid container inventory')
        name, label = fields
        if label == project or name.startswith((project + '-', project + '_')):
            fail('project already has containers')
    for kind in ('volume', 'network'):
        for name in names(kind, 'ls', '--format', '{{.Name}}'):
            if name == project or name.startswith((project + '_', project + '-')):
                fail(f'project already has {kind} resources')


def validate_artifacts(model, scalers):
    model, scalers = Path(model).resolve(strict=True), Path(scalers).resolve(strict=True)
    manifest = json.loads((scalers / 'preprocessing_manifest.json').read_text(encoding='utf-8'))
    if (sha(model) != CHECKPOINT_HASH or manifest.get('checkpoint_sha256') != CHECKPOINT_HASH
            or manifest.get('schema_version') != 'raw-24-v1' or manifest.get('window_size') != 30
            or manifest.get('datasets') != list(SCALERS)
            or manifest.get('feature_order') != [f'operational_setting_{i}' for i in range(1, 4)] + [f'sensor_{i}' for i in range(1, 22)]):
        fail('raw checkpoint or manifest mismatch')
    for fd, expected in SCALERS.items():
        if sha(scalers / f'scaler_{fd}.pkl') != expected or manifest['artifact_sha256'][fd]['scaler'] != expected:
            fail('scaler hash mismatch')
        stats = (scalers / f'scaler_{fd}_stats.json').read_bytes()
        lf = stats.replace(b'\r\n', b'\n')
        expected_stats = STATISTICS[fd]
        if manifest['artifact_sha256'][fd]['statistics'] != expected_stats:
            fail('statistics manifest mismatch')
        if b'\r' in lf or expected_stats not in {hashlib.sha256(x).hexdigest() for x in (stats, lf, lf.replace(b'\n', b'\r\n'))}:
            fail('statistics hash mismatch')


def validate_config(config, project, group, model, scalers, repo):
    if set(config) != {'name', 'services', 'networks', 'volumes'}:
        fail('unexpected top-level configuration')
    if (not re.fullmatch(r'pms-m6-[0-9]+-[0-9]+', project)
            or group != project.replace('pms-m6-', 'm6-acceptance-', 1) or config.get('name') != project):
        fail('invalid project or group')
    services = config.get('services', {})
    if set(services) != {'zookeeper', 'kafka', 'topic-init', 'influx', 'worker', 'probe'}:
        fail('unexpected services')
    permitted = {'image', 'build', 'command', 'entrypoint', 'environment', 'depends_on', 'healthcheck', 'networks', 'volumes'}
    for name, service in services.items():
        if (set(service) - permitted or set(service.get('networks', {})) != {'default'}
                or service['networks']['default'] is not None):
            fail(f'{name}: unexpected service configuration')
        if name != 'topic-init' and service.get('entrypoint') is not None:
            fail('unexpected entrypoint')
        if name not in {'topic-init', 'worker', 'probe'} and service.get('command') is not None:
            fail('unexpected command')
        if name in {'worker', 'probe'} and (service.get('image') or service.get('healthcheck')):
            fail('unexpected worker configuration')
    expected_dependencies = {
        'worker': {'topic-init': {'condition': 'service_completed_successfully', 'required': True},
                   'influx': {'condition': 'service_healthy', 'required': True}},
        'probe': {'worker': {'condition': 'service_started', 'required': True}},
    }
    for name, expected in expected_dependencies.items():
        if services[name].get('depends_on') != expected:
            fail('unexpected service dependencies')
    healthchecks = {
        'zookeeper': (['CMD-SHELL', "echo srvr | nc -w 2 127.0.0.1 2181 | grep -Eq '^Mode: (standalone|leader|follower)'"], '4s'),
        'kafka': (['CMD-SHELL', 'kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1'], '10s'),
        'influx': (['CMD', 'influx', 'ping'], '5s'),
    }
    for name, (expected, timeout) in healthchecks.items():
        check = services[name].get('healthcheck', {})
        if check != {'test': expected, 'timeout': timeout, 'interval': '5s', 'retries': 24}:
            fail('unexpected healthcheck')
    if services['topic-init'].get('environment') or services['topic-init'].get('healthcheck'):
        fail('unexpected topic initializer configuration')
    for name, image in {'zookeeper':'confluentinc/cp-zookeeper:7.4.0', 'kafka':'confluentinc/cp-kafka:7.4.0',
                        'topic-init':'confluentinc/cp-kafka:7.4.0', 'influx':'influxdb:2.7'}.items():
        if services[name].get('image') != image or 'build' in services[name]:
            fail('unexpected image')
    broker = services['kafka']['environment']
    expected_broker = {'KAFKA_BROKER_ID':'1', 'KAFKA_ZOOKEEPER_CONNECT':'zookeeper:2181',
        'KAFKA_LISTENERS':'PLAINTEXT://0.0.0.0:9092', 'KAFKA_ADVERTISED_LISTENERS':'PLAINTEXT://kafka:9092',
        'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR':'1', 'KAFKA_AUTO_CREATE_TOPICS_ENABLE':'false'}
    if broker != expected_broker or services['zookeeper']['environment'] != {'ZOOKEEPER_CLIENT_PORT':'2181', 'ZOOKEEPER_TICK_TIME':'2000'}:
        fail('unexpected broker configuration')
    influx_env = {'DOCKER_INFLUXDB_INIT_MODE':'setup', 'DOCKER_INFLUXDB_INIT_USERNAME':'m6test',
        'DOCKER_INFLUXDB_INIT_PASSWORD':'m6-test-password-only', 'DOCKER_INFLUXDB_INIT_ORG':'m6-test-org',
        'DOCKER_INFLUXDB_INIT_BUCKET':'m6-test-bucket', 'DOCKER_INFLUXDB_INIT_ADMIN_TOKEN':'m6-test-token-only'}
    if services['influx']['environment'] != influx_env:
        fail('unexpected influx configuration')
    worker_env = {'KAFKA_BOOTSTRAP_SERVERS':'kafka:9092', 'PREDICTION_INPUT_TOPIC':'m6-test-input',
        'PREDICTION_OUTPUT_TOPIC':'m6-test-output', 'PREDICTION_CONSUMER_GROUP':group,
        'PREDICTION_CHECKPOINT_PATH':'/checkpoint/state.sqlite', 'RUL_CHECKPOINT_PATH':'/model/lstm_raw_20260626T082955Z.pt',
        'PREPROCESSING_ARTIFACT_DIR':'/scalers', 'INFLUXDB_URL':'http://influx:8086',
        'INFLUXDB_TOKEN':'m6-test-token-only', 'INFLUXDB_ORG':'m6-test-org', 'INFLUXDB_BUCKET':'m6-test-bucket'}
    if services['worker']['environment'] != worker_env or services['probe']['environment'] != {'M6_GROUP':group}:
        fail('unexpected worker or probe configuration')
    for name in ('worker', 'probe'):
        service = services[name]
        if (service.get('command') != ['python', f'/app/tests/module06_integration_{name}.py']
                or set(service.get('build', {})) - {'context', 'dockerfile'}
                or Path(service['build']['context']).resolve() != Path(repo).resolve()
                or service['build'].get('dockerfile') != 'tests/module06.Dockerfile'):
            fail('unexpected build or command')
        mounts = service.get('volumes', [])
        expected = {('/checkpoint', 'volume'): 'checkpoint', ('/model/lstm_raw_20260626T082955Z.pt', 'bind'): str(Path(model).resolve()),
                    ('/scalers', 'bind'): str(Path(scalers).resolve())}
        if len(mounts) != len(expected): fail('unexpected mounts')
        for mount in mounts:
            key = (mount.get('target'), mount.get('type'))
            if key not in expected or set(mount) - {'type', 'source', 'target', 'read_only', 'bind', 'volume'}:
                fail('unexpected mount')
            if set(mount.get('bind', {})) - {'create_host_path'} or mount.get('volume'):
                fail('unexpected mount options')
            source = mount.get('source') if key[1] == 'volume' else str(Path(mount.get('source', '')).resolve())
            if source != expected.pop(key) or bool(mount.get('read_only')) != (name == 'probe' or key[1] == 'bind'):
                fail('unsafe mount')
        if expected: fail('missing mount')
    for name in ('zookeeper', 'kafka', 'topic-init'):
        if services[name].get('volumes'): fail('unexpected broker mount')
    influx_volumes = services['influx'].get('volumes', [])
    if any(set(m) - {'type', 'source', 'target', 'volume'} or m.get('volume') for m in influx_volumes):
        fail('unexpected influx mount options')
    influx_mounts = {(m.get('source'), m.get('target'), m.get('type')) for m in influx_volumes}
    if len(influx_volumes) != 2 or influx_mounts != {('influx-data', '/var/lib/influxdb2', 'volume'), ('influx-config', '/etc/influxdb2', 'volume')}:
        fail('unexpected influx mounts')
    volumes = config.get('volumes', {})
    if set(volumes) != {'checkpoint', 'influx-data', 'influx-config'}:
        fail('unexpected volumes')
    for name, volume in volumes.items():
        if set(volume) != {'name'} or volume.get('name') != f'{project}_{name}':
            fail('volume not project-scoped')
    networks = config.get('networks', {})
    if (set(networks) != {'default'} or set(networks['default']) - {'name', 'internal', 'ipam'}
            or networks['default'].get('ipam') or networks['default'].get('name') != f'{project}_default'
            or networks['default'].get('internal') is not True):
        fail('network not project-scoped and internal')
    init = services['topic-init']
    if init.get('entrypoint') != ['/bin/bash', '-ec'] or init.get('command') != ['for topic in m6-test-input m6-test-output; do\n  kafka-topics --bootstrap-server kafka:9092 --create --topic "$$topic" --partitions 1 --replication-factor 1\ndone\n']:
        fail('unexpected topic creation command')


def main():
    project, group = os.environ['M6_PROJECT'], os.environ['M6_GROUP']
    model, scalers = os.environ['M6_MODEL_PATH'], os.environ['M6_SCALER_DIR']
    validate_artifacts(model, scalers)
    validate_config(json.loads(Path(sys.argv[1]).read_text(encoding='utf-8')), project, group, model, scalers, Path(__file__).resolve().parent.parent)
    require_unused_project(project)
    print('PASS: approved raw artifacts and isolated Module 06 configuration')


if __name__ == '__main__':
    main()
