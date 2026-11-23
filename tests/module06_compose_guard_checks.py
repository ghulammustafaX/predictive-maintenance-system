"""Offline mutation tests for the rendered Compose isolation guard."""
import copy
import pytest

from module06_compose_guard import require_unused_project, validate_config


@pytest.fixture
def rendered(tmp_path):
    project, group = 'pms-m6-123-1', 'm6-acceptance-123-1'
    model, scalers, repo = tmp_path / 'raw.pt', tmp_path / 'scalers', tmp_path / 'repo'
    def service(image=None, env=None):
        result = {'command': None, 'entrypoint': None, 'networks': {'default': None}}
        if image: result['image'] = image
        if env is not None: result['environment'] = env
        return result
    zk = service('confluentinc/cp-zookeeper:7.4.0', {'ZOOKEEPER_CLIENT_PORT':'2181', 'ZOOKEEPER_TICK_TIME':'2000'})
    zk['healthcheck'] = {'test':['CMD-SHELL', "echo srvr | nc -w 2 127.0.0.1 2181 | grep -Eq '^Mode: (standalone|leader|follower)'"], 'interval':'5s','timeout':'4s','retries':24}
    kafka = service('confluentinc/cp-kafka:7.4.0', {'KAFKA_BROKER_ID':'1', 'KAFKA_ZOOKEEPER_CONNECT':'zookeeper:2181', 'KAFKA_LISTENERS':'PLAINTEXT://0.0.0.0:9092', 'KAFKA_ADVERTISED_LISTENERS':'PLAINTEXT://kafka:9092', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR':'1', 'KAFKA_AUTO_CREATE_TOPICS_ENABLE':'false'})
    kafka['healthcheck'] = {'test':['CMD-SHELL','kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1'], 'interval':'5s','timeout':'10s','retries':24}
    init = service('confluentinc/cp-kafka:7.4.0')
    init['entrypoint'] = ['/bin/bash','-ec']
    init['command'] = ['for topic in m6-test-input m6-test-output; do\n  kafka-topics --bootstrap-server kafka:9092 --create --topic "$$topic" --partitions 1 --replication-factor 1\ndone\n']
    influx = service('influxdb:2.7', {'DOCKER_INFLUXDB_INIT_MODE':'setup','DOCKER_INFLUXDB_INIT_USERNAME':'m6test', 'DOCKER_INFLUXDB_INIT_PASSWORD':'m6-test-password-only','DOCKER_INFLUXDB_INIT_ORG':'m6-test-org','DOCKER_INFLUXDB_INIT_BUCKET':'m6-test-bucket','DOCKER_INFLUXDB_INIT_ADMIN_TOKEN':'m6-test-token-only'})
    influx['healthcheck'] = {'test':['CMD','influx','ping'], 'interval':'5s','timeout':'5s','retries':24}
    influx['volumes'] = [{'type':'volume','source':'influx-data','target':'/var/lib/influxdb2','volume':{}}, {'type':'volume','source':'influx-config','target':'/etc/influxdb2','volume':{}}]
    worker_env = {'KAFKA_BOOTSTRAP_SERVERS':'kafka:9092','PREDICTION_INPUT_TOPIC':'m6-test-input','PREDICTION_OUTPUT_TOPIC':'m6-test-output','PREDICTION_CONSUMER_GROUP':group,'PREDICTION_CHECKPOINT_PATH':'/checkpoint/state.sqlite','RUL_CHECKPOINT_PATH':'/model/lstm_raw_20260626T082955Z.pt','PREPROCESSING_ARTIFACT_DIR':'/scalers','INFLUXDB_URL':'http://influx:8086','INFLUXDB_TOKEN':'m6-test-token-only','INFLUXDB_ORG':'m6-test-org','INFLUXDB_BUCKET':'m6-test-bucket'}
    def app(name, env):
        value = service(env=env)
        value['build'] = {'context':str(repo),'dockerfile':'tests/module06.Dockerfile'}
        value['command'] = ['python',f'/app/tests/module06_integration_{name}.py']
        value['volumes'] = [{'type':'volume','source':'checkpoint','target':'/checkpoint', 'volume':{}, **({'read_only':True} if name == 'probe' else {})},
                            {'type':'bind','source':str(model),'target':'/model/lstm_raw_20260626T082955Z.pt','read_only':True},
                            {'type':'bind','source':str(scalers),'target':'/scalers','read_only':True}]
        return value
    config = {'name':project, 'services':{'zookeeper':zk,'kafka':kafka,'topic-init':init,'influx':influx,'worker':app('worker',worker_env),'probe':app('probe',{'M6_GROUP':group})},
              'volumes':{n:{'name':f'{project}_{n}'} for n in ('checkpoint','influx-data','influx-config')},
              'networks':{'default':{'name':f'{project}_default','internal':True,'ipam':{}}}}
    config['services']['worker']['depends_on'] = {'topic-init':{'condition':'service_completed_successfully','required':True}, 'influx':{'condition':'service_healthy','required':True}}
    config['services']['probe']['depends_on'] = {'worker':{'condition':'service_started','required':True}}
    return config, project, group, model, scalers, repo


def test_expected_isolated_config_is_accepted(rendered):
    validate_config(*rendered)


@pytest.mark.parametrize('case', ['container_label', 'container_name', 'volume', 'network', 'command_failure'])
def test_project_collision_or_unavailable_inventory_rejected(monkeypatch, case):
    import subprocess
    from types import SimpleNamespace
    def run(command, **kwargs):
        kind = command[1]
        if case == 'command_failure':
            raise subprocess.CalledProcessError(1, command)
        output = ''
        if case == 'container_label' and kind == 'ps': output = 'other\tpms-m6-123-1\n'
        if case == 'container_name' and kind == 'ps': output = 'pms-m6-123-1-kafka-1\t\n'
        if case == 'volume' and kind == 'volume': output = 'pms-m6-123-1_checkpoint\n'
        if case == 'network' and kind == 'network': output = 'pms-m6-123-1_default\n'
        return SimpleNamespace(stdout=output)
    monkeypatch.setattr(subprocess, 'run', run)
    with pytest.raises(ValueError):
        require_unused_project('pms-m6-123-1')


def test_unrelated_project_resources_do_not_collide(monkeypatch):
    import subprocess
    from types import SimpleNamespace
    monkeypatch.setattr(subprocess, 'run', lambda command, **kwargs: SimpleNamespace(
        stdout='pms-kafka\tpredictive-maintenance-system\n' if command[1] == 'ps' else 'unrelated_default\n'))
    require_unused_project('pms-m6-123-1')


@pytest.mark.parametrize('case', ['project','group','port','host_network','external_network','non_internal_network',
    'external_volume','volume_driver','production_broker','production_topic','production_group','bad_checkpoint',
    'model_writeable','scaler_source','probe_checkpoint_writeable','extra_mount','extra_service','worker_entrypoint',
    'foreign_image','foreign_build','init_command','healthcheck','health_timeout','influx_mount','influx_url','external_secret','dependency'])
def test_unsafe_configuration_is_rejected(rendered, case):
    config, project, group, model, scalers, repo = copy.deepcopy(rendered)
    s = config['services']
    if case == 'project': config['name'] = 'predictive-maintenance-system'
    elif case == 'group': group = 'rul-prediction-service-group'
    elif case == 'port': s['kafka']['ports'] = ['9092:9092']
    elif case == 'host_network': s['worker']['network_mode'] = 'host'
    elif case == 'external_network': config['networks']['default']['external'] = True
    elif case == 'non_internal_network': config['networks']['default']['internal'] = False
    elif case == 'external_volume': config['volumes']['checkpoint']['external'] = True
    elif case == 'volume_driver': config['volumes']['checkpoint']['driver_opts'] = {'device':'/'}
    elif case == 'production_broker': s['worker']['environment']['KAFKA_BOOTSTRAP_SERVERS'] = 'pms-kafka:9092'
    elif case == 'production_topic': s['worker']['environment']['PREDICTION_INPUT_TOPIC'] = 'preprocessed-telemetry'
    elif case == 'production_group': s['worker']['environment']['PREDICTION_CONSUMER_GROUP'] = 'rul-prediction-service-group'
    elif case == 'bad_checkpoint': s['worker']['environment']['PREDICTION_CHECKPOINT_PATH'] = '/other/state.sqlite'
    elif case == 'model_writeable': s['worker']['volumes'][1]['read_only'] = False
    elif case == 'scaler_source': s['worker']['volumes'][2]['source'] = '/other'
    elif case == 'probe_checkpoint_writeable': s['probe']['volumes'][0]['read_only'] = False
    elif case == 'extra_mount': s['worker']['volumes'].append({'type':'bind','source':'/','target':'/host'})
    elif case == 'extra_service': s['other'] = {}
    elif case == 'worker_entrypoint': s['worker']['entrypoint'] = ['/bin/sh']
    elif case == 'foreign_image': s['kafka']['image'] = 'example/other'
    elif case == 'foreign_build': s['worker']['build']['context'] = '/other'
    elif case == 'init_command': s['topic-init']['command'] = ['echo unsafe']
    elif case == 'healthcheck': s['kafka']['healthcheck']['test'] = ['CMD','true']
    elif case == 'health_timeout': s['kafka']['healthcheck']['timeout'] = '24h'
    elif case == 'influx_mount': s['influx']['volumes'][0]['source'] = 'production'
    elif case == 'influx_url': s['worker']['environment']['INFLUXDB_URL'] = 'http://pms-influxdb:8086'
    elif case == 'external_secret': config['secrets'] = {'production': {'external':True}}
    elif case == 'dependency': s['worker']['depends_on'] = {}
    with pytest.raises(ValueError):
        validate_config(config, project, group, model, scalers, repo)
