import json
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ml.prediction.checkpoint import CheckpointError, PredictionCheckpoint, validate_checkpoint_path
from ml.prediction.service import _StopOnRevocation
from ml.prediction.service import PredictionService
from test_prediction_contiguity import service, record


def configured():
    worker = service()
    worker.consumer_group, worker.input_topic, worker.output_topic = 'test-group', 'test-topic', 'output'
    worker.expected_preprocessing_artifact_version = 'a1-v2'
    worker.compatibility_report = {'checkpoint_sha256': 'model-hash', 'artifact_version': 'a1-v2', 'artifact_sha256': {'FD001': {'scaler': 'hash'}}}
    return worker


def feature(cycle, unit=1, run='one'):
    item = record(cycle, unit, run)
    item.update(preprocessing_artifact_version='a1-v2', timestamp='2026-01-01T00:00:00Z')
    return item


def test_checkpoint_recovers_multiple_engines_and_runs(tmp_path):
    first = configured()
    for cycle in range(1, 30):
        first.process_record(feature(cycle, 1, 'one'))
        first.process_record(feature(cycle, 2, 'one'))
        first.process_record(feature(cycle, 1, 'other'))
    item = feature(29, 1, 'other')
    store = PredictionCheckpoint(str(tmp_path / 'state.sqlite'))
    store.save(first._checkpoint_snapshot(SimpleNamespace(partition=0, offset=86, value=item)))
    snapshot = store.load(); store.close()
    second = configured(); second._restore_checkpoint(snapshot)
    for unit, run in [(1, 'one'), (2, 'one'), (1, 'other')]:
        assert second.process_record(feature(30, unit, run)) == 3.0
    assert len(second.engine.windows) == 3
    assert all(window[:, 0].tolist() == list(range(1, 31)) for window in second.engine.windows)


def test_lock_corruption_and_incompatible_artifacts(tmp_path):
    path = str(tmp_path / 'state.sqlite')
    store = PredictionCheckpoint(path)
    with pytest.raises(CheckpointError, match='already owned'):
        PredictionCheckpoint(path)
    store.save({'schema_version': 2})
    with pytest.raises(CheckpointError, match='schema'): store.load()
    store.close()
    worker = configured(); worker.process_record(feature(1))
    snapshot = worker._checkpoint_snapshot(SimpleNamespace(partition=0, offset=0, value=feature(1)))
    snapshot['identity']['model_sha256'] = 'different'
    with pytest.raises(CheckpointError, match='mismatch'): configured()._restore_checkpoint(snapshot)


def test_checkpoint_rejects_unmounted_directory(tmp_path):
    with patch('ml.prediction.checkpoint._persistent_mount', return_value=False):
        with pytest.raises(CheckpointError, match='dedicated non-memory mount'):
            PredictionCheckpoint(str(tmp_path / 'state.sqlite'), require_mount=True)


def test_startup_requires_checkpoint_mount(tmp_path):
    with pytest.raises(CheckpointError, match='required'):
        validate_checkpoint_path(None)
    with patch('ml.prediction.checkpoint._persistent_mount', return_value=False):
        with pytest.raises(CheckpointError, match='mount'):
            validate_checkpoint_path(str(tmp_path / 'state.sqlite'))
    with patch('ml.prediction.checkpoint._persistent_mount', return_value=True):
        validate_checkpoint_path(str(tmp_path / 'state.sqlite'))


def test_corrupt_json_and_missing_history_fail_closed(tmp_path):
    path = str(tmp_path / 'state.sqlite')
    store = PredictionCheckpoint(path)
    store.db.execute('INSERT INTO checkpoint(id,payload) VALUES (1,?)', ('{bad',))
    store.db.commit()
    with pytest.raises(CheckpointError, match='corrupt'): store.load()
    store.close()
    from kafka.structs import TopicPartition
    partition = TopicPartition('test-topic', 0)
    class Reader:
        def beginning_offsets(self, parts): return {partition: 10}
        def end_offsets(self, parts): return {partition: 20}
        def close(self): pass
    worker = configured(); worker.kafka_servers = 'isolated'
    with patch('ml.prediction.service.KafkaConsumer', return_value=Reader()):
        with pytest.raises(CheckpointError, match='not retained'):
            worker._verify_checkpoint_history({'offset': 9}, partition)


def test_revocation_stops_processing():
    worker = configured()
    worker._stop, worker._consumer_ready = threading.Event(), threading.Event()
    worker._consumer_ready.set()
    listener = _StopOnRevocation(worker)
    listener.on_partitions_revoked(set())
    assert not worker._stop.is_set()
    listener.on_partitions_revoked({'partition'})
    assert worker._stop.is_set() and not worker._consumer_ready.is_set()


def test_retry_output_has_stable_identity_and_may_duplicate():
    worker = configured()
    sent = []
    class Future:
        def get(self, timeout): return None
    class Producer:
        def send(self, topic, key, value):
            sent.append((key, dict(value)))
            return Future()
    worker.producer = Producer()
    worker._write_prediction = lambda payload: None
    publish = PredictionService.publish_prediction.__get__(worker)
    payload = dict(dataset_id='FD001', unit_id='1', run_id='one', cycle=30,
                   source_timestamp='2026-01-01T00:00:00Z', rul=1.0,
                   window_size=30, model_version='model', preprocessing_artifact_version='a1-v2')
    publish(dict(payload)); publish(dict(payload))
    assert len(sent) == 2
    assert sent[0][0] == sent[1][0]
    assert sent[0][1]['prediction_id'] == sent[1][1]['prediction_id']


def test_existing_offset_without_checkpoint_stops(tmp_path):
    from kafka.structs import TopicPartition
    partition = TopicPartition('test-topic', 0)
    class Consumer:
        def subscribe(self, topics, listener): pass
        def __iter__(self):
            yield SimpleNamespace(topic='test-topic', partition=0, offset=29, value=feature(30))
        def assignment(self): return {partition}
        def partitions_for_topic(self, topic): return {0}
        def committed(self, part): return 29
        def commit(self, offsets): raise AssertionError('must not commit')
        def close(self): pass
    class Producer:
        def flush(self): pass
        def close(self): pass
    worker = configured(); worker.kafka_servers = 'unused'
    worker._consumer_ready, worker._stop = threading.Event(), threading.Event()
    worker.influx, worker.influx_write = None, object()
    with patch.dict('os.environ', {'PREDICTION_CHECKPOINT_PATH': str(tmp_path / 'empty.sqlite')}), \
         patch('ml.prediction.checkpoint._persistent_mount', return_value=True), \
         patch('ml.prediction.service.KafkaConsumer', return_value=Consumer()), \
         patch('ml.prediction.service.KafkaProducer', return_value=Producer()):
        worker.start_consumer()
    assert 'without a checkpoint' in worker._consumer_error


def test_recovered_gap_duplicate_and_new_run(tmp_path):
    first = configured()
    for cycle in range(1, 30): first.process_record(feature(cycle))
    snapshot = first._checkpoint_snapshot(SimpleNamespace(partition=0, offset=28, value=feature(29)))
    worker = configured(); worker._restore_checkpoint(snapshot)
    assert worker.process_record(feature(29)) is None
    assert worker.process_record(feature(31)) is None
    assert worker.process_record(feature(30, run='new')) is None
    assert len(worker.engine.windows) == 0


@pytest.mark.parametrize('failure', ['output', 'commit'])
def test_failed_output_or_commit_replays_without_missing_prediction(tmp_path, failure):
    from kafka.structs import TopicPartition
    partition = TopicPartition('test-topic', 0)
    messages = [SimpleNamespace(topic='test-topic', partition=0, offset=i, value=feature(i + 1)) for i in range(30)]
    committed = [0]
    outputs = []

    class Consumer:
        def __init__(self, items, fail_commit=False): self.items, self.fail_commit = items, fail_commit
        def subscribe(self, topics, listener): pass
        def __iter__(self):
            yield from self.items
            current_worker[0]._stop.set()
        def assignment(self): return {partition}
        def partitions_for_topic(self, topic): return {0}
        def committed(self, part): return committed[0]
        def commit(self, offsets):
            value = offsets[partition].offset
            if self.fail_commit and value == 30: raise RuntimeError('injected commit failure')
            committed[0] = value
        def close(self): pass

    class Producer:
        def flush(self): pass
        def close(self): pass

    current_worker = [None]
    def execute(worker, consumer):
        current_worker[0] = worker
        worker.kafka_servers = 'unused'
        worker._consumer_ready, worker._stop = threading.Event(), threading.Event()
        worker.influx = None
        worker.influx_write = object()
        with patch.dict('os.environ', {'PREDICTION_CHECKPOINT_PATH': str(tmp_path / 'state.sqlite')}), \
             patch('ml.prediction.checkpoint._persistent_mount', return_value=True), \
             patch('ml.prediction.service.KafkaConsumer', return_value=consumer), \
             patch('ml.prediction.service.KafkaProducer', return_value=Producer()), \
             patch.object(worker, '_verify_checkpoint_history'):
            worker.start_consumer()

    first = configured()
    def publish_first(payload):
        if failure == 'output': raise RuntimeError('injected output failure')
        outputs.append(payload)
    first.publish_prediction = publish_first
    execute(first, Consumer(messages, fail_commit=failure == 'commit'))
    assert committed[0] == 29
    second = configured(); second.publish_prediction = outputs.append
    execute(second, Consumer(messages[29:]))
    assert committed[0] == 30
    assert len(outputs) == 1
    assert outputs[0]['cycle'] == 30
