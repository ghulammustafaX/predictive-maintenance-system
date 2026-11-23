"""Focused output and pre-mutation validation checks; no live services."""
import numpy as np
import pytest
import torch

from ml.prediction.service import FEATURE_COLUMNS
from module06_focused_checks import engine, stream_worker


class Output(torch.nn.Module):
    def __init__(self, value):
        super().__init__()
        self.value = value

    def forward(self, _):
        return self.value


@pytest.mark.parametrize('shape', [(), (1, 1), (2,), (1, 2)])
def test_single_prediction_rejects_malformed_output_shape(shape):
    model = engine(1.)
    model.model = Output(torch.ones(shape))
    with pytest.raises(ValueError, match='shape'):
        model.predict(np.zeros((30, 24), dtype=np.float32))


@pytest.mark.parametrize('shape', [(), (1,), (2, 1), (2, 2), (3,)])
def test_batch_prediction_rejects_malformed_output_shape(shape):
    model = engine(1.)
    model.model = Output(torch.ones(shape))
    with pytest.raises(ValueError, match='shape'):
        model.predict_batch(np.zeros((2, 30, 24), dtype=np.float32))


def test_batch_rejects_partially_nonfinite_output():
    model = engine(1.)
    model.model = Output(torch.tensor([3., float('nan')]))
    with pytest.raises(ValueError, match='finite'):
        model.predict_batch(np.zeros((2, 30, 24), dtype=np.float32))


def test_finite_batch_predictions_keep_negative_clamp():
    model = engine(1.)
    model.model = Output(torch.tensor([-3., 4.]))
    np.testing.assert_array_equal(model.predict_batch(np.zeros((2, 30, 24), dtype=np.float32)), [0., 4.])


def test_invalid_gap_and_new_engine_do_not_clear_or_evict_existing_history():
    worker = stream_worker()
    worker.max_windows = 1
    def row(cycle, unit=1, value=2.):
        return {'dataset_id': 'FD001', 'unit_id': unit, 'run_id': 'r', 'cycle': cycle,
                'features': [value] * len(FEATURE_COLUMNS)}
    assert worker.process_record(row(1)) is None
    key = next(iter(worker.windows))
    original = worker.windows[key][0].copy()
    for invalid in (row(3, value=float('inf')), row(1, unit=2, value=float('nan')),
                    row(1, value=float('nan'))):
        with pytest.raises(ValueError, match='finite'):
            worker.process_record(invalid)
        assert list(worker.windows) == [key] and worker.last_cycles[key] == 1
        np.testing.assert_array_equal(worker.windows[key][0], original)
        assert not worker.published
    assert worker.process_record(row(2)) is None
    assert worker.last_cycles[key] == 2 and len(worker.windows[key]) == 2


def test_direct_multi_record_call_validates_all_before_mutating_history():
    worker = stream_worker()
    valid = {name: 1. for name in FEATURE_COLUMNS}
    invalid = dict(valid); invalid[FEATURE_COLUMNS[0]] = float('inf')
    with pytest.raises(ValueError, match='finite'):
        worker.predict_records('FD001', 1, 'r', [valid, invalid])
    assert not worker.windows and not worker.last_cycles and not worker.published


def test_bad_duplicate_does_not_advance_checkpoint_or_offset(tmp_path):
    import sqlite3
    import threading
    from types import SimpleNamespace
    from unittest.mock import patch
    from kafka.structs import TopicPartition
    worker = stream_worker()
    worker.input_topic, worker.output_topic, worker.kafka_servers, worker.consumer_group = 'input', 'output', 'unused', 'isolated'
    worker._consumer_ready, worker._stop = threading.Event(), threading.Event()
    worker._consumer_error, worker.influx, worker.influx_write = None, None, object()
    worker.compatibility_report = {'checkpoint_sha256': 'model', 'artifact_version': 'a1-v2', 'artifact_sha256': {}}
    worker.expected_preprocessing_artifact_version = 'a1-v2'
    row = {'dataset_id': 'FD001', 'unit_id': 1, 'run_id': 'r', 'cycle': 1,
           'features': [1.] * 24, 'preprocessing_artifact_version': 'a1-v2'}
    invalid = dict(row, features=[float('nan')] + [1.] * 23)
    part = TopicPartition('input', 0)
    class Consumer:
        def __init__(self): self.commits = []
        def __iter__(self):
            yield SimpleNamespace(topic='input', partition=0, offset=0, value=row)
            yield SimpleNamespace(topic='input', partition=0, offset=1, value=invalid)
        def subscribe(self, topics, listener): pass
        def commit(self, offsets): self.commits.append(offsets)
        def partitions_for_topic(self, topic): return {0}
        def assignment(self): return {part}
        def committed(self, partition): return 0
        def close(self): pass
    class Producer:
        def flush(self): pass
        def close(self): pass
    consumer = Consumer()
    path = tmp_path / 'state.sqlite'
    with patch.dict('os.environ', {'PREDICTION_CHECKPOINT_PATH': str(path)}), \
         patch('ml.prediction.checkpoint._persistent_mount', return_value=True), \
         patch('ml.prediction.service.KafkaConsumer', return_value=consumer), \
         patch('ml.prediction.service.KafkaProducer', return_value=Producer()):
        worker.start_consumer()
    with sqlite3.connect(path) as db:
        import json
        checkpoint = json.loads(db.execute('select payload from checkpoint').fetchone()[0])
    assert checkpoint['offset'] == 0
    assert [list(item.values())[0].offset for item in consumer.commits] == [1]
    assert not worker.published and worker._consumer_error
