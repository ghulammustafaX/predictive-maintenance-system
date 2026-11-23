"""Module 06 contract checks. Deliberate baseline failures remain ordinary failures."""
from collections import OrderedDict
from types import SimpleNamespace
import threading

import numpy as np
import pytest
import torch
from torch.utils.data import DataLoader, TensorDataset

from ml.models import train as training
from ml.models.dataset import WindowedNpzDataset
from ml.models.inference import RULInferenceEngine
from ml.models.lstm import LSTMConfig, RULLSTM
from ml.prediction.service import PredictionService, FEATURE_COLUMNS
from ml.prediction.checkpoint import PredictionCheckpoint


def write_npz(path, X=None, y=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(path, X=np.zeros((2, 30, 24), dtype=np.float32) if X is None else X,
                        y=np.ones(2, dtype=np.float32) if y is None else y)


class ReachedModel(Exception):
    pass


@pytest.mark.parametrize('case', ['sequence', 'features', 'x_nan', 'x_inf', 'y_nan', 'y_inf', 'length'])
def test_training_rejects_invalid_raw_arrays_before_model(case, tmp_path, monkeypatch):
    root = tmp_path / 'data'; d = root / 'raw'
    X = np.zeros((2, 30, 24), dtype=np.float32); y = np.ones(2, dtype=np.float32)
    if case == 'sequence': X = X[:, :29]
    elif case == 'features': X = X[:, :, :23]
    elif case == 'x_nan': X[0, 0, 0] = np.nan
    elif case == 'x_inf': X[0, 0, 0] = np.inf
    elif case == 'y_nan': y[0] = np.nan
    elif case == 'y_inf': y[0] = np.inf
    else: y = y[:1]
    write_npz(d / 'train.npz', X, y); write_npz(d / 'val.npz')
    monkeypatch.setattr(training, 'COMBINED_DIR', root)
    def stop(config): raise ReachedModel()
    monkeypatch.setattr(training, 'RULLSTM', stop)
    try:
        with pytest.raises(ValueError):
            training.train('raw', epochs=1)
    except ReachedModel:
        pytest.fail('training accepted invalid dataset and reached model construction')


def test_missing_training_file_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(training, 'COMBINED_DIR', tmp_path)
    with pytest.raises(FileNotFoundError):
        training.train('raw', epochs=1)


def test_historical_raw_npz_does_not_require_contract(tmp_path):
    p = tmp_path / 'legacy.npz'; write_npz(p)
    d = WindowedNpzDataset(p)
    assert tuple(d.X.shape) == (2, 30, 24) and len(d) == 2


@pytest.fixture(scope='module')
def verified_engineered_bundle(tmp_path_factory):
    from ml.features.dataset_generation import generate, verify_generated_dataset
    from module05_dataset_checks import create_inputs
    root = tmp_path_factory.mktemp('verified-m6-engineered')
    source = root / 'inputs'; create_inputs(source)
    output = root / 'bundle'; generate(source, output)
    verify_generated_dataset(output)
    return output


@pytest.mark.parametrize('case', ['missing_contract', 'wrong_contract', 'wrong_preprocessing',
                                  'missing_sources', 'missing_partitions', 'reordered_features'])
def test_engineered_training_requires_verified_contract(case, tmp_path, monkeypatch, verified_engineered_bundle):
    import json
    import shutil
    from ml.features.dataset_generation import verify_generated_dataset
    root = tmp_path / 'bundle'
    shutil.copytree(verified_engineered_bundle, root)
    verify_generated_dataset(root)
    manifest_path = root / 'manifest.json'
    meta = json.loads(manifest_path.read_text())
    if case == 'missing_contract':
        (root / 'feature_contract.json').unlink()
    elif case == 'wrong_contract':
        path = root / 'feature_contract.json'
        obj = json.loads(path.read_text()); obj['sha256'] = '0' * 64
        path.write_text(json.dumps(obj))
    elif case == 'wrong_preprocessing':
        meta['preprocessing_identity_sha256'] = '0' * 64
        manifest_path.write_text(json.dumps(meta))
    elif case == 'missing_sources':
        del meta['source_sha256']
        manifest_path.write_text(json.dumps(meta))
    elif case == 'missing_partitions':
        del meta['partitions']
        manifest_path.write_text(json.dumps(meta))
    elif case == 'reordered_features':
        path = root / 'engineered' / 'train.npz'
        with np.load(path, allow_pickle=False) as data:
            arrays = {key: data[key].copy() for key in data.files}
        assert not np.array_equal(arrays['X'][:, :, 0], arrays['X'][:, :, -1])
        arrays['X'][:, :, [0, -1]] = arrays['X'][:, :, [-1, 0]]
        np.savez_compressed(path, **arrays)
    # This independently proves the initially valid bundle became invalid.
    with pytest.raises(ValueError):
        verify_generated_dataset(root)
    monkeypatch.setattr(training, 'COMBINED_DIR', root)
    def stop(config): raise ReachedModel()
    monkeypatch.setattr(training, 'RULLSTM', stop)
    try:
        with pytest.raises((ValueError, FileNotFoundError)):
            training.train('engineered', epochs=1, dataset_bundle=root,
                           artifact_dir=tmp_path / 'new-checkpoints')
    except ReachedModel:
        pytest.fail('engineered training accepted a tampered, independently rejected bundle')

class TinyModel(torch.nn.Module):
    def __init__(self, config):
        super().__init__(); self.config = config; self.weight = torch.nn.Parameter(torch.tensor(0.0))
    def forward(self, X):
        return self.weight.expand(X.shape[0])


def fixture_train(tmp_path, monkeypatch, missing=None):
    root = tmp_path / 'data'; d = root / 'raw'
    write_npz(d / 'train.npz'); write_npz(d / 'val.npz')
    for fd in ('FD001', 'FD002', 'FD003', 'FD004'):
        if fd != missing: write_npz(d / f'test_{fd}.npz')
    monkeypatch.setattr(training, 'COMBINED_DIR', root)
    monkeypatch.setattr(training, 'CHECKPOINT_DIR', tmp_path / 'checkpoints')
    monkeypatch.setattr(training, 'RULLSTM', TinyModel)
    monkeypatch.setattr(torch.cuda, 'is_available', lambda: False)
    return root


@pytest.mark.parametrize('missing', ['FD001', 'FD002', 'FD003', 'FD004'])
def test_each_fd_test_dataset_is_required(missing, tmp_path, monkeypatch):
    fixture_train(tmp_path, monkeypatch, missing)
    with pytest.raises((FileNotFoundError, ValueError), match=missing):
        training.train('raw', epochs=1, batch_size=2)


def test_checkpoint_records_actual_best_epoch_and_model_config(tmp_path, monkeypatch):
    fixture_train(tmp_path, monkeypatch)
    scores = iter([(1., 1.), (2., 2.), (3., 3.)] + [(0., 0.)] * 4)
    observed = []
    def evaluate(model, loader, device):
        observed.append(float(model.weight.detach()))
        return next(scores)
    monkeypatch.setattr(training, 'evaluate', evaluate)
    metadata = training.train('raw', epochs=3, batch_size=2)
    path = tmp_path / 'checkpoints' / 'raw'
    ckpts = list(path.glob('*.pt')); assert len(ckpts) == 1
    saved = torch.load(ckpts[0], map_location='cpu', weights_only=False)
    assert float(saved['model_state']['weight']) == pytest.approx(observed[0])
    assert saved['config']['input_size'] == metadata['model_config']['input_size'] == 24
    assert saved['epoch'] == 1, 'checkpoint must identify best epoch, not last epoch'
    assert saved.get('resume_supported') is False or saved.get('optimizer_state') is not None


def test_metrics_are_correct():
    rmse, mae = training.rmse_mae(np.array([0., 3.]), np.array([0., 1.]))
    assert rmse == pytest.approx(np.sqrt(2)) and mae == pytest.approx(1.)


def test_evaluate_preserves_model_weights():
    model = TinyModel(LSTMConfig(input_size=24))
    before = {k: v.clone() for k, v in model.state_dict().items()}
    training.evaluate(model, DataLoader(TensorDataset(torch.zeros(2, 30, 24), torch.ones(2))), 'cpu')
    assert all(torch.equal(v, model.state_dict()[k]) for k, v in before.items())


class Constant(torch.nn.Module):
    def __init__(self, value): super().__init__(); self.value = value
    def forward(self, x): return torch.full((len(x),), self.value)


def engine(value):
    obj = RULInferenceEngine.__new__(RULInferenceEngine)
    obj.config = LSTMConfig(input_size=24); obj.device = 'cpu'; obj.model = Constant(value)
    return obj


def test_inference_valid_window_and_negative_clamp():
    assert engine(-3.).predict(np.zeros((30,24), dtype=np.float32)) == 0.


@pytest.mark.parametrize('shape', [(29,24), (30,23), (31,24)])
def test_inference_rejects_bad_shapes(shape):
    with pytest.raises(ValueError): engine(1.).predict(np.zeros(shape, dtype=np.float32))


@pytest.mark.parametrize('bad', [np.nan, np.inf, -np.inf])
def test_inference_rejects_nonfinite_input(bad):
    x = np.zeros((30,24), dtype=np.float32); x[0,0] = bad
    with pytest.raises(ValueError): engine(1.).predict(x)


@pytest.mark.parametrize('bad', [float('nan'), float('inf'), float('-inf')])
def test_inference_rejects_nonfinite_model_output(bad):
    with pytest.raises(ValueError, match='finite'):
        engine(bad).predict(np.zeros((30,24), dtype=np.float32))


def test_missing_checkpoint_is_rejected(tmp_path):
    with pytest.raises((FileNotFoundError, OSError)):
        RULInferenceEngine(tmp_path / 'missing.pt', device='cpu')


def test_incompatible_model_configuration_is_rejected(tmp_path):
    path = tmp_path / 'model.pt'
    config = LSTMConfig(input_size=24)
    model = RULLSTM(config)
    torch.save({'config': {**config.to_dict(), 'input_size': 23}, 'model_state': model.state_dict()}, path)
    with pytest.raises(RuntimeError):
        RULInferenceEngine(path, device='cpu')


def stream_worker():
    worker = PredictionService.__new__(PredictionService)
    worker.windows, worker.last_cycles, worker.last_metadata = OrderedDict(), {}, {}
    worker.max_windows = 5; worker.engine = engine(2.); worker.model_version = 'fixture'
    worker.published = []
    worker.publish_prediction = lambda payload: worker.published.append(payload)
    return worker


@pytest.mark.parametrize('bad', [np.nan, np.inf, -np.inf])
@pytest.mark.parametrize('order_present', [False, True])
def test_invalid_warmup_record_does_not_change_state_or_publish(bad, order_present):
    worker = stream_worker()
    row = {'dataset_id':'FD001','unit_id':1,'run_id':'r','cycle':1,
           'features':[bad]+[0.] * 23}
    if order_present: row['feature_order'] = list(FEATURE_COLUMNS)
    with pytest.raises(ValueError):
        worker.process_record(row)
    assert not worker.windows and not worker.last_cycles and not worker.last_metadata and not worker.published


def test_wrong_declared_feature_order_rejected():
    worker = stream_worker()
    row = {'dataset_id':'FD001','unit_id':1,'run_id':'r','cycle':1,'features':[0.] * 24,
           'feature_order': list(reversed(FEATURE_COLUMNS))}
    with pytest.raises(ValueError, match='feature_order'):
        worker.process_record(row)
    assert not worker.windows and not worker.last_cycles


def test_checkpoint_refuses_nonfinite_payload(tmp_path):
    path = tmp_path / 'state.sqlite'
    store = PredictionCheckpoint(str(path), require_mount=False)
    try:
        with pytest.raises(ValueError):
            store.save({'schema_version':1, 'value':float('nan')})
        assert store.load() is None
    finally: store.close()

def test_best_epoch_optimizer_snapshot_matches_saved_model(tmp_path, monkeypatch):
    import copy
    fixture_train(tmp_path, monkeypatch)
    real_adam = torch.optim.Adam
    made = []
    class ObservedAdam(real_adam):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs); made.append(self)
    monkeypatch.setattr(training.torch.optim, 'Adam', ObservedAdam)
    scores = iter([(1.,1.), (2.,2.), (3.,3.)] + [(0.,0.)] * 4)
    snapshots = []
    def evaluate(model, loader, device):
        snapshots.append(copy.deepcopy(made[0].state_dict()))
        return next(scores)
    monkeypatch.setattr(training, 'evaluate', evaluate)
    training.train('raw', epochs=3, batch_size=2)
    saved = torch.load(next((tmp_path / 'checkpoints' / 'raw').glob('*.pt')), map_location='cpu', weights_only=False)
    expected = next(iter(snapshots[0]['state'].values()))
    observed = next(iter(saved['optimizer_state']['state'].values()))
    assert torch.equal(observed['step'], expected['step'])
    assert torch.equal(observed['exp_avg'], expected['exp_avg'])


@pytest.mark.parametrize('order_present', [False, True])
def test_nonfinite_record_does_not_commit_or_persist(tmp_path, monkeypatch, order_present):
    import sqlite3
    from unittest.mock import patch
    from kafka.structs import TopicPartition
    worker = stream_worker()
    worker.input_topic, worker.output_topic, worker.kafka_servers, worker.consumer_group = 'input', 'output', 'unused', 'isolated'
    worker._consumer_ready, worker._stop = threading.Event(), threading.Event()
    worker._consumer_error, worker.influx, worker.influx_write = None, None, object()
    worker.compatibility_report = {'checkpoint_sha256':'model', 'artifact_version':'a1-v2', 'artifact_sha256':{}}
    worker.expected_preprocessing_artifact_version = 'a1-v2'
    row = {'dataset_id':'FD001','unit_id':1,'run_id':'r','cycle':1,'features':[float('nan')]+[0.] * 23,
           'preprocessing_artifact_version':'a1-v2'}
    if order_present: row['feature_order'] = list(FEATURE_COLUMNS)
    part = TopicPartition('input', 0)
    class Consumer:
        def __init__(self): self.commits = []
        def __iter__(self): yield SimpleNamespace(topic='input', partition=0, offset=0, value=row)
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
    with patch.dict('os.environ', {'PREDICTION_CHECKPOINT_PATH':str(path)}), \
         patch('ml.prediction.checkpoint._persistent_mount', return_value=True), \
         patch('ml.prediction.service.KafkaConsumer', return_value=consumer), \
         patch('ml.prediction.service.KafkaProducer', return_value=Producer()):
        worker.start_consumer()
    with sqlite3.connect(path) as db:
        assert db.execute('select payload from checkpoint').fetchone() is None
    assert consumer.commits == [] and worker.published == []
    assert worker._consumer_error

def test_prediction_metadata_matches_final_observation():
    from ml.prediction.service import PredictionRequest, build_request_prediction
    worker = stream_worker()
    rows = [{**{column: float(cycle) for column in FEATURE_COLUMNS}, 'cycle': cycle,
             'dataset_id':'FD001', 'unit_id':1, 'run_id':'run',
             'timestamp':'2026-01-01T00:00:00Z',
             'preprocessing_artifact_version':'a1-v2'} for cycle in range(1,31)]
    response = build_request_prediction(worker, PredictionRequest(dataset_id='FD001', unit_id=1,
                                                                  run_id='run', features=rows))
    assert response.rul == 2. and response.window_size == 30
    assert len(worker.published) == 1
    payload = worker.published[0]
    assert (payload['dataset_id'], payload['unit_id'], payload['run_id'], payload['cycle']) == ('FD001', 1, 'run', 30)
    assert payload['model_version'] == 'fixture' and payload['preprocessing_artifact_version'] == 'a1-v2'


@pytest.mark.parametrize('bad', [float('nan'), float('inf')])
def test_inference_batch_rejects_nonfinite_model_output(bad):
    with pytest.raises(ValueError, match='finite'):
        engine(bad).predict_batch(np.zeros((2,30,24), dtype=np.float32))
