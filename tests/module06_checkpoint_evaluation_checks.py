"""Small temporary checks for checkpoint and evaluation contracts."""
import copy

import numpy as np
import pytest
import torch

from ml.models import train as training
from ml.models.inference import RULInferenceEngine
from ml.models.lstm import LSTMConfig, RULLSTM
from module06_focused_checks import fixture_train, write_npz


@pytest.mark.parametrize('case', ['shape', 'x_nan', 'y_inf', 'length'])
def test_invalid_heldout_file_fails_before_model(case, tmp_path, monkeypatch):
    root = fixture_train(tmp_path, monkeypatch)
    x = np.zeros((2, 30, 24), dtype=np.float32)
    y = np.ones(2, dtype=np.float32)
    if case == 'shape': x = x[:, :29]
    elif case == 'x_nan': x[0, 0, 0] = np.nan
    elif case == 'y_inf': y[0] = np.inf
    else: y = y[:1]
    write_npz(root / 'raw' / 'test_FD003.npz', x, y)
    def fail_if_model_constructed(config):
        pytest.fail('model constructed before held-out validation')
    monkeypatch.setattr(training, 'RULLSTM', fail_if_model_constructed)
    with pytest.raises(ValueError):
        training.train('raw', epochs=1)
    assert not (tmp_path / 'checkpoints').exists()


def test_best_checkpoint_and_all_evaluations(tmp_path, monkeypatch):
    fixture_train(tmp_path, monkeypatch)
    real_scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau
    schedulers = []
    class ObservedScheduler(real_scheduler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            schedulers.append(self)
    monkeypatch.setattr(training.torch.optim.lr_scheduler, 'ReduceLROnPlateau', ObservedScheduler)
    values = iter([(1., .5), (2., 1.), (3., 2.)] + [(4., 3.)] * 4)
    scheduler_states, weights = [], []
    def evaluate(model, loader, device):
        weights.append(float(model.weight.detach()))
        scheduler_states.append(copy.deepcopy(schedulers[0].state_dict()))
        return next(values)
    monkeypatch.setattr(training, 'evaluate', evaluate)
    metadata = training.train('raw', epochs=3, batch_size=2)
    path = next((tmp_path / 'checkpoints' / 'raw').glob('*.pt'))
    checkpoint = torch.load(path, map_location='cpu', weights_only=False)
    assert checkpoint['epoch'] == checkpoint['metadata']['best_epoch'] == metadata['best_epoch'] == 1
    assert checkpoint['best_val_rmse'] == checkpoint['metadata']['best_val_rmse'] == 1.
    assert float(checkpoint['model_state']['weight']) == pytest.approx(weights[0])
    # Scheduler.step is after evaluation, so the next evaluation observes the selected state.
    assert checkpoint['scheduler_state'] == scheduler_states[1]
    assert checkpoint['resume_supported'] is checkpoint['metadata']['resume_supported'] is False
    assert checkpoint['metadata']['data_identity']['feature_order'] is None
    assert checkpoint['metadata']['input_feature_count'] == 24 and checkpoint['metadata']['window_size'] == 30
    assert list(metadata['per_fd_test_results']) == ['FD001', 'FD002', 'FD003', 'FD004']
    assert all(item['rmse'] == 4. and item['mae'] == 3. for item in metadata['per_fd_test_results'].values())


def test_legacy_inference_checkpoint_without_new_metadata_loads(tmp_path):
    config = LSTMConfig(input_size=24)
    model = RULLSTM(config)
    path = tmp_path / 'legacy.pt'
    torch.save({'config': config.to_dict(), 'model_state': model.state_dict()}, path)
    inference = RULInferenceEngine(path, device='cpu')
    assert inference.config.input_size == 24
    assert np.isfinite(inference.predict(np.zeros((30, 24), dtype=np.float32)))
