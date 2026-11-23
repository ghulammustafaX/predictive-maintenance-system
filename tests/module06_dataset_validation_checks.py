"""Temporary-data coverage for Module 06 training dataset validation."""
import shutil

import numpy as np
import pytest

from ml.models import train as training
from ml.models.dataset import WindowedNpzDataset
from module06_focused_checks import TinyModel, verified_engineered_bundle


class ModelReached(Exception):
    pass


def stop_model(config):
    raise ModelReached(config.input_size)


def test_verified_bundle_reaches_model_with_contract_width(verified_engineered_bundle, tmp_path, monkeypatch):
    from ml.features.feature_engineering import load_feature_contract
    monkeypatch.setattr(training, 'RULLSTM', stop_model)
    output = tmp_path / 'new-output'
    with pytest.raises(ModelReached) as exc:
        training.train('engineered', dataset_bundle=verified_engineered_bundle, artifact_dir=output)
    assert exc.value.args[0] == len(load_feature_contract(verified_engineered_bundle / 'feature_contract.json').feature_order)
    assert not output.exists()


def test_verified_bundle_uses_new_explicit_output(verified_engineered_bundle, tmp_path, monkeypatch):
    import torch
    from ml.features.feature_engineering import load_feature_contract
    monkeypatch.setattr(training, 'RULLSTM', TinyModel)
    monkeypatch.setattr(training.torch.cuda, 'is_available', lambda: False)
    monkeypatch.setattr(training, 'evaluate', lambda *args: (1., 1.))
    output = tmp_path / 'new-output'
    result = training.train('engineered', epochs=1, batch_size=128,
                            dataset_bundle=verified_engineered_bundle, artifact_dir=output)
    assert output.is_dir()
    assert len(list(output.glob('*.pt'))) == 1
    assert len(list(output.glob('*_metadata.json'))) == 1
    assert result['model_config']['input_size'] > 24
    checkpoint = torch.load(next(output.glob('*.pt')), map_location='cpu', weights_only=False)
    contract = load_feature_contract(verified_engineered_bundle / 'feature_contract.json')
    info = checkpoint['metadata']
    assert info['data_identity']['feature_order'] == list(contract.feature_order)
    assert info['data_identity']['feature_contract_sha256'] == contract.contract_hash
    assert info['data_identity']['preprocessing_identity_sha256'] == contract.payload['preprocessing_artifact_identity']
    assert info['input_feature_count'] == len(contract.feature_order) and info['window_size'] == 30
    assert checkpoint['resume_supported'] is False


@pytest.mark.parametrize('case', ['missing_bundle', 'missing_destination', 'existing_destination', 'inside_bundle', 'historical_destination'])
def test_engineered_requires_explicit_safe_paths(case, verified_engineered_bundle, tmp_path):
    bundle = verified_engineered_bundle
    output = tmp_path / 'output'
    if case == 'missing_bundle': bundle = None
    elif case == 'missing_destination': output = None
    elif case == 'existing_destination': output.mkdir()
    elif case == 'inside_bundle': output = bundle / 'new-output'
    elif case == 'historical_destination': output = training.CHECKPOINT_DIR / 'engineered' / 'new-output'
    with pytest.raises(ValueError):
        training.train('engineered', dataset_bundle=bundle, artifact_dir=output)


@pytest.mark.parametrize('case', ['x_rank', 'y_rank', 'x_empty', 'x_string', 'y_string', 'x_overflow', 'y_overflow', 'unit_length', 'dataset_length'])
def test_strict_raw_validation_rejects_malformed_arrays(case, tmp_path):
    x = np.zeros((2, 30, 24), dtype=np.float32)
    y = np.ones(2, dtype=np.float32)
    extra = {}
    if case == 'x_rank': x = np.zeros((2, 24))
    elif case == 'y_rank': y = y.reshape(-1, 1)
    elif case == 'x_empty': x = x[:0]; y = y[:0]
    elif case == 'x_string': x = x.astype(str)
    elif case == 'y_string': y = y.astype(str)
    elif case == 'x_overflow': x = x.astype(np.float64); x[0, 0, 0] = 1e100
    elif case == 'y_overflow': y = y.astype(np.float64); y[0] = 1e100
    elif case == 'unit_length': extra['unit_ids'] = np.ones(1)
    elif case == 'dataset_length': extra['dataset_ids'] = np.ones(3)
    path = tmp_path / 'input.npz'
    np.savez_compressed(path, X=x, y=y, **extra)
    with pytest.raises(ValueError):
        WindowedNpzDataset(path, expected_features=24)


def test_raw_validation_accepts_optional_identity_arrays(tmp_path):
    path = tmp_path / 'input.npz'
    np.savez_compressed(path, X=np.zeros((2, 30, 24), dtype=np.float32), y=np.ones(2),
                        unit_ids=np.array([1, 2]), dataset_ids=np.array(['FD001', 'FD002']))
    assert len(WindowedNpzDataset(path, expected_features=24)) == 2


def test_verified_bundle_output_destination_must_not_be_ancestor(verified_engineered_bundle, tmp_path):
    root = tmp_path / 'parent'
    shutil.copytree(verified_engineered_bundle, root / 'bundle')
    with pytest.raises(ValueError):
        training.train('engineered', dataset_bundle=root / 'bundle', artifact_dir=root)
