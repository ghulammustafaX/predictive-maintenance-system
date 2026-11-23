"""Offline acceptance checks for the engineered dataset adapter."""
import hashlib
import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ml.config import ALL_COLUMNS, OUTLIER_Z_THRESHOLD, SUB_DATASETS
from ml.data.preprocessing import FEATURE_COLUMNS, apply_scaler, clip_outliers, handle_missing_values, split_by_unit
from ml.features.dataset_generation import generate
from ml.features.feature_engineering import load_feature_contract, transform_features
from ml.models.dataset import WindowedNpzDataset


def make_frame(units, cycles=32):
    return pd.DataFrame([{**{'unit_id': unit, 'time_cycles': cycle},
        **{c: float(unit * 3 + cycle + i * .01) for i, c in enumerate(FEATURE_COLUMNS)}}
        for unit in units for cycle in range(1, cycles + 1)])


def create_inputs(root):
    root.mkdir()
    for fd in SUB_DATASETS:
        make_frame(range(1, 6)).to_csv(root / f'train_{fd}.txt', sep=' ', header=False, index=False, columns=ALL_COLUMNS)
        make_frame([1, 2], 32).to_csv(root / f'test_{fd}.txt', sep=' ', header=False, index=False, columns=ALL_COLUMNS)
        (root / f'RUL_{fd}.txt').write_text('7\n11\n')


def test_end_to_end_training_only_fit_and_reader_compatibility(tmp_path):
    first = tmp_path / 'first-input'; create_inputs(first)
    second = tmp_path / 'second-input'; create_inputs(second)
    # Modify only units selected as validation by the actual Module 04 split.
    for fd in SUB_DATASETS:
        path = second / f'train_{fd}.txt'
        df = pd.read_csv(path, sep=r'\s+', header=None, names=ALL_COLUMNS)
        _, val = split_by_unit(df)
        df.loc[df.unit_id.isin(val.unit_id), FEATURE_COLUMNS] = 1e5
        df.to_csv(path, sep=' ', header=False, index=False, columns=ALL_COLUMNS)
        (second / f'RUL_{fd}.txt').write_text('19\n23\n')
    a_dir, b_dir = tmp_path / 'a', tmp_path / 'b'
    a, b = generate(first, a_dir), generate(second, b_dir)
    assert a['contract_sha256'] == b['contract_sha256']
    assert a['preprocessing_identity_sha256'] == b['preprocessing_identity_sha256']
    assert hashlib.sha256((a_dir / 'preprocessing' / 'manifest.json').read_bytes()).hexdigest() == a['preprocessing_identity_sha256']
    assert a['feature_order'] == b['feature_order']
    assert len(a['feature_order']) == len(set(a['feature_order']))
    assert 'rul' not in a['feature_order']
    assert a['historical_checkpoint_compatibility'] == 'not_certified'
    contract = load_feature_contract(a_dir / 'feature_contract.json')
    assert list(contract.feature_order) == a['feature_order']
    assert contract.contract_hash == a['contract_sha256']
    for fd in SUB_DATASETS:
        original = pd.read_csv(first / f'train_{fd}.txt', sep=r'\s+', header=None, names=ALL_COLUMNS)
        expected_train, expected_val = split_by_unit(original)
        parts = a['partitions'][fd]
        assert set(parts['train_units']) == set(expected_train.unit_id)
        assert set(parts['validation_units']) == set(expected_val.unit_id)
        assert set(parts['train_units']).isdisjoint(parts['validation_units'])
        info = json.loads((a_dir / 'preprocessing' / f'{fd}_statistics.json').read_text())
        assert hashlib.sha256((a_dir / 'preprocessing' / f'{fd}_scaler.pkl').read_bytes()).hexdigest() == info['scaler_sha256']
        with (a_dir / 'preprocessing' / f'{fd}_scaler.pkl').open('rb') as stream:
            scaler = pickle.load(stream)
        source = pd.read_csv(first / f'test_{fd}.txt', sep=r'\s+', header=None, names=ALL_COLUMNS)
        clean = handle_missing_values(source, info['medians'])
        scaled = apply_scaler(clip_outliers(clean, OUTLIER_Z_THRESHOLD, info['clipping_statistics']), scaler)
        features = transform_features(scaled.assign(dataset_id=fd, split='test'), contract,
            preprocessing_artifact_identity=a['preprocessing_identity_sha256'])
        with np.load(a_dir / 'engineered' / f'test_{fd}.npz') as x, np.load(b_dir / 'engineered' / f'test_{fd}.npz') as y:
            assert x['X'].shape == (2, 30, len(a['feature_order']))
            for index, unit in enumerate(x['unit_ids']):
                expected = features[features.unit_id == unit].sort_values('time_cycles').loc[:, a['feature_order']].to_numpy(dtype=np.float32)[-30:]
                np.testing.assert_array_equal(x['X'][index], expected)
            np.testing.assert_array_equal(x['X'], y['X'])
            assert x['y'].tolist() == [7, 11]
            assert y['y'].tolist() == [19, 23]
            assert np.isfinite(x['X']).all()
    for split in ('train', 'val'):
        reader = WindowedNpzDataset(a_dir / 'engineered' / f'{split}.npz')
        assert tuple(reader.X.shape[1:]) == (30, len(contract.feature_order))
        assert len(reader.X) == len(reader.y)
    with np.load(a_dir / 'engineered' / 'train.npz') as x, np.load(b_dir / 'engineered' / 'train.npz') as y:
        np.testing.assert_array_equal(x['X'], y['X'])
        np.testing.assert_array_equal(x['y'], y['y'])


def test_future_test_cycle_and_validation_labels_do_not_leak(tmp_path, monkeypatch):
    import ml.features.dataset_generation as adapter
    first = tmp_path / 'input-a'; create_inputs(first)
    second = tmp_path / 'input-b'; create_inputs(second)
    path = second / 'test_FD001.txt'
    df = pd.read_csv(path, sep=r'\s+', header=None, names=ALL_COLUMNS)
    df.loc[(df.unit_id == 1) & (df.time_cycles == 32), 'sensor_2'] = 1000
    df.to_csv(path, sep=' ', header=False, index=False, columns=ALL_COLUMNS)
    a, b = tmp_path / 'a', tmp_path / 'b'
    first_manifest = generate(first, a)
    original = adapter.assign_rul_train
    def change_validation_labels(frame):
        labelled = original(frame)
        if labelled['split'].eq('validation').all():
            labelled['rul'] += 100
        return labelled
    monkeypatch.setattr(adapter, 'assign_rul_train', change_validation_labels)
    second_manifest = generate(second, b)
    assert first_manifest['contract_sha256'] == second_manifest['contract_sha256']
    assert first_manifest['preprocessing_identity_sha256'] == second_manifest['preprocessing_identity_sha256']
    with np.load(a / 'engineered' / 'train.npz') as x, np.load(b / 'engineered' / 'train.npz') as y:
        np.testing.assert_array_equal(x['X'], y['X'])
        np.testing.assert_array_equal(x['y'], y['y'])
    with np.load(a / 'engineered' / 'test_FD001.npz') as x, np.load(b / 'engineered' / 'test_FD001.npz') as y:
        np.testing.assert_array_equal(x['X'][0, :-1, :], y['X'][0, :-1, :])
    with np.load(a / 'engineered' / 'val.npz') as x, np.load(b / 'engineered' / 'val.npz') as y:
        np.testing.assert_array_equal(x['X'], y['X'])
        np.testing.assert_array_equal(y['y'], x['y'] + 100)


def test_missing_invalid_and_existing_destinations_are_rejected(tmp_path):
    inputs = tmp_path / 'input'; create_inputs(inputs)
    existing = tmp_path / 'existing'; existing.mkdir()
    with pytest.raises(FileExistsError):
        generate(inputs, existing)
    with pytest.raises(ValueError, match='input directory'):
        generate(inputs, inputs / 'output')
    path = inputs / 'train_FD001.txt'
    df = pd.read_csv(path, sep=r'\s+', header=None, names=ALL_COLUMNS)
    df.loc[0, 'unit_id'] = 0
    df.to_csv(path, sep=' ', header=False, index=False, columns=ALL_COLUMNS)
    with pytest.raises(ValueError, match='invalid unit_id'):
        generate(inputs, tmp_path / 'invalid')
    assert not (tmp_path / 'invalid' / 'manifest.json').exists()
    df.loc[0, 'unit_id'] = 1
    df['conflicting_extra_column'] = 0
    df.to_csv(path, sep=' ', header=False, index=False)
    with pytest.raises(ValueError, match='invalid column count'):
        generate(inputs, tmp_path / 'extra-column')
    path.unlink()
    with pytest.raises(FileNotFoundError, match='missing C-MAPSS input'):
        generate(inputs, tmp_path / 'missing')
    assert not (tmp_path / 'missing').exists()
