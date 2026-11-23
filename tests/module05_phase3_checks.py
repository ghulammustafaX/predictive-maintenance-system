"""Bounded, temporary cross-module checks and artifact limitations for Phase 3."""
import hashlib
import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ml.config import ALL_COLUMNS, OUTLIER_Z_THRESHOLD, SUB_DATASETS
from ml.data.preprocessing import FEATURE_COLUMNS, apply_scaler, clip_outliers, handle_missing_values, generate_windows
from ml.features.dataset_generation import generate
from ml.features.feature_engineering import load_feature_contract
from ml.models.dataset import WindowedNpzDataset
from module05_dataset_checks import create_inputs


@pytest.fixture(scope='module')
def generated(tmp_path_factory):
    root = tmp_path_factory.mktemp('m5-phase3')
    source = root / 'source'; create_inputs(source)
    out = root / 'output'; generate(source, out)
    return source, out


def test_manifest_source_hashes_partition_integrity_and_reader(generated):
    source, out = generated
    manifest = json.loads((out / 'manifest.json').read_text())
    contract = load_feature_contract(out / 'feature_contract.json')
    assert contract.contract_hash == manifest['contract_sha256']
    assert hashlib.sha256((out / 'preprocessing' / 'manifest.json').read_bytes()).hexdigest() == manifest['preprocessing_identity_sha256']
    assert list(contract.feature_order) == manifest['feature_order']
    assert len(set(contract.feature_order)) == len(contract.feature_order)
    assert list(contract.feature_order[:24]) == FEATURE_COLUMNS
    order_json = json.dumps(manifest['feature_order'], separators=(',', ':'), ensure_ascii=False)
    print('SYNTHETIC_EVIDENCE', json.dumps({'feature_order': manifest['feature_order'],
        'feature_order_sha256': hashlib.sha256(order_json.encode('utf-8')).hexdigest(),
        'contract_sha256': manifest['contract_sha256'],
        'preprocessing_identity_sha256': manifest['preprocessing_identity_sha256'],
        'tensor_shapes': manifest['tensor_shapes']}, sort_keys=True), flush=True)
    for fd in SUB_DATASETS:
        for name in ('train', 'test', 'RUL'):
            assert hashlib.sha256((source / f'{name}_{fd}.txt').read_bytes()).hexdigest() == manifest['source_sha256'][fd][name]
        part = manifest['partitions'][fd]
        assert set(part['train_units']).isdisjoint(part['validation_units'])
        info = json.loads((out / 'preprocessing' / f'{fd}_statistics.json').read_text())
        assert hashlib.sha256((out / 'preprocessing' / f'{fd}_scaler.pkl').read_bytes()).hexdigest() == info['scaler_sha256']
        reader = WindowedNpzDataset(out / 'engineered' / f'test_{fd}.npz')
        assert list(reader.X.shape) == manifest['tensor_shapes'][fd]['test']
        assert np.isfinite(reader.X.numpy()).all() and np.isfinite(reader.y.numpy()).all()
    for split, key in [('train','train'), ('val','validation')]:
        reader = WindowedNpzDataset(out / 'engineered' / f'{split}.npz')
        assert len(reader.X) == sum(manifest['tensor_shapes'][fd][key][0] for fd in SUB_DATASETS)
        assert tuple(reader.X.shape[1:]) == (30, len(contract.feature_order))
        assert np.isfinite(reader.X.numpy()).all() and np.isfinite(reader.y.numpy()).all()
    assert manifest['historical_checkpoint_compatibility'] == 'not_certified'
    with pytest.raises(FileExistsError):
        generate(source, out)


def test_raw24_and_last_cycle_labels_match_independent_module04_histories(generated):
    source, out = generated
    manifest = json.loads((out / 'manifest.json').read_text())
    for fd in SUB_DATASETS:
        info = json.loads((out / 'preprocessing' / f'{fd}_statistics.json').read_text())
        with (out / 'preprocessing' / f'{fd}_scaler.pkl').open('rb') as stream:
            scaler = pickle.load(stream)
        for filename, source_split, truth in [('train.npz','train',None), ('val.npz','train',None), (f'test_{fd}.npz','test',{1:7.,2:11.})]:
            df = pd.read_csv(source / f'{source_split}_{fd}.txt', sep=r'\s+', names=ALL_COLUMNS)
            transformed = apply_scaler(clip_outliers(handle_missing_values(df, info['medians']), OUTLIER_Z_THRESHOLD, info['clipping_statistics']), scaler)
            with np.load(out / 'engineered' / filename, allow_pickle=False) as data:
                mask = np.ones(len(data['y']), dtype=bool) if truth is not None else data['dataset_ids'] == fd
                for values, target, unit in zip(data['X'][mask], data['y'][mask], data['unit_ids'][mask]):
                    group = transformed[transformed.unit_id == unit].sort_values('time_cycles')
                    expected = group[FEATURE_COLUMNS].to_numpy(dtype=np.float32)
                    if truth is not None:
                        np.testing.assert_array_equal(values[:, :24], expected[-30:])
                        assert target == truth[int(unit)]
                    else:
                        candidates = [expected[i:i+30] for i in range(len(expected)-29)]
                        matches = [i for i, x in enumerate(candidates) if np.array_equal(values[:, :24], x)]
                        assert len(matches) == 1
                        assert target == len(expected) - (matches[0] + 30)
                        assert len(set(group.time_cycles.iloc[matches[0]:matches[0]+30])) == 30
    # All synthetic histories have at least 30 observations; no padding is involved.


def test_short_history_padding_is_not_30_distinct_observed_cycles():
    df = pd.DataFrame({'unit_id':[1,1], 'time_cycles':[1,2], 'rul':[1,0], 'sensor_1':[1.,2.]})
    result = generate_windows(df, feature_columns=['sensor_1'])
    assert result.X.shape == (1,30,1)
    assert np.count_nonzero(result.X[0,:,0] == 1.) == 29
    assert result.X[0,-1,0] == 2.


def test_npz_reader_has_no_feature_order_or_content_integrity_guard(generated, tmp_path):
    _, out = generated
    manifest = json.loads((out / 'manifest.json').read_text())
    assert 'output_sha256' not in manifest
    with np.load(out / 'engineered' / 'test_FD001.npz', allow_pickle=False) as original:
        assert 'feature_order' not in original.files
        values = original['X'].copy()
        values[:, :, [0,-1]] = values[:, :, [-1,0]]
        assert not np.array_equal(values, original['X'])
        corrupted = tmp_path / 'reordered.npz'
        np.savez_compressed(corrupted, X=values, y=original['y'], unit_ids=original['unit_ids'])
    loaded = WindowedNpzDataset(corrupted)
    np.testing.assert_array_equal(loaded.X.numpy(), values)


def test_feature_contract_detects_tampering(generated, tmp_path):
    _, out = generated
    value = json.loads((out / 'feature_contract.json').read_text())
    value['contract']['rolling_window'] += 1
    path = tmp_path / 'tampered.json'; path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='hash'):
        load_feature_contract(path)


def test_module04_leading_gap_backfill_depends_on_future_observation():
    """Characterize protected offline behavior; it is not causal for missing input."""
    frame = pd.DataFrame([{**{c: 1. for c in FEATURE_COLUMNS}, 'unit_id': 1, 'time_cycles': 1},
                          {**{c: 2. for c in FEATURE_COLUMNS}, 'unit_id': 1, 'time_cycles': 2}])
    frame.loc[0, 'sensor_1'] = np.nan
    changed = frame.copy(); changed.loc[1, 'sensor_1'] = 9.
    first = handle_missing_values(frame, {c: 0. for c in FEATURE_COLUMNS})
    second = handle_missing_values(changed, {c: 0. for c in FEATURE_COLUMNS})
    assert first.loc[first.time_cycles == 1, 'sensor_1'].iloc[0] == 2.
    assert second.loc[second.time_cycles == 1, 'sensor_1'].iloc[0] == 9.
