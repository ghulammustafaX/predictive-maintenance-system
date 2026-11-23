"""Integrity and short-history checks using generated temporary data only."""
import hashlib
import json
import pickle
import shutil

import numpy as np
import pytest

from ml.features.dataset_generation import generate, verify_generated_dataset
from module05_dataset_checks import create_inputs, make_frame


@pytest.fixture(scope='module')
def bundle(tmp_path_factory):
    root = tmp_path_factory.mktemp('integrity-source')
    source = root / 'source'
    create_inputs(source)
    # Four isolated test units exercise both padding and the exact boundary.
    from ml.config import ALL_COLUMNS, SUB_DATASETS
    import pandas as pd
    frame = pd.concat([make_frame([unit], cycles=n) for unit, n in enumerate((1, 12, 29, 30), 1)])
    for fd in SUB_DATASETS:
        frame.to_csv(source / f'test_{fd}.txt', sep=' ', header=False, index=False, columns=ALL_COLUMNS)
        (source / f'RUL_{fd}.txt').write_text('7\n11\n13\n17\n')
    output = root / 'output'
    generate(source, output)
    return output


@pytest.fixture
def copy_bundle(bundle, tmp_path):
    target = tmp_path / 'bundle'
    shutil.copytree(bundle, target)
    return target


def rewrite_manifest(root, edit):
    path = root / 'manifest.json'
    value = json.loads(path.read_text())
    edit(value)
    value.pop('manifest_sha256')
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
    value['manifest_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
    path.write_text(json.dumps(value))


def rewrite_npz(root, change):
    path = root / 'engineered' / 'test_FD001.npz'
    with np.load(path, allow_pickle=False) as data:
        arrays = {key: data[key].copy() for key in data.files}
    change(arrays)
    np.savez_compressed(path, **arrays)


def test_valid_bundle_and_padding(bundle):
    import pandas as pd
    from ml.config import ALL_COLUMNS, OUTLIER_Z_THRESHOLD
    from ml.data.preprocessing import FEATURE_COLUMNS, handle_missing_values, clip_outliers, apply_scaler
    manifest = verify_generated_dataset(bundle)
    for fd, provenance in manifest['test_history_provenance'].items():
        settings = json.loads((bundle / 'preprocessing' / f'{fd}_statistics.json').read_text())
        with (bundle / 'preprocessing' / f'{fd}_scaler.pkl').open('rb') as stream:
            scaler = pickle.load(stream)
        original = pd.read_csv(bundle.parent / 'source' / f'test_{fd}.txt', sep=r'\s+', names=ALL_COLUMNS)
        raw = apply_scaler(clip_outliers(handle_missing_values(original, settings['medians']),
                           OUTLIER_Z_THRESHOLD, settings['clipping_statistics']), scaler)
        assert [p['observed_cycles'] for p in provenance] == [1, 12, 29, 30]
        assert [p['padding_count'] for p in provenance] == [29, 18, 1, 0]
        with np.load(bundle / 'engineered' / f'test_{fd}.npz', allow_pickle=False) as data:
            assert data['X'].shape[1] == 30
            assert data['y'].tolist() == [7, 11, 13, 17]
            assert data['unit_ids'].tolist() == [1, 2, 3, 4]
            for index, item in enumerate(provenance):
                assert item['final_cycle'] == item['observed_cycles']
                assert np.all(data['X'][index, :item['padding_count'] + 1] == data['X'][index, item['padding_count']])
                expected = raw[raw.unit_id == item['unit_id']][FEATURE_COLUMNS].to_numpy(dtype=np.float32)
                np.testing.assert_array_equal(data['X'][index, item['padding_count']:, :24], expected[-30:])


@pytest.mark.parametrize('tamper', ['value', 'swap', 'substitute', 'missing', 'truncated'])
def test_file_tampering_rejected(copy_bundle, tamper):
    root = copy_bundle
    verify_generated_dataset(root)
    path = root / 'engineered' / 'test_FD001.npz'
    if tamper == 'value':
        rewrite_npz(root, lambda a: a['X'].__setitem__((0, 0, 0), a['X'][0, 0, 0] + 1))
    elif tamper == 'swap':
        def swap(a):
            assert not np.array_equal(a['X'][:, :, 0], a['X'][:, :, -1])
            a['X'][:, :, [0, -1]] = a['X'][:, :, [-1, 0]]
        rewrite_npz(root, swap)
    elif tamper == 'substitute':
        shutil.copyfile(root / 'engineered' / 'test_FD002.npz', path)
        # Identical source tensors are possible; make a different valid-shaped NPZ instead.
        rewrite_npz(root, lambda a: a['y'].__setitem__(0, a['y'][0] + 1))
    elif tamper == 'missing':
        path.unlink()
    else:
        path.write_bytes(path.read_bytes()[:50])
    with pytest.raises(ValueError):
        verify_generated_dataset(root)


@pytest.mark.parametrize('case', ['missing_manifest', 'altered_manifest', 'contract_hash', 'shape', 'dtype', 'split', 'dataset', 'order', 'contract_order', 'missing_provenance', 'missing_sources', 'missing_seed', 'dataset_shape'])
def test_metadata_tampering_rejected(copy_bundle, case):
    root = copy_bundle
    verify_generated_dataset(root)
    if case == 'missing_manifest':
        (root / 'manifest.json').unlink()
    elif case == 'altered_manifest':
        path = root / 'manifest.json'
        value = json.loads(path.read_text()); value['seed'] += 1
        path.write_text(json.dumps(value))
    elif case == 'contract_order':
        path = root / 'feature_contract.json'
        value = json.loads(path.read_text())
        order = value['contract']['feature_order']
        order[0], order[1] = order[1], order[0]
        path.write_text(json.dumps(value))
    else:
        def edit(m):
            output = m['outputs']['test_FD001.npz']
            if case == 'contract_hash': m['contract_sha256'] = '0' * 64
            elif case == 'shape': output['tensor_shape'][1] = 29
            elif case == 'dtype': output['tensor_dtype'] = 'float64'
            elif case == 'split': output['split'] = 'train'
            elif case == 'dataset': output['datasets'] = ['FD002']
            elif case == 'order': m['feature_order'][0], m['feature_order'][1] = m['feature_order'][1], m['feature_order'][0]
            elif case == 'missing_provenance': del m['partitions']['FD001']
            elif case == 'missing_sources': del m['source_sha256']
            elif case == 'missing_seed': del m['seed']
            elif case == 'dataset_shape': m['tensor_shapes']['FD001']['test'][0] += 1
        rewrite_manifest(root, edit)
    with pytest.raises(ValueError):
        verify_generated_dataset(root)


@pytest.mark.parametrize('case', ['shape', 'dtype'])
def test_actual_tensor_shape_or_dtype_rejected_even_with_rehashed_metadata(copy_bundle, case):
    root = copy_bundle
    verify_generated_dataset(root)
    def change(arrays):
        arrays['X'] = arrays['X'][:, :-1, :] if case == 'shape' else arrays['X'].astype('float64')
    rewrite_npz(root, change)
    path = root / 'engineered' / 'test_FD001.npz'
    def edit(manifest):
        meta = manifest['outputs']['test_FD001.npz']
        meta['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        if case == 'shape': meta['tensor_shape'][1] = 29
        else: meta['tensor_dtype'] = 'float64'
    rewrite_manifest(root, edit)
    with pytest.raises(ValueError, match='invalid tensors'):
        verify_generated_dataset(root)
