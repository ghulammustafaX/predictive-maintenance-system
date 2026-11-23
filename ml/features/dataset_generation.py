"""Build a new offline engineered C-MAPSS dataset using one fitted contract.

Run with --input-dir and a previously nonexistent --output-dir. All generated
scalers, contracts and tensors stay in that output directory. A failed run may
leave a partial directory without a manifest; inspect and remove that directory
manually before retrying. No existing model compatibility is implied.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import sklearn

from ml.build_combined_dataset import prepare_dataset
from ml.config import ALL_COLUMNS, ML_ROOT, OUTLIER_Z_THRESHOLD, RANDOM_SEED, RUL_CAP, SUB_DATASETS, VALIDATION_UNIT_FRACTION, WINDOW_SIZE
from ml.data.cmapss_loader import _read_space_delimited
from ml.data.preprocessing import (FEATURE_COLUMNS, apply_scaler, assign_rul_test, assign_rul_train,
    clip_outliers, generate_windows, handle_missing_values)
from ml.features.feature_engineering import fit_feature_contract, save_feature_contract, transform_features, load_feature_contract

GENERATOR_VERSION = 'module05-dataset-v2'


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _file_hash(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _validate_source(frame, dataset, name):
    if frame.empty or frame.columns.duplicated().any():
        raise ValueError(f'{dataset} {name}: empty or duplicate columns')
    for column in ('unit_id', 'time_cycles'):
        values = pd.to_numeric(frame[column], errors='coerce').to_numpy(dtype=float)
        if not np.isfinite(values).all() or (values <= 0).any() or not np.equal(values, np.floor(values)).all():
            raise ValueError(f'{dataset} {name}: invalid {column}')
    for column in ('dataset_id', 'run_id', 'split', 'rul'):
        if column in frame:
            raise ValueError(f'{dataset} {name}: conflicting metadata column {column}')


def _load_source(path, dataset, name):
    line_number = 0
    with path.open('r', encoding='utf-8') as stream:
        for line_number, line in enumerate(stream, 1):
            if len(line.split()) != len(ALL_COLUMNS):
                raise ValueError(f'{dataset} {name}: invalid column count on line {line_number}')
    if not line_number:
        raise ValueError(f'{dataset} {name}: empty dataset')
    frame = _read_space_delimited(path)
    _validate_source(frame, dataset, name)
    return frame


def _protect_destination(input_dir, output_dir):
    resolved = output_dir.resolve()
    if output_dir.exists() or output_dir.is_symlink():
        raise FileExistsError(f'output directory already exists: {output_dir}')
    if resolved.is_relative_to(input_dir.resolve()):
        raise ValueError('output directory must not be inside the input directory')
    if any(part.lower() in {'saved', 'saved_v1_pre_leakage_fix', 'saved_v2_a1'} for part in resolved.parts):
        raise ValueError('output directory must not be inside saved artifact directories')
    if resolved.is_relative_to(ML_ROOT.resolve()):
        raise ValueError('output directory must not be inside the ML source directory')


def generate(input_dir: Path, output_dir: Path, *, seed=RANDOM_SEED, val_fraction=VALIDATION_UNIT_FRACTION):
    input_dir, output_dir = Path(input_dir), Path(output_dir)
    _protect_destination(input_dir, output_dir)
    paths = {}
    for fd in SUB_DATASETS:
        paths[fd] = {}
        for prefix in ('train', 'test', 'RUL'):
            path = input_dir / f'{prefix}_{fd}.txt'
            if not path.is_file():
                raise FileNotFoundError(f'missing C-MAPSS input: {path}')
            paths[fd][prefix] = path

    output_dir.mkdir(parents=True, exist_ok=False)
    (output_dir / 'engineered').mkdir()
    (output_dir / 'preprocessing').mkdir()
    histories, preprocessing, partitions, sources = {}, {}, {}, {}
    for fd in SUB_DATASETS:
        train = _load_source(paths[fd]['train'], fd, 'train')
        test = _load_source(paths[fd]['test'], fd, 'test')
        truth_frame = pd.read_csv(paths[fd]['RUL'], sep=r'\s+', header=None)
        if truth_frame.shape[1] != 1:
            raise ValueError(f'{fd}: RUL file must have exactly one column')
        truth = truth_frame[0]
        truth.index = truth.index + 1
        # Module 04 owns the actual split and all preprocessing parameter fitting.
        fitted = prepare_dataset(train, test, truth, seed=seed, val_fraction=val_fraction)
        train_units, val_units = set(fitted['train_units']), set(fitted['validation_units'])
        if not train_units or not val_units or train_units & val_units or train_units | val_units != set(train.unit_id.unique()):
            raise ValueError(f'{fd}: invalid train/validation split')
        def preprocess(frame, split):
            clean = handle_missing_values(frame, fitted['medians'])
            clipped = clip_outliers(clean, OUTLIER_Z_THRESHOLD, fitted['stats'])
            scaled = apply_scaler(clipped, fitted['scaler'])
            return scaled.assign(dataset_id=fd, split=split)
        train_history = preprocess(train[train.unit_id.isin(train_units)], 'train')
        val_history = preprocess(train[train.unit_id.isin(val_units)], 'validation')
        test_history = preprocess(test, 'test')
        histories[fd] = {'train': assign_rul_train(train_history),
                         'validation': assign_rul_train(val_history),
                         'test': test_history, 'truth': truth}
        scaler_bytes = pickle.dumps(fitted['scaler'], protocol=pickle.HIGHEST_PROTOCOL)
        scaler_path = output_dir / 'preprocessing' / f'{fd}_scaler.pkl'
        with scaler_path.open('xb') as stream:
            stream.write(scaler_bytes)
        settings = {'dataset_id': fd, 'training_units': sorted(train_units),
                    'medians': fitted['medians'], 'clipping_statistics': fitted['stats'],
                    'outlier_z_threshold': OUTLIER_Z_THRESHOLD, 'scaler_sha256': _sha(scaler_bytes)}
        settings_path = output_dir / 'preprocessing' / f'{fd}_statistics.json'
        with settings_path.open('x', encoding='utf-8') as stream:
            stream.write(_json(settings))
        preprocessing[fd] = settings
        partitions[fd] = {'train_units': sorted(train_units), 'validation_units': sorted(val_units),
                          'test_units': sorted(test.unit_id.unique().tolist())}
        sources[fd] = {name: _file_hash(path) for name, path in paths[fd].items()}

    versions = {'python': '.'.join(map(str, sys.version_info[:3])), 'numpy': np.__version__,
                'pandas': pd.__version__, 'scikit_learn': sklearn.__version__}
    preprocessing_manifest = {'schema_version': 'raw-24-v1', 'feature_order': FEATURE_COLUMNS,
        'missing_policy': 'per-unit forward fill, backfill, training median fallback',
        'outlier_z_threshold': OUTLIER_Z_THRESHOLD, 'software_versions': versions, 'datasets': preprocessing}
    preprocessing_bytes = _json(preprocessing_manifest).encode('utf-8')
    preprocessing_identity = _sha(preprocessing_bytes)
    with (output_dir / 'preprocessing' / 'manifest.json').open('xb') as stream:
        stream.write(preprocessing_bytes)
    pooled = pd.concat([histories[fd]['train'] for fd in SUB_DATASETS], ignore_index=True)
    contract = fit_feature_contract(pooled, training_provenance={'split': 'train',
        'source': 'module04-engine-unit-split', 'software_versions': versions,
        'training_units': {fd: partitions[fd]['train_units'] for fd in SUB_DATASETS}},
        preprocessing_artifact_identity=preprocessing_identity, application_dataset_ids=tuple(SUB_DATASETS))
    save_feature_contract(contract, output_dir / 'feature_contract.json')
    order = list(contract.feature_order)
    if len(order) != len(set(order)) or 'rul' in order:
        raise ValueError('invalid engineered feature order')
    combined = {'train': [], 'val': []}
    output_summary = {}
    padding = {}
    for fd in SUB_DATASETS:
        output_summary[fd] = {}
        for split in ('train', 'validation', 'test'):
            source = histories[fd][split]
            if split == 'test':
                padding[fd] = [{'unit_id': int(unit), 'observed_cycles': int(len(group)),
                    'padding_count': max(0, WINDOW_SIZE - len(group)),
                    'final_cycle': int(group.time_cycles.max()), 'policy': 'repeat_first_observation'}
                    for unit, group in source.groupby('unit_id', sort=True)]
            # Test RUL ground truth is attached only after feature transformation.
            features = transform_features(source.drop(columns='rul', errors='ignore'), contract,
                                          preprocessing_artifact_identity=preprocessing_identity)
            labels = (assign_rul_test(source, histories[fd]['truth']) if split == 'test' else source)
            label_map = labels[['dataset_id', 'unit_id', 'time_cycles', 'rul']]
            features = features.merge(label_map, on=['dataset_id', 'unit_id', 'time_cycles'], validate='one_to_one')
            window = generate_windows(features, feature_columns=order, last_window_only=(split == 'test'))
            if window.X.shape[1:] != (WINDOW_SIZE, len(order)) or not np.isfinite(window.X).all() or not np.isfinite(window.y).all():
                raise ValueError(f'{fd} {split}: invalid engineered tensor')
            output_summary[fd][split] = list(window.X.shape)
            if split == 'test':
                np.savez_compressed(output_dir / 'engineered' / f'test_{fd}.npz', X=window.X, y=window.y, unit_ids=window.unit_ids)
            else:
                combined['train' if split == 'train' else 'val'].append((fd, window))
    for split, chunks in combined.items():
        np.savez_compressed(output_dir / 'engineered' / f'{split}.npz',
            X=np.concatenate([item.X for _, item in chunks]), y=np.concatenate([item.y for _, item in chunks]),
            unit_ids=np.concatenate([item.unit_ids for _, item in chunks]),
            dataset_ids=np.concatenate([np.full(len(item.y), fd) for fd, item in chunks]))
    manifest = {'schema_version': contract.schema_version, 'contract_sha256': contract.contract_hash,
        'generator_version': GENERATOR_VERSION, 'test_history_provenance': padding,
        'preprocessing_identity_sha256': preprocessing_identity, 'feature_order': order,
        'window_size': WINDOW_SIZE, 'rul_cap': RUL_CAP, 'seed': seed,
        'validation_unit_fraction': val_fraction, 'software_versions': versions, 'partitions': partitions,
        'source_sha256': sources, 'tensor_shapes': output_summary,
        'historical_checkpoint_compatibility': 'not_certified'}
    manifest['outputs'] = {}
    for name, split, datasets in [('train.npz', 'train', list(SUB_DATASETS)),
            ('val.npz', 'validation', list(SUB_DATASETS))] + [
            (f'test_{fd}.npz', 'test', [fd]) for fd in SUB_DATASETS]:
        path = output_dir / 'engineered' / name
        with np.load(path, allow_pickle=False) as data:
            manifest['outputs'][name] = {'sha256': _file_hash(path), 'tensor_shape': list(data['X'].shape),
                'tensor_dtype': str(data['X'].dtype), 'label_shape': list(data['y'].shape),
                'label_dtype': str(data['y'].dtype), 'feature_order_reference': 'manifest.feature_order',
                'contract_sha256': contract.contract_hash, 'preprocessing_identity_sha256': preprocessing_identity,
                'generator_version': GENERATOR_VERSION, 'split': split, 'datasets': datasets,
                'window_count': len(data['y']), 'unit_counts': {fd: len(partitions[fd][f'{split}_units']) for fd in datasets}}
    manifest['manifest_sha256'] = _sha(_json(manifest).encode('utf-8'))
    with (output_dir / 'manifest.json').open('x', encoding='utf-8') as stream:
        stream.write(json.dumps(manifest, indent=2, allow_nan=False))
    return manifest


def verify_generated_dataset(output_dir: Path):
    """Validate a generated bundle against its manifest; hashes are not signatures."""
    root = Path(output_dir)
    try:
        manifest = json.loads((root / 'manifest.json').read_text(encoding='utf-8'))
        digest = manifest.pop('manifest_sha256')
        if digest != _sha(_json(manifest).encode('utf-8')):
            raise ValueError('manifest hash mismatch')
        required = {'schema_version', 'contract_sha256', 'generator_version', 'test_history_provenance',
                    'preprocessing_identity_sha256', 'feature_order', 'window_size', 'rul_cap', 'seed',
                    'validation_unit_fraction', 'software_versions', 'partitions', 'source_sha256',
                    'tensor_shapes', 'historical_checkpoint_compatibility', 'outputs'}
        if (set(manifest) != required or not isinstance(manifest['seed'], int)
                or isinstance(manifest['seed'], bool) or manifest['rul_cap'] != RUL_CAP
                or not 0 < manifest['validation_unit_fraction'] < 1
                or manifest['historical_checkpoint_compatibility'] != 'not_certified'
                or not isinstance(manifest['software_versions'], dict)
                or set(manifest['software_versions']) != {'python', 'numpy', 'pandas', 'scikit_learn'}
                or not all(isinstance(v, str) and v for v in manifest['software_versions'].values())
                or set(manifest['tensor_shapes']) != set(SUB_DATASETS)):
            raise ValueError('missing or invalid manifest provenance')
        contract = load_feature_contract(root / 'feature_contract.json')
        identity = manifest['preprocessing_identity_sha256']
        if (manifest['generator_version'] != GENERATOR_VERSION or manifest['schema_version'] != contract.schema_version
                or manifest['contract_sha256'] != contract.contract_hash
                or manifest['feature_order'] != list(contract.feature_order)
                or contract.payload['preprocessing_artifact_identity'] != identity
                or _file_hash(root / 'preprocessing' / 'manifest.json') != identity
                or manifest['window_size'] != WINDOW_SIZE):
            raise ValueError('contract or preprocessing identity mismatch')
        expected = {'train.npz': ('train', list(SUB_DATASETS)), 'val.npz': ('validation', list(SUB_DATASETS))}
        expected.update({f'test_{fd}.npz': ('test', [fd]) for fd in SUB_DATASETS})
        if set(manifest['outputs']) != set(expected) or set(manifest['partitions']) != set(SUB_DATASETS):
            raise ValueError('missing output or partition provenance')
        if set(manifest['source_sha256']) != set(SUB_DATASETS) or set(manifest['test_history_provenance']) != set(SUB_DATASETS):
            raise ValueError('missing source or history provenance')
        upstream = json.loads((root / 'preprocessing' / 'manifest.json').read_text(encoding='utf-8'))
        for fd in SUB_DATASETS:
            part = manifest['partitions'][fd]
            hashes = manifest['source_sha256'][fd]
            if (set(hashes) != {'train', 'test', 'RUL'} or any(not isinstance(h, str) or len(h) != 64
                    or any(c not in '0123456789abcdef' for c in h) for h in hashes.values())
                    or list(contract.training_provenance['training_units'][fd]) != part['train_units']
                    or upstream['datasets'][fd]['training_units'] != part['train_units']
                    or _file_hash(root / 'preprocessing' / f'{fd}_scaler.pkl') != upstream['datasets'][fd]['scaler_sha256']
                    or json.loads((root / 'preprocessing' / f'{fd}_statistics.json').read_text(encoding='utf-8')) != upstream['datasets'][fd]):
                raise ValueError('invalid source or preprocessing provenance')
            if not all(isinstance(part[f'{s}_units'], list) and part[f'{s}_units']
                       and len(set(part[f'{s}_units'])) == len(part[f'{s}_units']) for s in ('train', 'validation', 'test')):
                raise ValueError('invalid partition provenance')
            if set(part['train_units']) & set(part['validation_units']):
                raise ValueError('overlapping partitions')
        for name, (split, datasets) in expected.items():
            meta = manifest['outputs'][name]
            if (meta['split'] != split or meta['datasets'] != datasets or meta['generator_version'] != GENERATOR_VERSION
                    or meta['contract_sha256'] != contract.contract_hash or meta['preprocessing_identity_sha256'] != identity
                    or meta['feature_order_reference'] != 'manifest.feature_order'
                    or meta['unit_counts'] != {fd: len(manifest['partitions'][fd][f'{split}_units']) for fd in datasets}):
                raise ValueError(f'{name}: invalid output provenance')
            path = root / 'engineered' / name
            if _file_hash(path) != meta['sha256']:
                raise ValueError(f'{name}: file hash mismatch')
            with np.load(path, allow_pickle=False) as data:
                x, y, units = data['X'], data['y'], data['unit_ids']
                if (list(x.shape) != meta['tensor_shape'] or list(y.shape) != meta['label_shape']
                        or str(x.dtype) != meta['tensor_dtype'] or str(y.dtype) != meta['label_dtype']
                        or x.dtype != np.float32 or y.dtype != np.float32
                        or x.ndim != 3 or x.shape[1:] != (WINDOW_SIZE, len(contract.feature_order))
                        or y.shape != (len(x),) or units.shape != (len(x),) or len(x) != meta['window_count']
                        or not np.isfinite(x).all() or not np.isfinite(y).all()):
                    raise ValueError(f'{name}: invalid tensors')
                ids = data['dataset_ids'] if split != 'test' else np.full(len(y), datasets[0])
                if ids.shape != (len(y),) or set(ids.tolist()) != set(datasets):
                    raise ValueError(f'{name}: invalid dataset identities')
                for fd in datasets:
                    actual = set(units[ids == fd].tolist())
                    expected_shape = [int(np.count_nonzero(ids == fd)), WINDOW_SIZE, len(contract.feature_order)]
                    if manifest['tensor_shapes'][fd][split] != expected_shape:
                        raise ValueError(f'{name}: dataset tensor summary mismatch')
                    if actual != set(manifest['partitions'][fd][f'{split}_units']):
                        raise ValueError(f'{name}: unit identities mismatch')
                    if split == 'test':
                        provenance = manifest['test_history_provenance'][fd]
                        if (len(units) != len(actual) or len(provenance) != len(actual) or {item['unit_id'] for item in provenance} != actual
                                or any(item['observed_cycles'] < 1 or item['padding_count'] != max(0, WINDOW_SIZE - item['observed_cycles'])
                                       or item['policy'] != 'repeat_first_observation' or item['final_cycle'] < 1 for item in provenance)):
                            raise ValueError(f'{name}: invalid padding provenance')
        manifest['manifest_sha256'] = digest
        return manifest
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f'invalid generated dataset: {exc}') from exc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--seed', type=int, default=RANDOM_SEED)
    parser.add_argument('--validation-fraction', type=float, default=VALIDATION_UNIT_FRACTION)
    args = parser.parse_args()
    generate(args.input_dir, args.output_dir, seed=args.seed, val_fraction=args.validation_fraction)


if __name__ == '__main__':
    main()
