"""Generate new raw C-MAPSS datasets without modifying deployed artifacts.

Run with explicit --input-dir and a new --output-dir.  The latter contains
raw/{train,val,test_FD00X}.npz and per-dataset fitting artifacts.  The
generated artifacts are not certified for the existing LSTM checkpoint.
"""
from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from ml.config import PROCESSED_DIR, SUB_DATASETS, WINDOW_SIZE, RUL_CAP, OUTLIER_Z_THRESHOLD, RANDOM_SEED, VALIDATION_UNIT_FRACTION
from ml.data.cmapss_loader import _read_space_delimited
from ml.data.preprocessing import (FEATURE_COLUMNS, SENSOR_COLUMNS, apply_scaler,
    assign_rul_test, assign_rul_train, clip_outliers, generate_windows,
    handle_missing_values, split_by_unit)

COMBINED_DIR = PROCESSED_DIR / 'combined'


def prepare_dataset(train, test, truth, *, seed=RANDOM_SEED, val_fraction=VALIDATION_UNIT_FRACTION):
    """Split first, fit on training units, and transform all partitions."""
    if train.empty or test.empty:
        raise ValueError('training and test datasets must be non-empty')
    if truth.index.has_duplicates or set(test.unit_id.unique()) != set(truth.index):
        raise ValueError('test RUL ground truth must cover each test unit exactly once')
    if not np.isfinite(pd.to_numeric(truth, errors='coerce')).all() or (truth < 0).any():
        raise ValueError('test RUL ground truth must be finite and nonnegative')
    training, validation = split_by_unit(train, val_fraction=val_fraction, seed=seed)
    numeric = training[FEATURE_COLUMNS].apply(pd.to_numeric, errors='raise').replace([np.inf, -np.inf], np.nan)
    medians = numeric.median()
    if not np.isfinite(medians.to_numpy()).all():
        raise ValueError('each feature requires a finite training median')
    medians = medians.to_dict()
    clean_train = handle_missing_values(training, medians)
    stats = {c: {'mean': float(clean_train[c].mean()), 'std': float(clean_train[c].std()),
                 'median': float(medians[c])} for c in FEATURE_COLUMNS}
    if any(not np.isfinite(v['mean']) or not np.isfinite(v['std']) for v in stats.values()):
        raise ValueError('training data must provide finite clipping statistics')
    clipped_train = clip_outliers(clean_train, OUTLIER_Z_THRESHOLD, stats)
    scaler = MinMaxScaler().fit(clipped_train[FEATURE_COLUMNS])

    def transform(frame):
        return apply_scaler(clip_outliers(handle_missing_values(frame, medians), OUTLIER_Z_THRESHOLD, stats), scaler)

    train_window = generate_windows(assign_rul_train(apply_scaler(clipped_train, scaler)))
    val_window = generate_windows(assign_rul_train(transform(validation)))
    test_window = generate_windows(assign_rul_test(transform(test), truth), last_window_only=True)
    return {'train': train_window, 'val': val_window, 'test': test_window,
            'medians': medians, 'stats': stats, 'scaler': scaler,
            'train_units': sorted(training.unit_id.unique().tolist()),
            'validation_units': sorted(validation.unit_id.unique().tolist())}


def generate(input_dir: Path, output_dir: Path, *, seed=RANDOM_SEED, val_fraction=VALIDATION_UNIT_FRACTION):
    input_dir, output_dir = Path(input_dir), Path(output_dir)
    if output_dir.exists():
        raise FileExistsError(f'output directory already exists: {output_dir}')
    # Check every input before creating any output.
    for fd in SUB_DATASETS:
        for prefix in ('train', 'test', 'RUL'):
            path = input_dir / f'{prefix}_{fd}.txt'
            if not path.is_file():
                raise FileNotFoundError(f'missing C-MAPSS input: {path}')
    output_dir.mkdir(parents=True, exist_ok=False)
    raw = output_dir / 'raw'
    artifacts = output_dir / 'artifacts'
    raw.mkdir()
    artifacts.mkdir()
    combined = {'train': [], 'val': []}
    manifest = {'schema_version': 'raw-24-v1', 'feature_order': FEATURE_COLUMNS,
                'window_size': WINDOW_SIZE, 'rul_cap': RUL_CAP,
                'outlier_z_threshold': OUTLIER_Z_THRESHOLD, 'seed': seed,
                'validation_unit_fraction': val_fraction, 'datasets': {}}
    for fd in SUB_DATASETS:
        train = _read_space_delimited(input_dir / f'train_{fd}.txt')
        test = _read_space_delimited(input_dir / f'test_{fd}.txt')
        truth = pd.read_csv(input_dir / f'RUL_{fd}.txt', sep=r'\s+', header=None)[0]
        truth.index = truth.index + 1
        result = prepare_dataset(train, test, truth, seed=seed, val_fraction=val_fraction)
        for split in ('train', 'val'):
            combined[split].append((fd, result[split]))
        item = result['test']
        np.savez_compressed(raw / f'test_{fd}.npz', X=item.X, y=item.y, unit_ids=item.unit_ids)
        with (artifacts / f'scaler_{fd}.pkl').open('xb') as f:
            pickle.dump(result['scaler'], f)
        manifest['datasets'][fd] = {'train_units': result['train_units'],
            'validation_units': result['validation_units'], 'medians': result['medians'],
            'clipping_statistics': result['stats']}
    for split, chunks in combined.items():
        np.savez_compressed(raw / f'{split}.npz',
            X=np.concatenate([w.X for _, w in chunks]), y=np.concatenate([w.y for _, w in chunks]),
            unit_ids=np.concatenate([w.unit_ids for _, w in chunks]),
            dataset_ids=np.concatenate([np.full(len(w.y), fd) for fd, w in chunks]))
    (output_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2, allow_nan=False), encoding='utf-8')
    return manifest


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
