"""Training-fitted Module 05 feature contracts and causal feature transforms.

Fit only on an explicitly identified training partition. Zero/undefined sensor
correlations with training RUL are excluded from the health index. All feature
operations require finite sensor values and consecutive cycles per identity.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.feature_selection import mutual_info_regression

from ml.config import FEATURE_STORE_DIR, SENSOR_COLUMNS, SETTING_COLUMNS

ROLLING_WINDOW = 5
SCHEMA_VERSION = 'pms-engineered-v1'
CONTRACT_VERSION = 1
LOW_VARIANCE_THRESHOLD = 1e-6


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def _digest(payload):
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _candidate_names():
    return ([f'{c}_roll_{stat}' for stat in ('mean', 'std', 'min', 'max') for c in SENSOR_COLUMNS]
            + [f'{c}_delta' for c in SENSOR_COLUMNS] + ['health_index'])


def _freeze(value):
    if isinstance(value, dict):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    return value


@dataclass(frozen=True)
class FeatureContract:
    """Immutable canonical JSON plus SHA-256; properties never expose mutable state."""
    _payload_json: str
    contract_hash: str

    @property
    def payload(self):
        return _freeze(json.loads(self._payload_json))

    @property
    def feature_order(self):
        return tuple(json.loads(self._payload_json)['feature_order'])

    @property
    def health_signs(self):
        return MappingProxyType(json.loads(self._payload_json)['health_signs'])

    @property
    def training_provenance(self):
        return _freeze(json.loads(self._payload_json)['training_provenance'])

    @property
    def schema_version(self):
        return json.loads(self._payload_json)['schema_version']


def _validate_contract(contract):
    if not isinstance(contract, FeatureContract):
        raise ValueError('a fitted feature contract is required')
    try:
        data = json.loads(contract._payload_json)
        if _canonical(data) != contract._payload_json or _digest(contract._payload_json) != contract.contract_hash:
            raise ValueError('feature contract hash mismatch')
        required = {'schema_version', 'contract_version', 'feature_order', 'input_features', 'rolling_window',
                    'health_signs', 'low_variance_threshold', 'correlation_threshold', 'correlation_selected',
                    'mi_top_k', 'mi_sample_size', 'mi_seed', 'mi_n_neighbors', 'training_provenance', 'dataset_ids', 'application_dataset_ids',
                    'preprocessing_artifact_identity'}
        if (not isinstance(data, dict) or set(data) != required
                or not isinstance(data.get('training_provenance'), dict)
                or not isinstance(data.get('health_signs'), dict)
                or any(not isinstance(data.get(k), list) for k in ('feature_order', 'input_features', 'correlation_selected', 'dataset_ids', 'application_dataset_ids'))
                or not isinstance(data.get('preprocessing_artifact_identity'), str)
                or data['schema_version'] != SCHEMA_VERSION or data['contract_version'] != CONTRACT_VERSION
                or not data['feature_order'] or len(set(data['feature_order'])) != len(data['feature_order'])
                or not data['input_features'] or not data['health_signs'] or not data['dataset_ids'] or not data['application_dataset_ids']
                or not data['preprocessing_artifact_identity'] or data['training_provenance'].get('split') != 'train'
                or not isinstance(data['training_provenance'].get('source'), str) or not data['training_provenance']['source'].strip()):
            raise ValueError('invalid or incompatible feature contract schema')
        if (data['input_features'] not in (SENSOR_COLUMNS, SETTING_COLUMNS + SENSOR_COLUMNS)
                or data['feature_order'][:len(data['input_features'])] != data['input_features']
                or not data['feature_order'][len(data['input_features']):]
                or len(data['correlation_selected']) != len(set(data['correlation_selected']))
                or not set(data['correlation_selected']).issubset(_candidate_names())
                or not set(data['feature_order'][len(data['input_features']):]).issubset(data['correlation_selected'])
                or not set(data['dataset_ids']).issubset(data['application_dataset_ids'])
                or len(set(data['dataset_ids'])) != len(data['dataset_ids'])
                or len(set(data['application_dataset_ids'])) != len(data['application_dataset_ids'])
                or any(not isinstance(x, str) or not x.strip() for x in data['application_dataset_ids'])
                or any(k not in SENSOR_COLUMNS or isinstance(v, bool) or v not in (-1.0, 1.0) for k, v in data['health_signs'].items())
                or not isinstance(data['rolling_window'], int) or isinstance(data['rolling_window'], bool) or data['rolling_window'] < 1
                or not isinstance(data['mi_top_k'], int) or data['mi_top_k'] < 1
                or not isinstance(data['mi_sample_size'], int) or data['mi_sample_size'] < 4
                or data['mi_n_neighbors'] != 3
                or not isinstance(data['mi_seed'], int)
                or not np.isfinite(data['low_variance_threshold']) or data['low_variance_threshold'] < 0
                or not np.isfinite(data['correlation_threshold']) or not 0 <= data['correlation_threshold'] <= 1):
            raise ValueError('invalid or incompatible feature contract parameters')
        return data
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError('invalid or incompatible feature contract') from exc


def _finite(frame, columns):
    try:
        values = frame.loc[:, columns].to_numpy(dtype=float)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError('missing or invalid numeric feature values') from exc
    if not np.isfinite(values).all():
        raise ValueError('feature values must be finite; missing or invalid values found')


def _ordered(df):
    if df.empty:
        raise ValueError('feature input must not be empty')
    if not df.columns.is_unique:
        raise ValueError('feature input columns must be unique')
    identities = [c for c in ('dataset_id', 'unit_id', 'run_id') if c in df.columns]
    if 'unit_id' not in identities or 'time_cycles' not in df:
        raise ValueError('unit_id and time_cycles are required')
    for name in identities:
        if df[name].isna().any() or df[name].map(lambda x: (isinstance(x, str) and not x.strip()) or (isinstance(x, (int, float, np.number)) and not np.isfinite(x))).any():
            raise ValueError(f'invalid {name} identity')
    _finite(df, SENSOR_COLUMNS)
    ordered = df.sort_values(identities + ['time_cycles']).copy()
    # A fresh positional index prevents accidental alignment across duplicate input indices.
    ordered = ordered.reset_index(drop=True)
    for _, group in ordered.groupby(identities, sort=False, dropna=False):
        cycles = pd.to_numeric(group['time_cycles'], errors='coerce').to_numpy(dtype=float)
        if not np.isfinite(cycles).all() or (cycles <= 0).any() or not np.equal(cycles, np.floor(cycles)).all() or not np.equal(np.diff(cycles), 1).all():
            raise ValueError('cycles must be positive consecutive integers without gaps or duplicates')
    return ordered, identities


def add_rolling_statistics(df: pd.DataFrame, window: int = ROLLING_WINDOW) -> pd.DataFrame:
    if not isinstance(window, int) or isinstance(window, bool) or window < 1:
        raise ValueError('rolling window must be a positive integer')
    df, identities = _ordered(df)
    grouped = df.groupby(identities, sort=False)[SENSOR_COLUMNS]
    columns = []
    for stat in ('mean', 'std', 'min', 'max'):
        rolled = grouped.transform(lambda s: s.rolling(window, min_periods=1).agg(stat))
        rolled.columns = [f'{c}_roll_{stat}' for c in SENSOR_COLUMNS]
        if stat == 'std':
            rolled = rolled.fillna(0.0)
        columns.append(rolled)
    result = pd.concat([df, *columns], axis=1)
    _finite(result, [c for frame in columns for c in frame.columns])
    return result


def add_trend_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df, identities = _ordered(df)
    deltas = df.groupby(identities, sort=False)[SENSOR_COLUMNS].diff().fillna(0.0)
    deltas.columns = [f'{c}_delta' for c in SENSOR_COLUMNS]
    _finite(deltas, list(deltas.columns))
    return pd.concat([df, deltas], axis=1)


def find_correlated_columns(df: pd.DataFrame, columns: list[str], threshold: float = 0.95, *, variance_threshold: float = LOW_VARIANCE_THRESHOLD) -> list[str]:
    if not columns or len(columns) != len(set(columns)):
        raise ValueError('candidate columns must be nonempty and unique')
    if not np.isfinite(threshold) or not 0 <= threshold <= 1 or not np.isfinite(variance_threshold) or variance_threshold < 0:
        raise ValueError('invalid correlation or variance threshold')
    _finite(df, columns)
    sub = df[columns]
    variances = sub.var()
    near = [c for c in columns if not np.isfinite(variances[c]) or variances[c] <= variance_threshold]
    corr = sub.drop(columns=near).corr().abs()
    if not np.isfinite(corr.to_numpy()).all():
        raise ValueError('undefined correlation among candidate features')
    dropped = set(near)
    for col in corr.columns:
        if col in dropped:
            continue
        for other in corr.columns:
            if other != col and other not in dropped and corr.loc[col, other] > threshold:
                dropped.add(other)
    return [c for c in columns if c in dropped]


def select_top_features(df: pd.DataFrame, candidate_columns: list[str], target_column: str = 'rul', top_k: int = 30, sample_size: int = 5000, *, seed: int = 42) -> list[str]:
    if (not candidate_columns or len(set(candidate_columns)) != len(candidate_columns)
            or target_column in candidate_columns or top_k < 1 or sample_size < 4):
        raise ValueError('selection requires nonempty unique candidates and valid selection settings')
    _finite(df, candidate_columns + [target_column])
    sample = df if len(df) <= sample_size else df.sample(sample_size, random_state=seed)
    if len(sample) < 4:
        raise ValueError('mutual information fitting requires at least four training rows')
    scores = mutual_info_regression(sample[candidate_columns], sample[target_column], n_neighbors=3, random_state=seed)
    ranked = sorted(zip(candidate_columns, scores), key=lambda pair: -pair[1])
    return [name for name, _ in ranked[:top_k]]


def add_health_index(df: pd.DataFrame, contract: FeatureContract | None = None) -> pd.DataFrame:
    """Apply training-fitted directions; never inspect labels or refit."""
    data = _validate_contract(contract)
    _finite(df, list(data['health_signs']))
    result = df.copy()
    signs = pd.Series({c: data['health_signs'][c] for c in SENSOR_COLUMNS if c in data['health_signs']})
    result['health_index'] = result[list(signs.index)].mul(signs, axis=1).mean(axis=1)
    return result


def _engineered(df, signs, window):
    result = add_trend_indicators(add_rolling_statistics(df, window))
    ordered_signs = {c: signs[c] for c in SENSOR_COLUMNS if c in signs}
    result['health_index'] = result[list(ordered_signs)].mul(pd.Series(ordered_signs), axis=1).mean(axis=1)
    return result


def fit_feature_contract(train: pd.DataFrame, *, training_provenance: dict, preprocessing_artifact_identity: str,
                         rolling_window: int = ROLLING_WINDOW, low_variance_threshold: float = LOW_VARIANCE_THRESHOLD,
                         correlation_threshold: float = 0.95, mi_top_k: int = 30, mi_sample_size: int = 5000,
                         mi_seed: int = 42, application_dataset_ids: tuple[str, ...] | None = None) -> FeatureContract:
    """Fit on caller-supplied training partitions; caller attests provenance."""
    if (not isinstance(training_provenance, dict) or training_provenance.get('split') != 'train'
            or not isinstance(training_provenance.get('source'), str) or not training_provenance['source'].strip()):
        raise ValueError('training provenance must identify split=train and a source')
    if not isinstance(preprocessing_artifact_identity, str) or not preprocessing_artifact_identity.strip():
        raise ValueError('upstream preprocessing artifact identity is required')
    if 'split' not in train or not train['split'].eq('train').all():
        raise ValueError('training rows require an explicit split=train column')
    if 'dataset_id' not in train:
        raise ValueError('training rows require dataset_id provenance')
    ordered, _ = _ordered(train)
    fitted_datasets = sorted(ordered['dataset_id'].unique().tolist())
    application_datasets = sorted(set(application_dataset_ids if application_dataset_ids is not None else fitted_datasets))
    if not application_datasets or not set(fitted_datasets).issubset(application_datasets) or any(not isinstance(x, str) or not x.strip() for x in application_datasets):
        raise ValueError('application dataset identities must include all training datasets')
    _finite(ordered, ['rul'])
    if not np.isfinite(low_variance_threshold) or low_variance_threshold < 0:
        raise ValueError('invalid low variance threshold')
    signs = {}
    for sensor in SENSOR_COLUMNS:
        if ordered[sensor].var() > low_variance_threshold:
            correlation = ordered[sensor].corr(ordered['rul'])
            if np.isfinite(correlation) and correlation != 0:
                signs[sensor] = 1.0 if correlation > 0 else -1.0
    if not signs:
        raise ValueError('no eligible sensors with defined nonzero RUL correlation')
    frame = _engineered(ordered, signs, rolling_window)
    candidates = _candidate_names()
    dropped = set(find_correlated_columns(frame, candidates, correlation_threshold, variance_threshold=low_variance_threshold))
    selected = [c for c in candidates if c not in dropped]
    if not selected:
        raise ValueError('feature selection produced an empty feature set')
    ranked = select_top_features(frame, selected, top_k=mi_top_k, sample_size=mi_sample_size, seed=mi_seed)
    inputs = ([c for c in SETTING_COLUMNS if c in ordered.columns] + list(SENSOR_COLUMNS))
    if any(c in ordered for c in SETTING_COLUMNS) and not all(c in ordered for c in SETTING_COLUMNS):
        raise ValueError('operational setting columns must be present together')
    _finite(ordered, inputs)
    payload = {'schema_version': SCHEMA_VERSION, 'contract_version': CONTRACT_VERSION,
               'feature_order': inputs + ranked, 'input_features': inputs, 'rolling_window': rolling_window,
               'health_signs': signs, 'low_variance_threshold': low_variance_threshold,
               'correlation_threshold': correlation_threshold, 'correlation_selected': selected,
               'mi_top_k': mi_top_k, 'mi_sample_size': mi_sample_size, 'mi_seed': mi_seed, 'mi_n_neighbors': 3,
               'training_provenance': training_provenance, 'dataset_ids': fitted_datasets,
               'application_dataset_ids': application_datasets,
               'preprocessing_artifact_identity': preprocessing_artifact_identity}
    raw = _canonical(payload)
    contract = FeatureContract(raw, _digest(raw))
    _validate_contract(contract)
    return contract


def transform_features(df: pd.DataFrame, contract: FeatureContract, *, preprocessing_artifact_identity: str | None = None) -> pd.DataFrame:
    """Apply frozen parameters after verifying caller-supplied upstream identity."""
    data = _validate_contract(contract)
    if preprocessing_artifact_identity != data['preprocessing_artifact_identity']:
        raise ValueError('preprocessing artifact identity does not match feature contract')
    if 'dataset_id' in df and not set(df['dataset_id'].unique()).issubset(data['application_dataset_ids']):
        raise ValueError('dataset is not covered by the fitted feature contract')
    _finite(df, data['input_features'])
    result = _engineered(df, data['health_signs'], data['rolling_window'])
    _finite(result, data['feature_order'])
    return result


def save_feature_contract(contract: FeatureContract, path: str | Path) -> None:
    _validate_contract(contract)
    with Path(path).open('x', encoding='utf-8') as stream:
        stream.write(_canonical({'contract': json.loads(contract._payload_json), 'sha256': contract.contract_hash}))


def load_feature_contract(path: str | Path) -> FeatureContract:
    try:
        envelope = json.loads(Path(path).read_text(encoding='utf-8'))
        if set(envelope) != {'contract', 'sha256'}:
            raise ValueError('invalid feature contract envelope')
        raw = _canonical(envelope['contract'])
        contract = FeatureContract(raw, envelope['sha256'])
        _validate_contract(contract)
        return contract
    except (OSError, TypeError, KeyError, json.JSONDecodeError) as exc:
        raise ValueError('missing or invalid feature contract') from exc


def _store_path(sub_dataset, split, config):
    if not all(isinstance(s, str) and s and s.replace('_', '').replace('-', '').isalnum() for s in (sub_dataset, split, config)):
        raise ValueError('invalid feature-store identity')
    if split not in {'train', 'val', 'validation', 'test', 'inference'}:
        raise ValueError('invalid feature-store split')
    return FEATURE_STORE_DIR / sub_dataset / f'{split}_{config}.parquet'


def _feature_data_hash(df: pd.DataFrame) -> str:
    """Hash ordered schema, shape and canonical float64 values for integrity."""
    digest = hashlib.sha256(_canonical({'columns': list(df.columns), 'shape': list(df.shape)}).encode('utf-8'))
    digest.update(np.ascontiguousarray(df.to_numpy(dtype='<f8')).tobytes())
    return digest.hexdigest()


def save_feature_set(df: pd.DataFrame, sub_dataset: str, split: str, config: str, *, contract: FeatureContract) -> None:
    data = _validate_contract(contract)
    if sub_dataset not in data['application_dataset_ids']:
        raise ValueError('feature-store dataset not covered by contract')
    if list(df.columns) != data['feature_order']:
        raise ValueError('feature order does not match fitted contract')
    _finite(df, data['feature_order'])
    path = _store_path(sub_dataset, split, config)
    path.parent.mkdir(parents=True, exist_ok=True)
    metadata = {'dataset_id': sub_dataset, 'split': split, 'config': config,
                'contract': data, 'contract_sha256': contract.contract_hash,
                'data_sha256': _feature_data_hash(df)}
    table = pa.Table.from_pandas(df, preserve_index=False)
    table = table.replace_schema_metadata({**(table.schema.metadata or {}), b'pms_feature_store': _canonical(metadata).encode('utf-8')})
    with path.open('xb') as stream:
        pq.write_table(table, stream)


def load_feature_set(sub_dataset: str, split: str, config: str, *, contract: FeatureContract) -> pd.DataFrame:
    data = _validate_contract(contract)
    try:
        table = pq.read_table(_store_path(sub_dataset, split, config))
        metadata = json.loads((table.schema.metadata or {})[b'pms_feature_store'])
        if not isinstance(metadata, dict):
            raise ValueError('invalid feature-store metadata')
        if (metadata != {'dataset_id': sub_dataset, 'split': split, 'config': config, 'contract': data,
                         'contract_sha256': contract.contract_hash, 'data_sha256': metadata.get('data_sha256')}
                or table.column_names != data['feature_order']):
            raise ValueError('feature-store provenance or feature order mismatch')
        frame = table.to_pandas()
        _finite(frame, data['feature_order'])
        if metadata['data_sha256'] != _feature_data_hash(frame):
            raise ValueError('feature-store data integrity hash mismatch')
        return frame
    except (OSError, KeyError, json.JSONDecodeError) as exc:
        raise ValueError('missing or invalid feature-store data or metadata') from exc


def engineer_features(df: pd.DataFrame, contract: FeatureContract, *, preprocessing_artifact_identity: str | None = None) -> tuple[pd.DataFrame, list[str]]:
    """Compatibility wrapper requiring a fitted contract; no implicit fitting."""
    result = transform_features(df, contract, preprocessing_artifact_identity=preprocessing_artifact_identity)
    return result, list(contract.feature_order)
