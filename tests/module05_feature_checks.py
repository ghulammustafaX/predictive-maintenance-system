"""Module 05 offline feature-contract checks.

These checks do not certify a deployed model or historical feature artifacts.
Run explicitly: pytest tests/module05_feature_checks.py
"""
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ml.config import SENSOR_COLUMNS
from ml.features import feature_engineering as f


def rows(items):
    return pd.DataFrame([{'dataset_id': dataset, 'unit_id': unit, 'run_id': run,
                          'time_cycles': cycle, 'rul': rul,
                          **{c: float(value if c == 'sensor_1' else 5.0) for c in SENSOR_COLUMNS}}
                         for dataset, unit, run, cycle, value, rul in items])


def fit_contract(train):
    factory = getattr(f, 'fit_feature_contract', None)
    if factory is None:
        pytest.fail('Proposed training-only fit_feature_contract API is missing')
    return factory(train, training_provenance={'split': 'train', 'source': 'synthetic-test'},
                   preprocessing_artifact_identity='test-preprocessing-v1',
                   application_dataset_ids=('FD001', 'FD002', 'FD003', 'FD004'))


def transform(frame, contract):
    fn = getattr(f, 'transform_features', None)
    if fn is None:
        pytest.fail('Proposed reusable transform_features API is missing')
    return fn(frame, contract, preprocessing_artifact_identity='test-preprocessing-v1')


def test_rolling_statistics_correct_sample_and_window_boundaries():
    df = rows([('FD001', 1, 'a', i, i, 10-i) for i in range(1, 7)])
    got = f.add_rolling_statistics(df)
    np.testing.assert_allclose(got.sensor_1_roll_mean, [1,1.5,2,2.5,3,4])
    np.testing.assert_allclose(got.sensor_1_roll_std.iloc[:2], [0, np.sqrt(.5)])
    assert got.sensor_1_roll_min.tolist() == [1,1,1,1,1,2]
    assert got.sensor_1_roll_max.tolist() == [1,2,3,4,5,6]


def test_rolling_is_causal_and_chronological():
    df = rows([('FD001', 1, 'a', i, i, 9-i) for i in [3,1,2]])
    got = f.add_rolling_statistics(df)
    assert got.time_cycles.tolist() == [1,2,3]
    assert got.sensor_1_roll_mean.tolist() == [1,1.5,2]
    changed = df.copy(); changed.loc[changed.time_cycles == 3, 'sensor_1'] = 1000
    assert f.add_rolling_statistics(changed).sensor_1_roll_mean.iloc[:2].tolist() == [1,1.5]


@pytest.mark.parametrize('identity', ['run_id', 'dataset_id', 'unit_id'])
def test_rolling_never_crosses_identity(identity):
    a = rows([('FD001',1,'a',1,10,5),('FD001',1,'a',2,11,4)])
    b = rows([('FD001',1,'a',1,100,5),('FD001',1,'a',2,101,4)])
    b[identity] = {'run_id':'b','dataset_id':'FD002','unit_id':2}[identity]
    got = f.add_rolling_statistics(pd.concat([a,b], ignore_index=True))
    assert got[(got[identity] == b[identity].iloc[0]) & (got.time_cycles == 1)].sensor_1_roll_mean.iloc[0] == 100


def test_trend_consecutive_and_first_observation():
    df = rows([('FD001',1,'a',i,v,10-i) for i,v in [(3,7),(1,2),(2,4)]])
    got = f.add_trend_indicators(df)
    assert got.time_cycles.tolist() == [1,2,3]
    assert got.sensor_1_delta.tolist() == [0,2,3]
    assert np.isfinite(got.sensor_1_delta).all()


@pytest.mark.parametrize('identity', ['run_id', 'dataset_id', 'unit_id'])
def test_trend_first_cycle_zero_for_each_identity(identity):
    a = rows([('FD001',1,'a',1,10,5),('FD001',1,'a',2,11,4)])
    b = rows([('FD001',1,'a',1,100,5),('FD001',1,'a',2,101,4)])
    b[identity] = {'run_id':'b','dataset_id':'FD002','unit_id':2}[identity]
    got = f.add_trend_indicators(pd.concat([a,b], ignore_index=True))
    assert got[(got[identity] == b[identity].iloc[0]) & (got.time_cycles == 1)].sensor_1_delta.iloc[0] == 0


@pytest.mark.parametrize('function', [f.add_rolling_statistics, f.add_trend_indicators])
@pytest.mark.parametrize('cycles', [[1,3],[1,1]])
def test_temporal_features_reject_gaps_and_duplicates(function, cycles):
    df = rows([('FD001',1,'a',c,c,10-c) for c in cycles])
    with pytest.raises(ValueError, match='cycle|consecutive|duplicate'):
        function(df)


def test_correlation_prunes_duplicate_and_constant_columns_deterministically():
    df = pd.DataFrame({'a':[1.,2.,3.,4.], 'b':[2.,4.,6.,8.], 'constant':[1.]*4,
                       'near':[1.,1.00001,1.,1.00001]})
    got = f.find_correlated_columns(df, list(df.columns))
    assert set(got) == {'b','constant','near'}
    assert f.find_correlated_columns(df,list(df.columns)) == got


@pytest.mark.parametrize('value', [np.nan, np.inf, -np.inf])
def test_correlation_rejects_nonfinite_or_undefined_values(value):
    df = pd.DataFrame({'invalid':[value,value,value], 'valid':[1.,2.,3.]})
    with pytest.raises(ValueError, match='finite|missing|invalid'):
        f.find_correlated_columns(df,list(df.columns))


def test_mutual_information_reproducible_with_ordered_candidates():
    x = np.arange(50, dtype=float)
    df = pd.DataFrame({'a':x, 'b':x**2, 'c':np.sin(x), 'rul':x})
    assert f.select_top_features(df, ['a','b','c'], top_k=2) == f.select_top_features(df, ['a','b','c'], top_k=2)


@pytest.mark.parametrize('value', [np.nan,np.inf,-np.inf])
def test_mutual_information_rejects_nonfinite_input(value):
    df = pd.DataFrame({'a':[1.,2.,value,4.,5.], 'rul':[1.,2.,3.,4.,5.]})
    with pytest.raises(ValueError, match='finite|missing|invalid|NaN|infinity'):
        f.select_top_features(df,['a'])


def training():
    return rows([('FD001',u,'a',cycle,cycle,20-cycle) for u in (1,2) for cycle in range(1,8)]).assign(split='train')


def test_training_only_fit_and_validation_extremes_do_not_change_selection():
    train = training()
    contract = fit_contract(train)
    validation = train.copy(); validation['sensor_1'] = 1e9; validation['rul'] = -999
    before = tuple(contract.feature_order)
    transform(validation, contract)
    assert tuple(contract.feature_order) == before
    assert tuple(fit_contract(train).feature_order) == before


def test_health_index_fit_sign_and_constant_sensor_exclusion():
    train = training()
    contract = fit_contract(train)
    assert contract.health_signs['sensor_1'] == -1
    assert 'sensor_2' not in contract.health_signs
    actual = transform(train.drop(columns='rul'), contract)
    assert actual.health_index.tolist() == pytest.approx([-float(c) for c in train.time_cycles])


@pytest.mark.parametrize('partition', ['validation','test'])
def test_evaluation_rul_cannot_change_features(partition):
    train = training(); contract = fit_contract(train)
    evaluation = train.copy(); other = evaluation.copy(); other['rul'] *= -1
    first = transform(evaluation, contract)
    second = transform(other, contract)
    pd.testing.assert_frame_equal(first.loc[:, contract.feature_order], second.loc[:, contract.feature_order])


def test_unlabelled_inference_uses_same_fitted_health_parameters():
    train = training(); contract = fit_contract(train)
    labelled = transform(train, contract)
    unlabelled = transform(train.drop(columns='rul'), contract)
    np.testing.assert_array_equal(labelled.health_index, unlabelled.health_index)


def test_missing_fitted_health_parameters_rejected():
    fn = getattr(f, 'transform_features', None)
    if fn is None:
        pytest.fail('Proposed reusable transform_features API is missing')
    with pytest.raises(ValueError, match='contract|fitted|parameter'):
        fn(training(), None)


def test_current_health_index_does_not_use_evaluation_rul():
    df = training(); inverted = df.copy(); inverted['rul'] *= -1
    contract = fit_contract(df)
    np.testing.assert_array_equal(f.add_health_index(df, contract).health_index,
                                  f.add_health_index(inverted, contract).health_index)


def test_parquet_roundtrip_preserves_feature_order(tmp_path, monkeypatch):
    monkeypatch.setattr(f, 'FEATURE_STORE_DIR', tmp_path)
    contract = fit_contract(training())
    df = transform(training(), contract).loc[:, list(contract.feature_order)]
    f.save_feature_set(df,'FD001','train','engineered', contract=contract)
    pd.testing.assert_frame_equal(f.load_feature_set('FD001','train','engineered', contract=contract),df)


def test_feature_store_rejects_silent_overwrite(tmp_path, monkeypatch):
    monkeypatch.setattr(f, 'FEATURE_STORE_DIR', tmp_path)
    contract = fit_contract(training())
    df = transform(training(), contract).loc[:, list(contract.feature_order)]
    f.save_feature_set(df,'FD001','train','engineered', contract=contract)
    with pytest.raises((ValueError, FileExistsError), match='exist|overwrite'):
        f.save_feature_set(df,'FD001','train','engineered', contract=contract)


def test_feature_store_persists_version_order_provenance_and_training_parameters(tmp_path):
    contract = fit_contract(training())
    save = getattr(f, 'save_feature_contract', None)
    load = getattr(f, 'load_feature_contract', None)
    if save is None or load is None:
        pytest.fail('Proposed versioned feature-contract persistence API is missing')
    save(contract, tmp_path / 'contract.json')
    restored = load(tmp_path / 'contract.json')
    assert restored.schema_version == contract.schema_version
    assert restored.feature_order == contract.feature_order
    assert restored.training_provenance == contract.training_provenance
    assert restored.health_signs == contract.health_signs


def test_feature_store_rejects_invalid_metadata(tmp_path):
    load = getattr(f, 'load_feature_contract', None)
    if load is None:
        pytest.fail('Proposed versioned feature-contract persistence API is missing')
    path = tmp_path / 'contract.json'; path.write_text('{"schema_version": "invalid"}')
    with pytest.raises(ValueError, match='schema|version|contract'):
        load(path)


def test_one_common_fitted_order_is_reused_across_fd_datasets():
    contract = fit_contract(training())
    for fd in ['FD001','FD002','FD003','FD004']:
        frame = training().assign(dataset_id=fd)
        actual = transform(frame, contract)
        assert list(actual.loc[:, contract.feature_order].columns) == list(contract.feature_order)



def test_trend_rejects_nonfinite_sensor_observations():
    df = rows([('FD001', 1, 'a', 1, 1, 5), ('FD001', 1, 'a', 2, np.inf, 4)])
    with pytest.raises(ValueError, match='finite|invalid'):
        f.add_trend_indicators(df)


def test_contract_is_immutable_hash_stable_and_detects_tampering(tmp_path):
    from dataclasses import FrozenInstanceError
    import json
    train = training()
    contract = fit_contract(train)
    reordered = fit_contract(train.sample(frac=1, random_state=4))
    assert contract.contract_hash == reordered.contract_hash
    with pytest.raises((FrozenInstanceError, AttributeError)):
        contract.contract_hash = 'changed'
    with pytest.raises(TypeError):
        contract.health_signs['sensor_1'] = 1
    path = tmp_path / 'feature-contract.json'
    f.save_feature_contract(contract, path)
    with pytest.raises(FileExistsError):
        f.save_feature_contract(contract, path)
    assert f.load_feature_contract(path).contract_hash == contract.contract_hash
    payload = json.loads(path.read_text(encoding='utf-8'))
    payload['contract']['rolling_window'] = 17
    path.write_text(json.dumps(payload), encoding='utf-8')
    with pytest.raises(ValueError, match='hash'):
        f.load_feature_contract(path)


def test_contract_rejects_mixed_split_and_missing_upstream_identity():
    mixed = training().assign(split='train')
    mixed.loc[mixed.index[0], 'split'] = 'validation'
    with pytest.raises(ValueError, match='training'):
        fit_contract(mixed)
    with pytest.raises(ValueError, match='split=train'):
        fit_contract(training().drop(columns='split'))
    with pytest.raises(ValueError, match='preprocessing'):
        f.fit_feature_contract(training(), training_provenance={'split': 'train', 'source': 'test'}, preprocessing_artifact_identity='')
    with pytest.raises(ValueError, match='provenance'):
        f.fit_feature_contract(training(), training_provenance={'split': 'validation'}, preprocessing_artifact_identity='x')


def test_feature_store_rejects_reordered_columns_and_contract_mismatch(tmp_path, monkeypatch):
    monkeypatch.setattr(f, 'FEATURE_STORE_DIR', tmp_path)
    contract = fit_contract(training())
    df = transform(training(), contract).loc[:, list(contract.feature_order)]
    with pytest.raises(ValueError, match='feature order'):
        f.save_feature_set(df.iloc[:, ::-1], 'FD001', 'train', 'engineered', contract=contract)
    f.save_feature_set(df, 'FD001', 'train', 'engineered', contract=contract)
    other = f.fit_feature_contract(training(), training_provenance={'split':'train', 'source':'other'},
                                   preprocessing_artifact_identity='test-preprocessing-v1')
    with pytest.raises(ValueError, match='provenance|order'):
        f.load_feature_set('FD001', 'train', 'engineered', contract=other)


def test_feature_store_detects_data_tampering(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq
    monkeypatch.setattr(f, 'FEATURE_STORE_DIR', tmp_path)
    contract = fit_contract(training())
    df = transform(training(), contract).loc[:, list(contract.feature_order)]
    f.save_feature_set(df, 'FD001', 'train', 'engineered', contract=contract)
    path = tmp_path / 'FD001' / 'train_engineered.parquet'
    table = pq.read_table(path)
    changed = table.to_pandas()
    changed.iloc[0, 0] += 1
    pq.write_table(pa.Table.from_pandas(changed, preserve_index=False).replace_schema_metadata(table.schema.metadata), path)
    with pytest.raises(ValueError, match='integrity'):
        f.load_feature_set('FD001', 'train', 'engineered', contract=contract)


def test_transform_requires_matching_upstream_identity():
    contract = fit_contract(training())
    for identity in (None, 'wrong-artifact'):
        with pytest.raises(ValueError, match='preprocessing artifact identity'):
            f.transform_features(training(), contract, preprocessing_artifact_identity=identity)


def test_multisensor_health_order_is_identical_after_contract_reload(tmp_path):
    train = training()
    train['sensor_2'] = train['time_cycles'] * 0.1
    train['sensor_10'] = -train['time_cycles'] * 0.01
    contract = fit_contract(train)
    before = transform(train, contract)
    path = tmp_path / 'contract.json'
    f.save_feature_contract(contract, path)
    restored = f.load_feature_contract(path)
    after = transform(train, restored)
    np.testing.assert_array_equal(before.health_index, after.health_index)
    assert before.loc[:, list(contract.feature_order)].equals(after.loc[:, list(restored.feature_order)])


def test_feature_selection_cannot_include_target_label():
    df = pd.DataFrame({'sensor_1': [1., 2., 3., 4.], 'rul': [4., 3., 2., 1.]})
    with pytest.raises(ValueError, match='candidate'):
        f.select_top_features(df, ['sensor_1', 'rul'])


def test_pooled_training_records_all_datasets_and_one_common_order():
    datasets = ('FD001', 'FD002', 'FD003', 'FD004')
    pooled = pd.concat([training().assign(dataset_id=fd) for fd in datasets], ignore_index=True)
    contract = fit_contract(pooled)
    assert contract.payload['dataset_ids'] == datasets
    for fd in datasets:
        frame = training().assign(dataset_id=fd)
        result = transform(frame, contract)
        assert tuple(result.loc[:, list(contract.feature_order)].columns) == contract.feature_order
