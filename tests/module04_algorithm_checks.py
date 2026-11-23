import numpy as np
import pandas as pd
import pytest
from sklearn.preprocessing import MinMaxScaler

from ml.config import SENSOR_COLUMNS
from ml.data import preprocessing as p
from ml.streaming.preprocessing import StreamingPreprocessor


def frame(rows):
    return pd.DataFrame([{**{c: float(cycle) for c in p.FEATURE_COLUMNS},
                          'unit_id': unit, 'time_cycles': cycle, **extra}
                         for unit, cycle, extra in rows])


def test_missing_leading_interior_and_unit_isolation():
    df = frame([(1, 1, {'sensor_1': np.nan}), (1, 2, {'sensor_1': 2.}),
                (1, 3, {'sensor_1': np.nan}), (2, 1, {'sensor_1': 9.}),
                (2, 2, {'sensor_1': np.nan})])
    got = p.handle_missing_values(df)
    assert got.sensor_1.tolist() == [2., 2., 2., 9., 9.]


def test_all_missing_uses_finite_training_fallback():
    df = frame([(1, 1, {'sensor_1': np.nan}), (1, 2, {'sensor_1': np.nan})])
    got = p.handle_missing_values(df, {'sensor_1': 7.})
    assert got.sensor_1.tolist() == [7., 7.]
    assert np.isfinite(got[p.FEATURE_COLUMNS].to_numpy()).all()


def test_missing_run_isolation():
    df = frame([(1, 1, {'run_id': 'a', 'sensor_1': 10.}),
                (1, 2, {'run_id': 'a', 'sensor_1': 11.}),
                (1, 1, {'run_id': 'b', 'sensor_1': np.nan}),
                (1, 2, {'run_id': 'b', 'sensor_1': 20.})])
    got = p.handle_missing_values(df)
    assert got[(got.run_id == 'b') & (got.time_cycles == 1)].sensor_1.iloc[0] == 20.


def test_existing_offline_and_causal_leading_gap_policies_differ():
    df = frame([(1, 1, {'sensor_1': np.nan}), (1, 2, {'sensor_1': 9.})])
    offline = p.handle_missing_values(df, {'sensor_1': 3.})
    causal = p.handle_missing_values(df.iloc[:1], {'sensor_1': 3.}, backfill=False)
    assert offline.sensor_1.iloc[0] == 9.
    assert causal.sensor_1.iloc[0] == 3.


def test_outlier_training_bounds_and_constant():
    df = frame([(1, 1, {'sensor_1': -9., 'sensor_2': 42.}),
                (1, 2, {'sensor_1': 20., 'sensor_2': 42.})])
    stats = {c: {'mean': 1., 'std': 2.} for c in SENSOR_COLUMNS}
    stats['sensor_2'] = {'mean': 42., 'std': 0.}
    got = p.clip_outliers(df, 2., stats)
    assert got.sensor_1.tolist() == [-3., 5.]
    assert got.sensor_2.tolist() == [42., 42.]


def test_normalization_fit_train_only_and_persisted_reuse(tmp_path, monkeypatch):
    monkeypatch.setattr(p, 'SCALER_DIR', tmp_path)
    train = frame([(1, 1, {}), (1, 3, {})])
    train['sensor_1'] = 4.
    validation = frame([(2, 10, {})])
    scaler = p.fit_scaler(train, 'FD001')
    loaded = p.load_scaler('FD001', tmp_path)
    assert list(loaded.feature_names_in_) == p.FEATURE_COLUMNS
    assert p.apply_scaler(train, loaded).sensor_1.tolist() == [0., 0.]
    assert p.apply_scaler(validation, loaded).sensor_2.iloc[0] == pytest.approx(4.5)
    assert np.isfinite(p.apply_scaler(train, loaded)[p.FEATURE_COLUMNS]).all().all()
    np.testing.assert_allclose(scaler.data_max_, loaded.data_max_)


def test_infinity_is_not_emitted_from_cleaner():
    df = frame([(1, 1, {'sensor_1': np.inf})])
    got = p.handle_missing_values(df, {'sensor_1': 3.})
    assert np.isfinite(got[p.FEATURE_COLUMNS].to_numpy()).all()


def test_window_shape_sort_and_label_alignment():
    df = frame([(1, cycle, {'rul': 100 - cycle}) for cycle in range(31, 0, -1)])
    got = p.generate_windows(df)
    assert got.X.shape == (2, 30, 24)
    np.testing.assert_array_equal(got.X[0, :, 0], np.arange(1, 31))
    np.testing.assert_array_equal(got.y, [70., 69.])


@pytest.mark.parametrize('cycles', [list(range(1, 30)) + [31], list(range(1, 30)) + [29]])
def test_window_rejects_missing_or_duplicate_cycles(cycles):
    df = frame([(1, cycle, {'rul': 1}) for cycle in cycles])
    try:
        result = p.generate_windows(df)
    except ValueError:
        return
    assert len(result.X) == 0, 'invalid cycle sequence produced a model window'


def test_window_never_mixes_runs():
    rows = [(1, c, {'run_id': 'a', 'rul': 1}) for c in range(1, 16)]
    rows += [(1, c, {'run_id': 'b', 'rul': 2}) for c in range(16, 31)]
    result = p.generate_windows(frame(rows))
    assert not any(set(window[:, 0]).intersection(range(1, 16)) and
                   set(window[:, 0]).intersection(range(16, 31)) for window in result.X)


def test_window_keeps_units_separate():
    rows = [(u, c, {'rul': u}) for u in (1, 2) for c in range(1, 31)]
    got = p.generate_windows(frame(rows))
    assert got.X.shape == (2, 30, 24)
    np.testing.assert_array_equal(got.unit_ids, [1, 2])
    np.testing.assert_array_equal(got.y, [1, 2])


def test_rul_train_test_caps_and_alignment():
    df = frame([(1, 1, {}), (1, 150, {}), (2, 1, {}), (2, 31, {})])
    train = p.assign_rul_train(df)
    assert train.rul.tolist() == [125, 0, 30, 0]
    test = p.assign_rul_test(df, pd.Series({1: 10, 2: 5}))
    assert test.rul.tolist() == [125, 10, 35, 5]
    # This label-only fixture intentionally omits intermediate cycles.
    got = p.generate_windows(test, last_window_only=True, validate_cycles=False)
    np.testing.assert_array_equal(got.y, [10, 5])


def test_split_deterministic_and_disjoint():
    df = frame([(u, c, {}) for u in range(1, 11) for c in (1, 2)])
    train, val = p.split_by_unit(df, seed=42)
    train2, val2 = p.split_by_unit(df, seed=42)
    assert set(train.unit_id).isdisjoint(val.unit_id)
    assert set(val.unit_id) == set(val2.unit_id)
    assert set(train.unit_id) == set(train2.unit_id)


def test_one_unit_split_does_not_silently_return_empty_training():
    df = frame([(1, 1, {})])
    try:
        train, val = p.split_by_unit(df)
    except ValueError:
        return
    assert not train.empty and not val.empty



def test_streaming_isolates_dataset_unit_run_and_uses_training_median(monkeypatch):
    scaler = MinMaxScaler().fit(pd.DataFrame([[0.] * 24, [10.] * 24], columns=p.FEATURE_COLUMNS))
    stats = {c: {'mean': 5., 'std': 100., 'median': 3.} for c in p.FEATURE_COLUMNS}
    worker = StreamingPreprocessor(artifact_dir='unused', validate_startup=False)
    monkeypatch.setattr(worker, '_artifacts', lambda dataset: (scaler, stats))
    def message(dataset, unit, run, cycle, value):
        row = {c: 5. for c in p.FEATURE_COLUMNS}
        row.update(dataset_id=dataset, unit_id=unit, run_id=run, time_cycles=cycle, sensor_1=value)
        return row
    index = p.FEATURE_COLUMNS.index('sensor_1')
    assert worker.transform(message('FD001', '1', 'a', 1, 8.))['features'][index] == pytest.approx(.8)
    for dataset, unit, run in [('FD001', '1', 'b'), ('FD001', '2', 'a'), ('FD002', '1', 'a')]:
        assert worker.transform(message(dataset, unit, run, 1, None))['features'][index] == pytest.approx(.3)
    assert worker.transform(message('FD001', '1', 'a', 2, None))['features'][index] == pytest.approx(.8)


def test_training_stats_and_clipping_ignore_validation_extreme(tmp_path, monkeypatch):
    monkeypatch.setattr(p, 'SCALER_DIR', tmp_path)
    train = frame([(1, 1, {}), (1, 3, {})])
    validation = frame([(2, 100, {})])
    stats = p.fit_training_stats(train, 'FD001')
    assert stats['sensor_1']['mean'] == 2.
    assert p.clip_outliers(validation, 1., stats).sensor_1.iloc[0] == pytest.approx(2 + np.sqrt(2))
    assert p.load_training_stats('FD001', tmp_path) == stats
