import numpy as np
import pandas as pd
import pytest
from ml.build_combined_dataset import generate, prepare_dataset
from ml.config import ALL_COLUMNS, SUB_DATASETS
from ml.data.preprocessing import FEATURE_COLUMNS, split_by_unit


def frame(units, cycles=32):
    return pd.DataFrame([{**{'unit_id': unit, 'time_cycles': cycle},
                          **{c: float(unit * 3 + cycle) for c in FEATURE_COLUMNS}}
                         for unit in units for cycle in range(1, cycles + 1)])


def test_generator_split_before_fit_is_deterministic_and_validation_extremes_do_not_leak():
    train = frame(range(1, 6))
    test = frame([1], cycles=30)
    truth = pd.Series({1: 7})
    a = prepare_dataset(train, test, truth)
    _, validation = split_by_unit(train)
    changed = train.copy()
    changed.loc[changed.unit_id.isin(validation.unit_id), FEATURE_COLUMNS] = 1e9
    b = prepare_dataset(changed, test, truth)
    assert set(a['train_units']).isdisjoint(a['validation_units'])
    assert a['train_units'] == b['train_units']
    assert a['validation_units'] == b['validation_units']
    assert a['medians'] == b['medians']
    assert a['stats'] == b['stats']
    np.testing.assert_array_equal(a['scaler'].data_min_, b['scaler'].data_min_)
    np.testing.assert_array_equal(a['scaler'].data_max_, b['scaler'].data_max_)
    np.testing.assert_array_equal(a['train'].X, b['train'].X)
    assert a['train'].X.shape[1:] == (30, 24)
    assert a['test'].y.tolist() == [7]
    assert a['train'].y[:3].tolist() == [2, 1, 0]


def test_generator_writes_training_contract_and_manifest(tmp_path):
    inputs = tmp_path / 'input'
    inputs.mkdir()
    for fd in SUB_DATASETS:
        frame(range(1, 5)).to_csv(inputs / f'train_{fd}.txt', sep=' ', header=False, index=False, columns=ALL_COLUMNS)
        frame([1], 30).to_csv(inputs / f'test_{fd}.txt', sep=' ', header=False, index=False, columns=ALL_COLUMNS)
        (inputs / f'RUL_{fd}.txt').write_text('7\n')
    out = tmp_path / 'new-output'
    m = generate(inputs, out)
    assert m['feature_order'] == FEATURE_COLUMNS
    assert list(m['datasets']) == SUB_DATASETS
    with np.load(out / 'raw' / 'train.npz') as result:
        assert result['X'].shape[1:] == (30, 24)
        assert len(result['X']) == len(result['y']) == len(result['dataset_ids'])
    with np.load(out / 'raw' / 'test_FD001.npz') as result:
        assert result['y'].tolist() == [7]
    assert (out / 'manifest.json').is_file()
    with pytest.raises(FileExistsError):
        generate(inputs, out)


def test_invalid_inputs_fail_clearly(tmp_path):
    with pytest.raises(FileNotFoundError, match='missing C-MAPSS input'):
        generate(tmp_path, tmp_path / 'out')
    with pytest.raises(ValueError, match='at least two'):
        prepare_dataset(frame([1]), frame([1], 30), pd.Series({1: 7}))
    with pytest.raises(ValueError, match='ground truth'):
        prepare_dataset(frame([1, 2]), frame([1], 30), pd.Series({2: 7}))
    bad = frame([1, 2, 3, 4])
    bad['sensor_1'] = np.nan
    with pytest.raises(ValueError, match='finite training median'):
        prepare_dataset(bad, frame([1], 30), pd.Series({1: 7}))
