import numpy as np
import pandas as pd
import pytest
from ml.data.preprocessing import FEATURE_COLUMNS, generate_windows, split_by_unit


def data(cycles):
    return pd.DataFrame([{**{col: float(cycle) for col in FEATURE_COLUMNS},
                          'unit_id': 1, 'time_cycles': cycle, 'rul': 100-cycle}
                         for cycle in cycles])


@pytest.mark.parametrize('cycles', [list(range(1, 30)) + [31], list(range(1, 30)) + [29]])
def test_strict_cycles_reject_invalid_but_legacy_remains_unchanged(cycles):
    df = data(cycles)
    assert generate_windows(df, validate_cycles=False).X.shape == (1, 30, 24)
    with pytest.raises(ValueError, match='cycles must be'):
        generate_windows(df, validate_cycles=True)


def test_strict_cycles_preserves_valid_window_and_padding():
    for cycles in (list(range(31, 0, -1)), list(range(3, 0, -1))):
        df = data(cycles)
        legacy = generate_windows(df, validate_cycles=False)
        strict = generate_windows(df, validate_cycles=True)
        np.testing.assert_array_equal(legacy.X, strict.X)
        np.testing.assert_array_equal(legacy.y, strict.y)
        np.testing.assert_array_equal(legacy.unit_ids, strict.unit_ids)


@pytest.mark.parametrize('fraction', [0, 1, -0.1, 1.1, float('nan'), float('inf')])
def test_invalid_split_fractions_are_rejected(fraction):
    df = pd.concat([data([1]), data([1]).assign(unit_id=2)])
    with pytest.raises(ValueError, match='val_fraction'):
        split_by_unit(df, val_fraction=fraction)


def test_empty_and_one_unit_splits_rejected():
    df = data([1])
    for candidate in (df, df.iloc[:0]):
        with pytest.raises(ValueError, match='at least two'):
            split_by_unit(candidate)


def test_dataset_and_run_identities_isolate_fill_and_windows():
    from ml.data.preprocessing import handle_missing_values
    first = data([1, 2]).assign(dataset_id='FD001', run_id='a')
    second = data([1, 2]).assign(dataset_id='FD002', run_id='a')
    first.loc[first.time_cycles == 1, 'sensor_1'] = 8.
    second.loc[second.time_cycles == 1, 'sensor_1'] = np.nan
    second.loc[second.time_cycles == 2, 'sensor_1'] = 20.
    combined = pd.concat([first, second], ignore_index=True)
    cleaned = handle_missing_values(combined)
    assert cleaned.loc[(cleaned.dataset_id == 'FD002') & (cleaned.time_cycles == 1), 'sensor_1'].iloc[0] == 20.
    result = generate_windows(cleaned)
    assert result.X.shape == (2, 30, 24)
    assert result.X[0, -2, FEATURE_COLUMNS.index('sensor_1')] == 8.
    assert result.X[1, -2, FEATURE_COLUMNS.index('sensor_1')] == 20.


@pytest.mark.parametrize('bad', [None, '', '  ', np.nan, np.inf])
def test_invalid_run_identity_rejected(bad):
    from ml.data.preprocessing import handle_missing_values
    df = data([1, 2]).assign(run_id=bad)
    with pytest.raises(ValueError, match='run_id'):
        handle_missing_values(df)
    with pytest.raises(ValueError, match='run_id'):
        generate_windows(df)


@pytest.mark.parametrize('fallback', [None, np.nan, np.inf])
def test_no_finite_fallback_raises(fallback):
    from ml.data.preprocessing import handle_missing_values
    df = data([1, 2])
    df['sensor_1'] = np.nan
    with pytest.raises(ValueError, match='finite replacement'):
        handle_missing_values(df, None if fallback is None else {'sensor_1': fallback})


def test_streaming_infinity_uses_prior_finite_value(monkeypatch):
    from sklearn.preprocessing import MinMaxScaler
    from ml.streaming.preprocessing import StreamingPreprocessor
    scaler = MinMaxScaler().fit(pd.DataFrame([[0.] * 24, [10.] * 24], columns=FEATURE_COLUMNS))
    stats = {c: {'mean': 5., 'std': 100., 'median': 3.} for c in FEATURE_COLUMNS}
    worker = StreamingPreprocessor(artifact_dir='unused', validate_startup=False)
    monkeypatch.setattr(worker, '_artifacts', lambda dataset: (scaler, stats))
    message = {c: 5. for c in FEATURE_COLUMNS}
    message.update(dataset_id='FD001', unit_id='1', run_id='a', time_cycles=1, sensor_1=8.)
    worker.transform(message)
    message.update(time_cycles=2, sensor_1=np.inf)
    output = worker.transform(message)
    assert output['features'][FEATURE_COLUMNS.index('sensor_1')] == pytest.approx(.8)
    assert np.isfinite(worker._states[('FD001', '1', 'a')].last_values['sensor_1'])


def test_malformed_stream_record_is_not_checkpointed_or_committed(tmp_path, monkeypatch):
    import sqlite3
    from types import SimpleNamespace
    from unittest.mock import patch
    from kafka.structs import TopicPartition
    from sklearn.preprocessing import MinMaxScaler
    from ml.streaming.preprocessing import StreamingPreprocessor
    from test_pipeline_preprocessing_safety import Consumer, Producer

    class MalformedConsumer(Consumer):
        def poll(self, timeout_ms, max_records):
            part = TopicPartition('input', 0)
            if part in self.paused:
                raise KeyboardInterrupt()
            message = {c: 5. for c in FEATURE_COLUMNS}
            message.update(dataset_id='FD001', unit_id='1', run_id='a', time_cycles=1, sensor_1='invalid')
            return {part: [SimpleNamespace(topic='input', partition=0, offset=0, value=message)]}

    scaler = MinMaxScaler().fit(pd.DataFrame([[0.] * 24, [10.] * 24], columns=FEATURE_COLUMNS))
    stats = {c: {'mean': 5., 'std': 100., 'median': 3.} for c in FEATURE_COLUMNS}
    worker = StreamingPreprocessor(artifact_dir='unused', validate_startup=False)
    monkeypatch.setattr(worker, '_artifacts', lambda dataset: (scaler, stats))
    consumer, producer = MalformedConsumer({0: 1}), Producer()
    checkpoint = tmp_path / 'state.sqlite'
    with (patch.dict('os.environ', {'PREPROCESSING_CHECKPOINT_PATH': str(checkpoint)}),
          patch('ml.streaming.preprocessing.validate_checkpoint_path'),
          patch('ml.streaming.preprocessing.artifact_identity', return_value='test'),
          patch('ml.streaming.preprocessing.KafkaConsumer', return_value=consumer),
          patch('ml.streaming.preprocessing.KafkaProducer', return_value=producer)):
        worker.run()
    assert consumer.commits == []
    assert producer.sent == []
    with sqlite3.connect(checkpoint) as db:
        assert db.execute('SELECT payload FROM checkpoints').fetchone()[0] is None
