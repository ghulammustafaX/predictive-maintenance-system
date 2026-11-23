"""Leakage check of an explicit split-before-fit recipe, not a production pipeline."""
import numpy as np
import pandas as pd
from ml.data import preprocessing as p


def test_validation_only_extremes_do_not_change_train_fitted_statistics_or_scaler(tmp_path, monkeypatch):
    monkeypatch.setattr(p, 'SCALER_DIR', tmp_path)
    rows = [{**{c: float(unit * 10 + cycle) for c in p.FEATURE_COLUMNS},
             'unit_id': unit, 'time_cycles': cycle}
            for unit in range(1, 6) for cycle in (1, 2, 3)]
    data = pd.DataFrame(rows)
    train, validation = p.split_by_unit(data, val_fraction=0.4, seed=42)
    assert set(train.unit_id).isdisjoint(validation.unit_id)
    validation_changed = validation.copy()
    validation_changed.loc[:, p.FEATURE_COLUMNS] = 1e9

    def fit_with_validation(held_out):
        # The split is explicit; no held-out rows are passed to fitting helpers.
        medians = train[p.FEATURE_COLUMNS].median().to_dict()
        train_clean = p.handle_missing_values(train, medians)
        stats = p.fit_training_stats(train_clean, 'FD001')
        clipped = p.clip_outliers(train_clean, p.OUTLIER_Z_THRESHOLD, stats)
        scaler = p.fit_scaler(clipped, 'FD001')
        p.apply_scaler(p.clip_outliers(p.handle_missing_values(held_out, medians),
                                      p.OUTLIER_Z_THRESHOLD, stats), scaler)
        return medians, stats, scaler.data_min_.copy(), scaler.data_max_.copy()

    ordinary = fit_with_validation(validation)
    extreme = fit_with_validation(validation_changed)
    assert ordinary[:2] == extreme[:2]
    np.testing.assert_array_equal(ordinary[2], extreme[2])
    np.testing.assert_array_equal(ordinary[3], extreme[3])
