"""Regression coverage for crest factor on asymmetric IMS signals."""

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "simulator" / "src"))
from ims_loader import extract_features


@pytest.mark.parametrize("signal", [
    [1.0, -8.0, 2.0, 0.0],
    [-1.0, 8.0, -2.0, 0.0],
    [0.0, 0.0, 0.0, 0.0],
])
def test_crest_factor_uses_largest_absolute_peak(signal):
    values = np.asarray(signal, dtype=np.float32)
    rms = float(np.sqrt(np.mean(values ** 2)))
    expected = float(np.max(np.abs(values)) / rms) if rms > 1e-12 else 0.0
    assert extract_features(values)["crest_factor"] == pytest.approx(expected, rel=1e-6)
