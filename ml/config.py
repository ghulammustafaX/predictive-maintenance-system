"""
Module 4 — Data Preprocessing: shared configuration.

Centralizes paths, column schema, and hyperparameters so every sub-module
(loader, preprocessing, feature engineering, LSTM) reads from one source
of truth. Avoids hardcoding magic numbers across files.
"""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
# This file lives at: <repo_root>/ml/config.py
ML_ROOT = Path(__file__).resolve().parent
REPO_ROOT = ML_ROOT.parent

RAW_DATA_DIR = REPO_ROOT / "simulator" / "data" / "cmapss"
PROCESSED_DIR = ML_ROOT / "saved" / "processed"
SCALER_DIR = ML_ROOT / "saved" / "scalers"
CHECKPOINT_DIR = ML_ROOT / "saved" / "checkpoints"
FEATURE_STORE_DIR = ML_ROOT / "saved" / "features"

for _dir in (PROCESSED_DIR, SCALER_DIR, CHECKPOINT_DIR, FEATURE_STORE_DIR):
    _dir.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# C-MAPSS column schema
# ---------------------------------------------------------------------------
# The raw train_FD00X.txt / test_FD00X.txt files are whitespace-delimited
# with no header: unit_id, time_cycles, 3 operational settings, 21 sensors.
INDEX_COLUMNS = ["unit_id", "time_cycles"]
SETTING_COLUMNS = [f"operational_setting_{i}" for i in range(1, 4)]
SENSOR_COLUMNS = [f"sensor_{i}" for i in range(1, 22)]
ALL_COLUMNS = INDEX_COLUMNS + SETTING_COLUMNS + SENSOR_COLUMNS

SUB_DATASETS = ["FD001", "FD002", "FD003", "FD004"]

# ---------------------------------------------------------------------------
# Preprocessing hyperparameters
# ---------------------------------------------------------------------------
WINDOW_SIZE = 30          # cycles per LSTM input sequence
RUL_CAP = 125             # piecewise-linear RUL ceiling (standard for C-MAPSS)
OUTLIER_Z_THRESHOLD = 5.0  # sensor readings beyond this many std devs are clipped
VALIDATION_UNIT_FRACTION = 0.2  # fraction of engine units held out for validation
RANDOM_SEED = 42

# Sensors known to be near-constant / uninformative across most C-MAPSS
# sub-datasets (kept here for Module 5 feature selection, not removed in
# Module 4 — preprocessing should stay generic and not bake in
# dataset-specific assumptions).
LOW_VARIANCE_SENSOR_CANDIDATES = [
    "sensor_1", "sensor_5", "sensor_6", "sensor_10",
    "sensor_16", "sensor_18", "sensor_19",
]
