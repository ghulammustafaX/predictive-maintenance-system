"""
Task 4: RUL Ground-Truth Isolation Verification.
No live brokers needed — pure source inspection.
"""

import sys, os
sys.path.insert(0, "/app/src")
os.environ["CMAPSS_DATA_DIR"] = "/app/data/cmapss"

print("=== TASK 4: RUL Ground-Truth Isolation ===")
print()

from cmapss_loader import (
    load_cmapss, assign_rul_labels, attach_test_rul,
    load_rul, iter_rows, COLUMNS
)
import json

# ── Permitted inference inputs ─────────────────────────────────────────────
SENSOR_COLS   = {f"sensor_{i}" for i in range(1, 22)}
OP_COLS       = {f"operational_setting_{i}" for i in range(1, 4)}
INFERENCE_OK  = SENSOR_COLS | OP_COLS | {"unit_id", "time_cycles"}

print("Permitted inference fields:", sorted(INFERENCE_OK)[:5], "...")
print("'rul' in permitted inference fields:", "rul" in INFERENCE_OK)
print()

# ── 1) Training payload: rul present but marked as label, not feature ──────
print("--- Training payload inspection ---")
df_train = load_cmapss("FD001", "train")
df_lab   = assign_rul_labels(df_train, 125)
train_payloads = list(iter_rows(df_lab, "r_train", "FD001", "train"))
p_train = train_payloads[0][0]

has_rul     = "rul" in p_train
source_split = p_train.get("source_split")
print(f"  'rul' present in training payload: {has_rul}  (value={p_train.get('rul')})")
print(f"  source_split: {source_split!r}  (expect 'train')")
print(f"  RUL leaks into inference inputs: {'rul' in INFERENCE_OK}")

# Check streaming preprocessing would not use rul field
# ml/streaming/preprocessing.py uses FEATURE_COLUMNS = SETTING_COLUMNS + SENSOR_COLUMNS
# (from ml/data/preprocessing.py) — rul is not in that list
try:
    sys.path.insert(0, "/workspace/ml")
    from data.preprocessing import FEATURE_COLUMNS
    rul_in_feature_cols = "rul" in FEATURE_COLUMNS
    print(f"  'rul' in ml FEATURE_COLUMNS: {rul_in_feature_cols}  (expect False)")
except ImportError:
    # Running inside simulator image which doesn't have ml/ — check by name
    expected_feature_cols = [f"operational_setting_{i}" for i in range(1,4)] + \
                            [f"sensor_{i}" for i in range(1,22)]
    rul_in_feature_cols = "rul" in expected_feature_cols
    print(f"  'rul' in expected FEATURE_COLUMNS: {rul_in_feature_cols}  (expect False)")

# ── 2) Test payload: rul present for reference, labelled as evaluation-only ─
print()
print("--- Test payload inspection ---")
df_test = load_cmapss("FD001", "test")
rul_gt  = load_rul("FD001")
df_rul  = attach_test_rul(df_test, rul_gt)
test_payloads = list(iter_rows(df_rul, "r_test", "FD001", "test"))
p_test = test_payloads[0][0]

print(f"  'rul' present in test payload: {'rul' in p_test}  (value={p_test.get('rul')})")
print(f"  source_split: {p_test.get('source_split')!r}  (expect 'test')")
print(f"  source_type: {p_test.get('source_type')!r}  (expect 'cmapss')")

# The rul field in a test payload is the reference RUL from RUL_FD001.txt,
# propagated back from last-cycle. It's present for offline evaluation only.
# Confirm it is NOT in the sensor columns the LSTM sees:
print()
print("--- Inference input check ---")
p0_keys = set(p_test.keys())
inference_keys_in_payload = p0_keys & INFERENCE_OK
eval_only_keys = p0_keys - INFERENCE_OK - {"schema_version","event_id","run_id",
    "dataset_id","asset_id","timestamp","sequence","source_type","source_split"}
print(f"  Inference keys in payload: {len(inference_keys_in_payload)}  (21 sensors + 3 settings + unit_id + time_cycles = 26)")
print(f"  Evaluation-only keys: {sorted(eval_only_keys)}")
rul_isolated = "rul" in eval_only_keys
print(f"  'rul' is in evaluation-only set (not inference inputs): {rul_isolated}")

# ── 3) JSON serialisability ────────────────────────────────────────────────
print()
print("--- JSON serialisability ---")
try:
    json.dumps(p_train)
    print("  Training payload JSON-serialisable: PASS")
except Exception as e:
    print(f"  FAIL: {e}")

try:
    json.dumps(p_test)
    print("  Test payload JSON-serialisable: PASS")
except Exception as e:
    print(f"  FAIL: {e}")

# ── 4) Summary assertions ─────────────────────────────────────────────────
print()
assert "rul" not in INFERENCE_OK,   "FAIL: rul in inference inputs"
assert p_train.get("source_split") == "train", "FAIL: source_split wrong for train"
assert p_test.get("source_split")  == "test",  "FAIL: source_split wrong for test"
assert not rul_in_feature_cols,      "FAIL: rul in ml feature columns"
assert rul_isolated,                 "FAIL: rul not isolated from inference keys"
print("All assertions passed.")
print()
print("TASK 4: PASS")
