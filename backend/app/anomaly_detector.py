"""
Module 7 — Anomaly Detection
Pure-Python anomaly detection engine. No external ML dependencies.
Runs as a stateless library called by the anomaly router.

Detection methods:
  1. Z-score   — rolling per-sensor mean/std from stored telemetry history
  2. Threshold — configurable hard upper/lower bounds per sensor

Schema contract (AnomalyEvent TypedDict):
  event_id, dataset_id, unit_id, run_id, cycle, timestamp,
  sensor, detection_method, observed_value, severity, score, status
"""

from __future__ import annotations

import hashlib
import math
import uuid
from datetime import datetime, timezone
from typing import Any, TypedDict

# ---------------------------------------------------------------------------
# Sensor threshold catalogue — values chosen from CMAPSS domain knowledge.
# All are configurable at runtime by passing overrides to detect_anomalies().
# ---------------------------------------------------------------------------
DEFAULT_SENSOR_THRESHOLDS: dict[str, dict[str, float | None]] = {
    "sensor_2":  {"low": 540.0,  "high": 650.0},
    "sensor_3":  {"low": 1350.0, "high": 1650.0},
    "sensor_4":  {"low": 1050.0, "high": 1450.0},
    "sensor_7":  {"low": 320.0,  "high": 430.0},
    "sensor_8":  {"low": 2380.0, "high": 2400.0},
    "sensor_9":  {"low": 9000.0, "high": 9500.0},
    "sensor_11": {"low": 40.0,   "high": 50.0},
    "sensor_12": {"low": 150.0,  "high": 400.0},
    "sensor_13": {"low": 2300.0, "high": 2400.0},
    "sensor_14": {"low": 8050.0, "high": 8350.0},
    "sensor_15": {"low": 8.0,    "high": 23.0},
    "sensor_17": {"low": 300.0,  "high": 420.0},
    "sensor_20": {"low": 24.0,   "high": 40.0},
    "sensor_21": {"low": 11.0,   "high": 24.0},
}

# Z-score threshold → severity mapping
ZSCORE_SEVERITY_THRESHOLDS = [
    (4.0, "critical"),
    (3.0, "high"),
    (2.5, "medium"),
    (2.0, "low"),
]

# Sensors analysed for Z-score (skip constant / near-zero-variance ones)
ZSCORE_SENSORS = {
    "sensor_2", "sensor_3", "sensor_4", "sensor_7", "sensor_8",
    "sensor_9", "sensor_11", "sensor_12", "sensor_13", "sensor_14",
    "sensor_15", "sensor_17", "sensor_20", "sensor_21",
}

MINIMUM_WINDOW_FOR_ZSCORE = 10   # discard windows smaller than this


class AnomalyEvent(TypedDict):
    event_id: str
    dataset_id: str
    unit_id: str
    run_id: str
    cycle: int
    timestamp: str                # ISO-8601 UTC
    sensor: str
    detection_method: str         # "zscore" | "threshold"
    observed_value: float
    severity: str                 # "low" | "medium" | "high" | "critical"
    score: float                  # z-score or distance-to-nearest-bound
    status: str                   # "open"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _mean_std(values: list[float]) -> tuple[float, float]:
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    mu = sum(values) / n
    var = sum((v - mu) ** 2 for v in values) / n
    return mu, math.sqrt(var)


def _zscore_severity(z: float) -> str | None:
    for threshold, label in ZSCORE_SEVERITY_THRESHOLDS:
        if abs(z) >= threshold:
            return label
    return None


def _threshold_severity(value: float, low: float | None, high: float | None) -> tuple[str | None, float]:
    """Returns (severity, score) where score = how far outside bounds."""
    if high is not None and value > high:
        distance = value - high
        normalised = distance / max(high * 0.05, 1e-6)
        if normalised >= 2.0:
            return "critical", normalised
        if normalised >= 1.0:
            return "high", normalised
        if normalised >= 0.5:
            return "medium", normalised
        return "low", normalised
    if low is not None and value < low:
        distance = low - value
        normalised = distance / max(low * 0.05, 1e-6)
        if normalised >= 2.0:
            return "critical", normalised
        if normalised >= 1.0:
            return "high", normalised
        if normalised >= 0.5:
            return "medium", normalised
        return "low", normalised
    return None, 0.0


def _stable_event_id(dataset_id: str, unit_id: str, run_id: str, cycle: int, sensor: str, method: str) -> str:
    """Deterministic event_id — same inputs always produce the same UUID.
    This enables idempotent upserts and deduplication without an extra DB lookup."""
    key = f"{dataset_id}|{unit_id}|{run_id}|{cycle}|{sensor}|{method}"
    digest = hashlib.sha256(key.encode()).hexdigest()[:32]
    return str(uuid.UUID(digest))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_anomalies(
    records: list[dict[str, Any]],
    dataset_id: str,
    unit_id: str,
    run_id: str,
    history: list[dict[str, Any]] | None = None,
    sensor_thresholds: dict[str, dict[str, float | None]] | None = None,
    zscore_min_std: float = 1e-4,
) -> list[AnomalyEvent]:
    """Detect anomalies in `records` using threshold and Z-score methods.

    Parameters
    ----------
    records:
        The telemetry records to evaluate.  Each dict must contain at
        least `cycle` (int), `timestamp` (str ISO-8601), and sensor fields.
    dataset_id, unit_id, run_id:
        Identifiers copied into every emitted event.
    history:
        Optional earlier records for the same engine/run used to build the
        rolling baseline statistics. If None, the records list itself is
        used as both baseline and evaluation (useful for batch evaluation).
    sensor_thresholds:
        Override the default threshold catalogue. Pass an empty dict {} to
        disable threshold detection entirely.
    zscore_min_std:
        Minimum std required before Z-score is computed (avoids div-by-zero
        on constant sensors like sensor_1 / sensor_5 / sensor_6 / sensor_10).
    """
    thresholds = DEFAULT_SENSOR_THRESHOLDS if sensor_thresholds is None else sensor_thresholds

    # Build baseline from history (or the records themselves for batch runs)
    baseline_records = history if history is not None else records
    baseline: dict[str, list[float]] = {}
    for rec in baseline_records:
        for sensor in ZSCORE_SENSORS:
            raw = rec.get(sensor)
            if raw is None:
                continue
            try:
                baseline.setdefault(sensor, []).append(float(raw))
            except (TypeError, ValueError):
                pass

    events: list[AnomalyEvent] = []
    seen_ids: set[str] = set()

    for rec in records:
        cycle = int(float(rec.get("cycle") or rec.get("time_cycles") or 0))
        ts = rec.get("timestamp") or datetime.now(timezone.utc).isoformat()

        for sensor in ZSCORE_SENSORS:
            raw = rec.get(sensor)
            if raw is None:
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue

            # ── Z-score detection ─────────────────────────────────────
            vals = baseline.get(sensor, [])
            if len(vals) >= MINIMUM_WINDOW_FOR_ZSCORE:
                mu, std = _mean_std(vals)
                if std >= zscore_min_std:
                    z = (value - mu) / std
                    sev = _zscore_severity(z)
                    if sev:
                        eid = _stable_event_id(dataset_id, unit_id, run_id, cycle, sensor, "zscore")
                        if eid not in seen_ids:
                            seen_ids.add(eid)
                            events.append(AnomalyEvent(
                                event_id=eid,
                                dataset_id=dataset_id,
                                unit_id=unit_id,
                                run_id=run_id,
                                cycle=cycle,
                                timestamp=ts,
                                sensor=sensor,
                                detection_method="zscore",
                                observed_value=value,
                                severity=sev,
                                score=round(abs(z), 4),
                                status="open",
                            ))

        # ── Threshold detection (all sensors with configured bounds) ─
        for sensor, bounds in thresholds.items():
            raw = rec.get(sensor)
            if raw is None:
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue

            sev, score = _threshold_severity(value, bounds.get("low"), bounds.get("high"))
            if sev:
                eid = _stable_event_id(dataset_id, unit_id, run_id, cycle, sensor, "threshold")
                if eid not in seen_ids:
                    seen_ids.add(eid)
                    events.append(AnomalyEvent(
                        event_id=eid,
                        dataset_id=dataset_id,
                        unit_id=unit_id,
                        run_id=run_id,
                        cycle=cycle,
                        timestamp=ts,
                        sensor=sensor,
                        detection_method="threshold",
                        observed_value=value,
                        severity=sev,
                        score=round(score, 4),
                        status="open",
                    ))

    return events


def evaluate_rul_alert(
    run_id: str,
    dataset_id: str,
    unit_id: str,
    cycle: int,
    predicted_rul: float,
    timestamp: str,
    critical_threshold: float = 30.0,
    warning_threshold: float = 60.0,
) -> dict[str, Any] | None:
    """Evaluate whether a stored RUL prediction should trigger an alert.

    Returns a dict with alert fields if the prediction breaches a threshold,
    or None if the prediction is healthy.

    NOTE: The acceptance run predictions (~118-122 cycles) will NOT trigger
    either threshold with the defaults of 30/60. This is intentional.
    """
    if predicted_rul <= critical_threshold:
        severity = "critical"
        message = (
            f"Engine {dataset_id}-{unit_id} (run {run_id}) is at cycle {cycle} "
            f"with RUL ≈ {predicted_rul:.1f} cycles — CRITICAL: below {critical_threshold:.0f}-cycle threshold."
        )
    elif predicted_rul <= warning_threshold:
        severity = "warning"
        message = (
            f"Engine {dataset_id}-{unit_id} (run {run_id}) is at cycle {cycle} "
            f"with RUL ≈ {predicted_rul:.1f} cycles — WARNING: below {warning_threshold:.0f}-cycle threshold."
        )
    else:
        return None

    alert_key = f"{dataset_id}|{unit_id}|{run_id}|rul_low|{severity}"
    alert_id_digest = hashlib.sha256(f"{alert_key}|{cycle}".encode()).hexdigest()[:32]
    dedup_key = hashlib.sha256(f"{dataset_id}|{unit_id}|{run_id}|rul_low".encode()).hexdigest()[:32]

    return {
        "alert_id": str(uuid.UUID(alert_id_digest)),
        "dedup_key": dedup_key,
        "alert_type": "rul_low",
        "severity": severity,
        "dataset_id": dataset_id,
        "unit_id": unit_id,
        "run_id": run_id,
        "cycle": cycle,
        "predicted_rul": predicted_rul,
        "message": message,
        "timestamp": timestamp,
        "status": "open",
    }
