"""
Module 7 — Anomaly Detection API
Read-only authenticated endpoints for anomaly history query and
on-demand detection against stored InfluxDB telemetry.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from influxdb_client import InfluxDBClient
from sqlalchemy.orm import Session

from app.anomaly_detector import detect_anomalies, ZSCORE_SENSORS
from app.database import get_db
from app.models import Alert, AnomalyRecord, User, UserRole
from app.rbac import require_role

router = APIRouter(tags=["anomalies"])

INFLUXDB_URL   = os.getenv("INFLUXDB_URL",   "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG   = os.getenv("INFLUXDB_ORG",   "pms-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor-data")

ENGINE_MEASUREMENT = "sensor_telemetry"
MAX_HISTORY = 500
DEFAULT_HISTORY = 100


def _get_influx() -> InfluxDBClient:
    if not INFLUXDB_TOKEN:
        raise HTTPException(status_code=500, detail="INFLUXDB_TOKEN not configured")
    return InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _flux_time(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _fetch_telemetry(
    client: InfluxDBClient,
    dataset_id: str,
    unit_id: str,
    run_id: str,
    start: datetime,
    stop: datetime,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Pivot sensor telemetry from InfluxDB into logical records."""
    sensors_filter = " or ".join(
        f'r._field == "{s}"' for s in (sorted(ZSCORE_SENSORS) + ["time_cycles"])
    )
    query = f'''from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: {_flux_time(start)}, stop: {_flux_time(stop)})
  |> filter(fn: (r) => r._measurement == "{ENGINE_MEASUREMENT}"
       and r.dataset_id == "{dataset_id}"
       and r.unit_id == "{unit_id}"
       and r.run_id == "{run_id}"
       and ({sensors_filter}))
  |> pivot(rowKey: ["_time", "dataset_id", "unit_id", "run_id", "cycle"],
           columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
  |> limit(n: {limit})'''

    records = []
    for table in client.query_api().query(query):
        for rec in table.records:
            row = dict(rec.values)
            row["timestamp"] = rec.get_time().astimezone(timezone.utc).isoformat()
            row["cycle"] = int(float(row.get("time_cycles") or row.get("cycle") or 0))
            records.append(row)
    return records


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/api/anomalies")
def list_anomalies(
    dataset_id: str = Query(...),
    unit_id: str = Query(...),
    run_id: str | None = Query(default=None),
    limit: int = Query(default=DEFAULT_HISTORY, ge=1, le=MAX_HISTORY),
    offset: int = Query(default=0, ge=0),
    severity: str | None = Query(default=None),
    db: Session = Depends(get_db),
    user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.FLOOR_OPERATOR, UserRole.VIEWER)),
) -> dict[str, Any]:
    q = db.query(AnomalyRecord).filter(
        AnomalyRecord.dataset_id == dataset_id,
        AnomalyRecord.unit_id == unit_id,
    )
    if run_id:
        q = q.filter(AnomalyRecord.run_id == run_id)
    if severity:
        q = q.filter(AnomalyRecord.severity == severity)
    total = q.count()
    rows = q.order_by(AnomalyRecord.cycle.desc()).offset(offset).limit(limit).all()
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "anomalies": [_anomaly_out(r) for r in rows],
    }


@router.post("/api/anomalies/detect")
def detect_and_persist(
    dataset_id: str = Query(...),
    unit_id: str = Query(...),
    run_id: str = Query(...),
    lookback_hours: float = Query(default=48.0, ge=0.5, le=720.0),
    db: Session = Depends(get_db),
    user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER)),
) -> dict[str, Any]:
    """Fetch stored telemetry for the given engine/run, run anomaly detection,
    persist new events, and return a summary."""
    stop  = _utc_now() + timedelta(hours=24)   # cover future-dated test data
    start = stop - timedelta(hours=lookback_hours + 24)

    client = _get_influx()
    try:
        records = _fetch_telemetry(client, dataset_id, unit_id, run_id, start, stop)
    finally:
        client.close()

    if not records:
        return {"message": "No telemetry found for the given parameters.", "new_events": 0, "anomalies": []}

    events = detect_anomalies(records, dataset_id, unit_id, run_id)

    new_count = 0
    persisted = []
    for ev in events:
        existing = db.get(AnomalyRecord, ev["event_id"])
        if existing is None:
            rec = AnomalyRecord(
                event_id=ev["event_id"],
                dataset_id=ev["dataset_id"],
                unit_id=ev["unit_id"],
                run_id=ev["run_id"],
                cycle=ev["cycle"],
                timestamp=ev["timestamp"],
                sensor=ev["sensor"],
                detection_method=ev["detection_method"],
                observed_value=ev["observed_value"],
                severity=ev["severity"],
                score=ev["score"],
                status="open",
            )
            db.add(rec)
            new_count += 1
            persisted.append(ev)

    db.commit()

    return {
        "message": f"Detection complete. {new_count} new anomaly events persisted.",
        "new_events": new_count,
        "total_detected": len(events),
        "anomalies": persisted[:50],
    }


@router.get("/api/anomalies/{event_id}")
def get_anomaly(
    event_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.FLOOR_OPERATOR, UserRole.VIEWER)),
) -> dict[str, Any]:
    rec = db.get(AnomalyRecord, event_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Anomaly event not found")
    return _anomaly_out(rec)


def _anomaly_out(rec: AnomalyRecord) -> dict[str, Any]:
    return {
        "event_id": rec.event_id,
        "dataset_id": rec.dataset_id,
        "unit_id": rec.unit_id,
        "run_id": rec.run_id,
        "cycle": rec.cycle,
        "timestamp": rec.timestamp,
        "sensor": rec.sensor,
        "detection_method": rec.detection_method,
        "observed_value": rec.observed_value,
        "severity": rec.severity,
        "score": rec.score,
        "status": rec.status,
        "created_at": rec.created_at.isoformat() if rec.created_at else None,
    }
