"""
Module 9 — Alerting and Notifications
Persisted, role-restricted alert management API.

Alert lifecycle:
  open  →  acknowledged  →  resolved

RBAC:
  - All authenticated roles: read alerts and history.
  - MAINTENANCE_ENGINEER and ADMIN: acknowledge alerts.
  - ADMIN only: resolve alerts, manual trigger.
  - VIEWER: HTTP 403 on any state-change endpoint.

Deduplication:
  - Each alert has a `dedup_key` (hash of dataset_id|unit_id|run_id|type).
  - Creating an alert for a key that already has an open alert is a no-op.
  - Cooldown: a resolved alert for the same key will not re-open for
    COOLDOWN_MINUTES unless the cycle has advanced significantly.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from influxdb_client import InfluxDBClient
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.anomaly_detector import evaluate_rul_alert
from app.database import get_db
from app.models import Alert, AlertAudit, AnomalyRecord, User, UserRole
from app.rbac import require_role

router = APIRouter(tags=["alerts"])

INFLUXDB_URL    = os.getenv("INFLUXDB_URL",    "http://influxdb:8086")
INFLUXDB_TOKEN  = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG    = os.getenv("INFLUXDB_ORG",    "pms-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor-data")

RUL_CRITICAL_THRESHOLD: float = float(os.getenv("ALERT_RUL_CRITICAL", "30"))
RUL_WARNING_THRESHOLD:  float = float(os.getenv("ALERT_RUL_WARNING",  "60"))
COOLDOWN_MINUTES: int = int(os.getenv("ALERT_COOLDOWN_MINUTES", "60"))

MAX_PAGE = 200
DEFAULT_PAGE = 50


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class AcknowledgeRequest(BaseModel):
    note: str | None = None


class ResolveRequest(BaseModel):
    note: str | None = None


class ManualRulEvalRequest(BaseModel):
    dataset_id: str
    unit_id: str
    run_id: str
    cycle: int
    predicted_rul: float
    timestamp: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.utcnow()


def _dedup_key(dataset_id: str, unit_id: str, run_id: str, alert_type: str) -> str:
    raw = f"{dataset_id}|{unit_id}|{run_id}|{alert_type}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def _alert_out(a: Alert) -> dict[str, Any]:
    return {
        "id": a.id,
        "alert_type": a.alert_type,
        "severity": a.severity,
        "dataset_id": a.dataset_id,
        "unit_id": a.unit_id,
        "run_id": a.run_id,
        "cycle": a.cycle,
        "message": a.message,
        "status": a.status,
        "source_event_id": a.source_event_id,
        "created_at": a.created_at.isoformat() if a.created_at else None,
        "acknowledged_at": a.acknowledged_at.isoformat() if a.acknowledged_at else None,
        "acknowledged_by": a.acknowledged_by,
        "resolved_at": a.resolved_at.isoformat() if a.resolved_at else None,
    }


def _can_create_alert(db: Session, dedup_key: str, cooldown_minutes: int) -> bool:
    """Return True if no open/acknowledged alert exists for this dedup_key
    and the cooldown window for the most recent resolved one has elapsed."""
    open_alert = (
        db.query(Alert)
        .filter(Alert.dedup_key == dedup_key, Alert.status.in_(["open", "acknowledged"]))
        .first()
    )
    if open_alert:
        return False  # duplicate suppression

    # Check cooldown on resolved alerts
    cutoff = _now() - timedelta(minutes=cooldown_minutes)
    recent_resolved = (
        db.query(Alert)
        .filter(
            Alert.dedup_key == dedup_key,
            Alert.status == "resolved",
            Alert.resolved_at >= cutoff,
        )
        .first()
    )
    return recent_resolved is None


def _create_alert(
    db: Session,
    alert_type: str,
    severity: str,
    dataset_id: str,
    unit_id: str,
    run_id: str,
    cycle: int | None,
    message: str,
    source_event_id: str | None = None,
) -> Alert | None:
    """Idempotent alert creation — returns None if dedup suppresses it."""
    dk = _dedup_key(dataset_id, unit_id, run_id, alert_type)
    if not _can_create_alert(db, dk, COOLDOWN_MINUTES):
        return None

    alert = Alert(
        id=str(uuid.uuid4()),
        dedup_key=dk,
        alert_type=alert_type,
        severity=severity,
        dataset_id=dataset_id,
        unit_id=unit_id,
        run_id=run_id,
        cycle=cycle,
        message=message,
        status="open",
        source_event_id=source_event_id,
    )
    db.add(alert)
    db.add(AlertAudit(
        alert_id=alert.id,
        user_id=None,
        action="created",
        detail=message[:500],
    ))
    db.commit()
    db.refresh(alert)
    return alert


# ---------------------------------------------------------------------------
# Alert READ endpoints (all roles)
# ---------------------------------------------------------------------------

@router.get("/api/alerts")
def list_alerts(
    dataset_id: str | None = Query(default=None),
    unit_id: str | None = Query(default=None),
    run_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    alert_type: str | None = Query(default=None),
    limit: int = Query(default=DEFAULT_PAGE, ge=1, le=MAX_PAGE),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
    user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.FLOOR_OPERATOR, UserRole.VIEWER)),
) -> dict[str, Any]:
    q = db.query(Alert)
    if dataset_id:
        q = q.filter(Alert.dataset_id == dataset_id)
    if unit_id:
        q = q.filter(Alert.unit_id == unit_id)
    if run_id:
        q = q.filter(Alert.run_id == run_id)
    if status:
        q = q.filter(Alert.status == status)
    if severity:
        q = q.filter(Alert.severity == severity)
    if alert_type:
        q = q.filter(Alert.alert_type == alert_type)

    total = q.count()
    rows = q.order_by(Alert.created_at.desc()).offset(offset).limit(limit).all()
    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "alerts": [_alert_out(a) for a in rows],
    }


@router.get("/api/alerts/{alert_id}")
def get_alert(
    alert_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.FLOOR_OPERATOR, UserRole.VIEWER)),
) -> dict[str, Any]:
    a = db.get(Alert, alert_id)
    if not a:
        raise HTTPException(status_code=404, detail="Alert not found")
    return _alert_out(a)


@router.get("/api/alerts/{alert_id}/audit")
def get_alert_audit(
    alert_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.FLOOR_OPERATOR, UserRole.VIEWER)),
) -> dict[str, Any]:
    a = db.get(Alert, alert_id)
    if not a:
        raise HTTPException(status_code=404, detail="Alert not found")
    rows = (
        db.query(AlertAudit)
        .filter(AlertAudit.alert_id == alert_id)
        .order_by(AlertAudit.timestamp)
        .all()
    )
    return {
        "alert_id": alert_id,
        "audit": [
            {
                "action": r.action,
                "user_id": r.user_id,
                "detail": r.detail,
                "timestamp": r.timestamp.isoformat() if r.timestamp else None,
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# Alert STATE-CHANGE endpoints (restricted roles)
# ---------------------------------------------------------------------------

@router.post("/api/alerts/{alert_id}/acknowledge")
def acknowledge_alert(
    alert_id: str,
    payload: AcknowledgeRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER)),
) -> dict[str, Any]:
    """VIEWER → HTTP 403. MAINTENANCE_ENGINEER and ADMIN → allowed."""
    a = db.get(Alert, alert_id)
    if not a:
        raise HTTPException(status_code=404, detail="Alert not found")
    if a.status != "open":
        raise HTTPException(
            status_code=409,
            detail=f"Alert is already '{a.status}'. Only open alerts can be acknowledged.",
        )

    a.status = "acknowledged"
    a.acknowledged_at = _now()
    a.acknowledged_by = user.id
    db.add(AlertAudit(
        alert_id=a.id,
        user_id=user.id,
        action="acknowledged",
        detail=payload.note,
    ))
    db.commit()
    db.refresh(a)
    return _alert_out(a)


@router.post("/api/alerts/{alert_id}/resolve")
def resolve_alert(
    alert_id: str,
    payload: ResolveRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_role(UserRole.ADMIN)),
) -> dict[str, Any]:
    """ADMIN only."""
    a = db.get(Alert, alert_id)
    if not a:
        raise HTTPException(status_code=404, detail="Alert not found")
    if a.status == "resolved":
        raise HTTPException(status_code=409, detail="Alert is already resolved.")

    a.status = "resolved"
    a.resolved_at = _now()
    db.add(AlertAudit(
        alert_id=a.id,
        user_id=user.id,
        action="resolved",
        detail=payload.note,
    ))
    db.commit()
    db.refresh(a)
    return _alert_out(a)


# ---------------------------------------------------------------------------
# Alert CREATION endpoints
# ---------------------------------------------------------------------------

@router.post("/api/alerts/evaluate-rul")
def evaluate_rul_and_alert(
    payload: ManualRulEvalRequest,
    db: Session = Depends(get_db),
    user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER)),
) -> dict[str, Any]:
    """Evaluate a known RUL prediction against thresholds and create an
    alert if warranted. Idempotent — safe to call multiple times."""
    ts = payload.timestamp or datetime.now(timezone.utc).isoformat()
    alert_dict = evaluate_rul_alert(
        run_id=payload.run_id,
        dataset_id=payload.dataset_id,
        unit_id=payload.unit_id,
        cycle=payload.cycle,
        predicted_rul=payload.predicted_rul,
        timestamp=ts,
        critical_threshold=RUL_CRITICAL_THRESHOLD,
        warning_threshold=RUL_WARNING_THRESHOLD,
    )

    if alert_dict is None:
        return {
            "triggered": False,
            "message": (
                f"RUL {payload.predicted_rul:.1f} cycles is healthy "
                f"(warning threshold: {RUL_WARNING_THRESHOLD}, critical: {RUL_CRITICAL_THRESHOLD})."
            ),
        }

    alert = _create_alert(
        db,
        alert_type="rul_low",
        severity=alert_dict["severity"],
        dataset_id=payload.dataset_id,
        unit_id=str(payload.unit_id),
        run_id=payload.run_id,
        cycle=payload.cycle,
        message=alert_dict["message"],
    )
    if alert is None:
        return {
            "triggered": False,
            "message": "Alert suppressed — an equivalent open alert already exists (dedup/cooldown).",
        }

    return {"triggered": True, "alert": _alert_out(alert)}


@router.post("/api/alerts/from-anomaly/{event_id}")
def alert_from_anomaly(
    event_id: str,
    db: Session = Depends(get_db),
    user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER)),
) -> dict[str, Any]:
    """Bridge: promote a persisted anomaly event into an alert.
    Idempotent — returns existing alert if one already exists for this anomaly."""
    anomaly = db.get(AnomalyRecord, event_id)
    if not anomaly:
        raise HTTPException(status_code=404, detail="Anomaly event not found")

    message = (
        f"Anomaly detected on {anomaly.sensor} for engine "
        f"{anomaly.dataset_id}-{anomaly.unit_id} (run {anomaly.run_id}), "
        f"cycle {anomaly.cycle}. Method: {anomaly.detection_method}. "
        f"Observed: {anomaly.observed_value:.4f} (score: {anomaly.score:.4f}). "
        f"Severity: {anomaly.severity}."
    )

    # Check if an alert already exists for this exact anomaly event
    existing = (
        db.query(Alert)
        .filter(Alert.source_event_id == event_id)
        .first()
    )
    if existing:
        return {"triggered": False, "message": "Alert already exists for this anomaly.", "alert": _alert_out(existing)}

    alert = _create_alert(
        db,
        alert_type="anomaly",
        severity=anomaly.severity,
        dataset_id=anomaly.dataset_id,
        unit_id=anomaly.unit_id,
        run_id=anomaly.run_id,
        cycle=anomaly.cycle,
        message=message,
        source_event_id=event_id,
    )
    if alert is None:
        return {"triggered": False, "message": "Alert suppressed by dedup/cooldown."}

    return {"triggered": True, "alert": _alert_out(alert)}
