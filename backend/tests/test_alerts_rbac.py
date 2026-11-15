"""
Module 9 — Integration tests for the alerts API and RBAC enforcement.

Uses SQLite in-memory DB and TestClient (no Docker required).

Tests cover:
 - Viewer cannot acknowledge or resolve alerts (HTTP 403)
 - Maintenance Engineer can acknowledge (HTTP 200)
 - Admin can resolve (HTTP 200)
 - Unauthenticated requests rejected (HTTP 401)
 - Idempotent alert creation (dedup)
 - evaluate-rul endpoint: healthy RUL returns triggered=False
 - evaluate-rul endpoint: critical RUL returns triggered=True
 - Alert audit trail populated on acknowledge
 - Alert-from-anomaly bridge
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base, get_db
from app.models import Alert, AlertAudit, AnomalyRecord, UserRole
from app.rbac import get_current_user
from app.routers import alerts as alerts_module

# ---------------------------------------------------------------------------
# Test fixtures — isolated SQLite
# ---------------------------------------------------------------------------

TEST_DB_URL = "sqlite:///./test_alerts_temp.db"

@pytest.fixture(scope="module")
def db_engine():
    engine = create_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)
    engine.dispose()
    try:
        os.remove("test_alerts_temp.db")
    except FileNotFoundError:
        pass


@pytest.fixture()
def db_session(db_engine):
    Session = sessionmaker(bind=db_engine)
    session = Session()
    yield session
    session.rollback()
    # Clean tables between tests
    for table in reversed(Base.metadata.sorted_tables):
        session.execute(table.delete())
    session.commit()
    session.close()


@pytest.fixture()
def app_factory(db_engine):
    """Returns a factory that creates a TestClient with a given role override."""
    def _make(role: UserRole | None = None):
        application = FastAPI()
        application.include_router(alerts_module.router)

        # Override DB session
        def override_db():
            Session = sessionmaker(bind=db_engine)
            s = Session()
            try:
                yield s
            finally:
                s.close()

        application.dependency_overrides[get_db] = override_db

        if role is not None:
            application.dependency_overrides[get_current_user] = lambda: SimpleNamespace(
                id="test-user-id", role=role, email="test@example.com"
            )
        return TestClient(application, raise_server_exceptions=True)

    return _make


# ---------------------------------------------------------------------------
# Helper: seed an alert and an anomaly record
# ---------------------------------------------------------------------------

def seed_alert(db_session, alert_id: str = "alert-001", status: str = "open") -> Alert:
    a = Alert(
        id=alert_id,
        dedup_key=f"dedup-{alert_id}",
        alert_type="rul_low",
        severity="critical",
        dataset_id="FD001",
        unit_id="1",
        run_id="run-test",
        cycle=50,
        message="Test alert",
        status=status,
    )
    db_session.add(a)
    db_session.commit()
    return a


def seed_anomaly(db_session, event_id: str = "anom-001") -> AnomalyRecord:
    r = AnomalyRecord(
        event_id=event_id,
        dataset_id="FD001",
        unit_id="1",
        run_id="run-test",
        cycle=30,
        timestamp="2026-11-12T00:15:29Z",
        sensor="sensor_2",
        detection_method="threshold",
        observed_value=800.0,
        severity="high",
        score=3.0,
        status="open",
    )
    db_session.add(r)
    db_session.commit()
    return r


# ---------------------------------------------------------------------------
# RBAC: Unauthenticated
# ---------------------------------------------------------------------------

def test_list_alerts_requires_auth(app_factory):
    client = app_factory(role=None)
    response = client.get("/api/alerts")
    assert response.status_code == 401


def test_acknowledge_requires_auth(app_factory):
    client = app_factory(role=None)
    response = client.post("/api/alerts/any-id/acknowledge", json={})
    assert response.status_code == 401


# ---------------------------------------------------------------------------
# RBAC: Viewer — read allowed, write denied
# ---------------------------------------------------------------------------

def test_viewer_can_list_alerts(app_factory):
    client = app_factory(role=UserRole.VIEWER)
    response = client.get("/api/alerts")
    assert response.status_code == 200
    assert "alerts" in response.json()


def test_viewer_cannot_acknowledge(app_factory, db_session):
    seed_alert(db_session, "alert-viewer-ack")
    client = app_factory(role=UserRole.VIEWER)
    response = client.post("/api/alerts/alert-viewer-ack/acknowledge", json={})
    assert response.status_code == 403


def test_viewer_cannot_resolve(app_factory, db_session):
    seed_alert(db_session, "alert-viewer-res")
    client = app_factory(role=UserRole.VIEWER)
    response = client.post("/api/alerts/alert-viewer-res/resolve", json={})
    assert response.status_code == 403


def test_viewer_cannot_evaluate_rul(app_factory):
    client = app_factory(role=UserRole.VIEWER)
    response = client.post("/api/alerts/evaluate-rul", json={
        "dataset_id": "FD001", "unit_id": "1", "run_id": "run-x",
        "cycle": 40, "predicted_rul": 15.0,
    })
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# RBAC: Maintenance Engineer — can acknowledge, cannot resolve
# ---------------------------------------------------------------------------

def test_engineer_can_acknowledge(app_factory, db_session):
    seed_alert(db_session, "alert-eng-ack")
    client = app_factory(role=UserRole.MAINTENANCE_ENGINEER)
    response = client.post("/api/alerts/alert-eng-ack/acknowledge", json={"note": "Looked into it"})
    assert response.status_code == 200
    assert response.json()["status"] == "acknowledged"


def test_engineer_cannot_resolve(app_factory, db_session):
    seed_alert(db_session, "alert-eng-res")
    client = app_factory(role=UserRole.MAINTENANCE_ENGINEER)
    response = client.post("/api/alerts/alert-eng-res/resolve", json={})
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# RBAC: Admin — full access
# ---------------------------------------------------------------------------

def test_admin_can_acknowledge_and_resolve(app_factory, db_session):
    seed_alert(db_session, "alert-admin-flow")
    client = app_factory(role=UserRole.ADMIN)

    ack = client.post("/api/alerts/alert-admin-flow/acknowledge", json={"note": "Admin ack"})
    assert ack.status_code == 200
    assert ack.json()["status"] == "acknowledged"

    res = client.post("/api/alerts/alert-admin-flow/resolve", json={"note": "Admin resolve"})
    assert res.status_code == 200
    assert res.json()["status"] == "resolved"


def test_audit_trail_populated(app_factory, db_session):
    seed_alert(db_session, "alert-audit")
    client = app_factory(role=UserRole.ADMIN)
    client.post("/api/alerts/alert-audit/acknowledge", json={"note": "checking"})

    audit_resp = client.get("/api/alerts/alert-audit/audit")
    assert audit_resp.status_code == 200
    actions = [entry["action"] for entry in audit_resp.json()["audit"]]
    assert "acknowledged" in actions


# ---------------------------------------------------------------------------
# Alert creation: evaluate-rul
# ---------------------------------------------------------------------------

def test_healthy_rul_does_not_trigger(app_factory):
    """Acceptance run RUL ≈ 121.6 — MUST NOT trigger any alert."""
    client = app_factory(role=UserRole.ADMIN)
    response = client.post("/api/alerts/evaluate-rul", json={
        "dataset_id": "FD001",
        "unit_id": "1",
        "run_id": "acceptance-fd001-unit1-20261112T0015Z",
        "cycle": 40,
        "predicted_rul": 121.645,
        "timestamp": "2026-11-12T00:15:39Z",
    })
    assert response.status_code == 200
    body = response.json()
    assert body["triggered"] is False
    assert "healthy" in body["message"].lower() or "threshold" in body["message"].lower()


def test_critical_rul_triggers_alert(app_factory):
    client = app_factory(role=UserRole.ADMIN)
    response = client.post("/api/alerts/evaluate-rul", json={
        "dataset_id": "FD001",
        "unit_id": "99",
        "run_id": "run-critical-test",
        "cycle": 200,
        "predicted_rul": 10.0,
        "timestamp": "2026-11-12T00:00:00Z",
    })
    assert response.status_code == 200
    body = response.json()
    assert body["triggered"] is True
    assert body["alert"]["severity"] == "critical"


def test_duplicate_rul_alert_suppressed(app_factory):
    """Second identical call within cooldown window → triggered=False."""
    client = app_factory(role=UserRole.ADMIN)
    payload = {
        "dataset_id": "FD001",
        "unit_id": "88",
        "run_id": "run-dedup-test",
        "cycle": 150,
        "predicted_rul": 20.0,
        "timestamp": "2026-11-12T00:00:00Z",
    }
    r1 = client.post("/api/alerts/evaluate-rul", json=payload)
    assert r1.json()["triggered"] is True

    # Second call — same dedup_key — should be suppressed
    payload["cycle"] = 151
    r2 = client.post("/api/alerts/evaluate-rul", json=payload)
    assert r2.json()["triggered"] is False


# ---------------------------------------------------------------------------
# Alert-from-anomaly bridge
# ---------------------------------------------------------------------------

def test_alert_from_anomaly(app_factory, db_session):
    seed_anomaly(db_session, "anom-bridge-001")
    client = app_factory(role=UserRole.ADMIN)
    response = client.post("/api/alerts/from-anomaly/anom-bridge-001")
    assert response.status_code == 200
    body = response.json()
    assert body["triggered"] is True
    assert body["alert"]["source_event_id"] == "anom-bridge-001"
    assert body["alert"]["alert_type"] == "anomaly"


def test_alert_from_anomaly_idempotent(app_factory, db_session):
    """Calling from-anomaly twice with the same event_id returns existing alert."""
    seed_anomaly(db_session, "anom-idem-001")
    client = app_factory(role=UserRole.ADMIN)
    r1 = client.post("/api/alerts/from-anomaly/anom-idem-001")
    r2 = client.post("/api/alerts/from-anomaly/anom-idem-001")
    assert r1.status_code == 200
    assert r2.status_code == 200
    # Second call should be suppressed (same alert returned, not duplicated)
    assert r2.json()["triggered"] is False


def test_alert_from_missing_anomaly(app_factory):
    client = app_factory(role=UserRole.ADMIN)
    response = client.post("/api/alerts/from-anomaly/nonexistent-id")
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# Filtering and pagination
# ---------------------------------------------------------------------------

def test_list_alerts_filtered_by_status(app_factory, db_session):
    seed_alert(db_session, "alert-open-1", "open")
    seed_alert(db_session, "alert-ack-1", "acknowledged")
    client = app_factory(role=UserRole.VIEWER)

    open_resp = client.get("/api/alerts?status=open")
    assert open_resp.status_code == 200
    assert all(a["status"] == "open" for a in open_resp.json()["alerts"])

    ack_resp = client.get("/api/alerts?status=acknowledged")
    assert ack_resp.status_code == 200
    assert all(a["status"] == "acknowledged" for a in ack_resp.json()["alerts"])


def test_acknowledge_already_acknowledged_returns_409(app_factory, db_session):
    seed_alert(db_session, "alert-double-ack", "acknowledged")
    client = app_factory(role=UserRole.ADMIN)
    response = client.post("/api/alerts/alert-double-ack/acknowledge", json={})
    assert response.status_code == 409
