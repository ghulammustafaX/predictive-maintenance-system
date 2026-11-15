"""
Module 8 — Four-role RBAC enforcement tests.

Tests cover:
 - Registration always creates VIEWER (self-promotion blocked)
 - Floor Operator can read alerts/anomalies (200)
 - Floor Operator cannot acknowledge/resolve/evaluate-rul (403)
 - Floor Operator cannot trigger anomaly detection (403)
 - Floor Operator cannot access user management (403)
 - Admin can list users and change roles
 - Admin cannot change own role
 - Non-admin cannot access user management (403)
 - Viewer is strictly read-only (403 on all write actions)
"""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base, get_db
from app.models import User, UserRole
from app.rbac import get_current_user
from app.routers import alerts as alerts_module
from app.routers import auth as auth_module
from app.routers import anomalies as anomalies_module
from app.security import hash_password

TEST_DB_URL = "sqlite:///./test_rbac_temp.db"


@pytest.fixture(scope="module")
def rbac_engine():
    engine = create_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)
    engine.dispose()
    try:
        os.remove("test_rbac_temp.db")
    except FileNotFoundError:
        pass


@pytest.fixture()
def rbac_db(rbac_engine):
    Session = sessionmaker(bind=rbac_engine)
    session = Session()
    yield session
    session.rollback()
    for table in reversed(Base.metadata.sorted_tables):
        session.execute(table.delete())
    session.commit()
    session.close()


@pytest.fixture()
def make_app(rbac_engine):
    """Factory that creates a TestClient with a given role and optional DB users."""
    def _make(role: UserRole | None = None, db_users: list[dict] | None = None):
        application = FastAPI()
        application.include_router(auth_module.router)
        application.include_router(alerts_module.router)
        application.include_router(anomalies_module.router)

        def override_db():
            Session = sessionmaker(bind=rbac_engine)
            s = Session()
            try:
                yield s
            finally:
                s.close()

        application.dependency_overrides[get_db] = override_db

        if role is not None:
            application.dependency_overrides[get_current_user] = lambda: SimpleNamespace(
                id="test-admin-id", role=role, email="admin@test.com"
            )

        client = TestClient(application, raise_server_exceptions=True)

        if db_users:
            Session = sessionmaker(bind=rbac_engine)
            s = Session()
            for u in db_users:
                s.add(User(
                    id=u.get("id", f"user-{u['email']}"),
                    email=u["email"],
                    full_name=u.get("full_name", "Test User"),
                    hashed_password=hash_password(u.get("password", "TestPass123!")),
                    role=u.get("role", UserRole.VIEWER),
                    is_verified=True,
                    is_active=True,
                ))
            s.commit()
            s.close()

        return client

    return _make


# ---------------------------------------------------------------------------
# Self-promotion prevention
# ---------------------------------------------------------------------------

class TestSelfPromotionPrevention:
    def test_register_with_admin_role_creates_viewer(self, make_app):
        """Security: sending role=admin in the request body must NOT grant admin."""
        client = make_app()
        response = client.post("/api/auth/register", json={
            "email": "attacker@test.com",
            "full_name": "Attacker",
            "password": "TestPass123!",
            "role": "admin",  # attempt self-promotion
        })
        assert response.status_code == 201
        body = response.json()
        assert body["role"] == "viewer", "Self-promotion must be blocked — role must always be viewer"

    def test_register_with_engineer_role_creates_viewer(self, make_app):
        client = make_app()
        response = client.post("/api/auth/register", json={
            "email": "user2@test.com",
            "full_name": "Test",
            "password": "TestPass123!",
            "role": "maintenance_engineer",
        })
        assert response.status_code == 201
        assert response.json()["role"] == "viewer"

    def test_register_with_floor_operator_role_creates_viewer(self, make_app):
        client = make_app()
        response = client.post("/api/auth/register", json={
            "email": "user3@test.com",
            "full_name": "Test",
            "password": "TestPass123!",
            "role": "floor_operator",
        })
        assert response.status_code == 201
        assert response.json()["role"] == "viewer"

    def test_register_default_role_is_viewer(self, make_app):
        client = make_app()
        response = client.post("/api/auth/register", json={
            "email": "user4@test.com",
            "full_name": "Test",
            "password": "TestPass123!",
        })
        assert response.status_code == 201
        assert response.json()["role"] == "viewer"


# ---------------------------------------------------------------------------
# Floor Operator permissions
# ---------------------------------------------------------------------------

class TestFloorOperatorRole:
    def test_operator_can_read_alerts(self, make_app):
        client = make_app(role=UserRole.FLOOR_OPERATOR)
        response = client.get("/api/alerts")
        assert response.status_code == 200

    def test_operator_cannot_acknowledge(self, make_app, rbac_db):
        # Seed an alert
        from app.models import Alert
        a = Alert(
            id="alert-op-001", dedup_key="dk-op", alert_type="rul_low",
            severity="critical", dataset_id="FD001", unit_id="1",
            run_id="run-x", cycle=50, message="test", status="open",
        )
        rbac_db.add(a)
        rbac_db.commit()

        client = make_app(role=UserRole.FLOOR_OPERATOR)
        response = client.post("/api/alerts/alert-op-001/acknowledge", json={"note": "test"})
        assert response.status_code == 403

    def test_operator_cannot_resolve(self, make_app):
        client = make_app(role=UserRole.FLOOR_OPERATOR)
        response = client.post("/api/alerts/any-id/resolve", json={})
        assert response.status_code == 403

    def test_operator_cannot_evaluate_rul(self, make_app):
        client = make_app(role=UserRole.FLOOR_OPERATOR)
        response = client.post("/api/alerts/evaluate-rul", json={
            "dataset_id": "FD001", "unit_id": "1", "run_id": "run-x",
            "cycle": 10, "predicted_rul": 5.0,
        })
        assert response.status_code == 403

    def test_operator_cannot_trigger_detection(self, make_app):
        client = make_app(role=UserRole.FLOOR_OPERATOR)
        response = client.post("/api/anomalies/detect?dataset_id=FD001&unit_id=1&run_id=run-x")
        assert response.status_code == 403

    def test_operator_cannot_create_alert_from_anomaly(self, make_app):
        client = make_app(role=UserRole.FLOOR_OPERATOR)
        response = client.post("/api/alerts/from-anomaly/any-id")
        assert response.status_code == 403

    def test_operator_cannot_list_users(self, make_app):
        client = make_app(role=UserRole.FLOOR_OPERATOR)
        response = client.get("/api/auth/users")
        assert response.status_code == 403

    def test_operator_cannot_change_roles(self, make_app):
        client = make_app(role=UserRole.FLOOR_OPERATOR)
        response = client.patch("/api/auth/users/some-id/role", json={"role": "admin"})
        assert response.status_code == 403


# ---------------------------------------------------------------------------
# Viewer permissions (strictly read-only)
# ---------------------------------------------------------------------------

class TestViewerRole:
    def test_viewer_cannot_acknowledge(self, make_app, rbac_db):
        from app.models import Alert
        a = Alert(
            id="alert-v-001", dedup_key="dk-v", alert_type="rul_low",
            severity="critical", dataset_id="FD001", unit_id="1",
            run_id="run-x", cycle=50, message="test", status="open",
        )
        rbac_db.add(a)
        rbac_db.commit()
        client = make_app(role=UserRole.VIEWER)
        response = client.post("/api/alerts/alert-v-001/acknowledge", json={})
        assert response.status_code == 403

    def test_viewer_cannot_resolve(self, make_app):
        client = make_app(role=UserRole.VIEWER)
        response = client.post("/api/alerts/any-id/resolve", json={})
        assert response.status_code == 403

    def test_viewer_cannot_list_users(self, make_app):
        client = make_app(role=UserRole.VIEWER)
        response = client.get("/api/auth/users")
        assert response.status_code == 403

    def test_viewer_cannot_change_roles(self, make_app):
        client = make_app(role=UserRole.VIEWER)
        response = client.patch("/api/auth/users/some-id/role", json={"role": "viewer"})
        assert response.status_code == 403


# ---------------------------------------------------------------------------
# Admin user management
# ---------------------------------------------------------------------------

class TestAdminUserManagement:
    def test_admin_can_list_users(self, make_app, rbac_engine):
        # Seed a user
        Session = sessionmaker(bind=rbac_engine)
        s = Session()
        s.add(User(
            id="target-user", email="target@test.com", full_name="Target",
            hashed_password=hash_password("TestPass123!"), role=UserRole.VIEWER,
            is_verified=True, is_active=True,
        ))
        s.commit()
        s.close()

        client = make_app(role=UserRole.ADMIN)
        response = client.get("/api/auth/users")
        assert response.status_code == 200
        users = response.json()
        assert len(users) >= 1

    def test_admin_can_promote_user(self, make_app, rbac_engine):
        Session = sessionmaker(bind=rbac_engine)
        s = Session()
        s.add(User(
            id="promote-target", email="promote@test.com", full_name="Promote Me",
            hashed_password=hash_password("TestPass123!"), role=UserRole.VIEWER,
            is_verified=True, is_active=True,
        ))
        s.commit()
        s.close()

        client = make_app(role=UserRole.ADMIN)
        response = client.patch("/api/auth/users/promote-target/role", json={"role": "maintenance_engineer"})
        assert response.status_code == 200
        assert response.json()["role"] == "maintenance_engineer"

    def test_admin_cannot_change_own_role(self, make_app, rbac_engine):
        # Seed the admin user so the DB lookup succeeds before the self-check
        Session = sessionmaker(bind=rbac_engine)
        s = Session()
        s.add(User(
            id="test-admin-id", email="admin@test.com", full_name="Admin",
            hashed_password=hash_password("TestPass123!"), role=UserRole.ADMIN,
            is_verified=True, is_active=True,
        ))
        s.commit()
        s.close()

        client = make_app(role=UserRole.ADMIN)
        response = client.patch("/api/auth/users/test-admin-id/role", json={"role": "viewer"})
        assert response.status_code == 400
        assert "own role" in response.json()["detail"].lower()

    def test_non_admin_cannot_access_user_management(self, make_app):
        for role in [UserRole.MAINTENANCE_ENGINEER, UserRole.FLOOR_OPERATOR, UserRole.VIEWER]:
            client = make_app(role=role)
            response = client.get("/api/auth/users")
            assert response.status_code == 403, f"{role.value} should not access user management"

    def test_unauthenticated_user_management(self, make_app):
        client = make_app(role=None)
        response = client.get("/api/auth/users")
        assert response.status_code == 401
