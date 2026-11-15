from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.models import UserRole
from app.rbac import get_current_user
from app.routers import rul


class Record:
    def __init__(self, values, timestamp=None):
        self.values = values
        self.timestamp = timestamp or datetime.now(timezone.utc)

    def get_time(self):
        return self.timestamp


class QueryApi:
    def __init__(self, responses=(), error=None):
        self.responses = list(responses)
        self.error = error
        self.queries = []

    def query(self, query):
        self.queries.append(query)
        if self.error:
            raise self.error
        return [SimpleNamespace(records=self.responses.pop(0) if self.responses else [])]


class InfluxClient:
    def __init__(self, responses=(), error=None):
        self.query_api_instance = QueryApi(responses, error)

    def query_api(self):
        return self.query_api_instance

    def close(self):
        pass


def prediction(run_id="run-new", unit_id="7.0", minutes_old=0, rul_value=87.5):
    return Record(
        {
            "dataset_id": "FD002",
            "unit_id": unit_id,
            "run_id": run_id,
            "rul": rul_value,
            "cycle": 140,
            "window_size": 30,
            "model_version": "lstm-v1",
            "preprocessing_artifact_version": "artifact-v1",
        },
        datetime.now(timezone.utc) - timedelta(minutes=minutes_old),
    )


@pytest.fixture()
def app(monkeypatch):
    application = FastAPI()
    application.include_router(rul.router)
    influx_client = InfluxClient([[prediction()]])
    monkeypatch.setattr(rul, "get_client", lambda: influx_client)
    application.state.influx_client = influx_client
    return application


def allow_role(app, role):
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(role=role)


def test_rul_routes_require_jwt(app):
    client = TestClient(app)
    assert client.get("/api/engines/FD001-007/rul/latest").status_code == 401
    assert client.get("/api/engines/FD001-007/rul/history").status_code == 401


@pytest.mark.parametrize("role", [UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.VIEWER])
def test_latest_rul_allows_dashboard_roles_and_normalizes_float_unit_tag(app, role):
    allow_role(app, role)
    response = TestClient(app).get("/api/engines/FD002-007/rul/latest")
    assert response.status_code == 200
    payload = response.json()
    assert payload["engine_id"] == "FD002-007"
    stored = payload["prediction"]
    assert stored["rul_cycles"] == 87.5 and stored["cycle"] == 140
    assert stored["timestamp"] and stored["run_id"] == "run-new"
    assert stored["window_size"] == 30 and stored["model_version"] == "lstm-v1"
    assert stored["preprocessing_artifact_version"] == "artifact-v1"
    query = app.state.influx_client.query_api_instance.queries[0]
    assert 'r.unit_id == "7.0"' in query
    assert 'range(start: -30d)' in query and 'limit(n: 1)' in query
    assert 'pivot(' in query and 'exists r.rul' in query


def test_history_uses_latest_run_only_and_bounds_response(app, monkeypatch):
    allow_role(app, UserRole.VIEWER)
    app.state.influx_client = InfluxClient([
        [prediction(run_id="run-new", minutes_old=1)],
        [prediction(run_id="run-new", minutes_old=2), prediction(run_id="run-new", minutes_old=3, rul_value=86.0)],
    ])
    monkeypatch.setattr(rul, "get_client", lambda: app.state.influx_client)
    response = TestClient(app).get("/api/engines/FD002-007/rul/history?points=1&run_id=run-new")
    assert response.status_code == 200
    payload = response.json()
    assert payload["engine_id"] == "FD002-007" and payload["run_id"] == "run-new"
    assert len(payload["points"]) == 1
    latest_query, history_query = app.state.influx_client.query_api_instance.queries
    assert 'r.run_id == "run-new"' in history_query
    assert 'limit(n: 1)' in latest_query and 'limit(n: 1)' in history_query
    assert 'range(start:' in history_query and 'stop:' in history_query


@pytest.mark.parametrize("path", [
    "/api/engines/7/rul/latest",
    "/api/engines/FD005-007/rul/latest",
    "/api/engines/FD001-007;drop/rul/latest",
    "/api/engines/FD001-007/rul/history?points=1001",
    "/api/engines/FD001-007/rul/history?points=0",
    "/api/engines/FD001-007/rul/history?run_id=bad%22run",
    "/api/engines/FD001-007/rul/history?from=2026-10-30T12:00:00Z&to=2026-10-30T11:00:00Z",
    "/api/engines/FD001-007/rul/history?from=2026-09-01T00:00:00Z&to=2026-10-30T00:00:00Z",
])
def test_rul_rejects_invalid_or_unbounded_inputs(app, path):
    allow_role(app, UserRole.ADMIN)
    assert TestClient(app).get(path).status_code == 422


def test_rul_empty_and_storage_failure_are_not_replaced_with_mock_data(app, monkeypatch):
    allow_role(app, UserRole.ADMIN)
    app.state.influx_client = InfluxClient([[]])
    monkeypatch.setattr(rul, "get_client", lambda: app.state.influx_client)
    client = TestClient(app, raise_server_exceptions=False)
    assert client.get("/api/engines/FD002-007/rul/latest").json()["prediction"] is None
    app.state.influx_client = InfluxClient(error=RuntimeError("Influx unavailable"))
    monkeypatch.setattr(rul, "get_client", lambda: app.state.influx_client)
    assert client.get("/api/engines/FD002-007/rul/latest").status_code == 500
