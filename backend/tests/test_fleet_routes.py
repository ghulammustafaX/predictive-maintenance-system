from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.models import UserRole
from app.rbac import get_current_user
from app.routers import fleet


class Record:
    def __init__(self, values, timestamp=None):
        self.values = values
        self.timestamp = timestamp or datetime.now(timezone.utc)

    def get_time(self):
        return self.timestamp


class QueryApi:
    def __init__(self, records=(), error=None):
        self.records = records
        self.error = error
        self.queries = []

    def query(self, query):
        self.queries.append(query)
        if self.error:
            raise self.error
        return [SimpleNamespace(records=self.records)]


class InfluxClient:
    def __init__(self, records=(), error=None):
        self.query_api_instance = QueryApi(records, error)

    def query_api(self):
        return self.query_api_instance

    def close(self):
        pass


def record(values, minutes_old=0):
    return Record(values, datetime.now(timezone.utc) - timedelta(minutes=minutes_old))


@pytest.fixture()
def app(monkeypatch):
    application = FastAPI()
    application.include_router(fleet.router)
    influx_client = InfluxClient([record({"dataset_id": "FD002", "unit_id": "7", "_value": 120, "time_cycles": 120, "sensor_1": 501.2})])
    monkeypatch.setattr(fleet, "get_client", lambda: influx_client)
    application.state.influx_client = influx_client
    return application


def allow_role(app, role):
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(role=role)


def test_routes_require_jwt(app):
    client = TestClient(app)
    assert client.get("/api/fleet/status").status_code == 401
    assert client.get("/api/engines/FD001-007/telemetry").status_code == 401


@pytest.mark.parametrize("role", [UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.VIEWER])
def test_fleet_allows_every_dashboard_role_and_returns_contract(app, role):
    allow_role(app, role)
    response = TestClient(app).get("/api/fleet/status")
    assert response.status_code == 200
    payload = response.json()
    assert set(payload) == {"generated_at", "fleet"}
    engine = payload["fleet"][0]
    assert engine["engine_id"] == "FD002-007"
    assert engine["status"] == "healthy" and engine["latest_cycle"] == 120
    assert engine["rul_cycles"] is engine["rul_percent"] is None
    assert engine["active_anomaly_count"] == engine["active_alert_count"] == 0
    queries = app.state.influx_client.query_api_instance.queries
    assert len(queries) == 2
    assert 'range(start: -24h, stop:' in queries[0] and 'limit(n: 5000)' in queries[0]
    assert 'range(start:' in queries[1] and 'stop:' in queries[1] and 'limit(n: 5000)' in queries[1]
    assert all('exists r.dataset_id' in query and 'else "FD001"' in query for query in queries)


def test_fleet_uses_fd001_for_legacy_tagless_points(app):
    allow_role(app, UserRole.VIEWER)
    app.state.influx_client.query_api_instance.records = [record({"dataset_id": "FD001", "unit_id": "7", "_value": 55})]
    payload = TestClient(app).get("/api/fleet/status").json()
    assert payload["fleet"][0]["engine_id"] == "FD001-007"


@pytest.mark.parametrize("role", [UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.VIEWER])
def test_telemetry_allows_every_dashboard_role_and_caps_total_rows(app, role):
    allow_role(app, role)
    app.state.influx_client.query_api_instance.records = [
        record({"dataset_id": "FD002", "unit_id": "7", "time_cycles": cycle, "sensor_1": 500 + cycle, "sensor_2": 600 + cycle})
        for cycle in range(10)
    ]
    response = TestClient(app).get("/api/engines/FD002-007/telemetry?channels=sensor_1,sensor_2&points=3")
    assert response.status_code == 200
    payload = response.json()
    assert payload["engine_id"] == "FD002-007"
    assert payload["channels"] == ["sensor_1", "sensor_2"]
    assert len(payload["points"]) <= 3
    assert set(payload["points"][0]) == {"timestamp", "cycle", "run_id", "sensor_1", "sensor_2"}
    query = app.state.influx_client.query_api_instance.queries[0]
    assert 'aggregateWindow' in query and 'fn: last' in query and 'timeDst: "bucket_time"' in query and 'limit(n: 3)' in query
    assert 'r.unit_id == "7"' in query and 'r.unit_id == "7.0"' in query and 'r.dataset_id == "FD002"' in query


def test_telemetry_returns_source_timestamp_not_downsample_bucket_time(app):
    allow_role(app, UserRole.VIEWER)
    source_time = datetime(2026, 10, 30, 12, 34, 56, tzinfo=timezone.utc)
    app.state.influx_client.query_api_instance.records = [
        Record({"dataset_id": "FD002", "unit_id": "7", "time_cycles": 140, "sensor_1": 501.2}, source_time)
    ]
    response = TestClient(app).get("/api/engines/FD002-007/telemetry?channels=sensor_1&points=3")
    assert response.status_code == 200
    assert response.json()["points"][0]["timestamp"] == source_time.isoformat()
    query = app.state.influx_client.query_api_instance.queries[0]
    assert 'fn: last' in query and 'timeDst: "bucket_time"' in query


@pytest.mark.parametrize("path", [
    "/api/engines/7/telemetry",
    "/api/engines/FD005-007/telemetry",
    "/api/engines/FD001-007;drop/telemetry",
    "/api/engines/FD001-007/telemetry?channels=sensor_22",
    "/api/engines/FD001-007/telemetry?channels=sensor_1,evil",
    "/api/engines/FD001-007/telemetry?points=5001",
    "/api/engines/FD001-007/telemetry?points=0",
    "/api/engines/FD001-007/telemetry?from=2026-10-30T12:00:00Z&to=2026-10-30T11:00:00Z",
    "/api/engines/FD001-007/telemetry?from=2026-10-01T00:00:00Z&to=2026-10-30T00:00:00Z",
])
def test_telemetry_rejects_unvalidated_inputs(app, path):
    allow_role(app, UserRole.ADMIN)
    assert TestClient(app).get(path).status_code == 422


def test_empty_results_and_influx_failure_return_real_empty_or_error(app):
    allow_role(app, UserRole.ADMIN)
    app.state.influx_client.query_api_instance.records = []
    client = TestClient(app, raise_server_exceptions=False)
    assert client.get("/api/fleet/status").json()["fleet"] == []
    assert client.get("/api/engines/FD002-007/telemetry").json()["points"] == []
    app.state.influx_client.query_api_instance.error = RuntimeError("Influx unavailable")
    assert client.get("/api/fleet/status").status_code == 500
    assert client.get("/api/engines/FD002-007/telemetry").status_code == 500
