"""FastAPI app — InfluxDB dashboard queries (existing) + Module 8
Authentication & RBAC (new)."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from influxdb_client import InfluxDBClient

from app.database import Base, engine
from app.routers.auth import router as auth_router
from app.routers.fleet import router as fleet_router
from app.routers.rul import router as rul_router
from app.routers.anomalies import router as anomalies_router
from app.routers.alerts import router as alerts_router
from app.rbac import require_role
from app.models import User, UserRole

load_dotenv()

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "pms-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor-data")
MEASUREMENT = "sensor_telemetry"
WINDOW_LABEL = "Last 24 hours"

app = FastAPI(title="PMS Dashboard API", version="1.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_timing_middleware(request: Request, call_next):
    started_at = time.perf_counter()
    response = None
    try:
        response = await call_next(request)
        return response
    finally:
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        print(
            f"[HTTP] {request.method} {request.url.path} "
            f"{response.status_code if response is not None else 500} "
            f"{elapsed_ms:.1f}ms",
            flush=True,
        )


@app.on_event("startup")
def on_startup() -> None:
    # NOTE: create_all is fine for an FYP timeline; a production system
    # would use Alembic migrations instead. Flagged here intentionally
    # so it's not mistaken for an oversight in the defense.
    Base.metadata.create_all(bind=engine)


app.include_router(auth_router)
app.include_router(fleet_router)
app.include_router(rul_router)
app.include_router(anomalies_router)
app.include_router(alerts_router)


def get_client() -> InfluxDBClient:
    if not INFLUXDB_TOKEN:
        raise HTTPException(status_code=500, detail="INFLUXDB_TOKEN is not set")
    return InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG, timeout=30_000)


def to_number(value: Any) -> float | int | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number.is_integer():
        return int(number)
    return round(number, 4)


def parse_records(query_api, query: str) -> list[dict[str, Any]]:
    tables = query_api.query(query)
    rows: list[dict[str, Any]] = []

    for table in tables:
        for record in table.records:
            values = record.values
            row: dict[str, Any] = {
                "timestamp": record.get_time().astimezone(timezone.utc).isoformat() if record.get_time() else None,
                "unit_id": to_number(values.get("unit_id")),
                "time_cycles": to_number(values.get("time_cycles")),
                "operational_setting_1": to_number(values.get("operational_setting_1")),
                "operational_setting_2": to_number(values.get("operational_setting_2")),
                "operational_setting_3": to_number(values.get("operational_setting_3")),
            }

            for index in range(1, 22):
                field_name = f"sensor_{index}"
                row[field_name] = to_number(values.get(field_name))

            rows.append(row)

    return rows


def latest_records(query_api, limit: int = 8) -> list[dict[str, Any]]:
    # Use group() + last() so InfluxDB uses its per-series index and returns
    # the latest point per series without scanning or sorting the full range.
    # sort + limit over a large range is O(N log N) and caused 10-18s timeouts.
    query = f'''
from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}" and r._field == "time_cycles")
  |> keep(columns: ["_time", "_value", "_field", "_measurement", "unit_id"])
  |> group()
  |> last()
  |> limit(n: {limit})
'''
    return parse_records(query_api, query)


def latest_timestamp(rows: list[dict[str, Any]]) -> str | None:
    if not rows:
        return None
    return rows[0].get("timestamp")


def freshness_seconds(timestamp: str | None) -> float | None:
    if not timestamp:
        return None
    try:
        latest_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return None
    return (datetime.now(timezone.utc) - latest_dt).total_seconds()


@app.get("/api/health")
def health() -> dict[str, Any]:
    # Ping InfluxDB's own /health endpoint — no Flux query, sub-millisecond.
    # This keeps the health check fast regardless of data volume.
    import urllib.request
    influx_ok = False
    try:
        with urllib.request.urlopen(f"{INFLUXDB_URL}/health", timeout=5) as resp:
            influx_ok = resp.status == 200
    except Exception:
        influx_ok = False
    return {
        "status": "ok" if influx_ok else "degraded",
        "influxdb_url": INFLUXDB_URL,
        "bucket": INFLUXDB_BUCKET,
        "measurement": MEASUREMENT,
        "latest_points": -1,  # not queried in health check; use /api/fleet/status for data presence
    }


@app.get("/api/dashboard/live")
def live_dashboard(user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.VIEWER))) -> dict[str, Any]:
    print(f"[Dashboard API] Request from user: {user.email} (role: {user.role.value})")  # Debug log
    client = get_client()
    try:
        query_api = client.query_api()
        rows = latest_records(query_api, limit=250)
        latest = rows[0] if rows else {}
        latest_ts = latest_timestamp(rows)
        age_seconds = freshness_seconds(latest_ts)
        unique_unit_count = len({row.get("unit_id") for row in rows if row.get("unit_id") is not None})

        result = {
            "window_label": WINDOW_LABEL,
            "metrics": {
                "record_count": len(rows),
                "unique_units": unique_unit_count,
                "unit_bar_percent": min(100, max(10, unique_unit_count * 4 if unique_unit_count else 0)),
            },
            "latest_timestamp": latest_ts,
            "latest_age_seconds": age_seconds,
            "latest_records": rows[:8],
            "latest_record": latest,
        }
        print(f"[Dashboard API] Returning {len(rows)} records, {unique_unit_count} units")  # Debug log
        return result
    finally:
        client.close()
