"""FastAPI app — InfluxDB dashboard queries (existing) + Module 8
Authentication & RBAC (new)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from influxdb_client import InfluxDBClient

from app.database import Base, engine
from app.routers.auth import router as auth_router
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


@app.on_event("startup")
def on_startup() -> None:
    # NOTE: create_all is fine for an FYP timeline; a production system
    # would use Alembic migrations instead. Flagged here intentionally
    # so it's not mistaken for an oversight in the defense.
    Base.metadata.create_all(bind=engine)


app.include_router(auth_router)


def get_client() -> InfluxDBClient:
    if not INFLUXDB_TOKEN:
        raise HTTPException(status_code=500, detail="INFLUXDB_TOKEN is not set")
    return InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)


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
    query = f'''
from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")
  |> pivot(rowKey: ["_time", "unit_id"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"], desc: true)
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
    client = get_client()
    try:
        query_api = client.query_api()
        rows = latest_records(query_api, limit=1)
        return {
            "status": "ok",
            "influxdb_url": INFLUXDB_URL,
            "bucket": INFLUXDB_BUCKET,
            "measurement": MEASUREMENT,
            "latest_points": len(rows),
        }
    finally:
        client.close()


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
