"""Fleet and bounded telemetry routes for the React telemetry dashboard."""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from influxdb_client import InfluxDBClient

from app.models import User, UserRole
from app.rbac import require_role

router = APIRouter(tags=["fleet"])

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "pms-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor-data")
MEASUREMENT = "sensor_telemetry"
DEFAULT_DATASET_ID = "FD001"
ENGINE_ID_PATTERN = re.compile(r"^(FD00[1-4])-(\d{1,6})$")
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
ALLOWED_CHANNELS = {"time_cycles", *(f"sensor_{index}" for index in range(1, 22)), *(f"operational_setting_{index}" for index in range(1, 4))}
MAX_POINTS = 5_000
DEFAULT_POINTS = 500
MAX_RANGE = timedelta(days=7)


def get_client() -> InfluxDBClient:
    if not INFLUXDB_TOKEN:
        raise HTTPException(status_code=500, detail="INFLUXDB_TOKEN is not set")
    return InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG, timeout=30_000)


def temporary_fleet_status(last_seen_at: datetime | None, _latest_cycle: int | None) -> str:
    """Temporary fleet-health mapping; the dashboard renders warning as stale."""
    if last_seen_at is None:
        return "offline"
    age = datetime.now(timezone.utc) - last_seen_at.astimezone(timezone.utc)
    if age.total_seconds() < 0:
        return "future"
    if age > timedelta(hours=24):
        return "offline"
    if age > timedelta(hours=1):
        return "warning"
    return "healthy"


def _engine_parts(engine_id: str) -> tuple[str, str]:
    match = ENGINE_ID_PATTERN.fullmatch(engine_id)
    if not match:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="engineId must match FD001-007 through FD004-<unit_id>")
    return match.group(1), str(int(match.group(2)))


def _unit_tag_filter(unit_id: str) -> str:
    numeric_id = int(unit_id)
    return " or ".join(
        f'r.unit_id == "{variant}"'
        for variant in (str(numeric_id), f"{numeric_id}.0", f"{numeric_id:03d}")
    )


def _safe_run_id(run_id: str | None) -> str | None:
    if run_id is not None and not RUN_ID_PATTERN.fullmatch(run_id):
        raise HTTPException(status_code=422, detail="run_id contains unsupported characters")
    return run_id


def _safe_channels(channels: str | None) -> list[str]:
    selected = [channel.strip() for channel in channels.split(",")] if channels else sorted(ALLOWED_CHANNELS)
    if not selected or any(channel not in ALLOWED_CHANNELS for channel in selected):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="channels must contain only sensor_1..sensor_21, operational_setting_1..3, or time_cycles")
    return list(dict.fromkeys(selected))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _flux_time(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _parse_time_range(start: datetime | None, end: datetime | None) -> tuple[datetime, datetime]:
    final_end = _utc(end) if end else datetime.now(timezone.utc)
    final_start = _utc(start) if start else final_end - timedelta(hours=1)
    if final_start >= final_end:
        raise HTTPException(status_code=422, detail="from must be earlier than to")
    if final_end - final_start > MAX_RANGE:
        raise HTTPException(status_code=422, detail="time range may not exceed 7 days")
    return final_start, final_end


def _rows(query_api: Any, query: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table in query_api.query(query):
        for record in table.records:
            values = dict(record.values)
            timestamp = record.get_time()
            values["timestamp"] = _utc(timestamp).isoformat() if timestamp else None
            rows.append(values)
    return rows


def _fleet_row_key(row: dict[str, Any]) -> tuple[str, str] | None:
    dataset_id = row.get("dataset_id")
    unit_id = row.get("unit_id")
    if not dataset_id or unit_id is None:
        return None
    return str(dataset_id), str(unit_id)


def _select_fleet_rows(
    current_rows: list[dict[str, Any]],
    future_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Prefer current telemetry while retaining future-only test runs."""
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for row in current_rows + future_rows:
        key = _fleet_row_key(row)
        if key is not None and key not in selected:
            selected[key] = row
    return list(selected.values())


@router.get("/api/fleet/status")
def fleet_status(user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.FLOOR_OPERATOR, UserRole.VIEWER))) -> dict[str, Any]:
    """Returns one latest telemetry point per composite engine ID, bounded around now."""
    now = datetime.now(timezone.utc)
    future_stop = _flux_time(now + timedelta(hours=24))
    current_query = f'''from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: -24h, stop: {_flux_time(now)})
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}" and r._field == "time_cycles")
  |> keep(columns: ["_time", "_value", "_field", "_measurement", "dataset_id", "unit_id"])
  |> map(fn: (r) => ({{ r with dataset_id: if exists r.dataset_id then r.dataset_id else "{DEFAULT_DATASET_ID}" }}))
  |> group(columns: ["dataset_id", "unit_id"])
  |> last()
  |> limit(n: {MAX_POINTS})
'''
    future_query = f'''from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: {_flux_time(now)}, stop: {future_stop})
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}" and r._field == "time_cycles")
  |> keep(columns: ["_time", "_value", "_field", "_measurement", "dataset_id", "unit_id"])
  |> map(fn: (r) => ({{ r with dataset_id: if exists r.dataset_id then r.dataset_id else "{DEFAULT_DATASET_ID}" }}))
  |> group(columns: ["dataset_id", "unit_id"])
  |> last()
  |> limit(n: {MAX_POINTS})
'''
    client = get_client()
    try:
        fleet = []
        query_api = client.query_api()
        current_rows = _rows(query_api, current_query)
        future_rows = _rows(query_api, future_query)
        for row in _select_fleet_rows(current_rows, future_rows):
            dataset_id, unit_id = row.get("dataset_id"), row.get("unit_id")
            if not dataset_id or unit_id is None:
                continue
            last_seen = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00")) if row["timestamp"] else None
            latest_cycle = int(float(row.get("_value", row.get("time_cycles", 0))))
            fleet.append({
                "engine_id": f"{dataset_id}-{int(unit_id):03d}",
                "status": temporary_fleet_status(last_seen, latest_cycle),
                "latest_cycle": latest_cycle,
                "last_seen_at": row["timestamp"],
                "rul_cycles": None,
                "rul_percent": None,
                "active_anomaly_count": 0,
                "active_alert_count": 0,
            })
        return {"generated_at": datetime.now(timezone.utc).isoformat(), "fleet": sorted(fleet, key=lambda item: item["engine_id"])[:MAX_POINTS]}
    finally:
        client.close()


def _telemetry_query(
    dataset_id: str,
    unit_id: str,
    selected_channels: list[str],
    start_at: datetime,
    end_at: datetime,
    points: int,
    run_id: str | None,
) -> str:
    """Return a bounded Flux query that aggregates *across* per-cycle series.

    The current writer stores cycle as a tag. Grouping by selected tags,
    excluding cycle, is required to reduce points within each run.
    """
    window_seconds = max(1, int((end_at - start_at).total_seconds() / points))
    fields = " or ".join(f'r._field == "{channel}"' for channel in selected_channels)
    run_filter = f'  |> filter(fn: (r) => r.run_id == "{run_id}")\\n' if run_id is not None else ""
    return f'''from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: {_flux_time(start_at)}, stop: {_flux_time(end_at)})
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}" and ({_unit_tag_filter(unit_id)}) and ({fields}))
  |> map(fn: (r) => ({{ r with dataset_id: if exists r.dataset_id then r.dataset_id else "{DEFAULT_DATASET_ID}", run_id: if exists r.run_id then r.run_id else "legacy" }}))
  |> filter(fn: (r) => r.dataset_id == "{dataset_id}")
{run_filter}  |> group(columns: ["dataset_id", "unit_id", "run_id", "_field"])
  |> aggregateWindow(every: {window_seconds}s, fn: last, createEmpty: false, timeSrc: "_time", timeDst: "_time")
  |> pivot(rowKey: ["_time", "dataset_id", "unit_id", "run_id"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: [])
  |> sort(columns: ["_time"])
  |> limit(n: {points})
'''


@router.get("/api/engines/{engine_id}/telemetry")
def engine_telemetry(
    engine_id: str,
    channels: str | None = Query(default=None),
    start: datetime | None = Query(default=None, alias="from"),
    end: datetime | None = Query(default=None, alias="to"),
    points: int = Query(default=DEFAULT_POINTS, ge=1, le=MAX_POINTS),
    run_id: str | None = Query(default=None, max_length=128),
    user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.FLOOR_OPERATOR, UserRole.VIEWER)),
) -> dict[str, Any]:
    dataset_id, unit_id = _engine_parts(engine_id)
    selected_channels = _safe_channels(channels)
    start_at, end_at = _parse_time_range(start, end)
    safe_run_id = _safe_run_id(run_id)
    query = _telemetry_query(dataset_id, unit_id, selected_channels, start_at, end_at, points, safe_run_id)

    client = get_client()
    try:
        mapped_points = []
        for row in _rows(client.query_api(), query):
            point: dict[str, Any] = {
                "timestamp": row["timestamp"],
                "cycle": round(float(row["time_cycles"])) if row.get("time_cycles") is not None else None,
                "run_id": str(row["run_id"]) if row.get("run_id") is not None else None,
            }
            point.update({channel: row[channel] for channel in selected_channels if channel != "time_cycles" and channel in row})
            mapped_points.append(point)
        return {"engine_id": f"{dataset_id}-{int(unit_id):03d}", "channels": selected_channels, "points": mapped_points[:points]}
    finally:
        client.close()
