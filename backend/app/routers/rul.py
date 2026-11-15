"""Authenticated, bounded read routes for stored RUL predictions."""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from influxdb_client import InfluxDBClient

from app.models import User, UserRole
from app.rbac import require_role

router = APIRouter(tags=["rul"])

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "pms-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor-data")
MEASUREMENT = "rul_predictions"
ENGINE_ID_PATTERN = re.compile(r"^(FD00[1-4])-(\d{1,6})$")
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
DEFAULT_POINTS = 200
MAX_POINTS = 1_000
MAX_HISTORY_RANGE = timedelta(days=30)
LATEST_LOOKBACK = timedelta(days=30)
PREDICTION_FIELDS = (
    "rul",
    "cycle",
    "window_size",
    "model_version",
    "preprocessing_artifact_version",
)


def get_client() -> InfluxDBClient:
    if not INFLUXDB_TOKEN:
        raise HTTPException(status_code=500, detail="INFLUXDB_TOKEN is not set")
    return InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)


def _engine_parts(engine_id: str) -> tuple[str, int, str]:
    match = ENGINE_ID_PATTERN.fullmatch(engine_id)
    if not match:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="engineId must match FD001-007 through FD004-<unit_id>",
        )
    dataset_id, unit_id = match.group(1), int(match.group(2))
    return dataset_id, unit_id, f"{dataset_id}-{unit_id:03d}"


def _unit_filter(unit_id: int) -> str:
    # Pipeline versions have stored both numeric-string and float-string tags.
    variants = (str(unit_id), f"{unit_id}.0", f"{unit_id:03d}")
    return " or ".join(f'r.unit_id == "{variant}"' for variant in variants)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _flux_time(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _flux_quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _safe_run_id(run_id: str) -> str:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise HTTPException(status_code=422, detail="run_id contains unsupported characters")
    return run_id


def _time_range(start: datetime | None, end: datetime | None) -> tuple[datetime, datetime]:
    final_end = _utc(end) if end else datetime.now(timezone.utc)
    final_start = _utc(start) if start else final_end - timedelta(hours=24)
    if final_start >= final_end:
        raise HTTPException(status_code=422, detail="from must be earlier than to")
    if final_end - final_start > MAX_HISTORY_RANGE:
        raise HTTPException(status_code=422, detail="time range may not exceed 30 days")
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


def _field_filter() -> str:
    return " or ".join(f'r._field == "{field}"' for field in PREDICTION_FIELDS)


def _prediction_from_row(row: dict[str, Any]) -> dict[str, Any] | None:
    if row.get("rul") is None or row.get("timestamp") is None:
        return None
    try:
        return {
            "rul_cycles": float(row["rul"]),
            "cycle": int(float(row["cycle"])) if row.get("cycle") is not None else None,
            "timestamp": row["timestamp"],
            "run_id": str(row.get("run_id", "")),
            "window_size": int(float(row["window_size"])) if row.get("window_size") is not None else None,
            "model_version": str(row.get("model_version", "unknown")),
            "preprocessing_artifact_version": str(row.get("preprocessing_artifact_version", "unknown")),
        }
    except (TypeError, ValueError):
        return None


def _latest_query(dataset_id: str, unit_id: int) -> str:
    return f'''from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: -{int(LATEST_LOOKBACK.total_seconds() // 86400)}d)
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}" and r.dataset_id == "{dataset_id}" and ({_unit_filter(unit_id)}) and ({_field_filter()}))
  |> pivot(rowKey: ["_time", "dataset_id", "unit_id", "run_id"], columnKey: ["_field"], valueColumn: "_value")
  |> filter(fn: (r) => exists r.rul)
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 1)
'''


def _latest_prediction(client: InfluxDBClient, dataset_id: str, unit_id: int) -> dict[str, Any] | None:
    for row in _rows(client.query_api(), _latest_query(dataset_id, unit_id)):
        prediction = _prediction_from_row(row)
        if prediction:
            return prediction
    return None


@router.get("/api/engines/{engine_id}/rul/latest")
def latest_rul(
    engine_id: str,
    user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.FLOOR_OPERATOR, UserRole.VIEWER)),
) -> dict[str, Any]:
    dataset_id, unit_id, canonical_id = _engine_parts(engine_id)
    client = get_client()
    try:
        return {"engine_id": canonical_id, "prediction": _latest_prediction(client, dataset_id, unit_id)}
    finally:
        client.close()


@router.get("/api/engines/{engine_id}/rul/history")
def rul_history(
    engine_id: str,
    start: datetime | None = Query(default=None, alias="from"),
    end: datetime | None = Query(default=None, alias="to"),
    points: int = Query(default=DEFAULT_POINTS, ge=1, le=MAX_POINTS),
    run_id: str | None = Query(default=None, max_length=128),
    user: User = Depends(require_role(UserRole.ADMIN, UserRole.MAINTENANCE_ENGINEER, UserRole.FLOOR_OPERATOR, UserRole.VIEWER)),
) -> dict[str, Any]:
    dataset_id, unit_id, canonical_id = _engine_parts(engine_id)
    start_at, end_at = _time_range(start, end)
    client = get_client()
    try:
        latest = _latest_prediction(client, dataset_id, unit_id)
        if latest is None and run_id is None:
            return {"engine_id": canonical_id, "run_id": None, "points": []}
        selected_run_id = _safe_run_id(run_id) if run_id is not None else latest["run_id"]
        run_id_filter = _flux_quote(selected_run_id)
        query = f'''from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: {_flux_time(start_at)}, stop: {_flux_time(end_at)})
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}" and r.dataset_id == "{dataset_id}" and ({_unit_filter(unit_id)}) and r.run_id == "{run_id_filter}" and ({_field_filter()}))
  |> pivot(rowKey: ["_time", "dataset_id", "unit_id", "run_id"], columnKey: ["_field"], valueColumn: "_value")
  |> filter(fn: (r) => exists r.rul)
  |> sort(columns: ["_time"])
  |> limit(n: {points})
'''
        history = [prediction for row in _rows(client.query_api(), query) if (prediction := _prediction_from_row(row))]
        return {"engine_id": canonical_id, "run_id": selected_run_id, "points": history[:points]}
    finally:
        client.close()
