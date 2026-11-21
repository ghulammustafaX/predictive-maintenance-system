"""Real InfluxDB verification for the existing authenticated telemetry Flux query.

Runs only in the dedicated Module 03 Compose query-verifier, never production.
Invokes the existing route function after its normal validation, without
requiring a live auth server or changes to the production DB.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

GUARD = {
    "MODULE3_TEST_GUARD": "m3-isolated-query-only",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
    "DATABASE_URL": "sqlite:////tmp/module3-query-verifier.sqlite",
}
for key, expected in GUARD.items():
    if os.getenv(key) != expected:
        raise SystemExit(f"REFUSED: non-isolated query verifier configuration: {key}")

from app.routers import fleet


def expect_invalid(description: str, call) -> None:
    try:
        call()
    except HTTPException as exc:
        assert exc.status_code == 422, (description, exc.status_code)
    else:
        raise AssertionError(f"{description}: invalid request unexpectedly accepted")


def main() -> None:
    run_a = "m3-query-a-" + uuid.uuid4().hex
    run_b = "m3-query-b-" + uuid.uuid4().hex
    now = datetime.now(timezone.utc)
    start = now.replace(second=0, microsecond=0) - timedelta(minutes=20)
    end = start + timedelta(seconds=120)
    client = InfluxDBClient(
        url=GUARD["INFLUXDB_URL"], token=GUARD["INFLUXDB_TOKEN"],
        org=GUARD["INFLUXDB_ORG"], timeout=10000,
    )
    try:
        writer = client.write_api(write_options=SYNCHRONOUS)
        points = []
        for dataset, unit, run in [
            ("FD001", "1", run_a), ("FD001", "1", run_b),
            ("FD002", "1", run_a), ("FD001", "2", run_a),
        ]:
            for cycle in (1, 2, 3):
                ts = start + timedelta(seconds=cycle + 2)
                points.append(
                    Point("sensor_telemetry")
                    .tag("dataset_id", dataset)
                    .tag("unit_id", unit)
                    .tag("run_id", run)
                    .tag("cycle", str(cycle))
                    .field("time_cycles", cycle)
                    .field("sensor_2", float(cycle * 100))
                    .field("sensor_14", float(cycle * 10))
                    .time(ts)
                )
        writer.write(bucket=GUARD["INFLUXDB_BUCKET"], org=GUARD["INFLUXDB_ORG"], record=points)

        def get(run_id=None, channels="time_cycles,sensor_2", limit=2, s=start, e=end):
            return fleet.engine_telemetry(
                engine_id="FD001-001", channels=channels, start=s,
                end=e, points=limit, run_id=run_id, user=None,
            )["points"]

        a = get(run_id=run_a)
        assert len(a) == 1, ("run A should aggregate 3 cycle-tagged points", a)
        assert a[0]["run_id"] == run_a and a[0]["cycle"] == 3, a
        assert a[0]["sensor_2"] == 300.0, a
        assert datetime.fromisoformat(a[0]["timestamp"]) == start + timedelta(seconds=5), a
        print("PASS: run-filtered last-observation aggregation across cycle tags")

        b = get(run_id=run_b)
        assert len(b) == 1 and b[0]["run_id"] == run_b and b[0]["cycle"] == 3, b
        both = get()
        assert len(both) == 2 and {p["run_id"] for p in both} == {run_a, run_b}, both
        assert all(p["cycle"] == 3 for p in both), both
        print("PASS: run isolation and a bounded global result count")

        selected = get(run_id=run_a, channels="time_cycles,sensor_14")
        assert len(selected) == 1 and selected[0]["sensor_14"] == 30.0, selected
        assert "sensor_2" not in selected[0], selected
        print("PASS: sensor channel selection and real source timestamps")

        expect_invalid("unsafe run ID", lambda: get(run_id='bad"\\injection'))
        expect_invalid("unknown channel", lambda: get(channels="not_a_sensor"))
        expect_invalid("inverted range", lambda: get(s=end, e=start))
        expect_invalid("oversized time range", lambda: get(s=start - timedelta(days=8)))
        expect_invalid("invalid engine", lambda: fleet.engine_telemetry(
            engine_id="FD005-001", channels="sensor_2", start=start, end=end,
            points=2, run_id=run_a, user=None,
        ))
        print("PASS: rejected unsafe filters, invalid engine IDs and time windows")
        print("MODULE 03 FLUX QUERY TEST: PASS")
    finally:
        client.close()


if __name__ == "__main__":
    main()
