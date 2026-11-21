"""Guarded real InfluxDB integration test for Module 03 scheduled downsampling.

Creates only throwaway task and aggregate bucket inside isolated CI InfluxDB.
Never deletes raw data, existing task, buckets, or production assets.
"""
from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

EXPECTED = {
    "MODULE3_DOWNSAMPLE_GUARD": "isolated-m3-downsampling-only",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
}
for key, expected in EXPECTED.items():
    if os.getenv(key) != expected:
        raise SystemExit(f"REFUSED: unsafe downsampling test configuration for {key}")

sys.path.insert(0, "/storage")
from bucket_policy import BucketPolicyError, ensure_bucket
from downsample_task import ensure_task, task_flux


def rejected(action, description):
    try:
        action()
    except BucketPolicyError:
        print("PASS:", description, flush=True)
    else:
        raise AssertionError("Expected refusal: " + description)


def values(client, bucket, run):
    query = f'''from(bucket: "{bucket}")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry_downsampled")
  |> filter(fn: (r) => r._field == "sensor_2" and r.run_id == "{run}")
'''
    return [record for table in client.query_api().query(query, org=EXPECTED["INFLUXDB_ORG"])
            for record in table.records]


def main():
    org = EXPECTED["INFLUXDB_ORG"]
    src = EXPECTED["INFLUXDB_BUCKET"]
    nonce = uuid.uuid4().hex[:20]
    dest = "m3-aggregate-" + nonce
    name = "m3-downsample-" + nonce
    run = "m3-run-" + nonce

    with InfluxDBClient(url=EXPECTED["INFLUXDB_URL"],
                        token=EXPECTED["INFLUXDB_TOKEN"], org=org, timeout=10000) as client:
        ensure_bucket(client, org, dest, 0, create_if_missing=True,
                      creation_confirmation=dest)
        rejected(
            lambda: ensure_task(client, org=org, name=name, source=src,
                                destination=dest, interval_seconds=60),
            "task creation is read-only by default",
        )
        rejected(
            lambda: ensure_task(client, org=org, name=name, source=src,
                                destination=dest, interval_seconds=60,
                                create_if_missing=True),
            "task creation needs exact-name confirmation",
        )
        task, created = ensure_task(
            client, org=org, name=name, source=src, destination=dest,
            interval_seconds=60, create_if_missing=True, confirmation=name,
        )
        assert created and task.status == "active"
        again, created_again = ensure_task(
            client, org=org, name=name, source=src, destination=dest,
            interval_seconds=60,
        )
        assert not created_again and again.id == task.id
        rejected(
            lambda: ensure_task(client, org=org, name=name, source=src,
                                destination=dest, interval_seconds=300),
            "existing task configuration cannot be overwritten",
        )
        rejected(
            lambda: task_flux(name, src, src, org, 60),
            "raw bucket cannot be the aggregate destination",
        )
        print("PASS: Created active scheduled task with idempotent, fail-closed policy", flush=True)

        # Keep both samples within one minute boundary for an exact 30.0 mean.
        now = datetime.now(timezone.utc)
        if now.second < 6:
            time.sleep(7)
            now = datetime.now(timezone.utc)
        point_time = now.replace(microsecond=0)
        sample_times = [point_time - timedelta(seconds=4), point_time - timedelta(seconds=2)]
        writer = client.write_api(write_options=SYNCHRONOUS)
        data = []
        for cycle, sample, ts in [(1, 20.0, sample_times[0]), (2, 40.0, sample_times[1])]:
            data.append(Point("sensor_telemetry")
                        .tag("dataset_id", "FD004")
                        .tag("unit_id", "77")
                        .tag("run_id", run)
                        .tag("cycle", str(cycle))
                        .field("time_cycles", cycle)
                        .field("sensor_2", sample)
                        .time(ts))
        writer.write(bucket=src, org=org, record=data)

        # Real Influx task engine execution, no client-side copy or fake rollup.
        manual = client.tasks_api().run_manually(task.id, scheduled_for=datetime.now(timezone.utc))
        assert manual is not None
        deadline = time.monotonic() + 45
        result = []
        while time.monotonic() < deadline:
            result = values(client, dest, run)
            if result:
                break
            time.sleep(2)
        assert len(result) == 1, f"Expected one 1-minute aggregate, got {len(result)}"
        assert abs(float(result[0].get_value()) - 30.0) < 0.001, result[0].get_value()
        assert result[0].values["dataset_id"] == "FD004"
        assert result[0].values["unit_id"] == "77"
        assert result[0].values["run_id"] == run
        assert "cycle" not in result[0].values
        assert result[0].get_time() is not None
        rawq = f'''from(bucket: "{src}") |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry" and r.run_id == "{run}")
  |> filter(fn: (r) => r._field == "sensor_2")
'''
        raw = [r for t in client.query_api().query(rawq, org=org) for r in t.records]
        assert len(raw) == 2, "Original raw samples must remain present"
        print("PASS: Influx task executed and stored mean=30, tags isolated, cycle tag dropped", flush=True)
        print("PASS: Both original raw samples remain unchanged", flush=True)
        print("MODULE 03 AUTOMATED DOWNSAMPLING: PASS", flush=True)


if __name__ == "__main__":
    main()
