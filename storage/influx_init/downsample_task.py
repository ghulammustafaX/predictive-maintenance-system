"""Explicit, create-only InfluxDB 2.x scheduled telemetry downsampling.

No production startup hooks. Never modifies raw points, existing buckets or
tasks. Existing task configuration must match exactly, or this fails closed.
"""
from __future__ import annotations

import argparse
import os
import re

from influxdb_client import InfluxDBClient, TaskCreateRequest
from bucket_policy import BucketPolicyError, NAME_PATTERN

INTERVALS = {60: "1m", 300: "5m", 900: "15m", 3600: "1h"}
RAW_MEASUREMENT = "sensor_telemetry"
AGG_MEASUREMENT = "sensor_telemetry_downsampled"


def task_flux(task_name: str, source_bucket: str, destination_bucket: str, org: str, interval_seconds: int) -> str:
    """Flux writes sensor/operating-setting means grouped by asset and run, NOT cycle."""
    for name in (task_name, source_bucket, destination_bucket, org):
        if not isinstance(name, str) or not NAME_PATTERN.fullmatch(name):
            raise BucketPolicyError("Unsafe task name, organization or bucket")
    if source_bucket == destination_bucket:
        raise BucketPolicyError("Raw and aggregate buckets must be different")
    if type(interval_seconds) is not int or interval_seconds not in INTERVALS:
        raise BucketPolicyError("Only 60, 300, 900 or 3600 second intervals are supported")
    interval = INTERVALS[interval_seconds]
    return f'''option task = {{name: "{task_name}", every: {interval}, offset: 10s}}

// Replayable, fixed-width windows. A 10s schedule offset allows slightly late
// arrivals. Writes are deterministic for identical series/window identities.
// Past late-arriving observations need an explicitly approved backfill.
from(bucket: "{source_bucket}")
  |> range(start: -task.every)
  |> filter(fn: (r) => r._measurement == "{RAW_MEASUREMENT}")
  |> filter(fn: (r) => r._field != "time_cycles")
  |> filter(fn: (r) => r._field =~ /^(sensor_([1-9]|1[0-9]|2[01])|operational_setting_[123])$/)
  |> filter(fn: (r) => exists r.dataset_id and exists r.unit_id and exists r.run_id)
  |> group(columns: ["dataset_id", "unit_id", "run_id", "_field"])
  |> aggregateWindow(every: {interval}, fn: mean, createEmpty: false)
  |> set(key: "_measurement", value: "{AGG_MEASUREMENT}")
  |> to(bucket: "{destination_bucket}", org: "{org}", tagColumns: ["dataset_id", "unit_id", "run_id"])
'''


def ensure_task(client: InfluxDBClient, *, org: str, name: str, source: str,
                destination: str, interval_seconds: int, create_if_missing: bool = False,
                confirmation: str | None = None):
    flux = task_flux(name, source, destination, org, interval_seconds)
    organizations = client.organizations_api().find_organizations(org=org)
    matches = [item for item in organizations if item.name == org]
    if len(matches) != 1:
        raise BucketPolicyError("Expected exactly one organization")
    org_id = matches[0].id
    buckets = client.buckets_api()
    raw = buckets.find_bucket_by_name(source)
    dest = buckets.find_bucket_by_name(destination)
    if not raw or not dest or raw.org_id != org_id or dest.org_id != org_id:
        raise BucketPolicyError("Both raw and aggregate buckets must already exist in the selected organization")
    api = client.tasks_api()
    candidates = [t for t in api.find_tasks(org_id=org_id, name=name) if t.name == name]
    if len(candidates) > 1:
        raise BucketPolicyError("Multiple tasks have the same name; no action taken")
    if candidates:
        task = candidates[0]
        if task.flux != flux or task.org_id != org_id or task.status != "active":
            raise BucketPolicyError("Existing task configuration differs; never patching it")
        return task, False
    if not create_if_missing:
        raise BucketPolicyError("Task missing, read-only inspection cannot create")
    if confirmation != name:
        raise BucketPolicyError("Explicit exact-name confirmation required to create task")
    task = api.create_task(task_create_request=TaskCreateRequest(
        org_id=org_id, flux=flux, status="active",
        description="PMS explicitly provisioned aggregate telemetry task",
    ))
    if task.org_id != org_id or task.flux != flux:
        raise BucketPolicyError("Unexpected created task identity")
    return task, True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True)
    parser.add_argument("--source-bucket", required=True)
    parser.add_argument("--aggregate-bucket", required=True)
    parser.add_argument("--interval-seconds", type=int, required=True, choices=sorted(INTERVALS))
    parser.add_argument("--create-if-missing", action="store_true")
    args = parser.parse_args()
    url, token, org = (os.getenv(name) for name in ("INFLUXDB_URL", "INFLUXDB_TOKEN", "INFLUXDB_ORG"))
    if not all((url, token, org)):
        parser.error("INFLUXDB_URL, INFLUXDB_TOKEN and INFLUXDB_ORG are required")
    try:
        task_flux(args.name, args.source_bucket, args.aggregate_bucket, org, args.interval_seconds)
        if args.create_if_missing and os.getenv("PMS_TASK_CREATE_CONFIRM") != args.name:
            raise BucketPolicyError("PMS_TASK_CREATE_CONFIRM must match the task name")
        with InfluxDBClient(url=url, token=token, org=org, timeout=10000) as client:
            task, created = ensure_task(
                client, org=org, name=args.name, source=args.source_bucket,
                destination=args.aggregate_bucket, interval_seconds=args.interval_seconds,
                create_if_missing=args.create_if_missing,
                confirmation=os.getenv("PMS_TASK_CREATE_CONFIRM"),
            )
    except BucketPolicyError as exc:
        parser.exit(2, f"REFUSED: {exc}\n")
    print(f"{'CREATED' if created else 'VERIFIED'} task={task.id} name={args.name} "
          f"source={args.source_bucket} target={args.aggregate_bucket} interval_seconds={args.interval_seconds}")


if __name__ == "__main__":
    main()
