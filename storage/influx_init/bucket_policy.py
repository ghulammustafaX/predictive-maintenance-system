"""Non-destructive InfluxDB 2.x bucket/retention policy inspection and opt-in creation.

The script NEVER edits or deletes an existing bucket. The default operation is
read-only. Creating a missing bucket needs BOTH a CLI flag and an independent
environment confirmation matching its exact name.
"""
from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass

from influxdb_client import BucketRetentionRules, InfluxDBClient

MIN_RETENTION_SECONDS = 3600
MAX_RETENTION_SECONDS = 315360000  # ten 365-day years
NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class BucketPolicyError(RuntimeError):
    """The desired policy is unsafe or does not match the existing bucket."""


@dataclass(frozen=True)
class BucketResult:
    name: str
    org: str
    retention_seconds: int
    created: bool
    bucket_id: str


def validate_config(org: str, bucket_name: str, seconds: int) -> None:
    if not org or not NAME_PATTERN.fullmatch(org):
        raise BucketPolicyError("Invalid organization name")
    if not bucket_name or not NAME_PATTERN.fullmatch(bucket_name):
        raise BucketPolicyError("Invalid bucket name")
    if type(seconds) is not int or seconds < 0:
        raise BucketPolicyError("Retention must be a non-negative integer")
    if seconds != 0 and not MIN_RETENTION_SECONDS <= seconds <= MAX_RETENTION_SECONDS:
        raise BucketPolicyError(
            f"Retention must be 0 (infinite) or {MIN_RETENTION_SECONDS} to "
            f"{MAX_RETENTION_SECONDS} seconds"
        )


def existing_retention_seconds(bucket) -> int:
    """Normalize absent rules and explicit zero to infinite retention."""
    rules = getattr(bucket, "retention_rules", None) or []
    if not rules:
        return 0
    if len(rules) != 1:
        raise BucketPolicyError("Unexpected existing retention rule count")
    rule = rules[0]
    if getattr(rule, "type", None) != "expire":
        raise BucketPolicyError("Unexpected existing retention rule type")
    seconds = getattr(rule, "every_seconds", None)
    if type(seconds) is not int or seconds < 0:
        raise BucketPolicyError("Unexpected existing retention period")
    return seconds


def ensure_bucket(
    client: InfluxDBClient,
    org: str,
    bucket_name: str,
    retention_seconds: int,
    *,
    create_if_missing: bool = False,
    creation_confirmation: str | None = None,
) -> BucketResult:
    """Inspect and optionally create; NEVER update a bucket or its retention.

    An existing bucket with different retention raises BucketPolicyError. It is
    never patched. Organization is validated before inspecting or creating.
    """
    validate_config(org, bucket_name, retention_seconds)
    organizations = client.organizations_api().find_organizations(org=org)
    matching = [item for item in organizations if item.name == org]
    if len(matching) != 1:
        raise BucketPolicyError(f"Expected exactly one existing organization: {org}")
    org_id = matching[0].id

    api = client.buckets_api()
    bucket = api.find_bucket_by_name(bucket_name)
    if bucket is not None:
        if bucket.org_id != org_id:
            raise BucketPolicyError("Existing bucket belongs to a different organization")
        actual = existing_retention_seconds(bucket)
        if actual != retention_seconds:
            raise BucketPolicyError(
                f"Existing bucket retention is {actual} seconds, requested "
                f"{retention_seconds}. No update attempted."
            )
        return BucketResult(bucket_name, org, actual, False, bucket.id)

    if not create_if_missing:
        raise BucketPolicyError("Bucket does not exist; read-only check cannot create it")
    if creation_confirmation != bucket_name:
        raise BucketPolicyError(
            "Creating a missing bucket requires an exact-name creation confirmation"
        )

    rules = (
        [] if retention_seconds == 0 else
        [BucketRetentionRules(type="expire", every_seconds=retention_seconds)]
    )
    created = api.create_bucket(
        bucket_name=bucket_name, org=org, retention_rules=rules,
        description="PMS explicitly provisioned bucket",
    )
    if created.org_id != org_id:
        raise BucketPolicyError("Unexpected organization on created bucket")
    actual = existing_retention_seconds(created)
    if actual != retention_seconds:
        raise BucketPolicyError("Created bucket retention differs from requested configuration")
    return BucketResult(bucket_name, org, actual, True, created.id)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--retention-seconds", type=int, required=True)
    parser.add_argument("--create-if-missing", action="store_true")
    args = parser.parse_args()

    url = os.environ.get("INFLUXDB_URL")
    token = os.environ.get("INFLUXDB_TOKEN")
    org = os.environ.get("INFLUXDB_ORG")
    if not all([url, token, org]):
        parser.error("INFLUXDB_URL, INFLUXDB_TOKEN and INFLUXDB_ORG are required")
    # Fail before requesting any remote state for unsafe inputs.
    try:
        validate_config(org, args.bucket, args.retention_seconds)
        if args.create_if_missing and os.environ.get("PMS_BUCKET_CREATE_CONFIRM") != args.bucket:
            raise BucketPolicyError(
                "Set PMS_BUCKET_CREATE_CONFIRM to the exact bucket name for explicit creation"
            )
        with InfluxDBClient(url=url, token=token, org=org, timeout=10000) as client:
            result = ensure_bucket(
                client, org, args.bucket, args.retention_seconds,
                create_if_missing=args.create_if_missing,
                creation_confirmation=os.environ.get("PMS_BUCKET_CREATE_CONFIRM"),
            )
    except BucketPolicyError as exc:
        parser.exit(2, f"REFUSED: {exc}\n")
    print(
        f"{'CREATED' if result.created else 'VERIFIED'} "
        f"bucket={result.name} org={result.org} retention_seconds={result.retention_seconds}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
