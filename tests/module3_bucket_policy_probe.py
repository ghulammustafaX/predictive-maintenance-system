"""Real isolated InfluxDB tests for non-destructive bucket provisioning.

The module refuses execution outside the dedicated Module 03 CI stack.
Only dedicated throwaway buckets are created and inspected; nothing is deleted.
"""
from __future__ import annotations

import os
import sys
import uuid

from influxdb_client import InfluxDBClient

EXPECTED = {
    "MODULE3_POLICY_TEST_GUARD": "isolated-m3-bucket-policy-only",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
}
for key, value in EXPECTED.items():
    if os.getenv(key) != value:
        raise SystemExit(f"REFUSED: {key} did not match isolated Module 03 stack")

sys.path.insert(0, "/storage")
from bucket_policy import BucketPolicyError, ensure_bucket, existing_retention_seconds

ORG = EXPECTED["INFLUXDB_ORG"]


def refuses(call, label: str) -> None:
    try:
        call()
    except BucketPolicyError:
        print(f"PASS: {label}", flush=True)
    else:
        raise AssertionError(f"Unsafe operation was not refused: {label}")


def main() -> None:
    with InfluxDBClient(
        url=EXPECTED["INFLUXDB_URL"], org=ORG,
        token=EXPECTED["INFLUXDB_TOKEN"], timeout=10000,
    ) as client:
        initial = client.buckets_api().find_bucket_by_name(EXPECTED["INFLUXDB_BUCKET"])
        assert initial is not None
        initial_id = initial.id
        original_retention = existing_retention_seconds(initial)
        # The existing ingest bucket must never be modified, even by this test.
        verified = ensure_bucket(client, ORG, initial.name, original_retention)
        assert not verified.created and verified.bucket_id == initial_id
        refuses(
            lambda: ensure_bucket(client, ORG, initial.name, 3600 if original_retention != 3600 else 7200),
            "existing bucket retention mismatch refused",
        )
        unchanged = client.buckets_api().find_bucket_by_name(initial.name)
        assert unchanged.id == initial_id
        assert existing_retention_seconds(unchanged) == original_retention
        print("PASS: Existing telemetry bucket verified and unmodified", flush=True)

        missing = "m3-policy-" + uuid.uuid4().hex
        refuses(
            lambda: ensure_bucket(client, ORG, missing, 3600),
            "read-only default refuses missing bucket",
        )
        refuses(
            lambda: ensure_bucket(client, ORG, missing, 3600, create_if_missing=True),
            "creation without explicit matching confirmation refused",
        )
        assert client.buckets_api().find_bucket_by_name(missing) is None
        created = ensure_bucket(
            client, ORG, missing, 3600,
            create_if_missing=True, creation_confirmation=missing,
        )
        assert created.created and created.retention_seconds == 3600
        stored = client.buckets_api().find_bucket_by_name(missing)
        assert stored.id == created.bucket_id
        assert existing_retention_seconds(stored) == 3600
        again = ensure_bucket(
            client, ORG, missing, 3600,
            create_if_missing=True, creation_confirmation=missing,
        )
        assert not again.created and again.bucket_id == created.bucket_id
        refuses(
            lambda: ensure_bucket(
                client, ORG, missing, 7200,
                create_if_missing=True, creation_confirmation=missing,
            ),
            "existing retention cannot be silently updated",
        )
        assert existing_retention_seconds(client.buckets_api().find_bucket_by_name(missing)) == 3600
        print("PASS: Finite retention created once and preserved on repeat", flush=True)

        infinite_name = "m3-infinite-" + uuid.uuid4().hex
        infinite = ensure_bucket(
            client, ORG, infinite_name, 0,
            create_if_missing=True, creation_confirmation=infinite_name,
        )
        assert infinite.created
        assert existing_retention_seconds(
            client.buckets_api().find_bucket_by_name(infinite_name)
        ) == 0
        refuses(
            lambda: ensure_bucket(client, ORG, infinite_name, -1),
            "negative retention rejected",
        )
        refuses(
            lambda: ensure_bucket(client, ORG, infinite_name, 60),
            "sub-hour retention rejected",
        )
        refuses(
            lambda: ensure_bucket(client, ORG, "../production", 3600),
            "unsafe bucket name rejected",
        )
        print("PASS: Infinite retention, input validation and read-only safety", flush=True)
        print("MODULE 03 BUCKET POLICY VERIFICATION: PASS", flush=True)


if __name__ == "__main__":
    main()
