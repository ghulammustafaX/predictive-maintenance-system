"""TEST ONLY: inject an InfluxDB failure for partition 0 in an isolated stack."""
from __future__ import annotations

import os
import sys
from pathlib import Path

EXPECTED = {
    "MODULE3_PARTITION_FAULT_GUARD": "m3-isolated-partition-fault-only",
    "PMS_BATCH_WORKER_CONFIRM": "opt-in-isolated-batch-worker",
    "PMS_INFLUX_BATCH_SIZE": "3",
    "PMS_PARTITION_RETRY_BASE_SECONDS": "0.5",
    "PMS_PARTITION_RETRY_MAX_SECONDS": "1",
    "KAFKA_BROKER": "kafka:9092",
    "SENSOR_TOPIC": "m3-partition-fault-telemetry",
    "CONSUMER_GROUP": "m3-partition-fault-group",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
}
for key, value in EXPECTED.items():
    if os.environ.get(key) != value:
        raise SystemExit(f"REFUSED: invalid partition-fault worker setting {key}")

COORDINATION = Path("/coordination")
FAILURE_MARKER = COORDINATION / "partition0_write_failed"
FAILURE_COUNT = COORDINATION / "partition0_failure_count"
RELEASE_MARKER = COORDINATION / "release_partition0"

sys.path.insert(0, "/app/src")
import kafka_batch_consumer as batch

original_persist = batch.persist_points


def fail_only_partition_zero(write_api, points):
    run_ids = {point._tags.get("run_id") for point in points}
    if "m3-partition-fault-p0" in run_ids and not RELEASE_MARKER.exists():
        failures = int(FAILURE_COUNT.read_text(encoding="utf-8")) if FAILURE_COUNT.exists() else 0
        failures += 1
        FAILURE_COUNT.write_text(str(failures), encoding="utf-8")
        FAILURE_MARKER.touch(exist_ok=True)
        print(f"M3_PARTITION_FAULT_P0_WRITE_INJECTED attempt={failures}", flush=True)
        raise TimeoutError("isolated test fault: repeated partition 0 InfluxDB unavailable")
    return original_persist(write_api, points)


batch.persist_points = fail_only_partition_zero

if __name__ == "__main__":
    batch.run()
