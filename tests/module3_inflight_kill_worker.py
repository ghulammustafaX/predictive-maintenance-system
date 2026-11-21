"""TEST ONLY: SIGKILL batch worker immediately after a confirmed InfluxDB write.

No production worker code is altered. All environment variables must match the
dedicated disposable Module 03 CI project before the original batch worker runs.
"""
from __future__ import annotations

import os
import signal
import sys

EXPECTED = {
    "MODULE3_INFLIGHT_FAULT_GUARD": "m3-isolated-crash-after-ack-only",
    "PMS_BATCH_WORKER_CONFIRM": "opt-in-isolated-batch-worker",
    "PMS_INFLUX_BATCH_SIZE": "6",
    "KAFKA_BROKER": "kafka:9092",
    "SENSOR_TOPIC": "m3-inflight-telemetry",
    "CONSUMER_GROUP": "m3-inflight-group",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
}
for key, value in EXPECTED.items():
    if os.environ.get(key) != value:
        raise SystemExit(f"REFUSED: invalid inflight worker guard {key}")

sys.path.insert(0, "/app/src")
import kafka_batch_consumer as batch

original_persist = batch.persist_points


def persist_then_kill(write_api, points):
    original_persist(write_api, points)
    # This log is emitted only AFTER the synchronous InfluxDB write returned.
    # The source Kafka commit is the next action in handle_partition, which
    # this SIGKILL prevents from executing.
    print("M3_FAULT_AFTER_INFLUX_ACK_BEFORE_KAFKA_COMMIT", flush=True)
    os.kill(os.getpid(), signal.SIGKILL)
    raise AssertionError("SIGKILL returned unexpectedly")


batch.persist_points = persist_then_kill

if __name__ == "__main__":
    batch.run()
