"""Real isolated InfluxDB single-point vs batch-write MICROBENCHMARK.

Not a Kafka worker throughput benchmark. Measures only synchronous Influx write
API latency for equal-size payloads in one throwaway CI InfluxDB instance.
No regression assertion assumes batching must always be faster.
"""
from __future__ import annotations

import json
import os
import statistics
import time
import uuid
from datetime import datetime, timedelta, timezone

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

GUARD = {
    "MODULE3_BENCH_GUARD": "m3-isolated-write-benchmark-only",
    "INFLUXDB_URL": "http://influxdb:8086",
    "INFLUXDB_ORG": "m3-isolated-org",
    "INFLUXDB_TOKEN": "m3-isolated-token",
    "INFLUXDB_BUCKET": "m3-isolated-bucket",
}
for key, expected in GUARD.items():
    if os.getenv(key) != expected:
        raise SystemExit(f"REFUSED: invalid isolated benchmark setting {key}")

N = 120
BATCH_SIZE = 20
ROUNDS = 2
BASE_TIME = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=4)
NONCE = uuid.uuid4().hex[:16]


def point(run: str, cycle: int) -> Point:
    return (
        Point("sensor_telemetry")
        .tag("dataset_id", "FD002")
        .tag("unit_id", "902")
        .tag("run_id", run)
        .tag("cycle", str(cycle))
        .field("time_cycles", cycle)
        .field("operational_setting_1", 0.1)
        .field("operational_setting_2", 0.2)
        .field("operational_setting_3", 100.0)
        .field("sensor_1", float(cycle))
        .field("sensor_21", float(cycle * 2))
        .time(BASE_TIME + timedelta(milliseconds=cycle * 100))
    )


def persisted_count(client: InfluxDBClient, run: str) -> int:
    query = f'''from(bucket: "{GUARD["INFLUXDB_BUCKET"]}")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "sensor_telemetry"
      and r._field == "time_cycles" and r.run_id == "{run}")
  |> group(columns: [])
  |> count()
'''
    records = [r for table in client.query_api().query(query, org=GUARD["INFLUXDB_ORG"])
               for r in table.records]
    return int(records[0].get_value()) if records else 0


def benchmark(writer, client, mode: str, round_id: int) -> dict:
    run = f"m3-benchmark-{mode}-{round_id}-{NONCE}"
    readings = [point(run, cycle) for cycle in range(1, N + 1)]
    latency_ms = []
    started = time.perf_counter()
    for index in range(0, N, (1 if mode == "single" else BATCH_SIZE)):
        records = readings[index:index + (1 if mode == "single" else BATCH_SIZE)]
        t0 = time.perf_counter()
        writer.write(bucket=GUARD["INFLUXDB_BUCKET"], org=GUARD["INFLUXDB_ORG"],
                     record=records[0] if mode == "single" else records)
        latency_ms.append((time.perf_counter() - t0) * 1000)
    elapsed = time.perf_counter() - started
    count = persisted_count(client, run)
    assert count == N, f"Expected {N} source points for {run}, received {count}"
    return {
        "mode": mode, "round": round_id, "points": N,
        "write_calls": len(latency_ms),
        "elapsed_seconds": round(elapsed, 4),
        "points_per_second": round(N / elapsed, 2),
        "mean_write_call_ms": round(statistics.mean(latency_ms), 3),
        "max_write_call_ms": round(max(latency_ms), 3),
        "persisted_points": count,
    }


def main():
    with InfluxDBClient(url=GUARD["INFLUXDB_URL"], token=GUARD["INFLUXDB_TOKEN"],
                        org=GUARD["INFLUXDB_ORG"], timeout=10000) as client:
        writer = client.write_api(write_options=SYNCHRONOUS)
        results = []
        for round_id in range(ROUNDS):
            # Alternate execution order to reduce systematic warmup bias.
            order = ("single", "batch") if round_id % 2 == 0 else ("batch", "single")
            for mode in order:
                result = benchmark(writer, client, mode, round_id)
                results.append(result)
                print("M3_BENCHMARK " + json.dumps(result, sort_keys=True), flush=True)
        summary = {
            mode: round(statistics.median(r["points_per_second"] for r in results
                                          if r["mode"] == mode), 2)
            for mode in ("single", "batch")
        }
        summary["relative_median_rate_batch_over_single"] = round(
            summary["batch"] / summary["single"], 3
        ) if summary["single"] else None
        print("M3_BENCHMARK_SUMMARY " + json.dumps(summary, sort_keys=True), flush=True)
        print("PASS: isolated real InfluxDB microbenchmark, both modes persisted equal records",
              flush=True)


if __name__ == "__main__":
    main()
