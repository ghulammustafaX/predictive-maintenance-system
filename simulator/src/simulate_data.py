"""Synthetic C-MAPSS-like data simulator  —  updated for PMS Module 1

Generates synthetic turbofan sensor telemetry.  Payloads include the full
metadata envelope (schema_version, event_id, run_id, dataset_id, asset_id,
sequence, source_type) so this script is compatible with any downstream
consumer that accepts real C-MAPSS events.

Synthetic payloads are labelled source_type='synthetic' and
dataset_id='SYNTHETIC', ensuring they are never confused with real
NASA C-MAPSS benchmark data.

Usage examples
--------------
  python simulate_data.py --units 5 --cycles 200 --out data/simulated.csv
  python simulate_data.py --units 10 --cycles 500 --kafka
  python simulate_data.py --units 3 --cycles 100 --kafka --continuous
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Synthetic dataset generation
# ---------------------------------------------------------------------------

def generate_unit_series(unit_id: int, cycles: int, seed: int | None = None) -> pd.DataFrame:
    """Generate one engine's worth of synthetic sensor data."""
    rng = np.random.default_rng(seed)
    time_cycles = np.arange(1, cycles + 1)

    op1 = float(rng.normal(20.0, 1.0))
    op2 = float(rng.normal(0.0, 0.1))
    op3 = float(rng.normal(100.0, 5.0))

    sensors: dict[str, np.ndarray] = {}
    for i in range(1, 22):
        baseline = rng.normal(100.0, 5.0)
        trend = rng.normal(0.0, 0.05) * time_cycles
        noise = rng.normal(0.0, 1.0, size=cycles)
        seasonal = 5.0 * np.sin(time_cycles / (20 + (i % 5)))
        sensors[f"sensor_{i}"] = baseline + trend + seasonal + noise

    df = pd.DataFrame(
        {
            "unit_id": unit_id,
            "time_cycles": time_cycles,
            "operational_setting_1": op1,
            "operational_setting_2": op2,
            "operational_setting_3": op3,
        }
    )
    for k, v in sensors.items():
        df[k] = v

    return df


def generate_dataset(units: int = 10, cycles: int = 300) -> pd.DataFrame:
    """Generate a multi-engine synthetic dataset."""
    pieces = []
    rng = np.random.default_rng(0)
    for u in range(1, units + 1):
        variation = int(rng.integers(-int(cycles * 0.1), int(cycles * 0.1) + 1))
        c = max(10, cycles + variation)
        pieces.append(generate_unit_series(u, c, seed=u))
    return pd.concat(pieces, ignore_index=True)


# ---------------------------------------------------------------------------
# Payload builder (with full envelope)
# ---------------------------------------------------------------------------

def build_payload(row: pd.Series, run_id: str, sequence: int) -> dict:
    """Build a complete telemetry payload including the metadata envelope."""
    # Import here so the module can be imported without simulator/src on sys.path
    # (e.g. in tests that set up sys.path themselves)
    try:
        from event_schema import SOURCE_SYNTHETIC, build_envelope
    except ImportError:
        # Fallback: minimal envelope without event_schema module
        import hashlib
        import uuid
        from datetime import datetime, timezone

        unit_id = int(row["unit_id"])
        asset_id = f"engine_{unit_id}"
        key = f"{run_id}:{asset_id}:{sequence}"
        event_id = hashlib.sha256(key.encode()).hexdigest()[:32]
        envelope = {
            "schema_version": "1.0.0",
            "event_id": event_id,
            "run_id": run_id,
            "dataset_id": "SYNTHETIC",
            "asset_id": asset_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sequence": sequence,
            "source_type": "synthetic",
            "source_split": "live",
        }
    else:
        unit_id = int(row["unit_id"])
        asset_id = f"engine_{unit_id}"
        env = build_envelope(
            run_id=run_id,
            dataset_id="SYNTHETIC",
            asset_id=asset_id,
            sequence=sequence,
            source_type=SOURCE_SYNTHETIC,
            source_split="live",
        )
        envelope = env.to_dict()

    sensor_data = {
        k: (None if pd.isna(v) else float(v))
        for k, v in row.items()
    }
    sensor_data["unit_id"] = unit_id
    sensor_data["time_cycles"] = int(row["time_cycles"])

    return {**envelope, **sensor_data}


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------

def stream_rows(
    df: pd.DataFrame,
    interval: float,
    run_id: str,
    kafka_producer=None,
    topic: str = "sensor-telemetry",
) -> None:
    """Stream rows to Kafka or stdout with full metadata envelope."""
    import json

    sequence = 0
    try:
        for _, row in df.iterrows():
            sequence += 1
            payload = build_payload(row, run_id=run_id, sequence=sequence)

            if kafka_producer is not None:
                kafka_key = f"SYNTHETIC:{int(row['unit_id'])}:{run_id}"
                kafka_producer.send(topic, payload, key=kafka_key)
            else:
                print(json.dumps(payload))

            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStream interrupted by user", file=sys.stderr)


def create_kafka_producer(broker: str):
    """Create the reusable KafkaProducerClient."""
    try:
        from kafka_producer import KafkaProducerClient
        producer = KafkaProducerClient(bootstrap_servers=broker)
        producer.connect()
        return producer
    except ImportError:
        # Fallback: direct kafka-python producer (legacy)
        import json
        from kafka import KafkaProducer

        class _LegacyAdapter:
            def __init__(self, inner):
                self._inner = inner

            def send(self, topic, payload, key=None):
                self._inner.send(topic, value=payload)

            def close(self):
                self._inner.flush()
                self._inner.close()

        inner = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
        return _LegacyAdapter(inner)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import uuid

    parser = argparse.ArgumentParser(description="Synthetic C-MAPSS-like data simulator")
    parser.add_argument("--units", type=int, default=5, help="Number of engine units")
    parser.add_argument("--cycles", type=int, default=200, help="Cycles per unit (approx)")
    parser.add_argument("--out", type=str, default=None, help="CSV output path")
    parser.add_argument("--kafka", action="store_true", help="Stream to Kafka")
    parser.add_argument("--broker", type=str, default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--interval", type=float, default=0.1, help="Seconds between rows")
    parser.add_argument("--topic", type=str, default=os.getenv("SENSOR_TOPIC", "sensor-telemetry"))
    parser.add_argument("--continuous", action="store_true", help="Replay indefinitely")
    parser.add_argument("--run-id", type=str, default=None, help="Fixed run ID")
    args = parser.parse_args()

    df = generate_dataset(units=args.units, cycles=args.cycles)

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(str(out_path), index=False)
        print(f"Wrote {len(df)} rows to {args.out}")

    if args.kafka:
        producer = create_kafka_producer(args.broker)
        print(f"Streaming to Kafka broker={args.broker} topic={args.topic}")
        try:
            while True:
                run_id = args.run_id or uuid.uuid4().hex
                print(f"Replay run_id={run_id}")
                stream_rows(df, args.interval, run_id=run_id, kafka_producer=producer, topic=args.topic)
                producer.flush() if hasattr(producer, "flush") else None
                if not args.continuous:
                    break
                time.sleep(1.0)
        finally:
            producer.close()
    elif not args.out:
        import json
        run_id = args.run_id or uuid.uuid4().hex
        for _, row in df.head(5).iterrows():
            print(json.dumps(build_payload(row, run_id=run_id, sequence=1)))


if __name__ == "__main__":
    main()
