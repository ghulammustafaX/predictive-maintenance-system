"""Synthetic C-MAPSS-like data simulator

Generates synthetic turbofan sensor telemetry for multiple engine units.
Can write to CSV or stream to Kafka topic `sensor-telemetry` when `--kafka` is set.

Usage examples:
  python simulate_data.py --units 5 --cycles 200 --out data/simulated.csv
  python simulate_data.py --units 10 --cycles 500 --kafka
"""

import os
import time
import json
import argparse
from random import random, gauss
import pandas as pd
import numpy as np


def generate_unit_series(unit_id, cycles, seed=None):
    rng = np.random.default_rng(seed)
    time_cycles = np.arange(1, cycles + 1)

    # Operational settings vary slowly per unit
    op1 = rng.normal(20.0, 1.0)
    op2 = rng.normal(0.0, 0.1)
    op3 = rng.normal(100.0, 5.0)

    # Create 21 sensors with different degradation behaviours
    sensors = {}
    for i in range(1, 22):
        baseline = rng.normal(100.0, 5.0)
        trend = rng.normal(0.0, 0.05) * time_cycles  # small linear drift
        noise = rng.normal(0.0, 1.0, size=cycles)
        seasonal = 5.0 * np.sin(time_cycles / (20 + (i % 5)))
        sensors[f'sensor_{i}'] = baseline + trend + seasonal + noise

    df = pd.DataFrame({
        'unit_id': unit_id,
        'time_cycles': time_cycles,
        'operational_setting_1': op1,
        'operational_setting_2': op2,
        'operational_setting_3': op3,
    })

    for k, v in sensors.items():
        df[k] = v

    return df


def generate_dataset(units=10, cycles=300):
    pieces = []
    for u in range(1, units + 1):
        # Vary cycles per unit slightly to simulate different lifetimes
        c = cycles + int(np.random.randint(-int(cycles * 0.1), int(cycles * 0.1)))
        pieces.append(generate_unit_series(u, max(10, c), seed=u))
    return pd.concat(pieces, ignore_index=True)


def stream_rows(df, interval, kafka_producer=None, topic='sensor-telemetry'):
    try:
        for _, row in df.iterrows():
            payload = row.to_dict()
            payload = {k: (None if pd.isna(v) else float(v)) for k, v in payload.items()}

            if kafka_producer is not None:
                kafka_producer.send(topic, value=payload)
            else:
                print(json.dumps(payload))

            time.sleep(interval)
    except KeyboardInterrupt:
        print('\nStream interrupted by user')


def create_kafka_producer(broker):
    try:
        from kafka import KafkaProducer
    except Exception as e:
        raise RuntimeError('kafka-python is required to stream to Kafka') from e

    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3,
        acks='all',
    )
    return producer


def main():
    parser = argparse.ArgumentParser(description='Synthetic C-MAPSS-like data simulator')
    parser.add_argument('--units', type=int, default=5, help='Number of engine units')
    parser.add_argument('--cycles', type=int, default=200, help='Cycles per unit (approx)')
    parser.add_argument('--out', type=str, default=None, help='CSV output path')
    parser.add_argument('--kafka', action='store_true', help='Stream to Kafka instead of writing CSV')
    parser.add_argument('--broker', type=str, default=os.getenv('KAFKA_BROKER', 'localhost:9092'), help='Kafka broker')
    parser.add_argument('--interval', type=float, default=0.1, help='Seconds between rows when streaming')

    args = parser.parse_args()

    df = generate_dataset(units=args.units, cycles=args.cycles)

    if args.out:
        out_dir = os.path.dirname(args.out)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)
        df.to_csv(args.out, index=False)
        print(f'Wrote {len(df)} rows to {args.out}')

    if args.kafka:
        producer = create_kafka_producer(args.broker)
        print(f'Streaming to Kafka broker {args.broker} on topic sensor-telemetry')
        stream_rows(df, args.interval, kafka_producer=producer, topic='sensor-telemetry')
        producer.flush()
        producer.close()
    elif not args.out:
        # If neither kafka nor out specified, print sample rows
        print(df.head().to_csv(index=False))


if __name__ == '__main__':
    main()