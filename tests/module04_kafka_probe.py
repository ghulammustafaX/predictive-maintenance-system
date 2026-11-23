"""Bounded real-broker Module 04 acceptance probe; isolated Compose only."""
import hashlib
import json
import math
import os
import re
import sqlite3
import time
from pathlib import Path

import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from ml.data.preprocessing import FEATURE_COLUMNS, clip_outliers, load_scaler, load_training_stats
from ml.config import OUTLIER_Z_THRESHOLD
from preprocessing_test_artifacts import create_synthetic_preprocessing_artifacts

BROKER = 'kafka:9092'
INPUT, OUTPUT = 'm4-test-input', 'm4-test-output'
GROUP = os.environ['M4_GROUP']
if not re.fullmatch(r'm4-acceptance-[0-9]+-[0-9]+', GROUP):
    raise RuntimeError('invalid isolated test group')

artifacts = create_synthetic_preprocessing_artifacts(Path('/tmp/m4-probe-fixtures'), Path('/app'))
stats = load_training_stats('FD001', artifacts)
scaler = load_scaler('FD001', artifacts)
row = {name: float(stats[name]['median']) for name in FEATURE_COLUMNS}
row['sensor_2'] = float(stats['sensor_2']['mean'])
base = dict(row, dataset_id='FD001', unit_id='1', run_id='m4-run', time_cycles=1, timestamp='2026-01-01T00:00:00Z')
second = dict(base, time_cycles=2, sensor_2=float('inf'))
invalid = dict(base, time_cycles=3, sensor_2='malformed')
producer = KafkaProducer(bootstrap_servers=BROKER, acks='all', value_serializer=lambda v: json.dumps(v).encode())
observer = KafkaConsumer(bootstrap_servers=BROKER, enable_auto_commit=False, group_id=None,
                         value_deserializer=lambda v: json.loads(v.decode()))
inspector = KafkaConsumer(bootstrap_servers=BROKER, enable_auto_commit=False, group_id=GROUP)
tp_in, tp_out = TopicPartition(INPUT, 0), TopicPartition(OUTPUT, 0)
observer.assign([tp_out])
observer.seek_to_beginning(tp_out)


def checkpoint():
    path = Path('/checkpoint/state.sqlite')
    if not path.is_file():
        return None
    try:
        with sqlite3.connect('file:/checkpoint/state.sqlite?mode=ro', uri=True, timeout=2) as db:
            item = db.execute('SELECT payload, checksum FROM checkpoints WHERE grp=? AND topic=? AND part=0', (GROUP, INPUT)).fetchone()
        if item is None or item[0] is None:
            return None
        if hashlib.sha256(item[0].encode()).hexdigest() != item[1]:
            raise AssertionError('checkpoint checksum mismatch')
        return json.loads(item[0])
    except sqlite3.OperationalError:
        return None


def output_poll(seconds):
    events = []
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        for records in observer.poll(timeout_ms=250).values():
            events.extend(record.value for record in records)
    return events


try:
    for expected_offset, event in enumerate((base, second)):
        result = producer.send(INPUT, value=event, partition=0).get(timeout=20)
        assert result.offset == expected_offset
    received = []
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        received.extend(output_poll(0.5))
        saved = checkpoint()
        if len(received) >= 2 and inspector.committed(tp_in) == 2 and saved and saved['next_offset'] == 2:
            break
    else:
        raise AssertionError('valid prefix did not produce output and durable commit before deadline')
    assert len(received) == 2
    expected = scaler.transform(clip_outliers(pd.DataFrame([row]), OUTLIER_Z_THRESHOLD, stats)[FEATURE_COLUMNS])[0]
    for cycle, event in enumerate(received, 1):
        assert event['cycle'] == cycle and event['run_id'] == 'm4-run'
        assert event['feature_order'] == FEATURE_COLUMNS
        assert all(math.isfinite(x) for x in event['features'])
        np.testing.assert_allclose(event['features'], expected, rtol=1e-6, atol=1e-7)
    print('PASS: valid and finite-fallback outputs; committed offset=2; checkpoint next_offset=2', flush=True)
    assert producer.send(INPUT, value=invalid, partition=0).get(timeout=20).offset == 2
    original_checkpoint = saved
    for _ in range(4):
        assert output_poll(2) == [], 'malformed input produced success output'
        assert inspector.committed(tp_in) == 2, 'malformed input advanced committed offset'
        assert checkpoint() == original_checkpoint, 'malformed input advanced or altered checkpoint'
    print('PASS: malformed offset=2 produced no output; committed offset and checkpoint stayed at 2 across retries', flush=True)
finally:
    producer.close(timeout=5)
    observer.close()
    inspector.close()
