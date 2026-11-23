"""Bounded synthetic happy-path probe for the isolated Module 06 stack only.

This is not a complete streaming acceptance suite; see the integration plan.
"""
import json
import math
import os
import re
import time

import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.structs import TopicPartition

from ml.models.compatibility import FEATURE_COLUMNS, validate_model_artifacts
from ml.models.inference import RULInferenceEngine

group = os.environ['M6_GROUP']
if not re.fullmatch(r'm6-acceptance-[0-9]+-[0-9]+', group):
    raise ValueError('unexpected integration group')
model_path = '/model/lstm_raw_20260626T082955Z.pt'
report = validate_model_artifacts(model_path, '/scalers')
engine = RULInferenceEngine(model_path, device='cpu')
admin = KafkaAdminClient(bootstrap_servers='kafka:9092', request_timeout_ms=10000)
deadline = time.monotonic() + 90
while True:
    state = admin.describe_consumer_groups([group])[0]
    if state.members and state.state == 'Stable':
        break
    if time.monotonic() >= deadline:
        raise TimeoutError('prediction group did not become ready')
    time.sleep(1)
admin.close()
producer = KafkaProducer(bootstrap_servers='kafka:9092', acks='all',
                         value_serializer=lambda value: json.dumps(value, allow_nan=False).encode())
output = KafkaConsumer(bootstrap_servers='kafka:9092', group_id=None, enable_auto_commit=False,
                       value_deserializer=lambda value: json.loads(value.decode()))
output.assign([TopicPartition('m6-test-output', 0)])
output.seek_to_beginning()
offsets = KafkaConsumer(bootstrap_servers='kafka:9092', group_id=group, enable_auto_commit=False)
input_partition = TopicPartition('m6-test-input', 0)
def record(cycle):
    return {'dataset_id': 'FD001', 'unit_id': 1, 'run_id': 'm6-synthetic', 'cycle': cycle,
            'timestamp': '2026-01-01T00:00:00Z', 'features': [0.25] * len(FEATURE_COLUMNS),
            'feature_order': list(FEATURE_COLUMNS), 'preprocessing_artifact_version': report['artifact_version']}

try:
    for cycle in range(1, 30):
        producer.send('m6-test-input', record(cycle)).get(timeout=10)
    deadline = time.monotonic() + 90
    while offsets.committed(input_partition) != 29:
        if time.monotonic() >= deadline:
            raise TimeoutError('worker did not commit the 29-cycle prefix')
        time.sleep(1)
    if output.poll(timeout_ms=1000):
        raise AssertionError('prediction produced before 30 cycles')
    producer.send('m6-test-input', record(30)).get(timeout=10)
    deadline = time.monotonic() + 90
    found = []
    while not found and time.monotonic() < deadline:
        for messages in output.poll(timeout_ms=1000).values():
            found.extend(message.value for message in messages)
    if len(found) != 1:
        raise AssertionError(f'expected one prediction, got {len(found)}')
    prediction = found[0]
    expected = engine.predict(np.full((30, 24), .25, dtype=np.float32))
    if (not math.isfinite(prediction['rul']) or abs(prediction['rul'] - expected) > 1e-5
            or prediction['dataset_id'] != 'FD001' or prediction['unit_id'] != 1
            or prediction['run_id'] != 'm6-synthetic' or prediction['cycle'] != 30
            or prediction['preprocessing_artifact_version'] != report['artifact_version']
            or prediction['model_version'] != 'lstm_raw_20260626T082955Z'):
        raise AssertionError('prediction does not match verified direct inference or metadata')
    print('PASS: no output at 29 cycles; cycle 30 matches direct raw-model inference')
finally:
    producer.close()
    output.close()
    offsets.close()
