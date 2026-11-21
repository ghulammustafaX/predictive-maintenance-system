from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pytest
from kafka.structs import TopicPartition

from ml.data.preprocessing import FEATURE_COLUMNS, load_training_stats
from ml.streaming.preprocessing import StreamingPreprocessor
from preprocessing_test_artifacts import create_synthetic_preprocessing_artifacts


class _Producer:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append(value)
        return SimpleNamespace(get=lambda timeout: None)

    def flush(self):
        pass

    def close(self):
        pass


class _Consumer:
    def __init__(self, records, committed):
        self.records = list(records)
        self.offsets = dict(committed)
        self.parts = {TopicPartition("input", p) for p in (0, 1)}

    def subscribe(self, topics, listener):
        listener.on_partitions_assigned(self.parts)

    def assignment(self):
        return self.parts

    def committed(self, part):
        return self.offsets.get(part.partition)

    def poll(self, **kwargs):
        if not self.records:
            raise KeyboardInterrupt()
        part, offset, message = self.records.pop(0)
        tp = TopicPartition("input", part)
        return {
            tp: [
                SimpleNamespace(
                    topic="input",
                    partition=part,
                    offset=offset,
                    value=message,
                )
            ]
        }

    def commit(self, offsets):
        for part, offset in offsets.items():
            self.offsets[part.partition] = offset.offset

    def close(self):
        pass


def test_restart_keeps_engine_run_and_partition_histories_separate(tmp_path):
    artifact_dir = create_synthetic_preprocessing_artifacts(
        tmp_path / "test-artifacts",
        Path(__file__).resolve().parents[1],
    )
    stats = load_training_stats("FD001", artifact_dir)
    identities = [(0, "1", "run-a"), (0, "2", "run-a"), (1, "1", "run-b")]
    records = [[], []]
    offsets = defaultdict(int)
    expected = {}

    for part, unit, run in identities:
        continuous = StreamingPreprocessor(
            artifact_dir=artifact_dir,
            validate_startup=False,
        )
        for cycle in (1, 2):
            message = {
                column: float(stats[column]["median"])
                for column in FEATURE_COLUMNS
            }
            message.update(
                dataset_id="FD001",
                unit_id=unit,
                run_id=run,
                time_cycles=cycle,
            )
            if cycle == 1:
                message["sensor_2"] += int(unit) * 10 + part * 20
            else:
                message["sensor_2"] = None

            output = continuous.transform(message)
            if cycle == 2:
                expected[(unit, run)] = output["features"]
            records[cycle - 1].append((part, offsets[part], message))
            offsets[part] += 1

    # Offsets for each partition must follow the phase in which records are consumed.
    for phase in (0, 1):
        phase_counts = defaultdict(int)
        for index, (part, _, message) in enumerate(records[phase]):
            base = (
                0
                if phase == 0
                else sum(1 for previous_part, _, _ in records[0] if previous_part == part)
            )
            records[phase][index] = (
                part,
                base + phase_counts[part],
                message,
            )
            phase_counts[part] += 1

    producer1, producer2 = _Producer(), _Producer()
    c1 = _Consumer(records[0], {})
    c2 = _Consumer(records[1], {0: 2, 1: 1})
    checkpoint_path = tmp_path / "checkpoint.sqlite"

    with (
        patch.dict(
            "os.environ",
            {"PREPROCESSING_CHECKPOINT_PATH": str(checkpoint_path)},
        ),
        patch("ml.streaming.preprocessing.validate_checkpoint_path"),
    ):
        for consumer, producer in ((c1, producer1), (c2, producer2)):
            with (
                patch("ml.streaming.preprocessing.KafkaConsumer", return_value=consumer),
                patch("ml.streaming.preprocessing.KafkaProducer", return_value=producer),
            ):
                StreamingPreprocessor(
                    artifact_dir=artifact_dir,
                    validate_startup=False,
                ).run()

    assert len(producer2.sent) == 3
    for value in producer2.sent:
        np.testing.assert_allclose(
            value["features"],
            expected[(value["unit_id"], value["run_id"])],
            rtol=1e-6,
            atol=1e-7,
        )
    assert c2.offsets == {0: 4, 1: 2}
