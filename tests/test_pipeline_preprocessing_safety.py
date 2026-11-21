import time
import tempfile
from pathlib import Path
import unittest
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import patch

from kafka.structs import TopicPartition
from ml.streaming.preprocessing import StreamingPreprocessor


class Future:
    def __init__(self, fail): self.fail = fail
    def get(self, timeout):
        if self.fail: raise TimeoutError('delivery unconfirmed')


class Producer:
    def __init__(self, fail_once=()):
        self.fail_once = set(fail_once)
        self.sent = []
    def send(self, topic, key, value):
        self.sent.append((key, value))
        fail = key in self.fail_once
        self.fail_once.discard(key)
        return Future(fail)
    def flush(self): pass
    def close(self): pass


class Consumer:
    def __init__(self, counts, commit_fail_once=None):
        self.parts = {TopicPartition('input', p) for p in counts}
        self.counts = {TopicPartition('input', p): n for p, n in counts.items()}
        self.positions = defaultdict(int)
        self.commits = []
        self.paused = set()
        self.commit_fail_once = commit_fail_once
    def subscribe(self, topics, listener):
        self.listener = listener
        listener.on_partitions_assigned(self.parts)
    def assignment(self): return self.parts
    def committed(self, part):
        matches = [offset for partition, offset in self.commits if partition == part.partition]
        return matches[-1] if matches else None
    def poll(self, timeout_ms, max_records):
        for part in sorted(self.parts, key=lambda p: p.partition):
            pos = self.positions[part]
            if part not in self.paused and pos < self.counts[part]:
                self.positions[part] += 1
                return {part: [SimpleNamespace(topic='input', partition=part.partition, offset=pos,
                                               value={'partition': part.partition, 'offset': pos})]}
        if not self.paused and all(self.positions[p] >= self.counts[p] for p in self.parts):
            raise KeyboardInterrupt()
        time.sleep(0.03)
        return {}
    def commit(self, offsets):
        part, offset = next(iter(offsets.items()))
        if self.commit_fail_once == (part.partition, offset.offset):
            self.commit_fail_once = None
            raise RuntimeError('commit failed')
        self.commits.append((part.partition, offset.offset))
    def pause(self, part): self.paused.add(part)
    def resume(self, part): self.paused.remove(part)
    def seek(self, part, offset): self.positions[part] = offset
    def close(self): pass


class PreprocessingSafetyTests(unittest.TestCase):
    def execute(self, consumer, producer):
        worker = StreamingPreprocessor(validate_startup=False)
        worker.transform = lambda value: value
        with tempfile.TemporaryDirectory() as directory, \
             patch.dict('os.environ', {'PREPROCESSING_RETRY_SECONDS': '0.1', 'PREPROCESSING_CHECKPOINT_PATH': str(Path(directory) / 'state.sqlite')}), \
             patch('ml.streaming.preprocessing.validate_checkpoint_path'), \
             patch('ml.streaming.preprocessing.KafkaConsumer', return_value=consumer), \
             patch('ml.streaming.preprocessing.KafkaProducer', return_value=producer):
            worker.run()

    def test_failed_partition_retries_while_healthy_progresses(self):
        consumer = Consumer({0: 2, 1: 2})
        producer = Producer({b'input:0:0'})
        self.execute(consumer, producer)
        self.assertEqual(consumer.commits, [(1, 1), (1, 2), (0, 1), (0, 2)])
        self.assertEqual([key for key, _ in producer.sent].count(b'input:0:0'), 2)

    def test_middle_failure_does_not_commit_past_gap(self):
        consumer = Consumer({0: 3, 1: 1})
        self.execute(consumer, Producer({b'input:0:1'}))
        self.assertEqual(consumer.commits, [(0, 1), (1, 1), (0, 2), (0, 3)])

    def test_commit_failure_retries_same_key(self):
        consumer = Consumer({0: 2, 1: 1}, commit_fail_once=(0, 1))
        producer = Producer()
        self.execute(consumer, producer)
        self.assertEqual(consumer.commits, [(1, 1), (0, 1), (0, 2)])
        self.assertEqual([key for key, _ in producer.sent].count(b'input:0:0'), 1)


if __name__ == '__main__': unittest.main()
