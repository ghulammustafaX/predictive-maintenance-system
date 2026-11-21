import unittest
from collections import OrderedDict
import threading
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

from ml.prediction.service import PredictionService, FEATURE_COLUMNS


class Engine:
    def __init__(self): self.windows = []
    def predict(self, values):
        self.windows.append(values.copy())
        return 3.0


def service():
    worker = PredictionService.__new__(PredictionService)
    worker.windows, worker.last_cycles, worker.last_metadata = OrderedDict(), {}, {}
    worker.max_windows, worker.engine, worker.model_version = 1024, Engine(), 'test'
    worker.publish_prediction = lambda payload: None
    return worker


def record(cycle, unit=1, run='run'):
    return dict(dataset_id='FD001', unit_id=unit, run_id=run, cycle=cycle,
                features=[float(cycle)] * len(FEATURE_COLUMNS))


class WindowTests(unittest.TestCase):
    def test_first_window_has_exactly_consecutive_vectors(self):
        worker = service()
        for cycle in range(1, 30): self.assertIsNone(worker.process_record(record(cycle)))
        self.assertEqual(worker.process_record(record(30)), 3.0)
        self.assertEqual(worker.engine.windows[0][:, 0].tolist(), list(range(1, 31)))

    def test_gap_discards_old_window_and_rewarms(self):
        worker = service()
        for cycle in list(range(1, 30)) + list(range(31, 60)):
            self.assertIsNone(worker.process_record(record(cycle)))
        self.assertEqual(worker.process_record(record(60)), 3.0)
        self.assertEqual(worker.engine.windows[0][:, 0].tolist(), list(range(31, 61)))

    def test_duplicate_out_of_order_and_separate_identity(self):
        worker = service()
        for cycle in range(1, 30): worker.process_record(record(cycle))
        self.assertIsNone(worker.process_record(record(29)))
        self.assertIsNone(worker.process_record(record(5)))
        self.assertIsNone(worker.process_record(record(30, unit=2)))
        self.assertIsNone(worker.process_record(record(30, run='new')))
        self.assertEqual(worker.process_record(record(30)), 3.0)
        self.assertEqual(len(worker.engine.windows), 1)

    def test_invalid_cycle_and_dataset(self):
        worker = service()
        for cycle in [0, -1, True, 1.5, '1']:
            with self.subTest(cycle=cycle), self.assertRaises(ValueError): worker.process_record(record(cycle))
        bad = record(1); bad['dataset_id'] = 'unknown'
        with self.assertRaises(ValueError): worker.process_record(bad)

    def test_validated_artifact_version_required(self):
        worker = service()
        worker.expected_preprocessing_artifact_version = 'a1-v2'
        with self.assertRaisesRegex(ValueError, 'artifact version'):
            worker.process_record(record(1))
        bad = record(1); bad['preprocessing_artifact_version'] = 'older'
        with self.assertRaisesRegex(ValueError, 'artifact version'):
            worker.process_record(bad)
        good = record(1); good['preprocessing_artifact_version'] = 'a1-v2'
        self.assertIsNone(worker.process_record(good))

    def test_delivery_failure_stops_before_later_offset(self):
        for failure in range(3):
            worker = service()
            worker.input_topic, worker.output_topic, worker.kafka_servers, worker.consumer_group = 'input', 'output', 'unused', 'isolated'
            worker._consumer_ready, worker._stop = threading.Event(), threading.Event()
            worker._consumer_error, worker.influx = None, None
            worker.influx_write = object()
            worker.compatibility_report = {'checkpoint_sha256': 'model', 'artifact_version': 'test', 'artifact_sha256': {}}
            records = [SimpleNamespace(topic='input', partition=0, offset=i, value=i) for i in range(3)]
            class Consumer:
                def __init__(self): self.commits = []
                def __iter__(self):
                    yield from records
                    worker._stop.set()
                def subscribe(self, topics, listener): pass
                def commit(self, offsets): self.commits.append(offsets)
                def partitions_for_topic(self, topic): return {0}
                def assignment(self):
                    from kafka.structs import TopicPartition
                    return {TopicPartition('input', 0)}
                def committed(self, partition): return 0
                def close(self): pass
            class Producer:
                def flush(self): pass
                def close(self): pass
            consumer = Consumer()
            def process(value):
                if value == failure: raise TimeoutError('delivery unconfirmed')
            worker.process_record = process
            with tempfile.TemporaryDirectory() as temp, \
                 patch.dict('os.environ', {'PREDICTION_CHECKPOINT_PATH': temp + '/state.sqlite'}), \
                 patch('ml.prediction.checkpoint._persistent_mount', return_value=True), \
                 patch('ml.prediction.service.KafkaConsumer', return_value=consumer), \
                 patch('ml.prediction.service.KafkaProducer', return_value=Producer()):
                worker.start_consumer()
            self.assertEqual([list(c.values())[0].offset for c in consumer.commits], list(range(1, failure + 1)))
            self.assertIn('delivery unconfirmed', worker._consumer_error)


if __name__ == '__main__': unittest.main()
