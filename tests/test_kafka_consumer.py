import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest


MODULE_PATH = Path(__file__).parents[1] / "ingestion" / "src" / "kafka_consumer.py"
SPEC = importlib.util.spec_from_file_location("pms_kafka_consumer", MODULE_PATH)
consumer = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(consumer)


def telemetry(cycle=1):
    return {
        "dataset_id": "FD001",
        "unit_id": 7,
        "run_id": "test-run",
        "time_cycles": cycle,
        "operational_setting_1": 0.1,
        "operational_setting_2": 0.2,
        "operational_setting_3": 0.3,
        "sensor_1": 1.5,
        "timestamp": "2026-11-11T18:00:00+00:00",
    }


def test_consumer_disables_auto_commit():
    with patch.object(consumer, "KafkaConsumer", return_value=Mock()) as factory:
        result = consumer.create_kafka_consumer()

    assert result is not None
    assert factory.call_args.kwargs["enable_auto_commit"] is False
    assert factory.call_args.kwargs["value_deserializer"](b'{"unit_id": 1}') == b'{"unit_id": 1}'


def test_point_identity_is_stable_for_replay():
    first = consumer.build_point(telemetry(12)).to_line_protocol()
    second = consumer.build_point(telemetry(12)).to_line_protocol()

    assert first == second
    assert "dataset_id=FD001" in first
    assert "cycle=12" in first


def test_absent_source_timestamps_are_rejected():
    message = telemetry(1)
    del message["timestamp"]

    with pytest.raises(ValueError, match="timestamp"):
        consumer.build_point(message)


def test_successful_write_returns_without_retry():
    api = Mock()

    assert consumer.write_to_influxdb(api, telemetry()) is True
    api.write.assert_called_once()


def test_transient_write_retries_then_succeeds():
    api = Mock()
    api.write.side_effect = [TimeoutError("timeout"), None]

    with patch.object(consumer.time, "sleep") as sleep:
        assert consumer.write_to_influxdb(api, telemetry()) is True

    assert api.write.call_count == 2
    sleep.assert_called_once()


@pytest.mark.parametrize("status", [401, 403])
def test_auth_write_failure_is_not_retried(status):
    api = Mock()
    error = RuntimeError("rejected")
    error.status = status
    api.write.side_effect = error

    with patch.object(consumer.time, "sleep") as sleep:
        with pytest.raises(RuntimeError):
            consumer.write_to_influxdb(api, telemetry())

    api.write.assert_called_once()
    sleep.assert_not_called()


def test_exhausted_write_failure_is_raised_for_replay():
    api = Mock()
    api.write.side_effect = TimeoutError("unavailable")

    with patch.object(consumer.time, "sleep"):
        with pytest.raises(TimeoutError):
            consumer.write_to_influxdb(api, telemetry())

    assert api.write.call_count == consumer.MAX_WRITE_RETRIES


def test_malformed_record_is_rejected_before_write():
    with pytest.raises(ValueError):
        consumer.decode_message({"unit_id": 1})


def test_offset_commit_is_next_offset():
    kafka_consumer = Mock()
    message = Mock(topic="sensor-telemetry", partition=2, offset=41)

    consumer.commit_message(kafka_consumer, message)

    partition, metadata = next(iter(kafka_consumer.commit.call_args.args[0].items()))
    assert (partition.topic, partition.partition) == ("sensor-telemetry", 2)
    assert metadata.offset == 42
    assert metadata.leader_epoch == -1
    assert kafka_consumer.commit.call_args.kwargs["timeout_ms"] == consumer.COMMIT_TIMEOUT_MS


def test_invalid_record_is_sent_to_dead_letter():
    producer = Mock()
    message = Mock(
        value=b'{"unit_id":',
        topic="sensor-telemetry",
        partition=0,
        offset=9,
    )

    consumer.dead_letter(producer, message, ValueError("invalid JSON"))

    producer.send.assert_called_once()
    assert producer.send.call_args.args[0] == consumer.DEAD_LETTER_TOPIC
    producer.send.return_value.get.assert_called_once_with(timeout=10)


def test_dead_letter_send_failure_is_not_acknowledgeable():
    producer = Mock()
    producer.send.return_value.get.side_effect = TimeoutError("broker unavailable")
    message = Mock(value=b"bad", topic="sensor-telemetry", partition=0, offset=9)

    with pytest.raises(TimeoutError):
        consumer.dead_letter(producer, message, ValueError("invalid"))


def test_dead_letter_producer_requires_all_acks():
    with patch.object(consumer, "KafkaProducer", return_value=Mock()) as factory:
        consumer.create_dead_letter_producer()

    assert factory.call_args.kwargs["acks"] == "all"
    assert factory.call_args.kwargs["retries"] == 5
    assert factory.call_args.kwargs["max_in_flight_requests_per_connection"] == 1


def test_persistence_failure_prevents_processing_success():
    api = Mock()
    api.write.side_effect = TimeoutError("unavailable")
    producer = Mock()
    message = Mock(value=json.dumps(telemetry()).encode(), topic="sensor-telemetry", partition=0, offset=2)

    with patch.object(consumer.time, "sleep"):
        with pytest.raises(TimeoutError):
            consumer.process_message(api, producer, message)

    producer.send.assert_not_called()
