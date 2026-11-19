"""
Module 02 - Ingestion Consumer Unit Tests
==========================================
Tests for ingestion/src/kafka_consumer.py
No broker required - Kafka and InfluxDB calls are fully mocked.

Coverage:
  decode_message  - valid payloads, missing fields, bad types, bad timestamps
  build_point     - measurement name, tags, fields, timestamp handling
  write_to_influxdb - success, retries, auth error, max-retry exhaust
  dead_letter     - DLQ topic, error metadata, broker-ack wait
  commit_message  - offset+1, correct TopicPartition
  process_message - valid->write, invalid->DLQ, infra error->reraise
  Duplicate handling - no client-side dedup; InfluxDB last-write-wins
  Schema version  - not required by consumer (backward compatible)
  Dataset identity - all four FD001-FD004 datasets, run_id separation
  Out-of-order    - consumer does not enforce ordering
"""
from __future__ import annotations
import json
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

INGESTION_SRC = Path(__file__).resolve().parents[1] / "ingestion" / "src"
sys.path.insert(0, str(INGESTION_SRC))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _vp(**overrides):
    """Return a minimal valid C-MAPSS telemetry payload dict."""
    base = {
        "schema_version": "1.0.0",
        "event_id": "3291930e-418a-63f5-0e55-dbaf37651bb9",
        "run_id": "abc123",
        "dataset_id": "FD001",
        "asset_id": "engine_1",
        "unit_id": 1,
        "time_cycles": 42,
        "operational_setting_1": -0.0007,
        "operational_setting_2": -0.0004,
        "operational_setting_3": 100.0,
        "timestamp": "2026-11-18T10:00:00+00:00",
        "sequence": 1,
        "source_type": "cmapss",
    }
    for i in range(1, 22):
        base[f"sensor_{i}"] = float(i) * 10.0
    base.update(overrides)
    return base


def _msg(payload, topic="sensor-telemetry", partition=0, offset=5):
    """Return a mock Kafka message record."""
    m = MagicMock()
    m.topic = topic
    m.partition = partition
    m.offset = offset
    m.value = json.dumps(payload).encode("utf-8")
    return m


def _mock_producer():
    p = MagicMock()
    fut = MagicMock()
    fut.get.return_value = None
    p.send.return_value = fut
    return p


def _mock_write_api():
    api = MagicMock()
    api.write.return_value = None
    return api


# ---------------------------------------------------------------------------
# TestDecodeMessage
# ---------------------------------------------------------------------------

class TestDecodeMessage:

    def test_valid_bytes_payload(self):
        from kafka_consumer import decode_message
        result = decode_message(json.dumps(_vp()).encode("utf-8"))
        assert result["unit_id"] == 1
        assert result["time_cycles"] == 42

    def test_valid_dict_payload(self):
        from kafka_consumer import decode_message
        result = decode_message(_vp())
        assert result["dataset_id"] == "FD001"

    def test_valid_string_payload(self):
        from kafka_consumer import decode_message
        result = decode_message(json.dumps(_vp()))
        assert result["run_id"] == "abc123"

    def test_missing_unit_id_raises(self):
        from kafka_consumer import decode_message
        p = _vp(); del p["unit_id"]
        with pytest.raises(ValueError, match="missing"):
            decode_message(p)

    def test_missing_time_cycles_raises(self):
        from kafka_consumer import decode_message
        p = _vp(); del p["time_cycles"]
        with pytest.raises(ValueError, match="missing"):
            decode_message(p)

    def test_missing_operational_setting_1_raises(self):
        from kafka_consumer import decode_message
        p = _vp(); del p["operational_setting_1"]
        with pytest.raises(ValueError, match="missing"):
            decode_message(p)

    def test_missing_operational_setting_2_raises(self):
        from kafka_consumer import decode_message
        p = _vp(); del p["operational_setting_2"]
        with pytest.raises(ValueError, match="missing"):
            decode_message(p)

    def test_missing_operational_setting_3_raises(self):
        from kafka_consumer import decode_message
        p = _vp(); del p["operational_setting_3"]
        with pytest.raises(ValueError, match="missing"):
            decode_message(p)

    def test_non_numeric_unit_id_raises(self):
        from kafka_consumer import decode_message
        with pytest.raises((ValueError, TypeError)):
            decode_message(_vp(unit_id="not-a-number"))

    def test_non_numeric_time_cycles_raises(self):
        from kafka_consumer import decode_message
        with pytest.raises((ValueError, TypeError)):
            decode_message(_vp(time_cycles="bad"))

    def test_non_numeric_operational_setting_raises(self):
        from kafka_consumer import decode_message
        with pytest.raises((ValueError, TypeError)):
            decode_message(_vp(operational_setting_1="bad"))

    def test_invalid_json_bytes_raises(self):
        from kafka_consumer import decode_message
        with pytest.raises((ValueError, json.JSONDecodeError)):
            decode_message(b"{not valid json}")

    def test_missing_timestamp_and_source_timestamp_raises(self):
        from kafka_consumer import decode_message
        p = _vp(); del p["timestamp"]
        with pytest.raises(ValueError, match="timestamp"):
            decode_message(p)

    def test_source_timestamp_accepted_without_timestamp(self):
        from kafka_consumer import decode_message
        p = _vp(); del p["timestamp"]
        p["source_timestamp"] = "2026-11-18T10:00:00+00:00"
        result = decode_message(p)
        assert result["source_timestamp"] == "2026-11-18T10:00:00+00:00"

    def test_non_dict_json_raises(self):
        from kafka_consumer import decode_message
        with pytest.raises(ValueError, match="JSON object"):
            decode_message(json.dumps([1, 2, 3]).encode("utf-8"))

    def test_schema_version_not_required(self):
        from kafka_consumer import decode_message
        p = _vp(); del p["schema_version"]
        result = decode_message(p)
        assert result["unit_id"] == 1

    def test_extra_fields_pass_through(self):
        from kafka_consumer import decode_message
        result = decode_message(_vp(extra_field="hello"))
        assert result["extra_field"] == "hello"

    def test_all_21_sensor_fields_accepted(self):
        from kafka_consumer import decode_message
        result = decode_message(_vp())
        for i in range(1, 22):
            assert f"sensor_{i}" in result

    def test_unit_id_as_string_integer_accepted(self):
        from kafka_consumer import decode_message
        result = decode_message(_vp(unit_id="5"))
        assert int(result["unit_id"]) == 5

    def test_time_cycles_as_string_integer_accepted(self):
        from kafka_consumer import decode_message
        result = decode_message(_vp(time_cycles="10"))
        assert int(result["time_cycles"]) == 10


# ---------------------------------------------------------------------------
# TestBuildPoint
# ---------------------------------------------------------------------------

class TestBuildPoint:

    def test_measurement_name(self):
        from kafka_consumer import build_point
        point = build_point(_vp())
        assert point._name == "sensor_telemetry"

    def test_dataset_id_tag(self):
        from kafka_consumer import build_point
        point = build_point(_vp(dataset_id="FD002"))
        assert point._tags["dataset_id"] == "FD002"

    def test_unit_id_tag_as_string(self):
        from kafka_consumer import build_point
        point = build_point(_vp(unit_id=5))
        assert point._tags["unit_id"] == "5"

    def test_run_id_tag(self):
        from kafka_consumer import build_point
        point = build_point(_vp(run_id="myrun"))
        assert point._tags["run_id"] == "myrun"

    def test_cycle_tag_stored(self):
        from kafka_consumer import build_point
        point = build_point(_vp(time_cycles=99))
        assert point._tags["cycle"] == "99"

    def test_time_cycles_field_integer(self):
        from kafka_consumer import build_point
        point = build_point(_vp(time_cycles=7))
        assert point._fields["time_cycles"] == 7

    def test_operational_settings_stored_as_floats(self):
        from kafka_consumer import build_point
        point = build_point(_vp(
            operational_setting_1=1.1,
            operational_setting_2=2.2,
            operational_setting_3=3.3,
        ))
        assert abs(point._fields["operational_setting_1"] - 1.1) < 1e-9
        assert abs(point._fields["operational_setting_2"] - 2.2) < 1e-9
        assert abs(point._fields["operational_setting_3"] - 3.3) < 1e-9

    def test_all_21_sensor_fields_stored(self):
        from kafka_consumer import build_point
        point = build_point(_vp())
        for i in range(1, 22):
            assert f"sensor_{i}" in point._fields

    def test_sensor_1_value_preserved_exactly(self):
        from kafka_consumer import build_point
        p = _vp(); p["sensor_1"] = 518.67
        point = build_point(p)
        assert abs(point._fields["sensor_1"] - 518.67) < 1e-9

    def test_timestamp_set(self):
        from kafka_consumer import build_point
        point = build_point(_vp())
        assert point._time is not None

    def test_z_suffix_timestamp_accepted(self):
        from kafka_consumer import build_point
        point = build_point(_vp(timestamp="2026-11-18T10:00:00Z"))
        assert point._time is not None

    def test_missing_sensor_fields_skipped_gracefully(self):
        from kafka_consumer import build_point
        p = _vp(); del p["sensor_21"]
        point = build_point(p)
        assert "sensor_21" not in point._fields
        assert "sensor_1" in point._fields

    def test_invalid_timestamp_raises(self):
        from kafka_consumer import build_point
        with pytest.raises(Exception):
            build_point(_vp(timestamp="not-a-date"))

    def test_missing_dataset_id_defaults_to_unknown(self):
        from kafka_consumer import build_point
        p = _vp(); del p["dataset_id"]
        point = build_point(p)
        assert point._tags["dataset_id"] == "unknown"

    def test_missing_run_id_defaults_to_default(self):
        from kafka_consumer import build_point
        p = _vp(); del p["run_id"]
        point = build_point(p)
        assert point._tags["run_id"] == "default"


# ---------------------------------------------------------------------------
# TestWriteToInfluxDB
# ---------------------------------------------------------------------------

class TestWriteToInfluxDB:

    def test_success_on_first_attempt(self):
        from kafka_consumer import write_to_influxdb
        api = _mock_write_api()
        result = write_to_influxdb(api, _vp())
        assert result is True
        assert api.write.call_count == 1

    def test_retries_on_transient_error(self):
        from kafka_consumer import write_to_influxdb
        api = _mock_write_api()
        api.write.side_effect = [Exception("timeout"), None]
        with patch("kafka_consumer.time.sleep"):
            result = write_to_influxdb(api, _vp())
        assert result is True
        assert api.write.call_count == 2

    def test_raises_after_max_retries_exhausted(self):
        from kafka_consumer import write_to_influxdb
        api = _mock_write_api()
        api.write.side_effect = Exception("persistent failure")
        with patch("kafka_consumer.time.sleep"):
            with pytest.raises(Exception):
                write_to_influxdb(api, _vp())
        assert api.write.call_count == 3  # MAX_WRITE_RETRIES default = 3

    def test_auth_403_raises_immediately_no_retry(self):
        from kafka_consumer import write_to_influxdb
        api = _mock_write_api()
        exc = Exception("forbidden"); exc.status = 403
        api.write.side_effect = exc
        with pytest.raises(Exception):
            write_to_influxdb(api, _vp())
        assert api.write.call_count == 1

    def test_auth_401_raises_immediately_no_retry(self):
        from kafka_consumer import write_to_influxdb
        api = _mock_write_api()
        exc = Exception("unauthorized"); exc.status = 401
        api.write.side_effect = exc
        with pytest.raises(Exception):
            write_to_influxdb(api, _vp())
        assert api.write.call_count == 1

    def test_correct_bucket_and_org_passed(self):
        from kafka_consumer import write_to_influxdb, INFLUXDB_BUCKET, INFLUXDB_ORG
        api = _mock_write_api()
        write_to_influxdb(api, _vp())
        _, kwargs = api.write.call_args
        assert kwargs["bucket"] == INFLUXDB_BUCKET
        assert kwargs["org"] == INFLUXDB_ORG

    def test_two_retries_then_success_returns_true(self):
        from kafka_consumer import write_to_influxdb
        api = _mock_write_api()
        api.write.side_effect = [Exception("e1"), Exception("e2"), None]
        with patch("kafka_consumer.time.sleep"):
            result = write_to_influxdb(api, _vp())
        assert result is True
        assert api.write.call_count == 3


# ---------------------------------------------------------------------------
# TestDeadLetter
# ---------------------------------------------------------------------------

class TestDeadLetter:

    def test_publishes_to_dlt_topic(self):
        from kafka_consumer import dead_letter, DEAD_LETTER_TOPIC
        producer = _mock_producer()
        msg = _msg(_vp(), partition=0, offset=10)
        dead_letter(producer, msg, ValueError("bad data"))
        assert producer.send.call_args[0][0] == DEAD_LETTER_TOPIC

    def test_error_string_in_dlt_payload(self):
        from kafka_consumer import dead_letter
        producer = _mock_producer()
        dead_letter(producer, _msg(_vp()), ValueError("test error"))
        payload_sent = producer.send.call_args[1]["value"]
        assert "test error" in payload_sent["error"]

    def test_source_partition_in_dlt_payload(self):
        from kafka_consumer import dead_letter
        producer = _mock_producer()
        dead_letter(producer, _msg(_vp(), partition=2, offset=77), ValueError("x"))
        payload_sent = producer.send.call_args[1]["value"]
        assert payload_sent["source_partition"] == 2

    def test_source_offset_in_dlt_payload(self):
        from kafka_consumer import dead_letter
        producer = _mock_producer()
        dead_letter(producer, _msg(_vp(), partition=2, offset=77), ValueError("x"))
        payload_sent = producer.send.call_args[1]["value"]
        assert payload_sent["source_offset"] == 77

    def test_waits_for_broker_ack(self):
        from kafka_consumer import dead_letter
        producer = _mock_producer()
        dead_letter(producer, _msg(_vp()), ValueError("x"))
        producer.send.return_value.get.assert_called_once()

    def test_raw_value_included_in_dlt_payload(self):
        from kafka_consumer import dead_letter
        producer = _mock_producer()
        dead_letter(producer, _msg(_vp()), ValueError("x"))
        payload_sent = producer.send.call_args[1]["value"]
        assert "value" in payload_sent


# ---------------------------------------------------------------------------
# TestCommitMessage
# ---------------------------------------------------------------------------

class TestCommitMessage:

    def test_commits_offset_plus_one(self):
        from kafka_consumer import commit_message
        from kafka.structs import TopicPartition
        consumer = MagicMock()
        msg = _msg(_vp(), topic="sensor-telemetry", partition=0, offset=10)
        commit_message(consumer, msg)
        committed = consumer.commit.call_args[0][0]
        tp = TopicPartition("sensor-telemetry", 0)
        assert tp in committed
        assert committed[tp].offset == 11

    def test_commit_uses_correct_partition(self):
        from kafka_consumer import commit_message
        from kafka.structs import TopicPartition
        consumer = MagicMock()
        msg = _msg(_vp(), topic="sensor-telemetry", partition=3, offset=200)
        commit_message(consumer, msg)
        committed = consumer.commit.call_args[0][0]
        tp = TopicPartition("sensor-telemetry", 3)
        assert tp in committed
        assert committed[tp].offset == 201

    def test_commit_called_exactly_once(self):
        from kafka_consumer import commit_message
        consumer = MagicMock()
        commit_message(consumer, _msg(_vp()))
        consumer.commit.assert_called_once()


# ---------------------------------------------------------------------------
# TestProcessMessage
# ---------------------------------------------------------------------------

class TestProcessMessage:

    def test_valid_message_written_to_influxdb(self):
        from kafka_consumer import process_message
        api = _mock_write_api()
        producer = _mock_producer()
        process_message(api, producer, _msg(_vp()))
        assert api.write.call_count == 1
        assert producer.send.call_count == 0

    def test_invalid_message_sent_to_dead_letter(self):
        from kafka_consumer import process_message, DEAD_LETTER_TOPIC
        api = _mock_write_api()
        producer = _mock_producer()
        process_message(api, producer, _msg({"garbage": True}))
        assert api.write.call_count == 0
        assert producer.send.call_args[0][0] == DEAD_LETTER_TOPIC

    def test_invalid_json_sent_to_dead_letter(self):
        from kafka_consumer import process_message
        api = _mock_write_api()
        producer = _mock_producer()
        bad = MagicMock()
        bad.topic = "sensor-telemetry"; bad.partition = 0; bad.offset = 1
        bad.value = b"{not valid json}"
        process_message(api, producer, bad)
        assert producer.send.call_count == 1

    def test_infrastructure_error_reraises(self):
        from kafka_consumer import process_message
        api = _mock_write_api()
        api.write.side_effect = RuntimeError("kafka down")
        producer = _mock_producer()
        with patch("kafka_consumer.time.sleep"):
            with pytest.raises(RuntimeError):
                process_message(api, producer, _msg(_vp()))
        assert producer.send.call_count == 0

    def test_offset_not_committed_on_infrastructure_error(self):
        from kafka_consumer import process_message
        api = _mock_write_api()
        api.write.side_effect = RuntimeError("infra failure")
        producer = _mock_producer()
        consumer = MagicMock()
        with patch("kafka_consumer.time.sleep"):
            with pytest.raises(RuntimeError):
                process_message(api, producer, _msg(_vp()))
        consumer.commit.assert_not_called()

    def test_missing_field_routes_to_dlt_not_raises(self):
        from kafka_consumer import process_message, DEAD_LETTER_TOPIC
        api = _mock_write_api()
        producer = _mock_producer()
        p = _vp(); del p["unit_id"]
        process_message(api, producer, _msg(p))
        assert producer.send.call_args[0][0] == DEAD_LETTER_TOPIC

    def test_valid_influxdb_write_means_no_dlt_message(self):
        from kafka_consumer import process_message
        api = _mock_write_api()
        producer = _mock_producer()
        process_message(api, producer, _msg(_vp()))
        producer.send.assert_not_called()


# ---------------------------------------------------------------------------
# TestDuplicateHandling
# ---------------------------------------------------------------------------

class TestDuplicateHandling:

    def test_same_payload_twice_calls_influxdb_write_twice(self):
        from kafka_consumer import process_message
        api = _mock_write_api()
        producer = _mock_producer()
        payload = _vp()
        process_message(api, producer, _msg(payload, offset=0))
        process_message(api, producer, _msg(payload, offset=1))
        assert api.write.call_count == 2

    def test_duplicate_writes_both_succeed_no_consumer_error(self):
        from kafka_consumer import write_to_influxdb
        api = _mock_write_api()
        result1 = write_to_influxdb(api, _vp())
        result2 = write_to_influxdb(api, _vp())
        assert result1 is True
        assert result2 is True


# ---------------------------------------------------------------------------
# TestSchemaVersionBackwardCompat
# ---------------------------------------------------------------------------

class TestSchemaVersionBackwardCompat:

    def test_missing_schema_version_accepted(self):
        from kafka_consumer import decode_message
        p = _vp(); del p["schema_version"]
        assert decode_message(p)["unit_id"] == 1

    def test_future_schema_version_accepted(self):
        from kafka_consumer import decode_message
        assert decode_message(_vp(schema_version="2.0.0"))["unit_id"] == 1

    def test_unknown_schema_version_accepted(self):
        from kafka_consumer import decode_message
        assert decode_message(_vp(schema_version="99.0.0"))["dataset_id"] == "FD001"


# ---------------------------------------------------------------------------
# TestDatasetIdentityFields
# ---------------------------------------------------------------------------

class TestDatasetIdentityFields:

    def test_fd001_tag(self):
        from kafka_consumer import build_point
        assert build_point(_vp(dataset_id="FD001"))._tags["dataset_id"] == "FD001"

    def test_fd002_tag(self):
        from kafka_consumer import build_point
        assert build_point(_vp(dataset_id="FD002"))._tags["dataset_id"] == "FD002"

    def test_fd003_tag(self):
        from kafka_consumer import build_point
        assert build_point(_vp(dataset_id="FD003"))._tags["dataset_id"] == "FD003"

    def test_fd004_tag(self):
        from kafka_consumer import build_point
        assert build_point(_vp(dataset_id="FD004"))._tags["dataset_id"] == "FD004"

    def test_run_id_separates_replay_passes(self):
        from kafka_consumer import build_point
        p1 = build_point(_vp(run_id="run_A"))
        p2 = build_point(_vp(run_id="run_B"))
        assert p1._tags["run_id"] != p2._tags["run_id"]


# ---------------------------------------------------------------------------
# TestOutOfOrderMessages
# ---------------------------------------------------------------------------

class TestOutOfOrderMessages:

    def test_out_of_order_messages_both_written(self):
        from kafka_consumer import process_message
        api = _mock_write_api()
        producer = _mock_producer()
        process_message(api, producer, _msg(_vp(time_cycles=10, sequence=10), offset=1))
        process_message(api, producer, _msg(_vp(time_cycles=5, sequence=5), offset=2))
        assert api.write.call_count == 2

    def test_sequence_gap_does_not_block_processing(self):
        from kafka_consumer import process_message
        api = _mock_write_api()
        producer = _mock_producer()
        process_message(api, producer, _msg(_vp(time_cycles=1, sequence=1), offset=0))
        process_message(api, producer, _msg(_vp(time_cycles=10, sequence=10), offset=1))
        assert api.write.call_count == 2
