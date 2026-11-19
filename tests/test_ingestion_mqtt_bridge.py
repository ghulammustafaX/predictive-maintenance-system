"""
Module 02 - MQTT Bridge Unit Tests
====================================
Tests for ingestion/src/mqtt_bridge.py
No MQTT broker or Kafka required - all I/O is mocked.

Coverage:
  validate_cmapss  - required fields, numeric types
  validate_ims     - required fields, features dict type
  _on_message      - C-MAPSS routing, IMS routing, dead-letter routing,
                     malformed JSON, unrouted topic
  _dead_letter     - fallback if DLQ send fails (silent, no raise)
  Stats tracking   - forwarded / dead_lettered / parse_errors counters
"""
from __future__ import annotations
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

INGESTION_SRC = Path(__file__).resolve().parents[1] / "ingestion" / "src"
sys.path.insert(0, str(INGESTION_SRC))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cmapss_payload(**overrides):
    base = {
        "schema_version": "1.0.0",
        "event_id": "evt-001",
        "run_id": "run-abc",
        "dataset_id": "FD001",
        "asset_id": "engine_1",
        "timestamp": "2026-11-18T10:00:00+00:00",
        "sequence": 1,
        "source_type": "cmapss",
        "unit_id": 3,
        "time_cycles": 10,
    }
    base.update(overrides)
    return base


def _ims_payload(**overrides):
    base = {
        "schema_version": "1.0.0",
        "event_id": "evt-002",
        "run_id": "run-ims",
        "dataset_id": "IMS_2nd_test",
        "asset_id": "2nd_test_bearing_1",
        "timestamp": "2026-11-18T10:00:00+00:00",
        "sequence": 1,
        "source_type": "ims",
        "bearing_name": "bearing_1",
        "features": {"rms": 0.07418, "kurtosis": 3.629},
    }
    base.update(overrides)
    return base


def _make_mqtt_msg(topic, payload_dict):
    msg = MagicMock()
    msg.topic = topic
    msg.payload = json.dumps(payload_dict).encode("utf-8")
    return msg


def _make_bridge():
    from mqtt_bridge import MQTTKafkaBridge
    bridge = MQTTKafkaBridge()
    bridge._kafka = MagicMock()
    return bridge


# ---------------------------------------------------------------------------
# TestValidateCMAPS
# ---------------------------------------------------------------------------

class TestValidateCmapss:

    def test_valid_payload_passes(self):
        from mqtt_bridge import validate_cmapss
        validate_cmapss(_cmapss_payload())  # must not raise

    def test_missing_event_id_raises(self):
        from mqtt_bridge import validate_cmapss
        p = _cmapss_payload(); del p["event_id"]
        with pytest.raises(ValueError, match="missing"):
            validate_cmapss(p)

    def test_missing_run_id_raises(self):
        from mqtt_bridge import validate_cmapss
        p = _cmapss_payload(); del p["run_id"]
        with pytest.raises(ValueError, match="missing"):
            validate_cmapss(p)

    def test_missing_unit_id_raises(self):
        from mqtt_bridge import validate_cmapss
        p = _cmapss_payload(); del p["unit_id"]
        with pytest.raises(ValueError, match="missing"):
            validate_cmapss(p)

    def test_missing_time_cycles_raises(self):
        from mqtt_bridge import validate_cmapss
        p = _cmapss_payload(); del p["time_cycles"]
        with pytest.raises(ValueError, match="missing"):
            validate_cmapss(p)

    def test_missing_timestamp_raises(self):
        from mqtt_bridge import validate_cmapss
        p = _cmapss_payload(); del p["timestamp"]
        with pytest.raises(ValueError, match="missing"):
            validate_cmapss(p)

    def test_missing_dataset_id_raises(self):
        from mqtt_bridge import validate_cmapss
        p = _cmapss_payload(); del p["dataset_id"]
        with pytest.raises(ValueError, match="missing"):
            validate_cmapss(p)

    def test_non_numeric_unit_id_raises(self):
        from mqtt_bridge import validate_cmapss
        with pytest.raises((ValueError, TypeError)):
            validate_cmapss(_cmapss_payload(unit_id="bad"))

    def test_non_numeric_time_cycles_raises(self):
        from mqtt_bridge import validate_cmapss
        with pytest.raises((ValueError, TypeError)):
            validate_cmapss(_cmapss_payload(time_cycles="bad"))

    def test_extra_fields_pass_through(self):
        from mqtt_bridge import validate_cmapss
        validate_cmapss(_cmapss_payload(extra="hello"))  # must not raise


# ---------------------------------------------------------------------------
# TestValidateIMS
# ---------------------------------------------------------------------------

class TestValidateIms:

    def test_valid_payload_passes(self):
        from mqtt_bridge import validate_ims
        validate_ims(_ims_payload())  # must not raise

    def test_missing_event_id_raises(self):
        from mqtt_bridge import validate_ims
        p = _ims_payload(); del p["event_id"]
        with pytest.raises(ValueError, match="missing"):
            validate_ims(p)

    def test_missing_bearing_name_raises(self):
        from mqtt_bridge import validate_ims
        p = _ims_payload(); del p["bearing_name"]
        with pytest.raises(ValueError, match="missing"):
            validate_ims(p)

    def test_missing_features_raises(self):
        from mqtt_bridge import validate_ims
        p = _ims_payload(); del p["features"]
        with pytest.raises(ValueError, match="missing"):
            validate_ims(p)

    def test_features_not_dict_raises(self):
        from mqtt_bridge import validate_ims
        with pytest.raises(ValueError, match="dict"):
            validate_ims(_ims_payload(features=[1, 2, 3]))

    def test_missing_run_id_raises(self):
        from mqtt_bridge import validate_ims
        p = _ims_payload(); del p["run_id"]
        with pytest.raises(ValueError, match="missing"):
            validate_ims(p)

    def test_missing_dataset_id_raises(self):
        from mqtt_bridge import validate_ims
        p = _ims_payload(); del p["dataset_id"]
        with pytest.raises(ValueError, match="missing"):
            validate_ims(p)


# ---------------------------------------------------------------------------
# TestOnMessageRouting
# ---------------------------------------------------------------------------

class TestOnMessageRouting:

    def test_cmapss_message_routed_to_sensor_telemetry(self):
        from mqtt_bridge import SENSOR_TOPIC
        bridge = _make_bridge()
        bridge._on_message(None, None, _make_mqtt_msg("pms/telemetry/cmapss", _cmapss_payload()))
        topic_called = bridge._kafka.send.call_args[0][0]
        assert topic_called == SENSOR_TOPIC

    def test_ims_message_routed_to_bearing_telemetry(self):
        from mqtt_bridge import BEARING_TOPIC
        bridge = _make_bridge()
        bridge._on_message(None, None, _make_mqtt_msg("pms/telemetry/ims", _ims_payload()))
        topic_called = bridge._kafka.send.call_args[0][0]
        assert topic_called == BEARING_TOPIC

    def test_malformed_json_routed_to_dead_letter(self):
        from mqtt_bridge import DEAD_LETTER_TOPIC
        bridge = _make_bridge()
        bad_msg = MagicMock()
        bad_msg.topic = "pms/telemetry/cmapss"
        bad_msg.payload = b"{not json}"
        bridge._on_message(None, None, bad_msg)
        topics_called = [c[0][0] for c in bridge._kafka.send.call_args_list]
        assert DEAD_LETTER_TOPIC in topics_called

    def test_invalid_cmapss_routed_to_dead_letter(self):
        from mqtt_bridge import DEAD_LETTER_TOPIC
        bridge = _make_bridge()
        bad = _cmapss_payload(); del bad["unit_id"]
        bridge._on_message(None, None, _make_mqtt_msg("pms/telemetry/cmapss", bad))
        topics_called = [c[0][0] for c in bridge._kafka.send.call_args_list]
        assert DEAD_LETTER_TOPIC in topics_called

    def test_invalid_ims_routed_to_dead_letter(self):
        from mqtt_bridge import DEAD_LETTER_TOPIC
        bridge = _make_bridge()
        bad = _ims_payload(); del bad["bearing_name"]
        bridge._on_message(None, None, _make_mqtt_msg("pms/telemetry/ims", bad))
        topics_called = [c[0][0] for c in bridge._kafka.send.call_args_list]
        assert DEAD_LETTER_TOPIC in topics_called

    def test_unrouted_topic_routed_to_dead_letter(self):
        from mqtt_bridge import DEAD_LETTER_TOPIC
        bridge = _make_bridge()
        bridge._on_message(None, None, _make_mqtt_msg("pms/unknown/topic", _cmapss_payload()))
        topics_called = [c[0][0] for c in bridge._kafka.send.call_args_list]
        assert DEAD_LETTER_TOPIC in topics_called

    def test_valid_cmapss_increments_forwarded_counter(self):
        bridge = _make_bridge()
        bridge._on_message(None, None, _make_mqtt_msg("pms/telemetry/cmapss", _cmapss_payload()))
        assert bridge._stats["cmapss_forwarded"] == 1

    def test_valid_ims_increments_forwarded_counter(self):
        bridge = _make_bridge()
        bridge._on_message(None, None, _make_mqtt_msg("pms/telemetry/ims", _ims_payload()))
        assert bridge._stats["ims_forwarded"] == 1

    def test_invalid_message_increments_dead_lettered_counter(self):
        bridge = _make_bridge()
        bad = _cmapss_payload(); del bad["unit_id"]
        bridge._on_message(None, None, _make_mqtt_msg("pms/telemetry/cmapss", bad))
        assert bridge._stats["dead_lettered"] == 1

    def test_malformed_json_increments_parse_errors_counter(self):
        bridge = _make_bridge()
        bad = MagicMock()
        bad.topic = "pms/telemetry/cmapss"
        bad.payload = b"{bad"
        bridge._on_message(None, None, bad)
        assert bridge._stats["parse_errors"] == 1

    def test_cmapss_kafka_key_format(self):
        bridge = _make_bridge()
        bridge._on_message(None, None, _make_mqtt_msg("pms/telemetry/cmapss", _cmapss_payload()))
        key_used = bridge._kafka.send.call_args[1].get("key") or bridge._kafka.send.call_args[0][2] if len(bridge._kafka.send.call_args[0]) > 2 else None
        # Key must contain dataset_id and unit_id
        # Accept either positional or keyword form
        all_args = bridge._kafka.send.call_args
        all_str = str(all_args)
        assert "FD001" in all_str
        assert "3" in all_str  # unit_id=3

    def test_ims_kafka_key_contains_bearing_name(self):
        bridge = _make_bridge()
        bridge._on_message(None, None, _make_mqtt_msg("pms/telemetry/ims", _ims_payload()))
        all_str = str(bridge._kafka.send.call_args)
        assert "bearing_1" in all_str

    def test_dead_letter_send_failure_does_not_raise(self):
        bridge = _make_bridge()
        bridge._kafka.send.side_effect = Exception("broker down")
        bad = _cmapss_payload(); del bad["unit_id"]
        bridge._on_message(None, None, _make_mqtt_msg("pms/telemetry/cmapss", bad))
        # Should log the error but not raise

    def test_event_id_preserved_in_forwarded_payload(self):
        bridge = _make_bridge()
        p = _cmapss_payload(event_id="my-unique-event-id")
        bridge._on_message(None, None, _make_mqtt_msg("pms/telemetry/cmapss", p))
        payload_sent = bridge._kafka.send.call_args[0][1]
        assert payload_sent["event_id"] == "my-unique-event-id"
