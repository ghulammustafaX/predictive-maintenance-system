"""
Module 1 Integration Tests
==========================
Runs against the isolated pms-m1-integration-test Compose project.
Requires: kafka:39092 and mosquitto:31883 to be reachable on localhost.

Start the stack first:
  docker compose -p pms-m1-test -f docker-compose.m1-integration-test.yml up -d
Then run:
  pytest tests/test_m1_integration.py -v
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from pathlib import Path
from threading import Event

import pytest

# Allow environment overrides so tests work both from the Windows host
# (using Docker-exposed ports) and from inside the test Docker network
# (using internal Docker hostnames).
KAFKA_HOST = os.environ.get("KAFKA_HOST", "localhost:39092")
MQTT_HOST  = os.environ.get("MQTT_HOST",  "localhost")
MQTT_PORT  = int(os.environ.get("MQTT_PORT", "31883"))

SRC = Path(__file__).resolve().parents[1] / "simulator" / "src"
sys.path.insert(0, str(SRC))

DATA_DIR = Path(__file__).resolve().parents[1] / "simulator" / "data" / "cmapss"


def _kafka_available() -> bool:
    try:
        from kafka import KafkaConsumer
        c = KafkaConsumer(bootstrap_servers=KAFKA_HOST, request_timeout_ms=3000)
        c.topics()
        c.close()
        return True
    except Exception:
        return False


def _mqtt_available() -> bool:
    try:
        import socket
        s = socket.create_connection((MQTT_HOST, MQTT_PORT), timeout=2)
        s.close()
        return True
    except Exception:
        return False


KAFKA_SKIP = pytest.mark.skipif(not _kafka_available(), reason="Kafka not reachable at localhost:39092")
MQTT_SKIP  = pytest.mark.skipif(not _mqtt_available(),  reason="MQTT not reachable at localhost:31883")


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3 — Simulation behaviour (pure Python, no broker needed)
# ─────────────────────────────────────────────────────────────────────────────

class TestSimulationBehaviour:
    """Pure-Python simulation tests — no broker required."""

    def _make_df(self, units=2, cycles=3):
        import pandas as pd
        rows = []
        for u in range(1, units + 1):
            for c in range(1, cycles + 1):
                row = {"unit_id": u, "time_cycles": c,
                       "operational_setting_1": 0.0, "operational_setting_2": 0.0,
                       "operational_setting_3": 100.0}
                for s in range(1, 22):
                    row[f"sensor_{s}"] = float(s)
                rows.append(row)
        df = pd.DataFrame(rows)
        df["unit_id"]     = df["unit_id"].astype(int)
        df["time_cycles"] = df["time_cycles"].astype(int)
        return df

    def test_single_replay_exact_row_count(self):
        from cmapss_loader import SimulationController
        from unittest.mock import patch
        df = self._make_df(2, 3)
        sent = []
        with patch.object(SimulationController, "_load", return_value=df), \
             patch.object(SimulationController, "_connect_kafka"), \
             patch.object(SimulationController, "_connect_mqtt"), \
             patch.object(SimulationController, "_shutdown"):
            ctrl = SimulationController(stream_interval=0.0, replay_mode="single")
            ctrl._publish = lambda p, k: sent.append(p)
            ctrl.run()
        assert len(sent) == 6

    def test_continuous_three_passes_new_run_id_each(self):
        from cmapss_loader import SimulationController
        from unittest.mock import patch
        df = self._make_df(1, 2)
        run_ids = []
        with patch.object(SimulationController, "_load", return_value=df), \
             patch.object(SimulationController, "_connect_kafka"), \
             patch.object(SimulationController, "_connect_mqtt"), \
             patch.object(SimulationController, "_shutdown"):
            ctrl = SimulationController(
                stream_interval=0.0, replay_mode="continuous",
                max_replay_cycles=3, replay_pause_seconds=0.0)
            ctrl._publish = lambda p, k: run_ids.append(p["run_id"])
            ctrl.run()
        unique_ids = list(dict.fromkeys(run_ids))
        assert len(unique_ids) == 3

    def test_configurable_interval_read_from_env(self, monkeypatch):
        monkeypatch.setenv("STREAM_INTERVAL", "0.25")
        import importlib, cmapss_loader
        importlib.reload(cmapss_loader)
        assert cmapss_loader.STREAM_INTERVAL == pytest.approx(0.25)

    def test_multiple_engine_identities_in_payloads(self):
        from cmapss_loader import iter_rows
        import pandas as pd
        df = self._make_df(5, 2)
        assets = {p["asset_id"] for p, _, _ in iter_rows(df, "r", "FD001", "train")}
        assert assets == {f"engine_{i}" for i in range(1, 6)}

    def test_sequence_monotonically_increases_across_replays(self):
        from cmapss_loader import SimulationController
        from unittest.mock import patch
        df = self._make_df(2, 3)
        seqs = []
        with patch.object(SimulationController, "_load", return_value=df), \
             patch.object(SimulationController, "_connect_kafka"), \
             patch.object(SimulationController, "_connect_mqtt"), \
             patch.object(SimulationController, "_shutdown"):
            ctrl = SimulationController(
                stream_interval=0.0, replay_mode="continuous",
                max_replay_cycles=2, replay_pause_seconds=0.0)
            ctrl._publish = lambda p, k: seqs.append(p["sequence"])
            ctrl.run()
        for i in range(1, len(seqs)):
            assert seqs[i] > seqs[i-1], f"Sequence not monotonic at pos {i}"

    def test_stable_event_id_on_retry(self):
        from cmapss_loader import iter_rows
        df = self._make_df(1, 3)
        ids1 = [p["event_id"] for p, _, _ in iter_rows(df, "fixed_run", "FD001", "train")]
        ids2 = [p["event_id"] for p, _, _ in iter_rows(df, "fixed_run", "FD001", "train")]
        assert ids1 == ids2

    def test_sigterm_stops_after_current_row(self):
        """SIGTERM sets _stop=True; the loop exits at the next check."""
        from cmapss_loader import SimulationController
        from unittest.mock import patch
        import threading
        df = self._make_df(1, 50)
        sent = []
        def _publish(p, k):
            sent.append(p)
            if len(sent) == 5:
                # Simulate SIGTERM by setting _stop directly
                ctrl._stop = True
        with patch.object(SimulationController, "_load", return_value=df), \
             patch.object(SimulationController, "_connect_kafka"), \
             patch.object(SimulationController, "_connect_mqtt"), \
             patch.object(SimulationController, "_shutdown"):
            ctrl = SimulationController(stream_interval=0.0, replay_mode="single")
            ctrl._publish = _publish
            ctrl.run()
        assert len(sent) >= 5
        assert len(sent) < 50, "Should have stopped well before all 50 rows"

    def test_per_asset_sequence_per_replay(self):
        """Each asset's sequence numbers within one replay must be unique."""
        from cmapss_loader import iter_rows
        df = self._make_df(3, 4)
        by_asset: dict[str, list[int]] = {}
        for p, _, _ in iter_rows(df, "r", "FD001", "train"):
            by_asset.setdefault(p["asset_id"], []).append(p["sequence"])
        for asset, seqs in by_asset.items():
            assert len(seqs) == len(set(seqs)), f"Duplicate sequences for {asset}"


# ─────────────────────────────────────────────────────────────────────────────
# Phase 4 — Messaging (requires running integration stack)
# ─────────────────────────────────────────────────────────────────────────────

class TestKafkaDirectDelivery:

    def _wait_for_assignment(self, consumer, timeout=15):
        """Poll until the consumer has an assigned partition or timeout."""
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            consumer.poll(timeout_ms=300)
            if consumer.assignment():
                return True
        return False

    @KAFKA_SKIP
    def test_publish_to_sensor_telemetry_and_consume(self):
        """Publish one C-MAPSS row directly to Kafka and read it back."""
        from kafka import KafkaConsumer
        from kafka_producer import KafkaProducerClient
        from event_schema import build_envelope, SOURCE_CMAPSS, new_run_id

        run_id  = new_run_id()
        env     = build_envelope(run_id, "FD001", "engine_1", 1, SOURCE_CMAPSS)
        payload = {**env.to_dict(), "unit_id": 1, "time_cycles": 1,
                   "operational_setting_1": 0.0, "operational_setting_2": 0.0,
                   "operational_setting_3": 100.0,
                   **{f"sensor_{i}": float(i) for i in range(1, 22)}}

        group = f"test-consumer-{uuid.uuid4().hex[:8]}"
        consumer = KafkaConsumer(
            "sensor-telemetry",
            bootstrap_servers=KAFKA_HOST,
            group_id=group,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=12000,
        )
        # Wait until partition is fully assigned before publishing
        assigned = self._wait_for_assignment(consumer)
        assert assigned, "Consumer failed to get partition assignment"

        client = KafkaProducerClient(bootstrap_servers=KAFKA_HOST, connect_attempts=3)
        client.connect()
        client.send("sensor-telemetry", payload, key=f"FD001:1:{run_id}")
        client.close()

        received = []
        for msg in consumer:
            if msg.value.get("run_id") == run_id:
                received.append(msg.value)
                break
        consumer.close()

        assert len(received) == 1, "No message received on sensor-telemetry"
        m = received[0]
        assert m["event_id"]    == payload["event_id"]
        assert m["run_id"]      == run_id
        assert m["unit_id"]     == 1
        assert m["time_cycles"] == 1
        assert m["source_type"] == "cmapss"

    @KAFKA_SKIP
    def test_dead_letter_routing_for_malformed_message(self):
        """A message published directly to the DLT topic must be consumable."""
        from kafka import KafkaConsumer, KafkaProducer

        group = f"dlt-test-{uuid.uuid4().hex[:8]}"
        consumer = KafkaConsumer(
            "sensor-telemetry-dead-letter",
            bootstrap_servers=KAFKA_HOST,
            group_id=group,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=12000,
        )
        assigned = self._wait_for_assignment(consumer)
        assert assigned, "Consumer failed to get partition assignment on DLT"

        sentinel = uuid.uuid4().hex
        bad_msg  = {"invalid": True, "sentinel": sentinel}
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_HOST,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send("sensor-telemetry-dead-letter", bad_msg)
        producer.flush()
        producer.close()

        received = [m.value for m in consumer if m.value.get("sentinel") == sentinel]
        consumer.close()
        assert len(received) == 1, "DLT message not received"


class TestMQTTBridge:

    def _wait_for_assignment(self, consumer, timeout=15):
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            consumer.poll(timeout_ms=300)
            if consumer.assignment():
                return True
        return False

    @MQTT_SKIP
    @KAFKA_SKIP
    def test_cmapss_message_through_mqtt_bridge_to_kafka(self):
        """Publish a valid C-MAPSS payload to MQTT and verify it arrives in Kafka."""
        import paho.mqtt.client as mqtt
        from paho.mqtt.enums import CallbackAPIVersion
        from kafka import KafkaConsumer
        from event_schema import build_envelope, SOURCE_CMAPSS, new_run_id
        import time

        run_id  = new_run_id()
        env     = build_envelope(run_id, "FD001", "engine_1", 1, SOURCE_CMAPSS)
        payload = {**env.to_dict(), "unit_id": 1, "time_cycles": 1,
                   "operational_setting_1": 0.0, "operational_setting_2": 0.0,
                   "operational_setting_3": 100.0,
                   **{f"sensor_{i}": float(i) for i in range(1, 22)}}

        group = f"mqttbridge-{uuid.uuid4().hex[:8]}"
        consumer = KafkaConsumer(
            "sensor-telemetry",
            bootstrap_servers=KAFKA_HOST,
            group_id=group,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=15000,
        )
        assigned = self._wait_for_assignment(consumer)
        assert assigned, "Consumer failed to get partition assignment"

        pub = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION1,
            client_id=f"test-pub-{uuid.uuid4().hex[:8]}",
        )
        pub.connect(MQTT_HOST, MQTT_PORT, keepalive=10)
        pub.loop_start()
        time.sleep(1.0)   # wait for CONNACK
        result = pub.publish(
            "pms/telemetry/cmapss",
            json.dumps(payload).encode("utf-8"),
            qos=1,
        )
        result.wait_for_publish(timeout=5)
        time.sleep(2.0)   # give bridge time to route to Kafka
        pub.loop_stop()
        pub.disconnect()

        received = [m.value for m in consumer if m.value.get("run_id") == run_id]
        consumer.close()
        assert len(received) >= 1, "Message did not arrive in sensor-telemetry via MQTT bridge"
        assert received[0]["event_id"] == payload["event_id"]

    @MQTT_SKIP
    @KAFKA_SKIP
    def test_malformed_mqtt_message_to_dead_letter(self):
        """A malformed MQTT message must be routed to the dead-letter Kafka topic."""
        import paho.mqtt.client as mqtt
        from paho.mqtt.enums import CallbackAPIVersion
        from kafka import KafkaConsumer
        import time

        sentinel    = uuid.uuid4().hex
        bad_payload = json.dumps({"garbage": True, "sentinel": sentinel}).encode("utf-8")

        group = f"dlt-mqtt-{uuid.uuid4().hex[:8]}"
        consumer = KafkaConsumer(
            "sensor-telemetry-dead-letter",
            bootstrap_servers=KAFKA_HOST,
            group_id=group,
            auto_offset_reset="latest",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=15000,
        )
        assigned = self._wait_for_assignment(consumer)
        assert assigned, "Consumer failed to get DLT partition assignment"

        pub = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION1,
            client_id=f"test-bad-{uuid.uuid4().hex[:8]}",
        )
        pub.connect(MQTT_HOST, MQTT_PORT, keepalive=10)
        pub.loop_start()
        time.sleep(1.0)
        result = pub.publish("pms/telemetry/cmapss", bad_payload, qos=1)
        result.wait_for_publish(timeout=5)
        time.sleep(3.0)   # give bridge time to validate and dead-letter
        pub.loop_stop()
        pub.disconnect()

        received = [m.value for m in consumer if m.value.get("sentinel") == sentinel]
        consumer.close()
        assert len(received) >= 1, "Malformed message did not land in dead-letter topic"


class TestSequenceIntegrity:
    """Verify duplicate and out-of-order detection with the fixed SequenceTracker."""

    def test_exact_duplicate_classified(self):
        from event_schema import SequenceTracker
        t = SequenceTracker()
        t.check("r", "e1", 1); t.check("r", "e1", 2)
        ok, reason = t.check("r", "e1", 2)
        assert not ok and reason == "duplicate"

    def test_out_of_order_classified_separately(self):
        from event_schema import SequenceTracker
        t = SequenceTracker()
        t.check("r", "e1", 1); t.check("r", "e1", 2); t.check("r", "e1", 3)
        ok_dup, r_dup = t.check("r", "e1", 3)   # exact repeat
        ok_ooo, r_ooo = t.check("r", "e1", 1)   # older arrival
        assert r_dup == "duplicate"
        assert r_ooo.startswith("out_of_order")
        assert r_dup != r_ooo, "duplicate and out-of-order must have different classifications"

    def test_gap_detected_with_count(self):
        from event_schema import SequenceTracker
        t = SequenceTracker()
        t.check("r", "e1", 1)
        ok, reason = t.check("r", "e1", 6)   # skipped 2,3,4,5
        assert not ok and reason == "gap:4"
