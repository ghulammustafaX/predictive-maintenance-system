"""
MQTT-to-Kafka Bridge  —  Module 2 (ingestion)

Subscribes to MQTT topics for C-MAPSS and IMS telemetry, validates each
message, and routes it to the appropriate Kafka topic.  Uses the shared
KafkaProducerClient for delivery guarantees.

Routing
-------
  pms/telemetry/cmapss  →  sensor-telemetry          (Kafka)
  pms/telemetry/ims     →  bearing-telemetry          (Kafka)
  <any malformed msg>   →  sensor-telemetry-dead-letter

Event-ID preservation: the event_id field from the MQTT payload is forwarded
unchanged so the Kafka consumers can detect duplicates.

Environment variables
---------------------
KAFKA_BROKER            Kafka bootstrap server     (default: kafka:9092)
SENSOR_TOPIC            C-MAPSS Kafka topic        (default: sensor-telemetry)
BEARING_TOPIC           IMS Kafka topic            (default: bearing-telemetry)
DEAD_LETTER_TOPIC       Dead-letter Kafka topic    (default: sensor-telemetry-dead-letter)
MQTT_BROKER             MQTT broker host           (default: mosquitto)
MQTT_PORT               MQTT broker port           (default: 1883)
MQTT_TOPIC_CMAPSS       MQTT C-MAPSS topic         (default: pms/telemetry/cmapss)
MQTT_TOPIC_IMS          MQTT IMS topic             (default: pms/telemetry/ims)
MQTT_QOS                Subscription QoS           (default: 1)
MQTT_CLIENT_ID          Stable bridge client ID    (default: pms-mqtt-bridge)
BRIDGE_ENABLED          'true' to run bridge; 'false' to skip (default: true)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any

logger = logging.getLogger("pms.mqtt_bridge")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
SENSOR_TOPIC = os.getenv("SENSOR_TOPIC", "sensor-telemetry")
BEARING_TOPIC = os.getenv("BEARING_TOPIC", "bearing-telemetry")
DEAD_LETTER_TOPIC = os.getenv("DEAD_LETTER_TOPIC", "sensor-telemetry-dead-letter")

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC_CMAPSS = os.getenv("MQTT_TOPIC_CMAPSS", "pms/telemetry/cmapss")
MQTT_TOPIC_IMS = os.getenv("MQTT_TOPIC_IMS", "pms/telemetry/ims")
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "pms-mqtt-bridge")

# Required fields for C-MAPSS messages
CMAPSS_REQUIRED = {
    "schema_version", "event_id", "run_id", "dataset_id",
    "asset_id", "timestamp", "sequence", "source_type",
    "unit_id", "time_cycles",
}

# Required fields for IMS messages
IMS_REQUIRED = {
    "schema_version", "event_id", "run_id", "dataset_id",
    "asset_id", "timestamp", "sequence", "source_type",
    "bearing_name", "features",
}


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_cmapss(payload: dict[str, Any]) -> None:
    """Raise ValueError if the payload is not a valid C-MAPSS event."""
    missing = CMAPSS_REQUIRED - payload.keys()
    if missing:
        raise ValueError(f"C-MAPSS message missing fields: {sorted(missing)}")
    # Check numeric types for critical fields
    int(payload["unit_id"])
    int(payload["time_cycles"])
    str(payload["run_id"])
    str(payload["event_id"])


def validate_ims(payload: dict[str, Any]) -> None:
    """Raise ValueError if the payload is not a valid IMS event."""
    missing = IMS_REQUIRED - payload.keys()
    if missing:
        raise ValueError(f"IMS message missing fields: {sorted(missing)}")
    if not isinstance(payload.get("features"), dict):
        raise ValueError("IMS payload 'features' must be a dict")
    str(payload["run_id"])
    str(payload["event_id"])


# ---------------------------------------------------------------------------
# Bridge
# ---------------------------------------------------------------------------

class MQTTKafkaBridge:
    """Subscribe to MQTT and forward validated messages to Kafka."""

    def __init__(self) -> None:
        self._kafka = None
        self._mqtt_client = None
        self._stop = False
        self._failure = None
        self._stats = {
            "cmapss_forwarded": 0,
            "ims_forwarded": 0,
            "dead_lettered": 0,
            "parse_errors": 0,
        }

    # ------------------------------------------------------------------
    # Kafka
    # ------------------------------------------------------------------

    def _init_kafka(self) -> None:
        """Connect the reusable Kafka producer."""
        try:
            # Try importing from the simulator src package
            sys.path.insert(0, "/app/src")
            from kafka_producer import KafkaProducerClient
        except ImportError:
            # Fallback: direct kafka-python (keeps ingestion self-contained)
            from kafka import KafkaProducer

            class _Adapter:
                def __init__(self):
                    self._p = KafkaProducer(
                        bootstrap_servers=KAFKA_BROKER,
                        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                        acks="all",
                        retries=5,
                        enable_idempotence=True,
                        max_in_flight_requests_per_connection=5,
                        max_block_ms=10000,
                    )

                def send(self, topic, payload, key=None):
                    kw = {"key": key} if key else {}
                    self._p.send(topic, value=payload, **kw).get(timeout=10)

                def close(self):
                    self._p.flush(timeout=10)
                    self._p.close(timeout=10)

            self._kafka = _Adapter()
            logger.info("Kafka producer connected (direct kafka-python fallback)")
            return

        client = KafkaProducerClient(bootstrap_servers=KAFKA_BROKER)
        client.connect()
        self._kafka = client
        logger.info("Kafka producer connected via KafkaProducerClient")

    # ------------------------------------------------------------------
    # MQTT
    # ------------------------------------------------------------------

    def _init_mqtt(self) -> None:
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise RuntimeError("paho-mqtt is required for the MQTT bridge") from exc

        from paho.mqtt.enums import CallbackAPIVersion
        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION1,
            client_id=MQTT_CLIENT_ID,
            clean_session=False,
            protocol=mqtt.MQTTv311,
            manual_ack=True,
        )
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.on_disconnect = self._on_disconnect

        mqtt_user = os.getenv("MQTT_USERNAME")
        mqtt_pass = os.getenv("MQTT_PASSWORD")
        if mqtt_user:
            client.username_pw_set(mqtt_user, mqtt_pass)

        client.reconnect_delay_set(min_delay=1, max_delay=30)

        logger.info("Connecting to MQTT broker %s:%d", MQTT_BROKER_HOST, MQTT_PORT)
        for attempt in range(15):
            try:
                client.connect(MQTT_BROKER_HOST, MQTT_PORT, keepalive=60)
                break
            except Exception as exc:
                logger.warning("MQTT connect attempt %d failed: %s", attempt + 1, exc)
                if attempt < 14:
                    time.sleep(2)
                else:
                    raise RuntimeError(
                        f"MQTT broker unreachable after 15 attempts: {exc}"
                    ) from exc

        self._mqtt_client = client

    def _on_connect(self, client, _ud, _flags, rc: int) -> None:
        if rc == 0:
            logger.info("MQTT bridge connected — subscribing to topics")
            client.subscribe(MQTT_TOPIC_CMAPSS, qos=MQTT_QOS)
            client.subscribe(MQTT_TOPIC_IMS, qos=MQTT_QOS)
        else:
            logger.error("MQTT connection refused rc=%d", rc)

    def _on_disconnect(self, _client, _ud, rc: int) -> None:
        if rc != 0:
            logger.warning("MQTT unexpected disconnect rc=%d", rc)

    def _on_message(self, client, _ud, msg) -> None:
        """Acknowledge only after Kafka confirms delivery; restart on failure."""
        try:
            self._route_message(msg)
            if msg.qos:
                result = client.ack(msg.mid, msg.qos)
                if result != 0:
                    raise RuntimeError(f"MQTT acknowledgement failed: {result}")
        except Exception as exc:
            logger.exception("MQTT delivery not acknowledged mid=%s qos=%s", msg.mid, msg.qos)
            self._failure = exc
            self._stop = True
            client.disconnect()

    def _route_message(self, msg) -> None:
        """Route one MQTT message to the appropriate Kafka topic."""
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            self._stats["parse_errors"] += 1
            logger.error("MQTT message not valid JSON topic=%s: %s", msg.topic, exc)
            self._dead_letter(
                {"raw": msg.payload.decode("utf-8", errors="replace"),
                 "error": str(exc),
                 "mqtt_topic": msg.topic}
            )
            return

        try:
            if not isinstance(payload, dict):
                raise ValueError("payload must be a JSON object")
            if msg.topic == MQTT_TOPIC_CMAPSS:
                validate_cmapss(payload)
                unit_id = str(int(payload["unit_id"]))
                kafka_key = f"{payload.get('dataset_id', 'UNK')}:{unit_id}:{payload['run_id']}"
                kafka_topic, stat = SENSOR_TOPIC, "cmapss_forwarded"

            elif msg.topic == MQTT_TOPIC_IMS:
                validate_ims(payload)
                kafka_key = (
                    f"{payload.get('dataset_id', 'IMS')}:"
                    f"{payload.get('bearing_name', '?')}:"
                    f"{payload['run_id']}"
                )
                kafka_topic, stat = BEARING_TOPIC, "ims_forwarded"

            else:
                logger.warning("Received message on unrouted topic=%s", msg.topic)
                self._dead_letter(
                    {**payload, "error": f"unrouted_topic:{msg.topic}"}
                )
                self._stats["dead_lettered"] += 1
                return

        except (ValueError, TypeError, KeyError) as exc:
            self._stats["dead_lettered"] += 1
            logger.warning(
                "Invalid message on topic=%s routed to dead-letter: %s",
                msg.topic,
                exc,
            )
            self._dead_letter({
                **({} if not isinstance(payload, dict) else payload),
                "error": str(exc),
                "mqtt_topic": msg.topic,
            })
            return

        # Producer errors are infrastructure failures, never validation errors.
        self._kafka.send(kafka_topic, payload, key=kafka_key)
        self._stats[stat] += 1
        total = sum(self._stats.values())
        if total % 500 == 0:
            logger.info("Bridge stats: %s", self._stats)

    def _dead_letter(self, payload: dict) -> None:
        """Forward an unprocessable message to the dead-letter topic."""
        self._kafka.send(DEAD_LETTER_TOPIC, payload)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the bridge.  Blocks until KeyboardInterrupt or SIGTERM."""
        import signal

        def _stop(_s, _f):
            logger.info("Shutdown signal received")
            self._stop = True

        signal.signal(signal.SIGTERM, _stop)

        self._init_kafka()
        self._init_mqtt()

        self._mqtt_client.loop_start()
        logger.info(
            "MQTT-to-Kafka bridge running: "
            "cmapss→%s  ims→%s  dlt→%s",
            SENSOR_TOPIC,
            BEARING_TOPIC,
            DEAD_LETTER_TOPIC,
        )

        try:
            while not self._stop:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Bridge interrupted")
        finally:
            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()
            if self._kafka:
                self._kafka.close()
            logger.info("Bridge stopped. Final stats: %s", self._stats)
        if self._failure is not None:
            raise RuntimeError("Bridge stopped after an unacknowledged delivery") from self._failure


def main() -> None:
    bridge = MQTTKafkaBridge()
    bridge.run()


if __name__ == "__main__":
    main()
