"""
MQTT Publisher for PMS Module 1.

Publishes telemetry payloads to an MQTT broker with QoS 1 by default,
reconnection logic, and graceful shutdown.  Used by SimulationController
when PUBLISH_TRANSPORT includes 'mqtt'.

This module is intentionally thin: it handles connection/reconnection and
message confirmation, but leaves routing and schema concerns to the caller.

Environment variables
---------------------
MQTT_BROKER     Broker hostname/IP   (default: mosquitto)
MQTT_PORT       Broker port          (default: 1883)
MQTT_CLIENT_ID  MQTT client ID       (default: pms-simulator-<uuid4>)
MQTT_QOS        Default QoS          (default: 1)
MQTT_USERNAME   Optional username
MQTT_PASSWORD   Optional password
MQTT_KEEPALIVE  Keepalive seconds    (default: 60)
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any

logger = logging.getLogger(__name__)


class MQTTPublisher:
    """Wraps paho-mqtt with retry-connect and delivery confirmation.

    Usage::

        with MQTTPublisher() as pub:
            pub.publish("pms/telemetry/cmapss", payload)
    """

    def __init__(
        self,
        broker: str | None = None,
        port: int | None = None,
        client_id: str | None = None,
        default_qos: int | None = None,
        username: str | None = None,
        password: str | None = None,
        keepalive: int | None = None,
    ) -> None:
        self.broker = broker or os.getenv("MQTT_BROKER", "mosquitto")
        self.port = port or int(os.getenv("MQTT_PORT", "1883"))
        self.client_id = client_id or os.getenv(
            "MQTT_CLIENT_ID", f"pms-simulator-{uuid.uuid4().hex[:8]}"
        )
        self.default_qos = default_qos if default_qos is not None else int(os.getenv("MQTT_QOS", "1"))
        self.username = username or os.getenv("MQTT_USERNAME")
        self.password = password or os.getenv("MQTT_PASSWORD")
        self.keepalive = keepalive or int(os.getenv("MQTT_KEEPALIVE", "60"))
        self._client = None
        self._connected = False

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self, max_attempts: int = 10, retry_delay: float = 2.0) -> None:
        """Connect to the MQTT broker with retry.

        Compatible with paho-mqtt 1.x and 2.x.
        """
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise RuntimeError(
                "paho-mqtt is required for MQTT publishing. "
                "Add 'paho-mqtt>=1.6.1' to requirements.txt."
            ) from exc

        import time

        # paho-mqtt 2.x changed the Client constructor to require
        # CallbackAPIVersion; fall back gracefully for 1.x environments.
        try:
            from paho.mqtt.enums import CallbackAPIVersion
            client = mqtt.Client(
                callback_api_version=CallbackAPIVersion.VERSION1,
                client_id=self.client_id,
            )
        except ImportError:
            # paho-mqtt 1.x
            client = mqtt.Client(client_id=self.client_id, protocol=mqtt.MQTTv311)

        def on_connect(c, _userdata, _flags, rc):
            if rc == 0:
                self._connected = True
                logger.info("MQTT connected to %s:%d", self.broker, self.port)
            else:
                self._connected = False
                logger.warning("MQTT connect refused (rc=%d)", rc)

        def on_disconnect(c, _userdata, rc):
            self._connected = False
            if rc != 0:
                logger.warning("MQTT unexpected disconnect (rc=%d)", rc)

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect

        if self.username:
            client.username_pw_set(self.username, self.password)

        client.reconnect_delay_set(min_delay=1, max_delay=30)

        for attempt in range(max_attempts):
            try:
                client.connect(self.broker, self.port, keepalive=self.keepalive)
                client.loop_start()
                # Wait for connection callback
                for _ in range(20):
                    if self._connected:
                        break
                    time.sleep(0.1)
                if self._connected:
                    self._client = client
                    return
                raise ConnectionError("Connection established but on_connect not fired")
            except Exception as exc:
                logger.warning(
                    "MQTT connect attempt %d/%d failed: %s",
                    attempt + 1,
                    max_attempts,
                    exc,
                )
                if attempt < max_attempts - 1:
                    time.sleep(retry_delay)

        raise RuntimeError(
            f"Could not connect to MQTT broker {self.broker}:{self.port} "
            f"after {max_attempts} attempts"
        )

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def publish(
        self,
        topic: str,
        payload: dict[str, Any],
        qos: int | None = None,
        retain: bool = False,
    ) -> None:
        """Publish a JSON payload and block until broker confirms delivery.

        For QoS 0 this returns immediately.
        For QoS 1 and 2 this blocks until the PUBACK/PUBCOMP is received.

        Raises
        ------
        RuntimeError  — publisher not connected
        """
        if self._client is None or not self._connected:
            raise RuntimeError(
                "MQTTPublisher.connect() must be called before publish()"
            )
        effective_qos = qos if qos is not None else self.default_qos
        encoded = json.dumps(payload, default=str).encode("utf-8")

        result = self._client.publish(topic, encoded, qos=effective_qos, retain=retain)
        if effective_qos >= 1:
            result.wait_for_publish(timeout=10)
            if not result.is_published():
                raise RuntimeError(
                    f"MQTT delivery not confirmed for topic={topic} qos={effective_qos}"
                )

        logger.debug("MQTT published topic=%s qos=%d len=%d", topic, effective_qos, len(encoded))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def disconnect(self) -> None:
        """Disconnect from the broker gracefully."""
        if self._client is not None:
            self._client.loop_stop()
            self._client.disconnect()
            self._client = None
            self._connected = False
            logger.info("MQTT publisher disconnected")

    def __enter__(self) -> "MQTTPublisher":
        self.connect()
        return self

    def __exit__(self, *_: Any) -> None:
        self.disconnect()
