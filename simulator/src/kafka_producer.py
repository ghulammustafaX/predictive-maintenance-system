"""
Reusable Kafka producer for the PMS simulator.

Provides a single KafkaProducerClient class used by cmapss_loader.py,
mqtt_bridge.py, and simulate_data.py.  Every published message is
synchronously confirmed (acks='all', idempotent=True).  Failed deliveries
raise, they are never swallowed silently.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

_DEFAULT_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
_DEFAULT_RETRIES = int(os.getenv("KAFKA_PRODUCER_RETRIES", "5"))
_DEFAULT_CONNECT_ATTEMPTS = int(os.getenv("KAFKA_CONNECT_ATTEMPTS", "30"))
_DEFAULT_CONNECT_DELAY = float(os.getenv("KAFKA_CONNECT_DELAY_SECONDS", "2.0"))
_DEFAULT_ACK_TIMEOUT = int(os.getenv("KAFKA_ACK_TIMEOUT_SECONDS", "10"))


class KafkaProducerClient:
    """Thread-safe, idempotent Kafka producer with structured logging.

    Usage::

        with KafkaProducerClient() as producer:
            producer.send("my-topic", payload, key="engine-1")

    Or without context manager::

        producer = KafkaProducerClient()
        producer.connect()
        producer.send("my-topic", payload)
        producer.close()
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        retries: int = _DEFAULT_RETRIES,
        connect_attempts: int = _DEFAULT_CONNECT_ATTEMPTS,
        connect_delay: float = _DEFAULT_CONNECT_DELAY,
        ack_timeout: int = _DEFAULT_ACK_TIMEOUT,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers or _DEFAULT_BROKER
        self.retries = retries
        self.connect_attempts = connect_attempts
        self.connect_delay = connect_delay
        self.ack_timeout = ack_timeout
        self._producer: KafkaProducer | None = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Connect to the Kafka broker with exponential-capped retry."""
        logger.info(
            "Connecting to Kafka broker=%s (up to %d attempts)",
            self.bootstrap_servers,
            self.connect_attempts,
        )
        for attempt in range(self.connect_attempts):
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                    acks="all",
                    retries=self.retries,
                    # Idempotent delivery: exactly-once sequence numbers.
                    # kafka-python requires max_in_flight=1 for idempotence.
                    enable_idempotence=True,
                    max_in_flight_requests_per_connection=1,
                    request_timeout_ms=30_000,
                )
                logger.info("Kafka producer connected to %s", self.bootstrap_servers)
                return
            except KafkaError as exc:
                logger.warning(
                    "Kafka connection attempt %d/%d failed: %s",
                    attempt + 1,
                    self.connect_attempts,
                    exc,
                )
                if attempt < self.connect_attempts - 1:
                    delay = min(self.connect_delay * (2 ** min(attempt, 5)), 30.0)
                    logger.info("Retrying in %.1f s…", delay)
                    time.sleep(delay)
            except Exception as exc:
                # Catch configuration errors (KafkaConfigurationError subclasses
                # Exception but not KafkaError in some kafka-python versions).
                logger.error("Kafka producer configuration error (not retrying): %s", exc)
                raise
        raise RuntimeError(
            f"Could not connect to Kafka after {self.connect_attempts} attempts"
        )

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def send(
        self,
        topic: str,
        payload: dict[str, Any],
        key: str | bytes | None = None,
        partition: int | None = None,
    ) -> None:
        """Publish one message and block until the broker acknowledges it.

        Raises ``KafkaError`` if delivery fails after configured retries.
        Never silently drops messages.
        """
        if self._producer is None:
            raise RuntimeError("KafkaProducerClient.connect() has not been called")

        kwargs: dict[str, Any] = {}
        if key is not None:
            kwargs["key"] = key
        if partition is not None:
            kwargs["partition"] = partition

        future = self._producer.send(topic, value=payload, **kwargs)
        try:
            meta = future.get(timeout=self.ack_timeout)
            logger.debug(
                "Kafka ack topic=%s partition=%d offset=%d",
                meta.topic,
                meta.partition,
                meta.offset,
            )
        except KafkaError as exc:
            logger.error(
                "Kafka delivery failed topic=%s key=%s error=%s",
                topic,
                key,
                exc,
            )
            raise

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def flush(self) -> None:
        """Flush any buffered messages to the broker."""
        if self._producer is not None:
            self._producer.flush()

    def close(self) -> None:
        """Flush then close the producer gracefully."""
        if self._producer is not None:
            logger.info("Flushing and closing Kafka producer")
            self._producer.flush()
            self._producer.close()
            self._producer = None

    def __enter__(self) -> "KafkaProducerClient":
        self.connect()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
