"""Reliable Kafka-to-InfluxDB telemetry consumer."""

from __future__ import annotations

import json
import logging
import math
import os
import time
from datetime import datetime
from typing import Any

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata, TopicPartition

load_dotenv()

logger = logging.getLogger("pms.ingestion")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
SENSOR_TOPIC = os.getenv("SENSOR_TOPIC", "sensor-telemetry")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "influxdb-writer-group")
DEAD_LETTER_TOPIC = os.getenv("DEAD_LETTER_TOPIC", f"{SENSOR_TOPIC}-dead-letter")

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "pms-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor-data")

MAX_WRITE_RETRIES = int(os.getenv("INFLUX_MAX_WRITE_RETRIES", "3"))
RETRY_BACKOFF_SECONDS = float(os.getenv("INFLUX_RETRY_BACKOFF_SECONDS", "1"))
INFLUX_TIMEOUT_MS = int(os.getenv("INFLUX_TIMEOUT_MS", "10000"))
POLL_TIMEOUT_MS = int(os.getenv("KAFKA_POLL_TIMEOUT_MS", "1000"))
MAX_POLL_RECORDS = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "5"))
MAX_POLL_INTERVAL_MS = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "600000"))
COMMIT_TIMEOUT_MS = int(os.getenv("KAFKA_COMMIT_TIMEOUT_MS", "10000"))

REQUIRED_FIELDS = (
    "unit_id",
    "time_cycles",
    "operational_setting_1",
    "operational_setting_2",
    "operational_setting_3",
)
NUMERIC_FIELDS = REQUIRED_FIELDS[2:]


def create_kafka_consumer() -> KafkaConsumer:
    """Initialize a raw-message Kafka consumer with manual commits."""
    logger.info("Connecting to Kafka broker=%s topic=%s", KAFKA_BROKER, SENSOR_TOPIC)
    for attempt in range(30):
        try:
            consumer = KafkaConsumer(
                SENSOR_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=CONSUMER_GROUP,
                value_deserializer=lambda value: value,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                max_poll_records=MAX_POLL_RECORDS,
                max_poll_interval_ms=MAX_POLL_INTERVAL_MS,
            )
            logger.info("Kafka consumer connected")
            return consumer
        except KafkaError:
            logger.exception("Kafka connection failed attempt=%d", attempt + 1)
            if attempt < 29:
                time.sleep(2)
    raise RuntimeError("Kafka connection failed after 30 attempts")


def create_influxdb_client() -> tuple[InfluxDBClient, Any]:
    """Initialize the synchronous InfluxDB writer."""
    logger.info("Connecting to InfluxDB url=%s bucket=%s", INFLUXDB_URL, INFLUXDB_BUCKET)
    if not INFLUXDB_TOKEN:
        raise ValueError("INFLUXDB_TOKEN not set")
    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
        timeout=INFLUX_TIMEOUT_MS,
    )
    return client, client.write_api(write_options=SYNCHRONOUS)


def decode_message(value: bytes | str | dict[str, Any]) -> dict[str, Any]:
    """Decode and validate a telemetry payload before it can be acknowledged."""
    if isinstance(value, dict):
        payload = value
    else:
        payload = json.loads(value.decode("utf-8") if isinstance(value, bytes) else value)
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")
    missing = [field for field in REQUIRED_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"missing required fields: {','.join(missing)}")
    for field in ("unit_id", "time_cycles"):
        value = payload[field]
        if isinstance(value, bool):
            raise ValueError(f"invalid integer field: {field}")
        try:
            converted = int(value)
        except (ValueError, TypeError, OverflowError) as exc:
            raise ValueError(f"invalid integer field: {field}") from exc
        if converted < 1 or (isinstance(value, float) and value != converted):
            raise ValueError(f"invalid integer field: {field}")
    for field in NUMERIC_FIELDS:
        if isinstance(payload[field], bool) or not math.isfinite(float(payload[field])):
            raise ValueError(f"invalid numeric field: {field}")
    for index in range(1, 22):
        field = f"sensor_{index}"
        if payload.get(field) is not None and (isinstance(payload[field], bool) or
                                               not math.isfinite(float(payload[field]))):
            raise ValueError(f"invalid numeric field: {field}")
    if "schema_version" in payload and payload["schema_version"] != "1.0.0":
        raise ValueError("unsupported schema version")
    if "schema_version" in payload:
        for field in ("event_id", "run_id", "dataset_id", "asset_id", "source_type", "sequence"):
            if field not in payload:
                raise ValueError(f"missing required field: {field}")
        for field in ("event_id", "run_id", "dataset_id", "asset_id", "source_type"):
            if not isinstance(payload[field], str) or not payload[field].strip():
                raise ValueError(f"invalid field: {field}")
        if payload["source_type"] not in ("cmapss", "synthetic"):
            raise ValueError("invalid source_type")
        sequence = payload["sequence"]
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
            raise ValueError("invalid sequence")
        if payload["asset_id"] != f"engine_{int(payload['unit_id'])}":
            raise ValueError("asset_id does not match unit_id")
    if not payload.get("source_timestamp") and not payload.get("timestamp"):
        raise ValueError("missing required field: timestamp")
    return payload


def build_point(message: dict[str, Any]) -> Point:
    """Build a deterministic point identity for safe replay."""
    payload = decode_message(message)
    dataset_id = str(payload.get("dataset_id", "unknown"))
    run_id = str(payload.get("run_id", "default"))
    cycle = int(payload["time_cycles"])
    point = (
        Point("sensor_telemetry")
        .tag("dataset_id", dataset_id)
        .tag("unit_id", str(int(payload["unit_id"])))
        .tag("run_id", run_id)
        .tag("cycle", str(cycle))
        .field("time_cycles", cycle)
        .field("operational_setting_1", float(payload["operational_setting_1"]))
        .field("operational_setting_2", float(payload["operational_setting_2"]))
        .field("operational_setting_3", float(payload["operational_setting_3"]))
    )
    for index in range(1, 22):
        sensor_key = f"sensor_{index}"
        if payload.get(sensor_key) is not None:
            point = point.field(sensor_key, float(payload[sensor_key]))

    source_timestamp = payload.get("source_timestamp") or payload.get("timestamp")
    if not source_timestamp:
        raise ValueError("missing required field: timestamp")
    point = point.time(datetime.fromisoformat(str(source_timestamp).replace("Z", "+00:00")))
    return point


def write_to_influxdb(write_api: Any, message: dict[str, Any]) -> bool:
    """Write a point with bounded retries; raise when it is not durable."""
    point = build_point(message)
    for attempt in range(MAX_WRITE_RETRIES):
        try:
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            return True
        except Exception as exc:
            status = getattr(exc, "status", None)
            if status in (401, 403):
                logger.error("Influx write rejected status=%s", status)
                raise
            if attempt == MAX_WRITE_RETRIES - 1:
                logger.error("Influx write exhausted retries attempts=%d", MAX_WRITE_RETRIES)
                raise
            delay = RETRY_BACKOFF_SECONDS * (2**attempt)
            logger.warning("Influx write failed; retrying attempt=%d delay=%.2f", attempt + 1, delay)
            time.sleep(delay)
    raise RuntimeError("unreachable")


def dead_letter(producer: KafkaProducer, message: Any, error: Exception) -> None:
    """Durably publish an invalid record before its source offset is committed."""
    raw = message.value if hasattr(message, "value") else message
    payload = {
        "error": str(error),
        "source_topic": getattr(message, "topic", SENSOR_TOPIC),
        "source_partition": getattr(message, "partition", None),
        "source_offset": getattr(message, "offset", None),
        "value": raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw,
    }
    producer.send(DEAD_LETTER_TOPIC, value=payload).get(timeout=10)
    logger.error(
        "Invalid telemetry sent to dead letter topic=%s partition=%s offset=%s",
        DEAD_LETTER_TOPIC,
        getattr(message, "partition", None),
        getattr(message, "offset", None),
    )


def create_dead_letter_producer() -> KafkaProducer:
    """Create a producer whose future confirms an all-replica broker ack."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        request_timeout_ms=10000,
        max_in_flight_requests_per_connection=1,
    )


def process_message(write_api: Any, producer: KafkaProducer, message: Any) -> None:
    """Handle one record; return only after persistence or durable DLQ."""
    try:
        payload = decode_message(message.value)
        build_point(payload)
    except (ValueError, TypeError, UnicodeDecodeError) as exc:
        dead_letter(producer, message, exc)
        return
    try:
        write_to_influxdb(write_api, payload)
    except Exception:
        logger.exception(
            "Telemetry processing failed; offset remains uncommitted topic=%s partition=%s offset=%s",
            message.topic,
            message.partition,
            message.offset,
        )
        raise


def commit_message(consumer: KafkaConsumer, message: Any) -> None:
    """Commit exactly the next offset after a durably handled record."""
    partition = TopicPartition(message.topic, message.partition)
    consumer.commit(
        {partition: OffsetAndMetadata(message.offset + 1, None, -1)},
        timeout_ms=COMMIT_TIMEOUT_MS,
    )


def consume_and_write() -> None:
    """Poll records, preserve order, and commit only after durable handling."""
    consumer = create_kafka_consumer()
    client, write_api = create_influxdb_client()
    producer = create_dead_letter_producer()
    message_count = 0
    try:
        while True:
            records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=MAX_POLL_RECORDS)
            for _, partition_records in records.items():
                for message in partition_records:
                    process_message(write_api, producer, message)
                    commit_message(consumer, message)
                    message_count += 1
                    if message_count % 100 == 0:
                        logger.info("Processed messages=%d", message_count)
    except KeyboardInterrupt:
        logger.info("Consumer interrupted")
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        client.close()
        logger.info("Consumer closed processed=%d", message_count)


def main() -> None:
    consume_and_write()


if __name__ == "__main__":
    main()
