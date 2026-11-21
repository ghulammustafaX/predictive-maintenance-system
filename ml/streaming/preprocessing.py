from __future__ import annotations

import json
import copy
import hashlib
import logging
import os
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer, ConsumerRebalanceListener
from kafka.structs import OffsetAndMetadata, TopicPartition

from ml.config import (
    OUTLIER_Z_THRESHOLD,
    STREAMING_ARTIFACT_DIR,
    SUB_DATASETS,
    WINDOW_SIZE,
)
from ml.data.preprocessing import (
    FEATURE_COLUMNS,
    apply_scaler,
    clip_outliers,
    handle_missing_values,
    load_scaler,
    load_training_stats,
)
from ml.models.compatibility import validate_preprocessing_artifacts
from ml.streaming.checkpoint import CheckpointBusy, CheckpointError, PreprocessingCheckpoint, artifact_identity, validate_checkpoint_path

logger = logging.getLogger(__name__)
SCHEMA_VERSION = "raw-24-v1"
ARTIFACT_VERSION = "a1-v2"


class _PartitionOwnership(ConsumerRebalanceListener):
    def __init__(self, processors, retries, owners, checkpoint):
        self.processors = processors
        self.retries = retries
        self.owners = owners
        self.checkpoint = checkpoint

    def on_partitions_revoked(self, partitions):
        for partition in partitions:
            self.processors.pop(partition, None)
            self.retries.pop(partition, None)
            self.owners.pop(partition, None)
            self.checkpoint.release(partition.topic, partition.partition)
            logger.warning("preprocessing_partition_revoked topic=%s partition=%s", partition.topic, partition.partition)

    def on_partitions_assigned(self, partitions):
        for partition in partitions:
            logger.info("preprocessing_partition_assigned topic=%s partition=%s", partition.topic, partition.partition)


@dataclass
class _EngineState:
    last_cycle: int
    rows: list[dict[str, Any]]
    last_values: dict[str, float]


class StreamingPreprocessor:
    """Transform telemetry with the same persisted batch artifacts.

    State is bounded by ``max_engines`` and keyed by dataset, unit, and run.
    Duplicate and out-of-order cycles are ignored. A new simulator run gets
    an independent stream even when its cycle counter restarts at one.
    """

    def __init__(
        self,
        artifact_dir: str | Path | None = None,
        max_engines: int = 1024,
        restart_cycle: int = 1,
        artifact_version: str = ARTIFACT_VERSION,
        validate_startup: bool | None = None,
    ) -> None:
        if validate_startup is None:
            validate_startup = artifact_dir is None
        self.artifact_dir = Path(
            artifact_dir
            or os.getenv("PREPROCESSING_ARTIFACT_DIR", STREAMING_ARTIFACT_DIR)
        )
        if validate_startup:
            validate_preprocessing_artifacts(self.artifact_dir)
        self.max_engines = max_engines
        self.restart_cycle = restart_cycle
        self.artifact_version = artifact_version
        self._states: OrderedDict[tuple[str, str, str], _EngineState] = OrderedDict()
        self._scalers: dict[str, Any] = {}
        self._stats: dict[str, dict[str, dict[str, float]]] = {}

    @property
    def state_count(self) -> int:
        return len(self._states)

    def _artifacts(self, dataset_id: str) -> tuple[Any, dict[str, dict[str, float]]]:
        dataset_id = dataset_id.upper()
        if dataset_id not in SUB_DATASETS:
            raise ValueError(f"Unsupported dataset_id: {dataset_id}")
        if dataset_id not in self._scalers:
            self._scalers[dataset_id] = load_scaler(dataset_id, self.artifact_dir)
        if dataset_id not in self._stats:
            self._stats[dataset_id] = load_training_stats(dataset_id, self.artifact_dir)
        return self._scalers[dataset_id], self._stats[dataset_id]

    @staticmethod
    def _validate_message(message: dict[str, Any]) -> tuple[str, str, str, int]:
        for field in ("dataset_id", "unit_id", "run_id"):
            if field not in message or message[field] in (None, ""):
                raise ValueError(f"Missing required field: {field}")
        dataset_id = str(message["dataset_id"]).upper()
        unit_id = str(message["unit_id"])
        run_id = str(message["run_id"])
        cycle_value = message.get("time_cycles", message.get("cycle"))
        if cycle_value is None:
            raise ValueError("Missing required field: time_cycles")
        return dataset_id, unit_id, run_id, int(cycle_value)

    def _bounded_state(self, key: tuple[str, str, str], cycle: int) -> _EngineState:
        state = self._states.get(key)
        if state is not None and cycle < state.last_cycle and cycle <= self.restart_cycle:
            state = _EngineState(last_cycle=0, rows=[], last_values={})
            self._states[key] = state
        elif state is None:
            if len(self._states) >= self.max_engines:
                self._states.popitem(last=False)
            state = _EngineState(last_cycle=0, rows=[], last_values={})
            self._states[key] = state
        self._states.move_to_end(key)
        return state

    def transform(self, message: dict[str, Any]) -> dict[str, Any] | None:
        dataset_id, unit_id, run_id, cycle = self._validate_message(message)
        scaler, stats = self._artifacts(dataset_id)
        key = (dataset_id, unit_id, run_id)
        state = self._bounded_state(key, cycle)
        if cycle <= state.last_cycle:
            logger.warning("Skipping duplicate/out-of-order cycle %s for %s", cycle, key)
            return None

        row = {column: message.get(column, np.nan) for column in FEATURE_COLUMNS}
        medians = {column: values["median"] for column, values in stats.items()}
        for column in FEATURE_COLUMNS:
            value = row[column]
            if value is None or pd.isna(value):
                row[column] = state.last_values.get(column, medians[column])
        frame = handle_missing_values(
            pd.DataFrame([{"unit_id": unit_id, "time_cycles": cycle, **row}]),
            medians,
            backfill=False,
        )
        frame = clip_outliers(frame, OUTLIER_Z_THRESHOLD, stats)
        frame = apply_scaler(frame, scaler)
        values = frame[FEATURE_COLUMNS].to_numpy(dtype=np.float32)[0]
        state.last_values = {
            column: float(row[column]) for column in FEATURE_COLUMNS
        }
        state.rows.append({column: float(value) for column, value in zip(FEATURE_COLUMNS, values)})
        if len(state.rows) > WINDOW_SIZE:
            state.rows.pop(0)
        state.last_cycle = cycle
        timestamp = message.get("timestamp") or datetime.now(timezone.utc).isoformat()
        return {
            "dataset_id": dataset_id,
            "unit_id": unit_id,
            "run_id": run_id,
            "cycle": cycle,
            "time_cycles": cycle,
            "timestamp": timestamp,
            "features": values.tolist(),
            "feature_order": list(FEATURE_COLUMNS),
            "schema_version": SCHEMA_VERSION,
            "preprocessing_artifact_version": self.artifact_version,
        }

    def process_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        try:
            return self.transform(message)
        except (KeyError, TypeError, ValueError, FileNotFoundError, OSError) as exc:
            logger.error("Rejected telemetry message: %s", exc)
            return None

    def run(self) -> None:
        checkpoint_path = os.getenv("PREPROCESSING_CHECKPOINT_PATH")
        validate_checkpoint_path(checkpoint_path)
        group = os.getenv("PREPROCESSING_CONSUMER_GROUP", "preprocessing-service-group")
        checkpoint = PreprocessingCheckpoint(checkpoint_path, group, artifact_identity(self.artifact_dir, self.artifact_version))
        consumer = KafkaConsumer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            group_id=group,
            # An uncommitted first record must be replayable after process loss.
            # Operators must establish an explicit starting offset when historical
            # records exist; earliest may otherwise replay retained history.
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=1,
        )
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
        )
        processors = {}
        retries = {}
        owners = {}
        retry_seconds = min(60.0, max(0.1, float(os.getenv("PREPROCESSING_RETRY_SECONDS", "1"))))
        consumer.subscribe([os.getenv("PREPROCESSING_INPUT_TOPIC", "sensor-telemetry")],
                           listener=_PartitionOwnership(processors, retries, owners, checkpoint))

        def record_hash(record):
            raw = record.value if isinstance(record.value, bytes) else json.dumps(record.value, sort_keys=True, separators=(",", ":")).encode()
            return hashlib.sha256(raw).hexdigest()

        def snapshot(processor):
            return [{"dataset": key[0], "unit": key[1], "run": key[2],
                     "last_cycle": state.last_cycle, "rows": state.rows,
                     "last_values": state.last_values}
                    for key, state in processor._states.items()]

        def restore(processor, rows):
            if not isinstance(rows, list) or len(rows) > processor.max_engines:
                raise CheckpointError("invalid checkpoint state")
            restored = OrderedDict()
            try:
                for item in rows:
                    key = (item["dataset"], item["unit"], item["run"])
                    if any(not isinstance(part, str) or not part for part in key) or key in restored:
                        raise ValueError("invalid identity")
                    cycle, history, values = item["last_cycle"], item["rows"], item["last_values"]
                    if not isinstance(cycle, int) or cycle < 0 or not isinstance(history, list) or len(history) > WINDOW_SIZE or not isinstance(values, dict):
                        raise ValueError("invalid engine state")
                    restored[key] = _EngineState(cycle, history, values)
            except (KeyError, TypeError, ValueError) as exc:
                raise CheckpointError("invalid checkpoint state") from exc
            processor._states = restored

        def process(partition, record, output_confirmed=False):
            if partition not in consumer.assignment():
                return False
            if partition not in processors:
                processor = copy.copy(self)
                processor._states = OrderedDict()
                try:
                    owner = checkpoint.acquire(partition.topic, partition.partition)
                except CheckpointBusy:
                    logger.warning("preprocessing_checkpoint_busy topic=%s partition=%s retry_seconds=%s", partition.topic, partition.partition, retry_seconds)
                    consumer.pause(partition)
                    retries[partition] = (record, time.monotonic() + retry_seconds, False)
                    return False
                saved = checkpoint.load(partition.topic, partition.partition, owner)
                committed = consumer.committed(partition)
                if saved is None:
                    if committed not in (None, 0) or record.offset != 0:
                        raise CheckpointError("missing state for committed or truncated input")
                else:
                    next_offset = saved.get("next_offset")
                    if not isinstance(next_offset, int) or next_offset < 1:
                        raise CheckpointError("invalid checkpoint offset")
                    baseline = 0 if committed is None else committed
                    if next_offset == baseline:
                        restore(processor, saved.get("states"))
                    elif next_offset == baseline + 1 and record.offset == baseline and saved.get("record_hash") == record_hash(record):
                        restore(processor, saved.get("states"))
                        output_confirmed = True
                    else:
                        raise CheckpointError("checkpoint and Kafka offsets are inconsistent")
                owners[partition] = owner
                processors[partition] = processor
            processor = processors[partition]
            previous_state = copy.deepcopy(processor._states)
            try:
                if not output_confirmed:
                    value = record.value
                    if isinstance(value, bytes):
                        value = json.loads(value.decode("utf-8"))
                    output = processor.transform(value)
                    if output is not None:
                        output_key = f"{record.topic}:{record.partition}:{record.offset}".encode("utf-8")
                        producer.send(os.getenv("PREPROCESSING_OUTPUT_TOPIC", "preprocessed-telemetry"),
                                      key=output_key, value=output).get(timeout=10)
                    output_confirmed = True
                if partition not in consumer.assignment():
                    raise RuntimeError("partition revoked before commit")
                checkpoint.save(partition.topic, partition.partition, owners[partition],
                                {"next_offset": record.offset + 1, "record_hash": record_hash(record),
                                 "states": snapshot(processor)})
                consumer.commit({partition: OffsetAndMetadata(record.offset + 1, None, -1)})
                return True
            except Exception:
                if not output_confirmed:
                    processor._states = previous_state
                logger.exception("preprocessing_partition_failed topic=%s partition=%s offset=%s stage=%s retry_seconds=%s", record.topic, record.partition, record.offset, "commit" if output_confirmed else "processing_or_output", retry_seconds)
                if partition in consumer.assignment():
                    consumer.pause(partition)
                    retries[partition] = (record, time.monotonic() + retry_seconds, output_confirmed)
                return False

        try:
            while True:
                now = time.monotonic()
                for partition, (record, due, output_confirmed) in list(retries.items()):
                    if due <= now and partition in consumer.assignment():
                        logger.info("preprocessing_partition_retry topic=%s partition=%s", partition.topic, partition.partition)
                        if process(partition, record, output_confirmed):
                            retries.pop(partition, None)
                            consumer.resume(partition)
                            consumer.seek(partition, record.offset + 1)
                batches = consumer.poll(timeout_ms=250, max_records=1)
                for partition, records in batches.items():
                    for record in records:
                        process(partition, record)
        except KeyboardInterrupt:
            logger.info("preprocessing_stopped")
        finally:
            producer.flush()
            producer.close()
            consumer.close()
            checkpoint.close()


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    StreamingPreprocessor().run()


if __name__ == "__main__":
    main()
