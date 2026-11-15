from __future__ import annotations

import json
import logging
import os
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

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

logger = logging.getLogger(__name__)
SCHEMA_VERSION = "raw-24-v1"
ARTIFACT_VERSION = "a1-v2"


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
        self._states: OrderedDict[str, _EngineState] = OrderedDict()
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

    def _bounded_state(self, key: str, cycle: int) -> _EngineState:
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
        key = f"{dataset_id}:{unit_id}:{run_id}"
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
        consumer = KafkaConsumer(
            os.getenv("PREPROCESSING_INPUT_TOPIC", "sensor-telemetry"),
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            group_id=os.getenv("PREPROCESSING_CONSUMER_GROUP", "preprocessing-service-group"),
            auto_offset_reset="latest",
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
        try:
            for record in consumer:
                try:
                    output = self.transform(record.value)
                    if output is not None:
                        producer.send(
                            os.getenv("PREPROCESSING_OUTPUT_TOPIC", "preprocessed-telemetry"),
                            value=output,
                        ).get(timeout=10)
                    consumer.commit()
                except (KeyError, TypeError, ValueError, FileNotFoundError, OSError) as exc:
                    logger.error("Rejected telemetry message: %s", exc)
                    consumer.commit()
                except Exception:
                    logger.exception("Preprocessed output failed; offset was not committed")
        finally:
            producer.flush()
            producer.close()
            consumer.close()


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    StreamingPreprocessor().run()


if __name__ == "__main__":
    main()
