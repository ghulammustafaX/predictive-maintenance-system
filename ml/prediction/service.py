from __future__ import annotations

import json
import hashlib
import logging
import os
import re
from urllib.request import urlopen
import threading
from collections import OrderedDict, deque
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer, KafkaProducer, ConsumerRebalanceListener
from kafka.structs import OffsetAndMetadata, TopicPartition
from pydantic import BaseModel, Field

from ml.config import ALL_COLUMNS, CHECKPOINT_DIR, STREAMING_ARTIFACT_DIR, SUB_DATASETS, WINDOW_SIZE
from ml.models.inference import RULInferenceEngine
from ml.models.compatibility import validate_model_artifacts
from ml.prediction.checkpoint import CheckpointError, PredictionCheckpoint

logger = logging.getLogger(__name__)
FEATURE_COLUMNS = ALL_COLUMNS[2:]


class _StopOnRevocation(ConsumerRebalanceListener):
    def __init__(self, service):
        self.service = service

    def on_partitions_revoked(self, revoked):
        if revoked:
            self.service._stop.set()
            self.service._consumer_ready.clear()

    def on_partitions_assigned(self, assigned):
        pass


def _messages_until_stopped(consumer: KafkaConsumer, stop: threading.Event):
    """Re-enter the bounded iterator so idle revocation can release ownership."""
    while not stop.is_set():
        for message in consumer:
            if stop.is_set():
                return
            yield message


class PredictionRequest(BaseModel):
    dataset_id: str
    unit_id: int | str
    run_id: str
    features: list[dict[str, Any]] = Field(min_length=WINDOW_SIZE)


class PredictionResponse(BaseModel):
    dataset_id: str
    unit_id: int | str
    run_id: str
    rul: float
    window_size: int


class PredictionService:
    def __init__(
        self,
        checkpoint_path: str | Path | None = None,
        kafka_bootstrap_servers: str | None = None,
        influx_url: str | None = None,
        influx_token: str | None = None,
        influx_org: str | None = None,
        influx_bucket: str | None = None,
        max_windows: int = 1024,
        validate_startup: bool | None = None,
    ) -> None:
        if validate_startup is None:
            validate_startup = checkpoint_path is None
        self.checkpoint_path = Path(checkpoint_path or os.getenv(
            "RUL_CHECKPOINT_PATH",
            CHECKPOINT_DIR / "raw" / "lstm_raw_20260626T082955Z.pt",
        ))
        self.compatibility_report = None
        if validate_startup:
            self.compatibility_report = validate_model_artifacts(
                self.checkpoint_path,
                os.getenv("PREPROCESSING_ARTIFACT_DIR", str(STREAMING_ARTIFACT_DIR)),
            )
        self.expected_preprocessing_artifact_version = (
            self.compatibility_report["artifact_version"]
            if self.compatibility_report is not None else None
        )
        self.engine = RULInferenceEngine(self.checkpoint_path, device="cpu")
        self.model_version = self.checkpoint_path.stem
        self.kafka_servers = kafka_bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
        )
        self.input_topic = os.getenv("PREDICTION_INPUT_TOPIC", "preprocessed-telemetry")
        self.output_topic = os.getenv("PREDICTION_OUTPUT_TOPIC", "rul-predictions")
        self.consumer_group = os.getenv(
            "PREDICTION_CONSUMER_GROUP", "rul-prediction-service-group"
        )
        self.max_windows = max_windows
        self.windows: OrderedDict[str, deque[np.ndarray]] = OrderedDict()
        self.last_cycles: dict[str, int] = {}
        self.last_metadata: dict[str, dict[str, Any]] = {}
        self.producer: KafkaProducer | None = None
        self.consumer: KafkaConsumer | None = None
        self.influx: InfluxDBClient | None = None
        self.influx_write = None
        self.influx_url = influx_url or os.getenv("INFLUXDB_URL", "http://influxdb:8086")
        token = influx_token or os.getenv("INFLUXDB_TOKEN")
        if token:
            self.influx = InfluxDBClient(
                url=self.influx_url,
                token=token,
                org=influx_org or os.getenv("INFLUXDB_ORG", "pms-org"),
            )
            self.influx_write = self.influx.write_api(write_options=SYNCHRONOUS)
            self.influx_bucket = influx_bucket or os.getenv(
                "INFLUXDB_BUCKET", "sensor-data"
            )
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._consumer_ready = threading.Event()
        self._consumer_error: str | None = None

    def _vector(self, record: dict[str, Any]) -> np.ndarray:
        missing = [column for column in FEATURE_COLUMNS if column not in record]
        if missing:
            raise ValueError(f"Feature record is missing columns: {missing}")
        return np.asarray([float(record[column]) for column in FEATURE_COLUMNS], dtype=np.float32)

    def predict_records(
        self,
        dataset_id: str,
        unit_id: int | str,
        run_id: str,
        records: list[dict[str, Any]],
    ) -> float | None:
        key = self._key(dataset_id, unit_id, run_id)
        window = self._window_for_key(key)
        for record in records:
            window.append(self._vector(record))
        if len(window) < WINDOW_SIZE:
            return None
        return self.engine.predict(np.stack(window))

    def _window_for_key(self, key: str) -> deque[np.ndarray]:
        window = self.windows.get(key)
        if window is None:
            if len(self.windows) >= self.max_windows:
                evicted_key, _ = self.windows.popitem(last=False)
                self.last_cycles.pop(evicted_key, None)
                self.last_metadata.pop(evicted_key, None)
            window = deque(maxlen=WINDOW_SIZE)
            self.windows[key] = window
        self.windows.move_to_end(key)
        return window

    @staticmethod
    def _key(dataset_id: str, unit_id: int | str, run_id: str) -> str:
        return f"{dataset_id.upper()}:{unit_id}:{run_id}"

    def _write_prediction(
        self, payload: dict[str, Any]
    ) -> None:
        if self.influx_write is None:
            return
        point = (
            Point("rul_predictions")
            .tag("dataset_id", payload["dataset_id"])
            .tag("unit_id", str(payload["unit_id"]))
            .tag("run_id", payload["run_id"])
            .field("rul", payload["rul"])
            .field("cycle", int(payload["cycle"]))
            .field("window_size", int(payload["window_size"]))
            .field("model_version", payload["model_version"])
            .field(
                "preprocessing_artifact_version",
                payload["preprocessing_artifact_version"],
            )
        )
        timestamp = payload.get("source_timestamp")
        if timestamp:
            point = point.time(datetime.fromisoformat(timestamp.replace("Z", "+00:00")))
        self.influx_write.write(bucket=self.influx_bucket, org=self.influx.org, record=point)

    def publish_prediction(self, payload: dict[str, Any]) -> None:
        if not payload.get("source_timestamp"):
            raise ValueError("prediction requires a stable source timestamp")
        if self.producer is not None:
            identity = json.dumps([payload["dataset_id"], str(payload["unit_id"]), payload["run_id"],
                                   payload["cycle"], payload["model_version"],
                                   payload["preprocessing_artifact_version"]], separators=(",", ":"))
            payload["prediction_id"] = hashlib.sha256(identity.encode()).hexdigest()
            self.producer.send(self.output_topic, key=payload["prediction_id"].encode(), value=payload).get(timeout=10)
        self._write_prediction(payload)

    def _checkpoint_identity(self) -> dict[str, Any]:
        if self.compatibility_report is None:
            raise CheckpointError("validated model and scaler artifacts are required")
        return {"consumer_group": self.consumer_group, "topic": self.input_topic,
                "output_topic": self.output_topic,
                "model_sha256": self.compatibility_report["checkpoint_sha256"],
                "artifact_version": self.compatibility_report["artifact_version"],
                "artifact_sha256": self.compatibility_report["artifact_sha256"]}

    @staticmethod
    def _record_hash(value: dict[str, Any]) -> str:
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()

    def _checkpoint_snapshot(self, message: Any) -> dict[str, Any]:
        return {"schema_version": 1, "identity": self._checkpoint_identity(),
                "partition": message.partition, "offset": message.offset,
                "record_sha256": self._record_hash(message.value),
                "windows": {key: [row.tolist() for row in window] for key, window in self.windows.items()},
                "last_cycles": self.last_cycles, "last_metadata": self.last_metadata}

    def _restore_checkpoint(self, snapshot: dict[str, Any]) -> None:
        if snapshot.get("identity") != self._checkpoint_identity():
            raise CheckpointError("checkpoint model, scaler, group or topic mismatch")
        windows, cycles, metadata = snapshot.get("windows"), snapshot.get("last_cycles"), snapshot.get("last_metadata")
        if not all(isinstance(x, dict) for x in (windows, cycles, metadata)) or set(windows) != set(cycles) or set(windows) != set(metadata):
            raise CheckpointError("invalid checkpoint state")
        if len(windows) > self.max_windows:
            raise CheckpointError("checkpoint exceeds window limit")
        restored = OrderedDict()
        for key, rows in windows.items():
            meta = metadata[key]
            cycle = cycles[key]
            if (not isinstance(meta, dict) or not isinstance(cycle, int) or isinstance(cycle, bool) or cycle < 1
                    or meta.get("cycle") != cycle or not isinstance(rows, list) or not 1 <= len(rows) <= WINDOW_SIZE or cycle < len(rows)
                    or self._key(str(meta.get("dataset_id", "")), meta.get("unit_id"), str(meta.get("run_id", ""))) != key
                    or meta.get("preprocessing_artifact_version") != self.expected_preprocessing_artifact_version):
                raise CheckpointError("invalid checkpoint window identity or cycle")
            values = np.asarray(rows, dtype=np.float32)
            if values.shape != (len(rows), len(FEATURE_COLUMNS)) or not np.isfinite(values).all():
                raise CheckpointError("invalid checkpoint feature values")
            restored[key] = deque(values, maxlen=WINDOW_SIZE)
        self.windows, self.last_cycles, self.last_metadata = restored, cycles, metadata

    def _verify_checkpoint_history(self, snapshot: dict[str, Any], partition: TopicPartition) -> None:
        """Refuse a checkpoint whose corresponding input is no longer retained."""
        reader = KafkaConsumer(bootstrap_servers=self.kafka_servers, group_id=None,
                               enable_auto_commit=False, auto_offset_reset="none",
                               value_deserializer=lambda value: json.loads(value.decode("utf-8")))
        try:
            offset = snapshot["offset"]
            if not (reader.beginning_offsets([partition])[partition] <= offset < reader.end_offsets([partition])[partition]):
                raise CheckpointError("checkpoint input offset is not retained")
            reader.assign([partition])
            reader.seek(partition, offset)
            batch = reader.poll(timeout_ms=10000, max_records=1)
            records = batch.get(partition, [])
            if not records or records[0].offset != offset or self._record_hash(records[0].value) != snapshot.get("record_sha256"):
                raise CheckpointError("checkpoint input is missing or changed")
        finally:
            reader.close()

    def process_record(self, record: dict[str, Any]) -> float | None:
        for field in ("dataset_id", "unit_id", "run_id", "cycle", "features"):
            if field not in record:
                raise ValueError(f"Preprocessed record must contain {field}")
        if not isinstance(record["dataset_id"], str) or not record["dataset_id"].strip():
            raise ValueError("Invalid dataset_id")
        if not isinstance(record["run_id"], str) or not record["run_id"].strip():
            raise ValueError("Invalid run_id")
        if isinstance(record["unit_id"], bool) or record["unit_id"] in (None, ""):
            raise ValueError("Invalid unit_id")
        if not re.fullmatch(r"[1-9][0-9]*(?:\.0+)?", str(record["unit_id"])):
            raise ValueError("Invalid unit_id")
        dataset_id = record["dataset_id"].upper()
        if dataset_id not in SUB_DATASETS:
            raise ValueError("Unsupported dataset_id")
        expected_version = getattr(self, "expected_preprocessing_artifact_version", None)
        if expected_version is not None and record.get("preprocessing_artifact_version") != expected_version:
            raise ValueError("Preprocessing artifact version does not match validated model artifacts")
        unit_id = record["unit_id"]
        run_id = str(record["run_id"])
        cycle = record["cycle"]
        if isinstance(cycle, bool) or not isinstance(cycle, int) or cycle < 1:
            raise ValueError("cycle must be a positive integer")
        key = self._key(dataset_id, unit_id, run_id)
        previous_cycle = self.last_cycles.get(key)
        if previous_cycle is not None and cycle <= previous_cycle:
            logger.warning("Skipping duplicate/out-of-order prediction cycle %s for %s", cycle, key)
            return None
        feature_order = record.get("feature_order")
        if feature_order is not None and feature_order != FEATURE_COLUMNS:
            raise ValueError("Preprocessed feature_order does not match the raw checkpoint")
        values = record["features"]
        if len(values) != len(FEATURE_COLUMNS):
            raise ValueError(f"Expected {len(FEATURE_COLUMNS)} preprocessed features")
        vector_record = dict(zip(FEATURE_COLUMNS, values))
        if previous_cycle is not None and cycle != previous_cycle + 1:
            logger.warning("Cycle gap for %s; restarting prediction warm-up at %s", key, cycle)
            self._window_for_key(key).clear()
        self.last_cycles[key] = cycle
        self.last_metadata[key] = record
        rul = self.predict_records(dataset_id, unit_id, run_id, [vector_record])
        if rul is not None:
            payload = {
                "dataset_id": dataset_id,
                "unit_id": unit_id,
                "run_id": run_id,
                "cycle": cycle,
                "source_timestamp": record.get("timestamp"),
                "rul": rul,
                "window_size": WINDOW_SIZE,
                "model_version": self.model_version,
                "preprocessing_artifact_version": record.get(
                    "preprocessing_artifact_version", "unknown"
                ),
            }
            self.publish_prediction(payload)
        return rul

    def start_consumer(self) -> None:
        self._consumer_error = None
        self._consumer_ready.clear()
        checkpoint = None
        try:
            checkpoint_path = os.getenv("PREDICTION_CHECKPOINT_PATH")
            if not checkpoint_path:
                raise CheckpointError("PREDICTION_CHECKPOINT_PATH is required on a persistent single-host volume")
            if self.influx_write is None:
                raise CheckpointError("prediction InfluxDB output must be configured")
            checkpoint = PredictionCheckpoint(checkpoint_path, require_mount=True)
            snapshot = checkpoint.load()
            if snapshot is not None:
                self._restore_checkpoint(snapshot)
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_servers,
                group_id=self.consumer_group,
                auto_offset_reset="latest",
                enable_auto_commit=False,
                consumer_timeout_ms=1000,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
            self.consumer.subscribe([self.input_topic], listener=_StopOnRevocation(self))
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                acks="all",
            )
            self._stop.clear()
            reconciled = False
            previous_offset = None
            for message in _messages_until_stopped(self.consumer, self._stop):
                if self._stop.is_set():
                    break
                try:
                    partition = TopicPartition(message.topic, message.partition)
                    if (self.consumer.partitions_for_topic(self.input_topic) != {message.partition}
                            or self.consumer.assignment() != {partition}):
                        raise CheckpointError("checkpoint mode requires exactly one assigned topic partition")
                    if not reconciled:
                        committed = self.consumer.committed(partition)
                        if snapshot is None:
                            if committed not in (None, 0) or message.offset != 0:
                                raise CheckpointError("cannot start beyond offset zero without a checkpoint")
                            previous_offset = message.offset - 1
                        else:
                            offset = snapshot.get("offset")
                            if (snapshot.get("partition") != message.partition or not isinstance(offset, int)
                                    or isinstance(offset, bool) or offset < 0 or committed not in (offset, offset + 1)):
                                raise CheckpointError("checkpoint and committed offset do not agree")
                            self._verify_checkpoint_history(snapshot, partition)
                            if committed == offset:
                                if message.offset != offset or self._record_hash(message.value) != snapshot.get("record_sha256"):
                                    raise CheckpointError("uncommitted checkpoint record is missing or changed")
                                self.consumer.commit({partition: OffsetAndMetadata(offset + 1, None, -1)})
                                previous_offset = offset
                                reconciled = True
                                self._consumer_ready.set()
                                continue
                            previous_offset = offset
                        reconciled = True
                        self._consumer_ready.set()
                    if message.offset != previous_offset + 1:
                        raise CheckpointError("unexpected input offset; refusing to skip records")
                    self.process_record(message.value)
                    if self.consumer.assignment() != {partition}:
                        raise CheckpointError("partition revoked before checkpoint")
                    checkpoint.save(self._checkpoint_snapshot(message))
                    self.consumer.commit({partition: OffsetAndMetadata(message.offset + 1, None, -1)})
                    previous_offset = message.offset
                except Exception:
                    self.windows.clear()
                    self.last_cycles.clear()
                    self.last_metadata.clear()
                    logger.exception("Prediction failed; stopping before later offsets")
                    raise
        except Exception as exc:
            if not self._stop.is_set():
                self._consumer_error = str(exc)
                logger.exception("Prediction consumer failed to start")
        finally:
            self._consumer_ready.clear()
            try:
                self.close()
            finally:
                if checkpoint is not None:
                    checkpoint.close()

    def readiness_status(self) -> dict[str, object]:
        model_loaded = self.engine is not None
        consumer_thread_alive = self._thread is not None and self._thread.is_alive()
        kafka_initialized = self._consumer_ready.is_set()
        influx_configured = self.influx is not None and self.influx_write is not None
        influx_reachable = False
        if influx_configured:
            try:
                with urlopen(f"{self.influx_url.rstrip('/')}/health", timeout=0.5) as response:
                    influx_reachable = response.status == 200
            except OSError:
                influx_reachable = False
        ready = all(
            (model_loaded, consumer_thread_alive, kafka_initialized, influx_configured, influx_reachable)
        )
        return {
            "ready": ready,
            "model_loaded": model_loaded,
            "consumer_thread_alive": consumer_thread_alive,
            "kafka_initialized": kafka_initialized,
            "influx_configured": influx_configured,
            "influx_reachable": influx_reachable,
            "influx_write_permission_verified": False,
            "model_version": self.model_version if model_loaded else None,
            "compatibility_validated": self.compatibility_report is not None,
            "reason": self._consumer_error if self._consumer_error and not ready else None,
        }

    def start_background(self) -> None:
        self._thread = threading.Thread(target=self.start_consumer, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self.producer is not None:
            self.producer.flush()
            self.producer.close()
        if self.consumer is not None:
            self.consumer.close()
        if self.influx is not None:
            self.influx.close()


def build_request_prediction(
    service: PredictionService, request: PredictionRequest
) -> PredictionResponse:
    if len(request.features) < WINDOW_SIZE:
        raise HTTPException(
            status_code=422,
            detail=f"At least {WINDOW_SIZE} feature records are required",
        )
    window = request.features[-WINDOW_SIZE:]
    cycles = [record.get("cycle") for record in window]
    if (request.dataset_id.upper() not in SUB_DATASETS or not request.run_id.strip()
            or any(isinstance(c, bool) or not isinstance(c, int) or c < 1 for c in cycles)
            or any(b != a + 1 for a, b in zip(cycles, cycles[1:]))):
        raise HTTPException(status_code=422, detail="A consecutive cycle window is required")
    for record in window:
        if (("dataset_id" in record and str(record["dataset_id"]).upper() != request.dataset_id.upper())
                or ("unit_id" in record and str(record["unit_id"]) != str(request.unit_id))
                or ("run_id" in record and record["run_id"] != request.run_id)):
            raise HTTPException(status_code=422, detail="Mixed window identities")
    try:
        rul = service.engine.predict(np.stack([service._vector(record) for record in window]))
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    service.publish_prediction({
        "dataset_id": request.dataset_id.upper(),
        "unit_id": request.unit_id,
        "run_id": request.run_id,
        "cycle": window[-1].get("cycle", WINDOW_SIZE),
        "source_timestamp": window[-1].get("timestamp"),
        "rul": rul,
        "window_size": WINDOW_SIZE,
        "model_version": service.model_version,
        "preprocessing_artifact_version": window[-1].get(
            "preprocessing_artifact_version", "unknown"
        ),
    })
    return PredictionResponse(
        dataset_id=request.dataset_id.upper(),
        unit_id=request.unit_id,
        run_id=request.run_id,
        rul=rul,
        window_size=WINDOW_SIZE,
    )
