from __future__ import annotations

import json
import logging
import os
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
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel, Field

from ml.config import ALL_COLUMNS, CHECKPOINT_DIR, STREAMING_ARTIFACT_DIR, WINDOW_SIZE
from ml.models.inference import RULInferenceEngine
from ml.models.compatibility import validate_model_artifacts

logger = logging.getLogger(__name__)
FEATURE_COLUMNS = ALL_COLUMNS[2:]


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
        if self.producer is not None:
            self.producer.send(self.output_topic, value=payload).get(timeout=10)
        self._write_prediction(payload)

    def process_record(self, record: dict[str, Any]) -> float | None:
        for field in ("dataset_id", "unit_id", "run_id", "cycle", "features"):
            if field not in record:
                raise ValueError(f"Preprocessed record must contain {field}")
        dataset_id = str(record["dataset_id"]).upper()
        unit_id = record["unit_id"]
        run_id = str(record["run_id"])
        cycle = int(record["cycle"])
        key = self._key(dataset_id, unit_id, run_id)
        previous_cycle = self.last_cycles.get(key)
        if previous_cycle is not None and cycle <= previous_cycle:
            if cycle <= 1 and previous_cycle > 1:
                self.windows[key].clear()
                self.last_metadata.pop(key, None)
                self.last_cycles[key] = 0
            else:
                logger.warning("Skipping duplicate/out-of-order prediction cycle %s for %s", cycle, key)
                return None
        feature_order = record.get("feature_order")
        if feature_order is not None and feature_order != FEATURE_COLUMNS:
            raise ValueError("Preprocessed feature_order does not match the raw checkpoint")
        values = record["features"]
        if len(values) != len(FEATURE_COLUMNS):
            raise ValueError(f"Expected {len(FEATURE_COLUMNS)} preprocessed features")
        vector_record = dict(zip(FEATURE_COLUMNS, values))
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
        try:
            self.consumer = KafkaConsumer(
                self.input_topic,
                bootstrap_servers=self.kafka_servers,
                group_id=self.consumer_group,
                auto_offset_reset="latest",
                enable_auto_commit=False,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
            self._consumer_ready.set()
            self._stop.clear()
            for message in self.consumer:
                if self._stop.is_set():
                    break
                try:
                    self.process_record(message.value)
                    self.consumer.commit()
                except (TypeError, ValueError, KeyError) as exc:
                    logger.error("Invalid feature record: %s", exc)
                    self.consumer.commit()
                except Exception:
                    logger.exception("Prediction output failed; offset was not committed")
        except Exception as exc:
            self._consumer_error = str(exc)
            logger.exception("Prediction consumer failed to start")
        finally:
            self._consumer_ready.clear()
            self.close()

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
    service._window_for_key(
        service._key(request.dataset_id, request.unit_id, request.run_id)
    ).clear()
    rul = service.predict_records(
        request.dataset_id, request.unit_id, request.run_id, window
    )
    if rul is None:
        raise HTTPException(status_code=422, detail="A complete feature window is required")
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
