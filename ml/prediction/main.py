from __future__ import annotations

import logging
import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from ml.prediction.service import (
    PredictionRequest,
    PredictionService,
    build_request_prediction,
)
from ml.prediction.checkpoint import validate_checkpoint_path

logging.basicConfig(level="INFO")
service = PredictionService()


@asynccontextmanager
async def lifespan(_: FastAPI):
    validate_checkpoint_path(os.getenv("PREDICTION_CHECKPOINT_PATH"))
    thread = threading.Thread(target=service.start_consumer, daemon=True)
    service._thread = thread
    thread.start()
    yield
    service.close()


app = FastAPI(title="PMS RUL Prediction Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, object]:
    readiness = service.readiness_status()
    return {
        "status": "ok" if readiness["ready"] else "not_ready",
        "readiness": readiness,
        "checkpoint": str(service.checkpoint_path),
        "input_topic": service.input_topic,
        "output_topic": service.output_topic,
    }


@app.get("/ready")
def ready() -> dict[str, object]:
    readiness = service.readiness_status()
    if not readiness["ready"]:
        raise HTTPException(status_code=503, detail=readiness)
    return readiness


@app.post("/predict")
def predict(request: PredictionRequest):
    return build_request_prediction(service, request)
