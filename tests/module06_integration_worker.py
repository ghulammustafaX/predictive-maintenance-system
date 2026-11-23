"""Isolated harness entry point; a processing failure must exit nonzero."""
import logging

from ml.prediction.service import PredictionService

logging.basicConfig(level=logging.INFO)
service = PredictionService()
service.start_consumer()
if service._consumer_error:
    raise SystemExit('prediction worker stopped on a processing error')
