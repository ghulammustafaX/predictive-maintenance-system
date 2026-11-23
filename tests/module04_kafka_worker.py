"""Test-only worker with synthetic scaler fixtures and real checkpoint validation."""
import logging
from pathlib import Path
from preprocessing_test_artifacts import create_synthetic_preprocessing_artifacts
from ml.streaming.preprocessing import StreamingPreprocessor

logging.basicConfig(level=logging.INFO)
artifacts = create_synthetic_preprocessing_artifacts(Path('/tmp/m4-fixtures'), Path('/app'))
StreamingPreprocessor(artifact_dir=artifacts, validate_startup=False).run()
