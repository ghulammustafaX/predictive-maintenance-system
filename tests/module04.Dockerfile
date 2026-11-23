FROM python:3.11-slim
WORKDIR /app
COPY ml/streaming/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY ml/__init__.py ml/config.py /app/ml/
COPY ml/data /app/ml/data
COPY ml/models/__init__.py ml/models/compatibility.py /app/ml/models/
COPY ml/streaming /app/ml/streaming
COPY ml/saved_v2_a1/scalers/*_stats.json /app/ml/saved_v2_a1/scalers/
COPY tests/preprocessing_test_artifacts.py tests/module04_kafka_probe.py tests/module04_kafka_worker.py /app/tests/
ENV PYTHONPATH=/app:/app/tests PYTHONUNBUFFERED=1
