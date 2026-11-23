FROM python:3.11-slim
WORKDIR /app
COPY ml/prediction/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu 'torch>=2.6,<3'
COPY ml/ /app/ml/
COPY tests/module06_integration_worker.py tests/module06_integration_probe.py /app/tests/
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
