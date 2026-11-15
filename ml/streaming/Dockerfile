FROM python:3.11-slim

WORKDIR /app

COPY ml/streaming/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ml/ ./ml/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["python", "-m", "ml.streaming.preprocessing"]
