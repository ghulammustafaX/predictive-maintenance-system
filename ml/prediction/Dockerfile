FROM python:3.11-slim

WORKDIR /app

COPY ml/prediction/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch

COPY ml/ ./ml/

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["uvicorn", "ml.prediction.main:app", "--host", "0.0.0.0", "--port", "8001"]
