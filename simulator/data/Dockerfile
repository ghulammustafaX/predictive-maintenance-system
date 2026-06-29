FROM python:3.10-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Data directory will be mounted as volume
# simulator/data/ -> /app/data/

CMD ["python", "src/cmapss_loader.py"]