FROM python:3.11-slim

# Prevent Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System packages: unrar for IMS archive extraction
RUN apt-get update \
    && apt-get install -y --no-install-recommends unrar-free \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY src/ ./src/

# Data directory is mounted as a Docker volume at runtime.
# Host: simulator/data/ → Container: /app/data/

# Default: run the C-MAPSS production simulator.
# Override CMD or ENTRYPOINT to run simulate_data.py or ims_loader.py.
CMD ["python", "-u", "src/cmapss_loader.py"]
