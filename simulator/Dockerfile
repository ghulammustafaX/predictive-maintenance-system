FROM python:3.11-slim

# Prevent Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY src/ ./src/

# Data directory is mounted as a Docker volume
# Host: simulator/data/ -> Container: /app/data/

# Run the simulator in unbuffered mode for real-time logs
CMD ["python", "-u", "src/cmapss_loader.py"]