"""
Run the Module 1 integration tests inside a Docker container connected to
the pms-m1-test network, so Kafka and Mosquitto are reachable by their
service hostnames (kafka:9092, mosquitto:1883).

Usage (from project root):
    python tests/run_integration_in_docker.py
"""

import subprocess
import sys
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NETWORK      = "pms-m1-test_m1-test-net"
IMAGE        = "pms-simulator-v2-test"

# Override the port constants to use Docker-internal addresses
KAFKA_HOST_OVERRIDE = "kafka:9092"
MQTT_HOST_OVERRIDE  = "mosquitto"
MQTT_PORT_OVERRIDE  = "1883"

cmd = [
    "docker", "run", "--rm",
    "--network", NETWORK,
    "-v", f"{PROJECT_ROOT}:/workspace",
    "-w", "/workspace",
    "-e", f"KAFKA_HOST={KAFKA_HOST_OVERRIDE}",
    "-e", f"MQTT_HOST={MQTT_HOST_OVERRIDE}",
    "-e", f"MQTT_PORT={MQTT_PORT_OVERRIDE}",
    "-e", "CMAPSS_DATA_DIR=/workspace/simulator/data/cmapss",
    IMAGE,
    "bash", "-c",
    "pip install pytest paho-mqtt --quiet 2>/dev/null && "
    "python -m pytest tests/test_m1_integration.py -v --tb=short --no-header 2>&1"
]

print(f"Running integration tests inside Docker network: {NETWORK}")
print(f"Image: {IMAGE}")
print()

result = subprocess.run(cmd, cwd=PROJECT_ROOT)
sys.exit(result.returncode)
