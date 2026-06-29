"""
NASA C-MAPSS Turbofan Dataset Loader & Kafka Streamer
Reads train_FD001.txt and streams sensor data to Kafka topic: sensor-telemetry
"""

import os
import time
import json
import pandas as pd
import numpy as np
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
SENSOR_TOPIC = os.getenv('SENSOR_TOPIC', 'sensor-telemetry')
DATA_PATH = '/app/data/cmapss/train_FD001.txt'
# DATA_PATH = 'data/cmapss/train_FD001.txt'
STREAM_INTERVAL = 1.0  # seconds between each row

# Column names for C-MAPSS dataset
COLUMNS = [
    'unit_id', 'time_cycles', 'operational_setting_1', 'operational_setting_2', 'operational_setting_3'
] + [f'sensor_{i}' for i in range(1, 22)]  # 21 sensor columns


def load_cmapss_data(filepath):
    """Load NASA C-MAPSS dataset from text file."""
    print(f"📂 Loading C-MAPSS dataset from: {filepath}")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Dataset not found at {filepath}")
    
    # C-MAPSS files are space-separated with no header
    df = pd.read_csv(filepath, sep=r'\s+', header=None, names=COLUMNS)
    
    print(f"✅ Loaded {len(df)} rows, {len(df['unit_id'].unique())} engine units")
    return df


def create_kafka_producer():
    """Initialize Kafka producer with retry logic."""
    print(f"🔌 Connecting to Kafka broker: {KAFKA_BROKER}")
    
    max_retries = 30
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(f"✅ Connected to Kafka broker")
            return producer
        except KafkaError as e:
            print(f"⚠️  Kafka connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                print(f"   Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed to connect to Kafka after {max_retries} attempts")


def stream_data_to_kafka(df, producer):
    """Stream C-MAPSS data to Kafka topic row by row."""
    print(f"🚀 Starting data stream to topic: {SENSOR_TOPIC}")
    print(f"⏱️  Stream interval: {STREAM_INTERVAL} seconds per row")
    print("-" * 60)
    
    row_count = 0
    
    try:
        for index, row in df.iterrows():
            # Convert row to dictionary and handle NaN values
            payload = row.to_dict()
            
            # Replace NaN with None for JSON serialization
            payload = {k: (None if pd.isna(v) else float(v)) for k, v in payload.items()}
            
            # Send to Kafka
            future = producer.send(SENSOR_TOPIC, value=payload)
            
            # Wait for confirmation (optional, ensures delivery)
            try:
                record_metadata = future.get(timeout=10)
                row_count += 1
                
                # Print progress every 100 rows
                if row_count % 100 == 0:
                    print(f"📤 Sent {row_count} rows | Unit: {int(payload['unit_id'])} | Cycle: {int(payload['time_cycles'])}")
            
            except KafkaError as e:
                print(f"❌ Failed to send row {row_count}: {e}")
            
            # Simulate real-time streaming
            time.sleep(STREAM_INTERVAL)
    
    except KeyboardInterrupt:
        print("\n⚠️  Stream interrupted by user")
    
    finally:
        producer.flush()
        producer.close()
        print("-" * 60)
        print(f"✅ Stream completed. Total rows sent: {row_count}")


def main():
    """Main execution flow."""
    print("=" * 60)
    print("NASA C-MAPSS TURBOFAN SIMULATOR")
    print("Predictive Maintenance System — Phase 1")
    print("=" * 60)
    print()
    
    # Step 1: Load dataset
    df = load_cmapss_data(DATA_PATH)
    
    # Step 2: Connect to Kafka
    producer = create_kafka_producer()
    
    # Step 3: Stream data
    stream_data_to_kafka(df, producer)


if __name__ == "__main__":
    main()