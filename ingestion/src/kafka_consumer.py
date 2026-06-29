"""
Kafka Consumer → InfluxDB Writer
Module 2: Stream Ingestion
"""

import os
import json
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
SENSOR_TOPIC = os.getenv('SENSOR_TOPIC', 'sensor-telemetry')
CONSUMER_GROUP = 'influxdb-writer-group'

INFLUXDB_URL = os.getenv('INFLUXDB_URL', 'http://influxdb:8086')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', 'pms-org')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', 'sensor-data')


def create_kafka_consumer():
    """Initialize Kafka consumer with retry."""
    print(f"🔌 Connecting to Kafka: {KAFKA_BROKER}")
    print(f"📥 Topic: {SENSOR_TOPIC}")
    
    max_retries = 30
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                SENSOR_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=CONSUMER_GROUP,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            print(f"✅ Connected to Kafka")
            return consumer
        except KafkaError as e:
            print(f"⚠️  Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise


def create_influxdb_client():
    """Initialize InfluxDB client."""
    print(f"🔌 Connecting to InfluxDB: {INFLUXDB_URL}")
    
    if not INFLUXDB_TOKEN:
        raise ValueError("INFLUXDB_TOKEN not set")
    
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    print(f"✅ Connected to InfluxDB")
    print(f"📊 Bucket: {INFLUXDB_BUCKET}")
    
    return client, write_api


def write_to_influxdb(write_api, message):
    """Write Kafka message to InfluxDB."""
    try:
        point = Point("sensor_telemetry") \
            .tag("unit_id", str(int(message['unit_id']))) \
            .field("time_cycles", int(message['time_cycles'])) \
            .field("operational_setting_1", float(message['operational_setting_1'])) \
            .field("operational_setting_2", float(message['operational_setting_2'])) \
            .field("operational_setting_3", float(message['operational_setting_3']))
        
        # Add 21 sensors
        for i in range(1, 22):
            sensor_key = f'sensor_{i}'
            if sensor_key in message and message[sensor_key] is not None:
                point = point.field(sensor_key, float(message[sensor_key]))
        
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        return True
    
    except Exception as e:
        print(f"❌ Write failed: {e}")
        return False


def consume_and_write():
    """Main consumer loop."""
    consumer = create_kafka_consumer()
    client, write_api = create_influxdb_client()
    
    print("🚀 Starting consumer loop...")
    print("-" * 60)
    
    message_count = 0
    
    try:
        for message in consumer:
            payload = message.value
            
            if write_to_influxdb(write_api, payload):
                message_count += 1
                
                if message_count % 100 == 0:
                    print(f"✅ {message_count} messages | Unit: {int(payload['unit_id'])} | Cycle: {int(payload['time_cycles'])}")
    
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted")
    
    finally:
        consumer.close()
        client.close()
        print(f"✅ Total processed: {message_count}")


def main():
    print("=" * 60)
    print("KAFKA → INFLUXDB CONSUMER")
    print("Module 2: Stream Ingestion")
    print("=" * 60)
    print()
    
    consume_and_write()


if __name__ == "__main__":
    main()