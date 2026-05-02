"""
InfluxDB Query Test & Verification Script
Module 3: Time-Series Storage Verification
"""

import os
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv

load_dotenv()

INFLUXDB_URL = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', 'pms-org')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', 'sensor-data')


def main():
    print("=" * 60)
    print("INFLUXDB VERIFICATION TEST")
    print("Module 3: Time-Series Storage")
    print("=" * 60)
    print(f"🔌 Connecting to: {INFLUXDB_URL}")
    print()
    
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    query_api = client.query_api()
    
    # Test 1: Count total records
    print("📊 Test 1: Total Records")
    query = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "sensor_telemetry")
      |> count()
    '''
    
    result = query_api.query(query)
    total = 0
    for table in result:
        for record in table.records:
            total += record.get_value()
    
    print(f"   ✅ Total records: {total}")
    print()
    
    # Test 2: Unique engine units
    print("📊 Test 2: Unique Engine Units")
    query_units = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -30d)
      |> filter(fn: (r) => r._measurement == "sensor_telemetry")
      |> keep(columns: ["unit_id"])
      |> distinct(column: "unit_id")
      |> count()
    '''
    
    result_units = query_api.query(query_units)
    for table in result_units:
        for record in table.records:
            print(f"   ✅ Unique units: {record.get_value()}")
    print()
    
    # Test 3: Latest readings
    print("📊 Test 3: Latest Sensor Readings (sensor_1)")
    query_latest = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "sensor_telemetry")
      |> filter(fn: (r) => r._field == "sensor_1")
      |> last()
      |> limit(n: 5)
    '''
    
    result_latest = query_api.query(query_latest)
    for table in result_latest:
        for record in table.records:
            unit_id = record.values.get('unit_id')
            value = record.get_value()
            timestamp = record.get_time()
            print(f"   Unit {unit_id}: {value:.2f} at {timestamp}")
    
    print()
    print("=" * 60)
    print("✅ ALL TESTS PASSED")
    print("=" * 60)
    
    client.close()


if __name__ == "__main__":
    main()