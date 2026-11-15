#!/usr/bin/env python3
"""Test authenticated API endpoints to verify run_id data"""

import requests
import json
import sys
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8000"

# Login and get token
print("🔐 Authenticating...")
login_response = requests.post(
    f"{BASE_URL}/api/auth/login",
    json={"email": "admin@pms.local", "password": "admin123"}
)

if login_response.status_code != 200:
    print(f"❌ Login failed: {login_response.status_code}")
    print(login_response.text)
    sys.exit(1)

token = login_response.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}
print(f"✅ Authenticated as admin@pms.local")

# Test telemetry endpoint for FD001-001
print("\n📡 Fetching telemetry for FD001-001...")
now = datetime.utcnow()
from_time = (now - timedelta(days=1)).isoformat() + "Z"
to_time = (now + timedelta(days=1)).isoformat() + "Z"

telemetry_response = requests.get(
    f"{BASE_URL}/api/engines/FD001-001/telemetry",
    headers=headers,
    params={
        "channels": "time_cycles",
        "points": 8,
        "from": from_time,
        "to": to_time
    }
)

if telemetry_response.status_code != 200:
    print(f"❌ Telemetry request failed: {telemetry_response.status_code}")
    print(telemetry_response.text)
    sys.exit(1)

telemetry_data = telemetry_response.json()
points = telemetry_data.get("points", [])

print(f"✅ Received {len(points)} telemetry points")

# Extract unique run_ids
run_ids = list(set(p["run_id"] for p in points if p.get("run_id")))
print(f"\n📋 Unique run_ids found: {len(run_ids)}")
for i, run_id in enumerate(run_ids, 1):
    point_count = sum(1 for p in points if p.get("run_id") == run_id)
    print(f"  {i}. {run_id} ({point_count} points)")

# Check for acceptance run
acceptance_run = "acceptance-fd001-unit1-20261112T0015Z"
has_acceptance = acceptance_run in run_ids

print(f"\n🎯 Acceptance run '{acceptance_run}': {'✅ FOUND' if has_acceptance else '❌ NOT FOUND'}")

if not has_acceptance:
    print("\n⚠️  Acceptance run is missing from telemetry API response")
    print("   This explains why dropdown doesn't show it.")

# Test RUL history endpoint
if has_acceptance:
    print(f"\n📊 Fetching RUL history for acceptance run...")
    rul_response = requests.get(
        f"{BASE_URL}/api/engines/FD001-001/rul/history",
        headers=headers,
        params={
            "run_id": acceptance_run,
            "points": 200,
            "from": from_time,
            "to": to_time
        }
    )
    
    if rul_response.status_code != 200:
        print(f"❌ RUL request failed: {rul_response.status_code}")
        print(rul_response.text)
    else:
        rul_data = rul_response.json()
        predictions = rul_data.get("points", [])
        print(f"✅ Received {len(predictions)} RUL predictions")
        
        if predictions:
            cycles = [p.get("cycle") for p in predictions if p.get("cycle") is not None]
            ruls = [p.get("rul_cycles") for p in predictions if p.get("rul_cycles") is not None]
            
            print(f"   Cycles: {min(cycles)} to {max(cycles)}")
            print(f"   RUL range: {min(ruls)} to {max(ruls)} cycles")
            print(f"   Latest: Cycle {predictions[-1].get('cycle')}, RUL {predictions[-1].get('rul_cycles')}")
        else:
            print("   ⚠️  No predictions returned")

print("\n" + "="*60)
print("Summary:")
print(f"  Telemetry points: {len(points)}")
print(f"  Unique runs: {len(run_ids)}")
print(f"  Run options: {run_ids}")
print(f"  Acceptance run present: {has_acceptance}")
