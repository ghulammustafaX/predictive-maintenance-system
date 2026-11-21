# API Reference

Base URL: `http://localhost:8000`

All endpoints except `/api/health` and the auth endpoints require a valid JWT access token in the `Authorization: Bearer <token>` header.

---

## Authentication

### POST /api/auth/register

Create a new account. Sends a verification email before the account can be used.

**Body**
```json
{
  "email": "user@example.com",
  "full_name": "Jane Smith",
  "password": "SecurePass@1"
}
```

**Response** `201`
```json
{
  "message": "Registration successful. Please check your email to verify your account."
}
```

---

### POST /api/auth/login

Authenticate with email and password.

**Body**
```json
{
  "email": "user@example.com",
  "password": "SecurePass@1"
}
```

**Response** `200`
```json
{
  "access_token": "<jwt>",
  "refresh_token": "<jwt>",
  "token_type": "bearer"
}
```

**Errors**
- `401` — wrong password, unrecognised email, or account linked to Google only
- `403` — account disabled or email not verified

---

### POST /api/auth/refresh

Exchange a refresh token for a new access/refresh token pair.

**Body**
```json
{ "refresh_token": "<jwt>" }
```

**Response** `200` — same shape as `/login`

---

### POST /api/auth/logout

Revoke the current tokens. Requires a valid access token.

---

### GET /api/auth/me

Returns the authenticated user's profile.

**Response** `200`
```json
{
  "id": "uuid",
  "email": "user@example.com",
  "full_name": "Jane Smith",
  "role": "admin",
  "is_verified": true,
  "auth_provider": "local",
  "created_at": "2026-01-15T10:30:00"
}
```

---

### POST /api/auth/google

Sign in or register with a Google ID token (from the Google Sign-In button).

**Body**
```json
{ "id_token": "<google_id_token>" }
```

**Response** `200` — same shape as `/login`

---

### POST /api/auth/forgot-password

Send a password reset email.

**Body**
```json
{ "email": "user@example.com" }
```

---

### POST /api/auth/reset-password

Set a new password using the token from the reset email.

**Body**
```json
{
  "token": "<reset_token>",
  "new_password": "NewSecure@1"
}
```

---

### GET /api/auth/users

List all users. **Admin only.**

**Response** `200`
```json
{
  "users": [
    {
      "id": "uuid",
      "email": "...",
      "full_name": "...",
      "role": "viewer",
      "is_active": true,
      "is_verified": true
    }
  ]
}
```

---

### PATCH /api/auth/users/:id/role

Change a user's role. **Admin only.**

**Body**
```json
{ "role": "maintenance_engineer" }
```

Valid roles: `admin`, `maintenance_engineer`, `floor_operator`, `viewer`

---

## Fleet & Telemetry

### GET /api/health

Liveness check — no authentication required.

**Response** `200`
```json
{
  "status": "ok",
  "influxdb_url": "http://influxdb:8086",
  "bucket": "sensor-data",
  "measurement": "sensor_telemetry",
  "latest_points": -1
}
```

---

### GET /api/fleet/status

Returns the latest telemetry point for every known engine.

**Roles:** all authenticated

**Response** `200`
```json
{
  "generated_at": "2026-11-12T11:00:00+00:00",
  "fleet": [
    {
      "engine_id": "FD001-001",
      "status": "healthy",
      "latest_cycle": 192,
      "last_seen_at": "2026-11-12T10:59:00+00:00",
      "rul_cycles": null,
      "rul_percent": null,
      "active_anomaly_count": 0,
      "active_alert_count": 0
    }
  ]
}
```

**Status values:** `healthy` (last seen < 1 h ago), `warning` (1–24 h), `offline` (> 24 h), `future` (timestamp ahead of now — test data)

---

### GET /api/engines/:engine_id/telemetry

Returns bounded sensor telemetry for one engine, downsampled to `points` rows.

**Roles:** all authenticated

**Path param:** `engine_id` — format `FD001-007` (dataset + 1–6 digit unit number)

**Query params**

| Param | Default | Description |
|---|---|---|
| `channels` | all | Comma-separated sensor names e.g. `time_cycles,sensor_2,sensor_14` |\n| `run_id` | all runs | Optional validated run ID. When omitted, rows from multiple runs can appear, each with its own `run_id` |
| `from` | now − 1 h | ISO-8601 start time |
| `to` | now | ISO-8601 end time |
| `points` | 500 | Maximum returned rows (1–5000), with Flux last-value aggregation per dataset, engine, run and selected field |

**Response** `200`
```json
{
  "engine_id": "FD001-001",
  "channels": ["time_cycles", "sensor_2", "sensor_14"],
  "points": [
    {
      "timestamp": "2026-11-12T10:50:00+00:00",
      "cycle": 188,
      "run_id": "abc123",
      "sensor_2": 641.82,
      "sensor_14": 8.4195
    }
  ]
}
```

---

## RUL Predictions

### GET /api/engines/:engine_id/rul/latest

Most recent RUL prediction stored for this engine.

**Roles:** all authenticated

**Response** `200`
```json
{
  "engine_id": "FD001-001",
  "prediction": {
    "rul_cycles": 42,
    "cycle": 192,
    "run_id": "abc123",
    "timestamp": "2026-11-12T10:59:00+00:00",
    "model_version": "lstm_raw_20260626T082955Z",
    "preprocessing_artifact_version": "scaler_v2_a1"
  }
}
```

---

### GET /api/engines/:engine_id/rul/history

RUL prediction history for a specific run, downsampled to `points` rows.

**Query params**

| Param | Default | Description |
|---|---|---|
| `run_id` | — | Required — filter to one run |
| `from` | now − 1 h | Start time |
| `to` | now | End time |
| `points` | 200 | Max rows |

---

## Anomalies

### GET /api/anomalies

List persisted anomaly events for an engine.

**Roles:** all authenticated

**Query params**

| Param | Required | Description |
|---|---|---|
| `dataset_id` | yes | e.g. `FD001` |
| `unit_id` | yes | e.g. `1` |
| `run_id` | no | Filter to one run |
| `severity` | no | `low`, `medium`, `high`, `critical` |
| `limit` | no | Default 100, max 500 |
| `offset` | no | Default 0 |

**Response** `200`
```json
{
  "total": 3,
  "offset": 0,
  "limit": 100,
  "anomalies": [
    {
      "event_id": "sha256hex",
      "dataset_id": "FD001",
      "unit_id": "1",
      "run_id": "run-fd001-001-a",
      "cycle": 192,
      "timestamp": "2026-11-12T10:50:00",
      "sensor": "sensor_2",
      "detection_method": "zscore",
      "observed_value": 518.67,
      "severity": "critical",
      "score": 4.21,
      "status": "open",
      "created_at": "2026-11-12T09:00:00"
    }
  ]
}
```

---

### POST /api/anomalies/detect

Run anomaly detection against stored telemetry and persist new events.

**Roles:** Admin, Maintenance Engineer

**Query params**

| Param | Required | Description |
|---|---|---|
| `dataset_id` | yes | |
| `unit_id` | yes | |
| `run_id` | yes | |
| `lookback_hours` | no | Default 48, max 720 |

**Response** `200`
```json
{
  "message": "Detection complete. 3 new anomaly events persisted.",
  "new_events": 3,
  "total_detected": 3,
  "anomalies": [...]
}
```

**Detection methods:**
- **Z-score** — flags a sensor reading if it exceeds 3 standard deviations from the engine's mean for that sensor in the lookback window
- **Threshold** — flags readings that cross hard limits (e.g. sensor_9 > 9100, sensor_14 > 8.7)

---

## Alerts

### GET /api/alerts

List persisted alerts with optional filters.

**Roles:** all authenticated

**Query params:** `dataset_id`, `unit_id`, `run_id`, `status`, `severity`, `alert_type`, `limit` (default 50, max 200), `offset`

**Response** `200`
```json
{
  "total": 14,
  "offset": 0,
  "limit": 50,
  "alerts": [
    {
      "id": "uuid",
      "alert_type": "rul_low",
      "severity": "critical",
      "dataset_id": "FD001",
      "unit_id": "5",
      "run_id": "run-fd001-005-a",
      "cycle": 320,
      "message": "RUL critically low: 18 cycles remaining for FD001-005.",
      "status": "open",
      "source_event_id": null,
      "created_at": "2026-11-12T10:30:00",
      "acknowledged_at": null,
      "acknowledged_by": null,
      "resolved_at": null
    }
  ]
}
```

**Alert types:** `rul_low` (RUL threshold crossed), `anomaly` (promoted from an anomaly event)  
**Status values:** `open` → `acknowledged` → `resolved`

---

### POST /api/alerts/:id/acknowledge

Move an open alert to `acknowledged`. Records the acting user.

**Roles:** Admin, Maintenance Engineer

**Body**
```json
{ "note": "Dispatching inspection team to unit 5." }
```

---

### POST /api/alerts/:id/resolve

Move an alert to `resolved`.

**Roles:** Admin only

**Body**
```json
{ "note": "Bearing replaced. Sensor readings normalised." }
```

---

### GET /api/alerts/:id/audit

Full audit trail for one alert — every state change with timestamp and user.

**Response** `200`
```json
{
  "alert_id": "uuid",
  "audit": [
    { "action": "created",      "user_id": null,   "detail": "...", "timestamp": "..." },
    { "action": "acknowledged", "user_id": "uuid", "detail": "...", "timestamp": "..." },
    { "action": "resolved",     "user_id": "uuid", "detail": "...", "timestamp": "..." }
  ]
}
```

---

### POST /api/alerts/evaluate-rul

Evaluate a known RUL value against configured thresholds and create an alert if warranted. Idempotent.

**Roles:** Admin, Maintenance Engineer

**Body**
```json
{
  "dataset_id": "FD001",
  "unit_id": "5",
  "run_id": "run-fd001-005-a",
  "cycle": 320,
  "predicted_rul": 18.0
}
```

Default thresholds: warning at 60 cycles, critical at 30 cycles.

---

### POST /api/alerts/from-anomaly/:event_id

Promote an existing anomaly record into an alert. Idempotent.

**Roles:** Admin, Maintenance Engineer
