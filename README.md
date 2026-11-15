# Predictive Maintenance System

A real-time predictive maintenance platform built on the NASA C-MAPSS turbofan engine dataset. The system streams sensor telemetry through Apache Kafka, stores it in InfluxDB, runs an LSTM model to predict Remaining Useful Life (RUL), and surfaces everything through a live web dashboard with authentication and role-based access control.

---

## What it does

Industrial equipment degrades over time. This system monitors 100 simulated turbofan engines continuously and predicts how many operational cycles remain before each engine is likely to fail. When an engine approaches its end of life, the system raises alerts so maintenance can be scheduled before failure occurs.

The pipeline runs entirely in Docker and processes data end-to-end: a simulator replays real NASA sensor recordings into Kafka, a consumer writes them to InfluxDB, a preprocessing service normalises the stream, and an LSTM prediction service publishes RUL estimates back to Kafka and InfluxDB. The dashboard reads from InfluxDB and PostgreSQL in real time.

> 📽️ **Demo video** — watch the full system running live:
>
> [![PMS Demo Video](https://img.youtube.com/vi/4_dOkwqwShI/maxresdefault.jpg)](https://youtu.be/4_dOkwqwShI)

---

## System architecture

<p align="center">
  <img src="docs/architecture.png" alt="PMS System Architecture" width="90%">
</p>

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for a detailed breakdown of each component.

---

## Repository structure

```
predictive-maintenance-system/
│
├── backend/                  # FastAPI application
│   ├── app/
│   │   ├── routers/          # API route handlers
│   │   │   ├── auth.py       # Authentication & token management
│   │   │   ├── fleet.py      # Engine fleet status & telemetry
│   │   │   ├── alerts.py     # Alert lifecycle management
│   │   │   ├── anomalies.py  # Anomaly detection & history
│   │   │   └── rul.py        # RUL prediction queries
│   │   ├── models.py         # SQLAlchemy ORM models
│   │   ├── schemas.py        # Pydantic request/response schemas
│   │   ├── security.py       # JWT, bcrypt, token utilities
│   │   ├── rbac.py           # Role-based access control
│   │   ├── anomaly_detector.py # Z-score & threshold detection logic
│   │   └── database.py       # DB session and connection setup
│   ├── main.py               # App entry point, middleware, router registration
│   ├── Dockerfile
│   └── requirements.txt
│
├── frontend/
│   ├── authentication/       # Login, register, password reset, Google OAuth
│   └── dashboard/            # Live operations, alerts, anomalies, users
│
├── ml/
│   ├── prediction/           # LSTM inference service (Kafka consumer + REST)
│   ├── streaming/            # Real-time preprocessing service
│   ├── data/                 # Preprocessing pipeline scripts
│   └── models/               # Saved model checkpoints (not in git)
│
├── simulator/                # Replays NASA C-MAPSS dataset into Kafka
├── ingestion/                # Kafka → InfluxDB consumer
├── tests/                    # Integration and unit tests
├── docs/                     # Architecture and API documentation
└── docker-compose.yml        # Full stack definition
```

---

## Technology stack

| Layer | Technology |
|---|---|
| Data source | NASA C-MAPSS turbofan dataset (FD001–FD004) |
| Message broker | Apache Kafka 3.4 + Zookeeper |
| Time-series store | InfluxDB 2.7 (Flux query language) |
| Relational store | PostgreSQL 16 |
| ML framework | PyTorch 2.x — LSTM sequence model |
| Backend API | FastAPI (Python 3.11), SQLAlchemy 2.0 |
| Authentication | JWT (HS256), bcrypt, Google OAuth 2.0 |
| Frontend | Vanilla JavaScript, Chart.js, CSS3 |
| Containerisation | Docker, Docker Compose |

---

## Key features

**Data pipeline**
- Simulator streams all four C-MAPSS sub-datasets (FD001–FD004) at configurable speed
- Preprocessing service normalises sensor readings and builds fixed-length input windows without data leakage
- LSTM model runs inference on each window and publishes RUL predictions back to Kafka and InfluxDB

**Dashboard**
- Live fleet view — all 100 engines with status, latest cycle, and freshness
- Per-engine telemetry chart with sensor selection
- RUL trend history per run
- Anomaly detection — Z-score and threshold methods across 21 sensors
- Alert centre with open / acknowledged / resolved lifecycle and audit trail

**Authentication & access control**
- Email/password registration with verification flow
- Google OAuth with automatic account linking
- Four roles: Admin, Maintenance Engineer, Floor Operator, Viewer
- JWT access tokens (30 min) + refresh tokens (7 days)
- Full audit log on every auth event and alert state change

---

## Documentation

| Document | Description |
|---|---|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Component breakdown, data flow, design decisions |
| [`docs/API.md`](docs/API.md) | Full REST API reference with request/response shapes |

---

## Dataset

This project uses the [NASA C-MAPSS Turbofan Engine Degradation Simulation dataset](https://data.nasa.gov/dataset/C-MAPSS-Aircraft-Engine-Simulator-Data/xaut-bemq). The dataset is not included in this repository. Place the raw files under `simulator/data/cmapss/` before starting the simulator.

---

## Project status

Active development — Final Year Project, University of Engineering and Technology.

**Modules completed**
- Kafka ingestion pipeline and InfluxDB storage
- Real-time preprocessing with feature scaling and windowing
- LSTM model training and streaming inference
- FastAPI backend with full auth, RBAC, and audit logging
- Live dashboard, alert centre, and anomaly detection UI

**In progress**
- Automated alert triggering from the prediction pipeline
- Advanced analytics page
