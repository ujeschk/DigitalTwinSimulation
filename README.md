# Digital Twin Simulation with Predictive Maintenance (PdM)

## Overview
This project implements an end-to-end **Digital Twin system** for a building environment with:

- IoT telemetry simulation
- Semantic modeling using Brick + GraphDB
- Data storage in SQLite
- Machine Learning-based anomaly detection (Isolation Forest)
- REST API (Flask)
- 3D visualization (IFC.js)

The system is fully containerized using Docker.

---

## Architecture

```
Telemetry → GraphDB (Brick) → SQLite (telemetry.db)
                                  ↓
                           PDM Pipeline
                    (features → train → infer)
                                  ↓
                          anomalies.db
                                  ↓
                                API
                                  ↓
                             Viewer (3D)
```

---

## Components

### 1. Telemetry Service
- Generates realistic sensor data (temperature, humidity)
- Queries GraphDB via SPARQL to resolve sensor-room mapping
- Writes to SQLite database (`telemetry.db`)

### 2. GraphDB
- Stores Brick schema
- Provides semantic relationships between entities

### 3. PDM (Predictive Maintenance)
- Feature engineering (rolling mean, std, diff, z-score)
- Model: Isolation Forest (per room)
- Training: periodic
- Inference: near real-time
- Output: `anomalies.db`

### 4. API (Flask)
Endpoints:
- `/api/health`
- `/api/telemetry-guid`
- `/api/anomalies`

### 5. Viewer (IFC.js)
- Loads IFC building model
- Displays live telemetry
- Highlights anomalies

---

## Prerequisites

- Docker
- Docker Compose
- Git

---

## Setup & Run

### 1. Clone repository

```bash
git clone git@github.com:ujeschk/DigitalTwinSimulation.git
cd DigitalTwinSimulation
```

### 2. Start all services

```bash
docker compose up -d --build
```

---

## Access Services

- Viewer: http://localhost:3000
- API: http://localhost:5000
- GraphDB: http://localhost:7200

---

## Verify System

### Check telemetry

```bash
sqlite3 data/telemetry.db "SELECT COUNT(*) FROM telemetry;"
```

### Check anomalies

```bash
sqlite3 data/anomalies.db "SELECT * FROM anomalies ORDER BY timestamp DESC LIMIT 5;"
```

### API test

```bash
curl http://localhost:5000/api/health
curl http://localhost:5000/api/anomalies
```

---

## PDM Pipeline Details

### Training
- Uses last N days of telemetry
- Generates per-room models
- Stored in `/data/models`

### Inference
- Runs every 60 seconds
- Uses rolling window features
- Detects anomalies based on model score

---

## Data Storage

- `data/telemetry.db`
- `data/anomalies.db`
- `data/models/`

These are mounted as Docker volumes.

---

## Notes

- SQLite runs in WAL mode for concurrent read/write
- API uses read-only connections
- Feature consistency between training and inference is required

---

## Future Improvements

- Latency benchmarking
- Multi-room scaling
- Distributed database support
- Advanced anomaly visualization

---

## License

MIT License

