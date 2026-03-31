# Digital Twin–Based Predictive Maintenance (DT-PdM)

This repository provides a reproducible research prototype for a **Digital Twin–enabled Predictive Maintenance system for smart buildings**.

The system integrates:
- IoT telemetry simulation
- Brick ontology (GraphDB)
- SQLite-based time-series storage
- Flask REST API
- IFC.js-based 3D building viewer
- Machine-learning–based anomaly detection (offline)

---

## Architecture Overview

Components:
- **Telemetry service**: generates synthetic temperature & humidity data and stores it in SQLite
- **GraphDB**: hosts Brick ontology and semantic room–sensor relationships
- **API service**: exposes telemetry data via REST endpoints
- **Viewer**: browser-based IFC.js visualization
- **PdM scripts**: anomaly detection (currently executed externally / experimental)

---

## Quick Start (Docker)

### Prerequisites
- Docker
- Docker Compose

### Run
```bash
docker compose up -d --build

## IFC Model (Required for 3D Viewer)

The IFC building model file is not stored in the Git repository due to size.

To enable the 3D viewer:
1. Download `model.ifc` from the **GitHub Releases (v1.0)** page
2. Place it here: ifc-viewer/
3. Reload the Viewer: http://localhost:3000


