import os

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
send_telemetry_sparql_log_no_bindings_new.py
- SPARQL via direct HTTP (requests), no rdflib/SPARQLStore
- Robust logging; uses brick:hasLocation
- Falls back cleanly if SPARQL fails
"""

import sys, time, json, random, sqlite3, signal
from datetime import datetime, timezone

import math  # for diurnal patterns

# ==== Telemetry realism params ====
TEMP_MIN, TEMP_MAX = 20.0, 27.0
HUM_MIN,  HUM_MAX  = 35.0, 65.0

TEMP_MEAN = 23.0
TEMP_DIURNAL_AMPL = 2.0

HUM_MEAN = 45.0
HUM_DIURNAL_AMPL = 5.0

TEMP_STEP_STD = 0.2
HUM_STEP_STD  = 0.7

_prev_temp = None
_prev_hum  = None


# ==== Static config ====
USE_SPARQL   = True
GRAPHDB_URL = os.environ.get("GRAPHDB_URL", "http://graphdb:7200").rstrip("/")
BRICK_SPARQL = f"{GRAPHDB_URL}/repositories/brick"  # GraphDB query endpoint
BRICK_USER   = ""   # leave empty if GraphDB allows anonymous read
BRICK_PASS   = ""
BRICK_BASE   = "http://example.com/building#"

DB_PATH      = os.environ.get("DB_PATH", "/data/telemetry.db")
TABLE        = "telemetry"

DEVICE_ID    = "virtual_temp_sensor_1"
ROOM_DEFAULT = "Room1"

# ==== SPARQL helpers ====
_brick_cache = {}

def _sparql_escape_string(s: str) -> str:
    return s.replace('\\', '\\\\').replace('"', '\\"')

def resolve_brick_from_sparql(device_id: str):
    """Resolve (brick_uri, room_uri) from GraphDB.
    First tries direct sensor -> room via brick:hasLocation (q2).
    Then tries timeseriesId path (q1). Returns (None, None) on failure.
    """
    if not USE_SPARQL:
        return (None, None)

    if device_id in _brick_cache:
        return _brick_cache[device_id]

    import requests

    endpoint   = BRICK_SPARQL
    dev_lit    = _sparql_escape_string(device_id)
    sensor_uri = f"{BRICK_BASE}{device_id}"

    # q2: direct sensor URI → room (validated via curl)
    q2 = f"""
    PREFIX brick: <https://brickschema.org/schema/Brick#>
    SELECT ?room WHERE {{
      VALUES ?sensor {{ <{sensor_uri}> }}
      ?sensor brick:hasLocation ?room .
    }} LIMIT 1
    """

    # q1: via timeseries (optional; depends on TTL)
    q1 = f"""
    PREFIX brick: <https://brickschema.org/schema/Brick#>
    SELECT ?sensor ?room WHERE {{
      ?ts a brick:Timeseries ;
          brick:timeseriesId "{dev_lit}" .
      ?sensor brick:hasTimeseries ?ts ;
              brick:hasLocation ?room .
    }} LIMIT 1
    """

    headers = {"Accept": "application/sparql-results+json"}
    auth = (BRICK_USER, BRICK_PASS) if BRICK_USER else None

    def run(query: str):
        r = requests.get(endpoint, params={"query": query}, headers=headers, auth=auth, timeout=10)
        try:
            r.raise_for_status()
            return r.json()
        except Exception as e:
            # Print full response body for diagnosis
            print("[SPARQL][DEBUG] status:", getattr(r, "status_code", "no resp"), file=sys.stderr)
            print("[SPARQL][DEBUG] body:\n" + getattr(r, "text", ""), file=sys.stderr)
            raise

    # Prefer q2 (you confirmed hasLocation works)
    try:
        print(f"[SPARQL] connecting to {endpoint}")
        data = run(q2)
        rows = data.get("results", {}).get("bindings", [])
        if rows:
            room_uri = rows[0]["room"]["value"]
            val = (sensor_uri, room_uri)
            _brick_cache[device_id] = val
            print(f"[SPARQL] resolved (q2): {val}")
            return val
        else:
            print("[SPARQL] q2 returned 0 rows; trying q1…", file=sys.stderr)
    except Exception as e:
        print(f"[SPARQL][q2-error] {e}", file=sys.stderr)

    # Try q1 (may be empty if no timeseries modeled in TTL)
    try:
        data = run(q1)
        rows = data.get("results", {}).get("bindings", [])
        if rows:
            room_uri  = rows[0]["room"]["value"]
            sensor_v  = rows[0].get("sensor", {}).get("value", sensor_uri)
            val = (sensor_v, room_uri)
            _brick_cache[device_id] = val
            print(f"[SPARQL] resolved (q1): {val}")
            return val
        else:
            print("[SPARQL] q1 returned 0 rows", file=sys.stderr)
    except Exception as e:
        print(f"[SPARQL][q1-error] {e}", file=sys.stderr)

    return (None, None)

# ==== SQLite ====
def ensure_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        brick_uri TEXT,
        device_id TEXT,
        temperature REAL,
        humidity REAL,
        timestamp TEXT,
        room TEXT
    );
    """)
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()

def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    ensure_db(conn)
    return conn

def insert_telemetry(conn, brick_uri, device_id, temp, hum, ts_iso, room):
    conn.execute(
        f"INSERT INTO {TABLE} (brick_uri, device_id, temperature, humidity, timestamp, room) VALUES (?, ?, ?, ?, ?, ?)",
        (brick_uri, device_id, float(temp), float(hum), ts_iso, room),
    )
    conn.commit()

def now_iso_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_brick_and_room(device_id: str):
    b_uri, r_uri = resolve_brick_from_sparql(device_id)
    if b_uri and r_uri:
        print("[SPARQL] resolved from GraphDB:", b_uri, "->", r_uri)
        room = r_uri.split("#")[-1] if "#" in r_uri else r_uri.rsplit("/", 1)[-1]
        return b_uri, room
    print("[FALLBACK] SPARQL unavailable; using BRICK_BASE + default room")
    return f"{BRICK_BASE}{device_id}", ROOM_DEFAULT


def generate_reading():
    """
    Daha gerçekçi sıcaklık & nem üretimi:
    - Gün içi (diurnal) pattern
    - Bir önceki değere göre küçük değişimler (smooth time series)
    - Sıcaklık ve nem arasında hafif korelasyon
    - Ara sıra sentetik anomaly (spike vs)
    """
    global _prev_temp, _prev_hum

    now = datetime.now(timezone.utc)
    hour = now.hour + now.minute / 60.0

    # 1) Gün içi baz sıcaklık ve nem
    base_temp = TEMP_MEAN + TEMP_DIURNAL_AMPL * math.sin(2 * math.pi * (hour - 14) / 24.0)
    base_hum  = HUM_MEAN  - HUM_DIURNAL_AMPL * math.sin(2 * math.pi * (hour - 14) / 24.0)

    if _prev_temp is None:
        _prev_temp = base_temp
    if _prev_hum is None:
        _prev_hum = base_hum

    # 2) Bir önceki değere göre küçük oynama
    temp = _prev_temp + random.gauss(0.0, TEMP_STEP_STD)
    hum  = _prev_hum  + random.gauss(0.0, HUM_STEP_STD)

    # 3) Hafif korelasyon: sıcaklık yükseldikçe nem biraz azalsın
    temp_delta = temp - TEMP_MEAN
    hum -= 0.3 * temp_delta

    # 4) Fiziksel sınırlar
    temp = max(TEMP_MIN, min(TEMP_MAX, temp))
    hum  = max(HUM_MIN,  min(HUM_MAX,  hum))

    # 5) Ara sıra sentetik anomaly enjekte et
    r = random.random()
    anomaly_type = None

    if r < 0.005:
        temp += random.uniform(3.0, 6.0)
        anomaly_type = "temp_spike"
    elif r < 0.008:
        hum += random.uniform(10.0, 15.0)
        anomaly_type = "hum_spike"
    elif r < 0.010:
        temp = _prev_temp
        hum  = _prev_hum
        anomaly_type = "sensor_stuck"

    temp = max(TEMP_MIN - 2.0, min(TEMP_MAX + 6.0, temp))
    hum  = max(HUM_MIN,       min(HUM_MAX + 20.0, hum))

    _prev_temp = temp
    _prev_hum  = hum

    if anomaly_type:
        print(f"[SIM_ANOMALY] type={anomaly_type} temp={temp:.2f} hum={hum:.2f}", file=sys.stderr)

    temp = round(float(temp), 2)
    hum  = round(float(hum), 2)
    return temp, hum


stop = False
def _sigint(_a, _b):
    global stop
    stop = True

def main():
    print("🧱 Logging Brick telemetry to SQLite (SPARQL-enabled). Ctrl+C to stop.")
    signal.signal(signal.SIGINT, _sigint)
    conn = open_db(DB_PATH)
    brick_uri, room = get_brick_and_room(DEVICE_ID)

    while not stop:
        ts = now_iso_utc()
        temp, hum = generate_reading()
        payload = {
            "brick_uri": brick_uri,
            "device_id": DEVICE_ID,
            "temperature": temp,
            "humidity": hum,
            "timestamp": ts
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        try:
            insert_telemetry(conn, brick_uri, DEVICE_ID, temp, hum, ts, room)
        except Exception as e:
            print(f"DB write error: {e}", file=sys.stderr)
        time.sleep(10)
    print("🛑 Telemetry stopped.")
    conn.close()

if __name__ == "__main__":
    main()
