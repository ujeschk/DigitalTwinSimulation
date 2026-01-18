import os

from flask import Flask, jsonify, request
from flask_cors import CORS
from functools import lru_cache
import sqlite3, json

TELEMETRY_DB = os.environ.get("TELEMETRY_DB", "/data/telemetry.db")
ANOMALY_DB   = os.environ.get("ANOMALY_DB", "/data/anomalies.db")
DT_MODEL_PATH = "/root/ifc-viewernew/digital_twin_model.json"

app = Flask(__name__)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False
CORS(app, resources={r"/api/*": {"origins": "*"}})  # 3000 -> 5000

# ---- HTTP keep-alive kapat: her yanıt bağlantıyı kapatsın (pending/CLOSE_WAIT olmasın)
@app.after_request
def force_close(resp):
    resp.headers["Connection"] = "close"
    return resp

# ---- Yardımcı: istek başına, read-only SQLite bağlantısı ----
def ro_connect(db_path: str, timeout: int = 10) -> sqlite3.Connection:
    conn = sqlite3.connect(
        f"file:{db_path}?mode=ro&cache=shared",
        uri=True,
        timeout=timeout,
        isolation_level=None,         # autocommit
        check_same_thread=False,      # threaded server güvenliği
    )
    try:
        conn.execute("PRAGMA query_only=ON;")
        conn.execute("PRAGMA busy_timeout=10000;")
    except Exception:
        pass
    conn.row_factory = sqlite3.Row
    return conn

@lru_cache(maxsize=1)
def load_dt_model():
    with open(DT_MODEL_PATH, "r") as f:
        return json.load(f)

# ========== HEALTH ==========
@app.get("/api/health")
def api_health():
    try:
        conn = ro_connect(ANOMALY_DB)
        conn.execute("SELECT 1;").fetchone()
        conn.close()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        try: conn.close()
        except Exception: pass
        return jsonify({"status": "error", "error": str(e)}), 500

# ========== TELEMETRY ==========
@app.get("/api/telemetry")
def api_telemetry():
    """Son 1000 telemetri kaydı (eski davranışa uyumlu)."""
    conn = ro_connect(TELEMETRY_DB)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT room, timestamp, temperature, humidity
            FROM telemetry
            ORDER BY rowid DESC
            LIMIT 1000
        """)
        rows = cur.fetchall()
        out = [{
            "room": r["room"],
            "timestamp": r["timestamp"],
            "temperature": r["temperature"],
            "humidity": r["humidity"],
        } for r in rows]
        return jsonify(out)
    finally:
        conn.close()

@app.get("/api/telemetry-guid")
def api_telemetry_guid():
    """Digital twin bilgileriyle zenginleşmiş son 1000 kayıt."""
    dt_model = load_dt_model()  # diskten bir kez
    conn = ro_connect(TELEMETRY_DB)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT room, timestamp, temperature, humidity
            FROM telemetry
            ORDER BY rowid DESC
            LIMIT 1000
        """)
        rows = cur.fetchall()
        out = []
        for r in rows:
            out.append({
                "room": r["room"],
                "timestamp": r["timestamp"],
                "temperature": r["temperature"],
                "humidity": r["humidity"],
                "room_uri":   dt_model["brick"]["room_uri"],
                "sensor_uri": dt_model["brick"]["sensor_uri"],
                "timeseries": dt_model["brick"]["timeseries"],
                "storey_guid": dt_model["bim"]["storey_guid"],
                "storey_name": dt_model["bim"]["storey_name"],
                "x": dt_model["location_hint"]["x"],
                "y": dt_model["location_hint"]["y"],
                "z": dt_model["location_hint"]["z"],
            })
        return jsonify(out)
    finally:
        conn.close()

# ========== ANOMALIES ==========
@app.get("/api/anomalies")
def api_anomalies():
    """
    Frontend uyumu: DİZİ döner.
    Opsiyonel query params:
      window (saniye) → verilirse ts_epoch ile filtre (indeksli).
      limit  → default 500, max 5000
      room   → oda filtresi
      only_anomalies → '1'/'true' ise is_anomaly=1
    """
    def as_int(val, default, min_v=None, max_v=None):
        try: x = int(val)
        except (TypeError, ValueError): return default
        if min_v is not None and x < min_v: x = min_v
        if max_v is not None and x > max_v: x = max_v
        return x

    window = request.args.get("window")      # None ise penceresiz, LIMIT ile
    limit  = as_int(request.args.get("limit"), 500, 1, 5000)
    room   = request.args.get("room")
    only_a = str(request.args.get("only_anomalies", "")).lower() in ("1", "true")

    conn = ro_connect(ANOMALY_DB)
    try:
        cur = conn.cursor()
        sql = ["SELECT timestamp, room, score, is_anomaly FROM anomalies"]
        params = []

        if window is not None:
            sql.append("WHERE ts_epoch >= strftime('%s','now') - ?")
            params.append(as_int(window, 3600, 60, 86400*7))

        if room:
            sql.append("AND" if params else "WHERE"); sql.append("room = ?")
            params.append(room)

        if only_a:
            sql.append("AND" if params else "WHERE"); sql.append("is_anomaly = 1")

        # ÖNEMLİ: saf indeksli sıralama (ts_epoch NOT NULL olduğu için güvenli)
        sql.append("ORDER BY ts_epoch DESC")
        sql.append("LIMIT ?"); params.append(limit)

        q = " ".join(sql)
        cur.execute(q, params)
        rows = [dict(r) for r in cur.fetchall()]
        return jsonify(rows)
    finally:
        conn.close()

# ========== MAIN ==========
if __name__ == "__main__":
    # Çoklu istek desteği + keep-alive yok → uzun okuma birikmez
    app.run(host="0.0.0.0", port=5000, threaded=True)
