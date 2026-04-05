#!/usr/bin/env python3
import os
import argparse
import sqlite3
import joblib
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta

from pdm_features import make_features

ANOMALY_DB_PATH = "/data/anomalies.db"


def to_iso_z(ts_val) -> str:
    if isinstance(ts_val, pd.Timestamp):
        if ts_val.tzinfo is None:
            ts_val = ts_val.tz_localize("UTC")
        else:
            ts_val = ts_val.tz_convert("UTC")
        return ts_val.strftime("%Y-%m-%dT%H:%M:%SZ")
    s = str(ts_val)
    try:
        dt = pd.to_datetime(s, utc=True, errors="raise")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return s


def load_models(models_dir: str):
    models = {}
    for name in os.listdir(models_dir):
        if not name.endswith(".joblib"):
            continue
        room = name.replace("iforest_", "").replace(".joblib", "")
        models[room] = joblib.load(os.path.join(models_dir, name))
    if not models:
        raise SystemExit(f"No models found in {models_dir}")
    return models


def resolve_model_and_feature_cols(obj):
    if isinstance(obj, dict) and "model" in obj and "feature_cols" in obj:
        return obj["model"], list(obj["feature_cols"])
    raise SystemExit("Model artifact must be dict with keys 'model' and 'feature_cols'")


def load_since(sqlite_path: str, table: str, since_iso: str) -> pd.DataFrame:
    q = f"""
    SELECT id, brick_uri, device_id, temperature, humidity, timestamp, room
    FROM {table}
    WHERE timestamp >= ?
    ORDER BY timestamp ASC
    """
    con = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True, timeout=30.0)
    try:
        return pd.read_sql_query(q, con, params=(since_iso,))
    finally:
        con.close()


def ensure_anomaly_table():
    con = sqlite3.connect(ANOMALY_DB_PATH, timeout=30.0, isolation_level=None)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA busy_timeout=30000;")
        con.execute("""
        CREATE TABLE IF NOT EXISTS anomalies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            room TEXT NOT NULL,
            score REAL NOT NULL,
            is_anomaly INTEGER NOT NULL,
            details TEXT,
            ts_epoch INTEGER
        );
        """)
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_anom_room_ts_epoch ON anomalies(room, ts_epoch);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_anom_ts_epoch ON anomalies(ts_epoch);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_anom_room_ts_epoch ON anomalies(room, ts_epoch DESC);")
    finally:
        con.close()


def write_anomalies(records):
    if not records:
        return
    con = sqlite3.connect(ANOMALY_DB_PATH, timeout=30.0, isolation_level=None)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA busy_timeout=30000;")
        sql = """
        INSERT OR IGNORE INTO anomalies
        (timestamp, room, score, is_anomaly, details, ts_epoch)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        con.executemany(sql, records)
    finally:
        con.close()


def consecutive_confirm(flags: np.ndarray, min_consecutive: int) -> np.ndarray:
    if min_consecutive <= 1:
        return flags.astype(int)
    out = np.zeros_like(flags, dtype=int)
    run = 0
    for i, v in enumerate(flags):
        if v:
            run += 1
        else:
            if run >= min_consecutive:
                out[i-run:i] = 1
            run = 0
    if run >= min_consecutive:
        out[len(flags)-run:] = 1
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--telemetry-db", default="/root/telemetry.db")
    ap.add_argument("--table", default="telemetry")
    ap.add_argument("--models-dir", default="/root/models")
    ap.add_argument("--room-col", default="room")

    ap.add_argument("--feature-lookback-sec", type=int, default=21600)  # 6h
    ap.add_argument("--emit-window-sec", type=int, default=600)         # 10m

    ap.add_argument("--roll-n", type=int, default=12)
    ap.add_argument("--min-consecutive", type=int, default=5)
    ap.add_argument("--score-threshold", type=float, default=None)
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    feature_cutoff = (now - timedelta(seconds=int(args.feature_lookback_sec))).strftime("%Y-%m-%dT%H:%M:%SZ")
    emit_cutoff    = (now - timedelta(seconds=int(args.emit_window_sec))).strftime("%Y-%m-%dT%H:%M:%SZ")

    models = load_models(args.models_dir)

    raw = load_since(args.telemetry_db, args.table, feature_cutoff)
    if raw.empty:
        print("No telemetry rows in feature lookback.")
        return

    feats, _ = make_features(raw, room_col=args.room_col, numeric_cols=["temperature", "humidity"], roll_window=args.roll_n)

    ts = pd.to_datetime(feats["timestamp"], utc=True, errors="coerce")
    feats["ts_epoch"] = (ts.astype("int64") // 10**9)
    feats = feats.dropna(subset=["ts_epoch"]).copy()
    feats["ts_epoch"] = feats["ts_epoch"].astype(int)

    ensure_anomaly_table()

    total_emit_rows = 0
    total_raw = 0
    total_conf = 0
    total_written = 0

    for room, obj in models.items():
        model, feat_cols = resolve_model_and_feature_cols(obj)

        g = feats[(feats[args.room_col] == room) & (feats["timestamp"] >= emit_cutoff)].copy()
        if g.empty:
            continue

        missing = [c for c in feat_cols if c not in g.columns]
        if missing:
            raise SystemExit(f"Feature mismatch for room={room}. Missing: {missing[:10]} (total {len(missing)})")

        X = g[feat_cols].fillna(0.0).to_numpy()
        preds = model.predict(X)                 # -1 anomaly
        scores = model.decision_function(X)      # lower => more anomalous

        raw_flags = (preds == -1).astype(int)
        if args.score_threshold is not None:
            raw_flags = (raw_flags & (scores < args.score_threshold)).astype(int)

        conf_flags = consecutive_confirm(raw_flags, args.min_consecutive)

        total_emit_rows += len(g)
        total_raw += int(raw_flags.sum())
        total_conf += int(conf_flags.sum())

        details = f"lookback={args.feature_lookback_sec};emit={args.emit_window_sec};roll_n={args.roll_n};min_consecutive={args.min_consecutive};score_thr={args.score_threshold}"

        records = []
        for i, flag in enumerate(conf_flags):
            if not flag:
                continue
            records.append((
                to_iso_z(g.iloc[i]['timestamp']),
                room,
                float(scores[i]),
                1,
                details,
                int(g.iloc[i]["ts_epoch"]),
            ))

        write_anomalies(records)
        total_written += len(records)

    print(f"Processed {total_emit_rows} feature rows in last {args.emit_window_sec}s (features from {args.feature_lookback_sec}s lookback).")
    print(f"Model flagged {total_raw} anomalies (raw).")
    print(f"Confirmed {total_conf} anomalies.")
    print(f"Wrote {total_written} anomaly rows to anomalies.db.")


if __name__ == "__main__":
    main()
