#!/usr/bin/env python3
import os
import argparse
import sqlite3
import joblib
import numpy as np
import pandas as pd
import time

ANOMALY_DB_PATH = "/root/anomalies.db"
DEFAULT_WINDOW_SEC = 120


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


def load_data(sqlite_path: str, table: str, window_sec: int):
    cutoff = int(time.time()) - int(window_sec)

    q = f"""
    SELECT *
    FROM {table}
    WHERE CAST(
        strftime('%s', replace(replace(timestamp,'T',' '),'Z',''))
        AS INTEGER
    ) >= ?
    """

    con = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True, timeout=15.0)
    try:
        return pd.read_sql_query(q, con, params=(cutoff,))
    finally:
        con.close()


def ensure_anomaly_table():
    con = sqlite3.connect(ANOMALY_DB_PATH, timeout=30.0, isolation_level=None)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA busy_timeout=30000;")
        con.execute("PRAGMA wal_autocheckpoint=1000;")
        con.execute("PRAGMA journal_size_limit=67108864;")

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

        con.execute("CREATE INDEX IF NOT EXISTS idx_anom_ts_epoch ON anomalies(ts_epoch);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_anom_room_ts_epoch ON anomalies(room, ts_epoch DESC);")
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_anom_room_ts_epoch ON anomalies(room, ts_epoch);")
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


def resolve_model_and_feature_cols(obj):
    if isinstance(obj, dict):
        return obj["model"], list(obj["feature_cols"])
    raise SystemExit("Model artifact must be dict with keys 'model' and 'feature_cols'")


def add_engineered_features(room_df, roll_n, z_clip):
    df = room_df.sort_values("ts_epoch").copy()

    t = df["temperature"].astype(float)
    h = df["humidity"].astype(float)

    df["temperature_roll_mean"] = t.rolling(roll_n, min_periods=2).mean().fillna(t)
    df["temperature_roll_std"]  = t.rolling(roll_n, min_periods=2).std(ddof=0).fillna(0.0)
    df["temperature_diff"]      = t.diff().fillna(0.0)

    df["humidity_roll_mean"] = h.rolling(roll_n, min_periods=2).mean().fillna(h)
    df["humidity_roll_std"]  = h.rolling(roll_n, min_periods=2).std(ddof=0).fillna(0.0)
    df["humidity_diff"]      = h.diff().fillna(0.0)

    eps = 1e-9
    df["temperature_z"] = ((t - df["temperature_roll_mean"]) /
                            (df["temperature_roll_std"] + eps)).clip(-z_clip, z_clip)
    df["humidity_z"] = ((h - df["humidity_roll_mean"]) /
                         (df["humidity_roll_std"] + eps)).clip(-z_clip, z_clip)

    return df


def consecutive_confirm(flags, min_consecutive):
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
    ap.add_argument("--window-sec", type=int, default=DEFAULT_WINDOW_SEC)
    ap.add_argument("--room-col", default="room")

    ap.add_argument("--roll-n", type=int, default=24)
    ap.add_argument("--z-clip", type=float, default=3.0)
    ap.add_argument("--min-consecutive", type=int, default=2)

    # ✅ NEW
    ap.add_argument("--score-threshold", type=float, default=None,
                    help="Only accept anomalies with decision_function < threshold")

    args = ap.parse_args()

    models = load_models(args.models_dir)
    df = load_data(args.telemetry_db, args.table, args.window_sec)

    if df.empty:
        print("No telemetry rows in window.")
        return

    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["ts_epoch"] = (ts.astype("int64") // 10**9)
    df = df.dropna(subset=["ts_epoch"])

    ensure_anomaly_table()

    total_rows = total_raw = total_conf = total_written = 0

    for room, obj in models.items():
        model, feat_cols = resolve_model_and_feature_cols(obj)
        room_df = df[df[args.room_col] == room].copy()
        if room_df.empty:
            continue

        room_df = add_engineered_features(room_df, args.roll_n, args.z_clip)
        X = room_df[feat_cols].to_numpy()

        preds  = model.predict(X)
        scores = model.decision_function(X)

        raw_flags = (preds == -1).astype(int)

        # ✅ SCORE FILTER
        if args.score_threshold is not None:
            raw_flags &= (scores < args.score_threshold)

        conf_flags = consecutive_confirm(raw_flags, args.min_consecutive)

        total_rows += len(room_df)
        total_raw  += raw_flags.sum()
        total_conf += conf_flags.sum()

        records = []
        for i, flag in enumerate(conf_flags):
            if flag:
                records.append((
                    room_df.iloc[i]["timestamp"],
                    room,
                    float(scores[i]),
                    1,
                    f"roll_n={args.roll_n};z_clip={args.z_clip};"
                    f"min_consecutive={args.min_consecutive};"
                    f"score_thr={args.score_threshold}",
                    int(room_df.iloc[i]["ts_epoch"])
                ))

        total_written += len(records)
        write_anomalies(records)

    print(f"Processed {total_rows} telemetry rows in last {args.window_sec}s.")
    print(f"Model flagged {total_raw} anomalies (raw).")
    print(f"Confirmed {total_conf} anomalies.")
    print(f"Wrote {total_written} anomaly rows to anomalies.db.")


if __name__ == "__main__":
    main()
