# pdm_train.py
# Train IsolationForest anomaly model(s) per room from SQLite telemetry

import argparse, os, sqlite3, joblib
import pandas as pd
from sklearn.ensemble import IsolationForest
from pdm_features import make_features, infer_time_col

def load_data(sqlite_path: str, table: str) -> pd.DataFrame:
    con = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True, timeout=10)
    try:
        return pd.read_sql_query(f"SELECT * FROM {table}", con)
    finally:
        con.close()

def train_per_room(df: pd.DataFrame, room_col="room", numeric_cols=None, contamination=0.02):
    feats, used_numeric = make_features(df, room_col=room_col, numeric_cols=numeric_cols)
    ts_col = infer_time_col(feats)

    models = {}
    feature_cols = [c for c in feats.columns if any(c.startswith(n) for n in used_numeric) and ("roll" in c or "diff" in c or c.endswith("_z"))]
    for room, g in feats.groupby(room_col):
        X = g[feature_cols].fillna(0.0).values
        if len(g) < 60:
            continue
        clf = IsolationForest(n_estimators=300, contamination=contamination, max_samples=0.2, random_state=42)
        clf.fit(X)
        models[room] = {
            "model": clf,
            "feature_cols": feature_cols
        }
    return models, feats

def save_models(models, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    for room, obj in models.items():
        path = os.path.join(out_dir, f"iforest_{room}.joblib")
        joblib.dump(obj, path)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sqlite", required=True)
    ap.add_argument("--table", default="telemetry")
    ap.add_argument("--room-col", default="room")
    ap.add_argument("--numeric-cols", nargs="*", default=None)
    ap.add_argument("--out", default="models")
    ap.add_argument("--contamination", type=float, default=0.02)
    args = ap.parse_args()

    df = load_data(args.sqlite, args.table)
    models, feats = train_per_room(df, room_col=args.room_col, numeric_cols=args.numeric_cols, contamination=args.contamination)
    save_models(models, args.out)

    print(f"Trained {len(models)} room models -> {os.path.abspath(args.out)}")
