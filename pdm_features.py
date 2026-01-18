# pdm_features.py
# Rolling feature engineering helpers for IoT telemetry (temperature/humidity)

from __future__ import annotations
import pandas as pd
import numpy as np

ROLL_WINDOW = 12   # default: 12 samples (~ if 5 min sampling => 1 hour window)

def infer_time_col(df: pd.DataFrame) -> str:
    for c in df.columns:
        if "time" in c.lower() or "ts" == c.lower():
            return c
    return "timestamp"

def basic_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    ts_col = infer_time_col(df)
    if not pd.api.types.is_datetime64_any_dtype(df[ts_col]):
        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=False)
    df = df.dropna(subset=[ts_col]).sort_values(ts_col)
    return df

def make_features(df: pd.DataFrame, room_col: str = "room",
                  numeric_cols: list[str] | None = None,
                  roll_window: int = ROLL_WINDOW) -> pd.DataFrame:
    """Create per-room rolling features (mean, std, diff, zscores)."""
    df = basic_clean(df)
    if numeric_cols is None:
        numeric_cols = [c for c in df.columns if c not in [room_col, infer_time_col(df)] and pd.api.types.is_numeric_dtype(df[c])]
    if not numeric_cols:
        raise ValueError("No numeric columns found to featurize. Provide numeric_cols explicitly.")
    ts_col = infer_time_col(df)

    feats = []
    for room, g in df.groupby(room_col):
        g = g.sort_values(ts_col).copy()
        for col in numeric_cols:
            g[f"{col}_roll_mean"] = g[col].rolling(roll_window, min_periods=max(3, roll_window//3)).mean()
            g[f"{col}_roll_std"]  = g[col].rolling(roll_window, min_periods=max(3, roll_window//3)).std()
            g[f"{col}_diff"]      = g[col].diff()
            g[f"{col}_z"] = (g[col] - g[f"{col}_roll_mean"]) / (g[f"{col}_roll_std"].replace(0, np.nan))
        g["room"] = room
        feats.append(g)
    out = pd.concat(feats, ignore_index=True)
    out = out.dropna(subset=[f"{numeric_cols[0]}_roll_mean"])
    return out, numeric_cols
