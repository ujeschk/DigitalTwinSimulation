# pdm_features.py
from __future__ import annotations
import pandas as pd
import numpy as np

ROLL_WINDOW = 12

def infer_time_col(df: pd.DataFrame) -> str:
    for c in df.columns:
        if "time" in c.lower() or c.lower() in ("ts", "ts_epoch"):
            return c
    return "timestamp"

def basic_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    ts_col = infer_time_col(df)
    if not pd.api.types.is_datetime64_any_dtype(df[ts_col]):
        # IMPORTANT: your DB timestamps are Z-ISO; force utc=True for consistency
        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df = df.dropna(subset=[ts_col]).sort_values(ts_col)
    return df

def make_features(
    df: pd.DataFrame,
    room_col: str = "room",
    numeric_cols: list[str] | None = None,
    roll_window: int = ROLL_WINDOW,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Per-room rolling features:
      - roll_mean, roll_std (with stable min_periods)
      - diff
      - z = (x-mean)/std

    Returns:
      (features_df, used_numeric_cols)
    """
    df = basic_clean(df)
    ts_col = infer_time_col(df)

    if numeric_cols is None:
        numeric_cols = [
            c for c in df.columns
            if c not in (room_col, ts_col)
            and pd.api.types.is_numeric_dtype(df[c])
        ]
    if not numeric_cols:
        raise ValueError("No numeric columns found; provide numeric_cols explicitly.")

    # stable min_periods: allow earlier rows but avoid tiny-window std explosion
    minp = max(10, roll_window // 2)  # <-- key change (was roll_window//3 with floor 3)
    # With roll_window=288 => minp=144. Still stable, and less jittery.
    # If you want earlier availability, you can set minp=max(20, roll_window//4).

    feats = []
    for room, g in df.groupby(room_col, sort=False):
        g = g.sort_values(ts_col).copy()

        for col in numeric_cols:
            r = g[col].rolling(roll_window, min_periods=minp)
            g[f"{col}_roll_mean"] = r.mean()
            g[f"{col}_roll_std"]  = r.std(ddof=0)

            g[f"{col}_diff"] = g[col].diff()

            std = g[f"{col}_roll_std"].replace(0, np.nan)
            g[f"{col}_z"] = (g[col] - g[f"{col}_roll_mean"]) / std

        g[room_col] = room
        feats.append(g)

    out = pd.concat(feats, ignore_index=True)

    # Drop rows where any roll stats are missing (consistency!)
    required = []
    for col in numeric_cols:
        required += [f"{col}_roll_mean", f"{col}_roll_std", f"{col}_z", f"{col}_diff"]
    out = out.dropna(subset=required)

    return out, numeric_cols
