#!/usr/bin/env bash
set -euo pipefail

# === Config (override via env or flags) ===
DB_PATH="${DB_PATH:-/root/telemetry.db}"
TABLE="${TABLE:-telemetry}"
ROOM_COL="${ROOM_COL:-room}"
NUMERIC_COLS_DEFAULT="temperature humidity"
NUMERIC_COLS="${NUMERIC_COLS:-$NUMERIC_COLS_DEFAULT}"
MODELS_DIR="${MODELS_DIR:-/root/models}"
CONTAMINATION="${CONTAMINATION:-0.02}"
PDM_DIR="${PDM_DIR:-/root}"  # where pdm_*.py live
ANOMALY_DB="${ANOMALY_DB:-/root/anomalies.db}"

# === Flags ===
while [[ $# -gt 0 ]]; do
  case "$1" in
    --db) DB_PATH="$2"; shift 2;;
    --table) TABLE="$2"; shift 2;;
    --room-col) ROOM_COL="$2"; shift 2;;
    --numeric-cols) NUMERIC_COLS="$2"; shift 2;;
    --models-dir) MODELS_DIR="$2"; shift 2;;
    --contamination) CONTAMINATION="$2"; shift 2;;
    --pdm-dir) PDM_DIR="$2"; shift 2;;
    --anomaly-db) ANOMALY_DB="$2"; shift 2;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

echo "== PdM run =="
echo "DB_PATH:        $DB_PATH"
echo "TABLE:          $TABLE"
echo "ROOM_COL:       $ROOM_COL"
echo "NUMERIC_COLS:   $NUMERIC_COLS"
echo "MODELS_DIR:     $MODELS_DIR"
echo "CONTAMINATION:  $CONTAMINATION"
echo "PDM_DIR:        $PDM_DIR"
echo "ANOMALY_DB:     $ANOMALY_DB"
echo

# === Checks ===
if [[ ! -f "$DB_PATH" ]]; then
  echo "ERROR: DB not found at $DB_PATH"
  exit 2
fi

# Ensure required packages (best-effort)
if ! ${PDM_PY:-python} -c "import sklearn, pandas, numpy, joblib" >/dev/null 2>&1; then
  echo "Installing required Python packages..."
  pip install --quiet --upgrade scikit-learn pandas numpy joblib
fi

# Ensure scripts exist
for f in pdm_train.py pdm_infer.py pdm_features.py; do
  if [[ ! -f "$PDM_DIR/$f" ]]; then
    echo "ERROR: Missing $PDM_DIR/$f"
    exit 3
  fi
done

# Quick schema sanity
echo "Checking telemetry schema..."
sqlite3 "$DB_PATH" "SELECT name FROM sqlite_master WHERE type='table' AND name='$TABLE';" | grep -q "$TABLE" || {
  echo "ERROR: Table '$TABLE' not found in $DB_PATH"
  exit 4
}

echo "Sample rows:"
sqlite3 -csv "$DB_PATH" "SELECT timestamp, $ROOM_COL, * FROM $TABLE ORDER BY datetime(timestamp) DESC LIMIT 3;" || true
echo

# === Train ===
echo "Training IsolationForest models..."
${PDM_PY:-python} "$PDM_DIR/pdm_train.py" \
  --sqlite "$DB_PATH" \
  --table "$TABLE" \
  --room-col "$ROOM_COL" \
  --numeric-cols $NUMERIC_COLS \
  --out "$MODELS_DIR" \
  --contamination "$CONTAMINATION"

# === Inference ===
echo "Running inference & writing anomalies..."
${PDM_PY:-python} "$PDM_DIR/pdm_infer.py" \
  --sqlite "$DB_PATH" \
  --table "$TABLE" \
  --room-col "$ROOM_COL" \
  --numeric-cols $NUMERIC_COLS \
  --models-dir "$MODELS_DIR"

# === Report ===
echo
echo "Last 10 anomalies:"
sqlite3 -header -column "$ANOMALY_DB" \
  "SELECT timestamp, room, score FROM anomalies WHERE is_anomaly=1 ORDER BY datetime(timestamp) DESC LIMIT 10;" \
  || true

echo
echo "Done."
