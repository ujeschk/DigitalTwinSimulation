#!/usr/bin/env bash
echo "[run_pdm_infer] running $(date -u +%FT%TZ)" >&2
set -euo pipefail
PDM_PY=${PDM_PY:-/root/azure-iot-env/bin/python}
PDM_DIR=${PDM_DIR:-/root}

"${PDM_PY}" "${PDM_DIR}/pdm_infer.py" \
  --telemetry-db /data/telemetry.db \
  --table telemetry \
  --room-col room \
  --models-dir /data/models \
  --feature-lookback-sec 21600 \
  --emit-window-sec 600 \
  --roll-n 144 \
  --min-consecutive 1 \
  --score-threshold -0.01
