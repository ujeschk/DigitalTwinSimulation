#!/usr/bin/env bash
set -euo pipefail
PDM_PY=${PDM_PY:-/root/azure-iot-env/bin/python}
PDM_DIR=${PDM_DIR:-/root}

"${PDM_PY}" "${PDM_DIR}/pdm_train.py" \
  --sqlite /data/telemetry.db \
  --table telemetry \
  --room-col room \
  --numeric-cols temperature humidity \
  --out /data/models \
  --contamination 0.02 \
  --train-days 180 \
  --roll-n 144 \
