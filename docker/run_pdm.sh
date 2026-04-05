#!/bin/bash

echo "Starting PDM pipeline..."

# İlk model yoksa train et
if [ ! -f /data/models/iforest_Room1.joblib ]; then
	echo "No model found → training..."
	python /app/pdm/pdm_train.py \
	--sqlite /data/telemetry.db
fi

LAST_TRAIN=0

while true; do
	NOW=$(date +%s)

	# INFER (her 60 sn)
	echo "Running inference..."
	python /app/pdm/pdm_infer.py \
	--telemetry-db /data/telemetry.db \
	--models-dir /data/models \
	--emit-window-sec 600 \
	--feature-lookback-sec 21600 \
	--roll-n 144 \
	--min-consecutive 1

	# TRAIN (6 saatte 1)
	if (( NOW - LAST_TRAIN > 21600 )); then
		echo "Running training..."
		python /app/pdm/pdm_train.py \
		--sqlite /data/telemetry.db \
		--numeric-cols temperature humidity \
		--out /data/models
		LAST_TRAIN=$NOW
	fi

	sleep 60
done
