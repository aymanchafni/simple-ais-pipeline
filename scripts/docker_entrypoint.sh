#!/bin/bash
# One-shot pipeline run used by the `data-init` service.
# PIPELINE_ARGS defaults to the bundled sample; override it to load NOAA data, e.g.
#   PIPELINE_ARGS="--noaa-date 2023-01-01 --max-records 200000"
set -euo pipefail

echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" >/dev/null 2>&1; do
  sleep 2
done

echo "Running pipeline with: ${PIPELINE_ARGS}"
# shellcheck disable=SC2086
python scripts/run_pipeline.py ${PIPELINE_ARGS}
