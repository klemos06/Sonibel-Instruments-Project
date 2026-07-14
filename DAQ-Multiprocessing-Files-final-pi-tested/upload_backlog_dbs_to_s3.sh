#!/bin/bash
set -euo pipefail

BLOB_SENDER_DIR="${BLOB_SENDER_DIR:-/home/gholi/blob_sender}"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
export PYTHONUNBUFFERED=1

PENDING_DIR="${PENDING_DIR:-/opt/Pilot_Deployment/daq_data/backlog/pending}"
UPLOADED_DIR="${UPLOADED_DIR:-/opt/Pilot_Deployment/daq_data/backlog/uploaded}"
FAILED_DIR="${FAILED_DIR:-/opt/Pilot_Deployment/daq_data/backlog/failed}"
STATE_DIR="${STATE_DIR:-/opt/Pilot_Deployment/daq_data/backlog/upload_state}"
LOCK_FILE="${LOCK_FILE:-/opt/Pilot_Deployment/daq_data/backlog/upload.lock}"

S3_BUCKET="${S3_BUCKET:-sonibel-testing}"
S3_PREFIX="${S3_PREFIX:-daq-unmodified-backlog-uploads}"
TIME_BUDGET_MIN="${TIME_BUDGET_MIN:-30}"
MAX_CHUNK_MB="${MAX_CHUNK_MB:-64}"
STOP_MARGIN_SECONDS="${STOP_MARGIN_SECONDS:-120}"
FOLLOW="${FOLLOW:-0}"
CPU_CORE="${CPU_CORE:-}"

cd "$BLOB_SENDER_DIR"

UPLOAD_ARGS=(
  --pending-dir "$PENDING_DIR"
  --uploaded-dir "$UPLOADED_DIR"
  --failed-dir "$FAILED_DIR"
  --state-dir "$STATE_DIR"
  --lock-file "$LOCK_FILE"
  --bucket "$S3_BUCKET"
  --prefix "$S3_PREFIX"
  --time-budget-min "$TIME_BUDGET_MIN"
  --max-chunk-mb "$MAX_CHUNK_MB"
  --stop-margin-seconds "$STOP_MARGIN_SECONDS"
)

if [[ "$FOLLOW" == "1" ]]; then
  UPLOAD_ARGS+=(--follow)
fi

if command -v taskset >/dev/null 2>&1 && [[ -n "$CPU_CORE" ]]; then
  exec taskset -c "$CPU_CORE" "$PYTHON_BIN" tools/upload_backlog_dbs_to_s3.py "${UPLOAD_ARGS[@]}"
fi

exec "$PYTHON_BIN" tools/upload_backlog_dbs_to_s3.py "${UPLOAD_ARGS[@]}"
