#!/bin/bash
set -euo pipefail

# Moving paused DBs into backlog upload queue

BLOB_SENDER_DIR="${BLOB_SENDER_DIR:-/home/gholi/blob_sender}"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"

ACTIVE_DB="${ACTIVE_DB:-/opt/Pilot_Deployment/daq_data/audio_log.db}"
PENDING_DIR="${PENDING_DIR:-/opt/Pilot_Deployment/daq_data/backlog/pending}"
EMPTY_DIR="${EMPTY_DIR:-/opt/Pilot_Deployment/daq_data/backlog/empty}"
SEQUENCE_FILE="${SEQUENCE_FILE:-/opt/Pilot_Deployment/daq_data/backlog/session_sequence.txt}"
WAIT_UNTIL_STABLE="${WAIT_UNTIL_STABLE:-3}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-60}"

cd "$BLOB_SENDER_DIR"

exec "$PYTHON_BIN" tools/seal_active_daq_db_to_backlog.py \
  --active-db "$ACTIVE_DB" \
  --pending-dir "$PENDING_DIR" \
  --empty-dir "$EMPTY_DIR" \
  --sequence-file "$SEQUENCE_FILE" \
  --wait-until-stable "$WAIT_UNTIL_STABLE" \
  --wait-timeout "$WAIT_TIMEOUT"
