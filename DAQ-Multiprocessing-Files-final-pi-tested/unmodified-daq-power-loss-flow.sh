#!/bin/bash
set -euo pipefail
# Power loss flow for upload
DAQ_SERVICE="${DAQ_SERVICE:-nucorDAQ.service}"
BACKGROUND_UPLOAD_SERVICE="${BACKGROUND_UPLOAD_SERVICE:-unmodified-daq-backlog-uploader.service}"
BATTERY_UPLOAD_SERVICE="${BATTERY_UPLOAD_SERVICE:-unmodified-daq-battery-upload.service}"
SEAL_SCRIPT="${SEAL_SCRIPT:-/opt/Pilot_Deployment/seal_active_daq_db_to_backlog.sh}"
POWER_OFF_AFTER_UPLOAD="${POWER_OFF_AFTER_UPLOAD:-1}"

log() {
  printf '%s %s\n' "$(date -Is)" "$*"
}

log "Stopping background backlog uploader: $BACKGROUND_UPLOAD_SERVICE"
systemctl stop "$BACKGROUND_UPLOAD_SERVICE" || true

if [[ -n "$DAQ_SERVICE" ]]; then
  log "Stopping DAQ service cleanly: $DAQ_SERVICE"
  systemctl stop "$DAQ_SERVICE"
else
  log "No DAQ service configured; skipping DAQ stop"
fi

log "Sealing active DAQ DB into backlog"
"$SEAL_SCRIPT"

log "Starting battery-budget backlog upload: $BATTERY_UPLOAD_SERVICE"
systemctl start "$BATTERY_UPLOAD_SERVICE"
UPLOAD_RC=$?
log "Battery-budget upload finished with rc=$UPLOAD_RC"

sync

if [[ "$POWER_OFF_AFTER_UPLOAD" == "1" ]]; then
  log "Powering off"
  systemctl poweroff
fi

exit "$UPLOAD_RC"
