#!/bin/bash
# run_flow_hourly.sh — Run a pyperun flow in a loop, processing a rolling window each time.
#
# Each run covers the past 2 × interval (to ensure overlap with previous run).
# Use this for long-running processes instead of cron.
#
# Usage:
#   ./run_flow_hourly.sh <flow-name> [interval-seconds]
#
# Examples:
#   ./run_flow_hourly.sh my-experiment 3600     # every hour
#   ./run_flow_hourly.sh my-experiment 1800     # every 30 min

FLOW="${1:?Usage: $0 <flow-name> [interval-seconds]}"
INTERVAL="${2:-3600}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYPERUN_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PYPERUN_DIR/logs"
LOG_FILE="$LOG_DIR/${FLOW}.log"

mkdir -p "$LOG_DIR"

log() {
    echo "[$(date '+%Y-%m-%dT%H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log "Starting loop runner: flow=$FLOW interval=${INTERVAL}s"
log "Logs: $LOG_FILE"

while true; do
    # Rolling window: 2× interval back → current hour boundary
    from_ts=$(date -u -d "${INTERVAL} seconds ago" +%Y-%m-%dT%H:00:00Z)
    to_ts=$(date -u +%Y-%m-%dT%H:00:00Z)

    log "--- Run start  from=$from_ts  to=$to_ts ---"
    if pyperun flow "$FLOW" \
        --from "$from_ts" \
        --to   "$to_ts" \
        >> "$LOG_FILE" 2>&1; then
        log "--- Run OK ---"
    else
        log "--- Run FAILED (exit $?) ---"
    fi
    log "Next run in ${INTERVAL}s"
    sleep "$INTERVAL"
done
