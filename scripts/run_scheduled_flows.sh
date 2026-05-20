#!/bin/bash
# run_scheduled_flows.sh — Run all flows listed in scheduled_flows.txt with yesterday's data.
# Called by cron, e.g. every hour.
#
# Usage: ./run_scheduled_flows.sh [--flows-file path/to/file]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYPERUN_DIR="$(dirname "$SCRIPT_DIR")"
FLOWS_FILE="$SCRIPT_DIR/scheduled_flows.txt"

# Optional override: --flows-file <path>
while [[ $# -gt 0 ]]; do
    case "$1" in
        --flows-file) FLOWS_FILE="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

LOG_DIR="$PYPERUN_DIR/logs"
mkdir -p "$LOG_DIR"

export PATH="$HOME/.local/bin:$PYPERUN_DIR/.venv/bin:$PYPERUN_DIR/venv/bin:$PATH"

log() {
    echo "[$(date '+%Y-%m-%dT%H:%M:%S')] $*"
}

if [[ ! -f "$FLOWS_FILE" ]]; then
    log "ERROR: flows file not found: $FLOWS_FILE"
    exit 1
fi

# Compute yesterday and today in UTC
yesterday=$(date -u -d "yesterday" +%Y-%m-%d)
today=$(date -u +%Y-%m-%d)

log "=== Scheduled run start ==="
log "Flows file: $FLOWS_FILE"
log "Window: ${yesterday}T00:00:00Z → ${today}T00:00:00Z"

while IFS= read -r line; do
    # Skip empty lines and comments
    [[ -z "$line" || "$line" =~ ^# ]] && continue

    flow="$line"
    log_file="$LOG_DIR/${flow}.log"

    log "Running flow: $flow"
    if pyperun flow "$flow" \
        --from "${yesterday}T00:00:00Z" \
        --to   "${today}T00:00:00Z" \
        >> "$log_file" 2>&1; then
        log "  OK: $flow"
    else
        log "  FAILED: $flow (see $log_file)"
    fi
done < "$FLOWS_FILE"

log "=== Scheduled run done ==="
