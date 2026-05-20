#!/usr/bin/env bash
# hourly_sync.sh — Run a pyperun flow over the past 2 hours (rolling window).
#
# Processes data from 2 hours ago to the current hour boundary, so late-arriving
# data is always captured on the next run.
#
# Install in crontab:
#   crontab -e
#   0 * * * * /path/to/pyperun/scripts/hourly_sync.sh my-flow >> /var/log/pyperun_hourly.log 2>&1

set -euo pipefail

PYPERUN_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOGFILE="${PYPERUN_ROOT}/logs/pyperun_hourly.log"
FLOW="${1:?Usage: $0 <flow-name>}"

cd "$PYPERUN_ROOT"
mkdir -p "$(dirname "$LOGFILE")"

export PATH="$HOME/.local/bin:$PYPERUN_ROOT/.venv/bin:$PYPERUN_ROOT/venv/bin:$PATH"

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

# 2-hour rolling window, floored to the hour boundary
from_ts=$(date -u -d "2 hours ago" +%Y-%m-%dT%H:00:00Z)
to_ts=$(date -u +%Y-%m-%dT%H:00:00Z)

echo "--- [$(timestamp)] hourly_sync START  flow=$FLOW  from=$from_ts  to=$to_ts ---" >> "$LOGFILE"

pyperun flow "$FLOW" \
    --from "$from_ts" \
    --to   "$to_ts" \
    >> "$LOGFILE" 2>&1
EXIT_CODE=$?

echo "--- [$(timestamp)] hourly_sync END (exit=$EXIT_CODE) ---" >> "$LOGFILE"
exit $EXIT_CODE
