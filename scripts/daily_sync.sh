#!/usr/bin/env bash
# daily_sync.sh — Run a pyperun flow for the current day every hour.
# At 5am, also consolidates yesterday (J-1).
#
# Crontab (one entry only):
#   0 * * * * PYPERUN_BIN=/home/litistech/.local/bin/pyperun /path/to/pyperun/scripts/daily_sync.sh my-flow >> /var/log/pyperun_daily.log 2>&1

set -euo pipefail

FLOW="${1:?Usage: $0 <flow-name>}"
PYPERUN_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PYPERUN_ROOT"

# Use PYPERUN_BIN env var if set, otherwise find pyperun in PATH or venv
if [ -n "${PYPERUN_BIN:-}" ]; then
    PYPERUN="$PYPERUN_BIN"
else
    if ! command -v pyperun &>/dev/null; then
        for venv in .venv venv env; do
            if [ -f "$PYPERUN_ROOT/$venv/bin/activate" ]; then
                # shellcheck disable=SC1090
                source "$PYPERUN_ROOT/$venv/bin/activate"
                break
            fi
        done
    fi
    PYPERUN="pyperun"
fi

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

today=$(date -u +%Y-%m-%d)
tomorrow=$(date -u -d "tomorrow" +%Y-%m-%d)
yesterday=$(date -u -d "yesterday" +%Y-%m-%d)
hour=$(date -u +%H)

# At 5am: consolidate yesterday (J-1)
if [ "$hour" = "05" ]; then
    echo "--- [$(timestamp)] CONSOLIDATE  flow=$FLOW  ${yesterday} → ${today} ---"
    "$PYPERUN" flow "$FLOW" \
        --from "${yesterday}T00:00:00Z" \
        --to   "${today}T00:00:00Z"
    echo "--- [$(timestamp)] CONSOLIDATE done ---"
fi

# Every hour: update today (live)
echo "--- [$(timestamp)] TODAY  flow=$FLOW  ${today} → ${tomorrow} ---"
"$PYPERUN" flow "$FLOW" \
    --from "${today}T00:00:00Z" \
    --to   "${tomorrow}T00:00:00Z"
echo "--- [$(timestamp)] TODAY done ---"
