#!/usr/bin/env bash
# backfill.sh — Run a pyperun flow day by day over a date range.
#
# Each day is processed independently with --output-mode replace, so:
#   - Only that day's files are read at each step (via runner symlink filtering)
#   - Processing time stays constant regardless of total dataset size
#   - A failed day can be retried without reprocessing the whole range
#
# Usage:
#   bash scripts/backfill.sh <flow> <from-date> <to-date> [extra pyperun args...]
#
# Examples:
#   bash scripts/backfill.sh expo_pre_grace_2_init 2026-03-31 2026-04-24
#   bash scripts/backfill.sh expo_pre_grace_2_init 2026-04-11 2026-04-24 --from-step clean

set -euo pipefail

FLOW="${1:?Usage: backfill.sh <flow> <from-date> <to-date> [extra args]}"
FROM_DATE="${2:?Missing from-date (YYYY-MM-DD)}"
TO_DATE="${3:?Missing to-date (YYYY-MM-DD)}"
shift 3
EXTRA_ARGS=("$@")

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

current="$FROM_DATE"
day_count=0
fail_count=0
failed_days=()

echo "[$(timestamp)] backfill START  flow=$FLOW  from=$FROM_DATE  to=$TO_DATE"

while [[ "$current" < "$TO_DATE" || "$current" == "$TO_DATE" ]]; do
    next=$(date -d "$current + 1 day" +%Y-%m-%d)
    day_count=$((day_count + 1))

    echo ""
    echo "[$(timestamp)] Day $current → $next"

    if pyperun flow "$FLOW" \
        --from "${current}T00:00:00Z" \
        --to   "${next}T00:00:00Z" \
        --output-mode replace \
        "${EXTRA_ARGS[@]}"; then
        echo "[$(timestamp)] OK  $current"
    else
        echo "[$(timestamp)] FAILED  $current" >&2
        fail_count=$((fail_count + 1))
        failed_days+=("$current")
    fi

    current="$next"
done

echo ""
echo "[$(timestamp)] backfill DONE  days=$day_count  failed=$fail_count"
if [[ ${#failed_days[@]} -gt 0 ]]; then
    echo "Failed days: ${failed_days[*]}" >&2
    exit 1
fi
