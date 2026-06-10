from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from croniter import croniter

from pyperun.core.api import get_flow_summary
from pyperun.core.logger import LOGS_ROOT

_SCHEDULES_FILE = Path("schedules.json")


def _lock_path(flow: str) -> Path:
    return LOGS_ROOT / "flows" / flow / ".lock"


def is_locked(flow: str) -> bool:
    """Return True if flow is currently running (valid lockfile with live PID)."""
    path = _lock_path(flow)
    if not path.exists():
        return False
    try:
        pid = int(path.read_text().strip())
        os.kill(pid, 0)
        return True
    except (ValueError, ProcessLookupError):
        path.unlink(missing_ok=True)
        return False
    except PermissionError:
        return True  # process alive but we can't signal it


def _is_due(schedule: str, tz_name: str, last_run_utc: datetime | None) -> bool:
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)
    cron = croniter(schedule, now_local)
    prev_fire = cron.get_prev(datetime)
    if prev_fire.tzinfo is None:
        prev_fire = prev_fire.replace(tzinfo=tz)
    if last_run_utc is None:
        return True
    return last_run_utc < prev_fire


def tick(schedules_path: str | None = None, dry_run: bool = False) -> None:
    path = Path(schedules_path) if schedules_path else _SCHEDULES_FILE
    if not path.exists():
        print(f"[tick] No schedules file: {path}")
        return

    with open(path) as f:
        schedules = json.load(f)

    for entry in schedules:
        flow = entry["flow"]
        schedule = entry["schedule"]
        tz_name = entry.get("timezone", "UTC")
        enabled = entry.get("enabled", True)

        if not enabled:
            print(f"[tick] {flow}: disabled")
            continue

        summary = get_flow_summary(flow)
        last_run_utc = None
        if summary and summary.get("ts_start"):
            last_run_utc = datetime.fromisoformat(summary["ts_start"].replace("Z", "+00:00"))

        if not _is_due(schedule, tz_name, last_run_utc):
            last = summary["ts_start"] if summary else "never"
            print(f"[tick] {flow}: not due (last run {last})")
            continue

        if is_locked(flow):
            print(f"[tick] {flow}: already running, skip")
            continue

        print(f"[tick] {flow}: {'would launch' if dry_run else 'launching'}")
        if not dry_run:
            log_dir = LOGS_ROOT / "flows" / flow
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = open(log_dir / "stdout.log", "w")
            subprocess.Popen(["pyperun", "flow", flow], stdout=log_file, stderr=log_file)
            log_file.close()
