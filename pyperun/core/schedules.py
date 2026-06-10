"""Schedule store — single source of truth for `schedules.json`.

One module, used by every façade so the logic and validation live in one place:

  - the scheduler tick   (core/scheduler.py)  reads it
  - the REST API         (server.py)          CRUDs it
  - the CLI              (`pyperun schedule`)  CRUDs it
  - the MCP tools        (mcp.py)             CRUDs it

A schedule entry is ``{flow, schedule, timezone, enabled}`` where ``schedule``
is a standard 5-field cron expression and ``timezone`` an IANA name.
"""

from __future__ import annotations

import json
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_SCHEDULES_FILE = Path("schedules.json")


def _path(path: str | Path | None) -> Path:
    return Path(path) if path else _SCHEDULES_FILE


def _load(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path) as f:
        return json.load(f)


def _save(path: Path, schedules: list[dict]) -> None:
    path.write_text(json.dumps(schedules, indent=4, ensure_ascii=False) + "\n")


def validate_timezone(timezone: str) -> None:
    """Raise ValueError if *timezone* is not a known IANA name."""
    try:
        ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError) as e:
        raise ValueError(f"unknown timezone '{timezone}'") from e


def validate_cron(schedule: str) -> None:
    """Raise ValueError if *schedule* is not a valid cron expression.

    Best-effort: validation is skipped when croniter is not installed
    (``pip install pyperun[scheduler]``), since the scheduler can't run anyway.
    """
    try:
        from croniter import croniter
    except ImportError:
        return
    if not croniter.is_valid(schedule):
        raise ValueError(f"invalid cron expression '{schedule}'")


def list_schedules(path: str | Path | None = None) -> list[dict]:
    """Return all schedule entries (``[]`` if the file does not exist yet)."""
    return _load(_path(path))


def upsert_schedule(
    flow: str,
    schedule: str,
    timezone: str = "UTC",
    enabled: bool = True,
    path: str | Path | None = None,
) -> dict:
    """Add or update the schedule for *flow*.

    Validates *schedule* (cron) and *timezone* (IANA) before writing.
    Returns ``{flow, schedule, timezone, enabled, action}`` where ``action`` is
    ``"created"`` or ``"updated"``.
    """
    validate_cron(schedule)
    validate_timezone(timezone)

    p = _path(path)
    schedules = _load(p)
    action = "created"
    for entry in schedules:
        if entry["flow"] == flow:
            entry["schedule"] = schedule
            entry["timezone"] = timezone
            entry["enabled"] = enabled
            action = "updated"
            break
    else:
        schedules.append(
            {"flow": flow, "schedule": schedule, "timezone": timezone, "enabled": enabled}
        )

    _save(p, schedules)
    return {
        "flow": flow,
        "schedule": schedule,
        "timezone": timezone,
        "enabled": enabled,
        "action": action,
    }


def remove_schedule(flow: str, path: str | Path | None = None) -> dict:
    """Remove *flow* from the store.

    Returns ``{removed: True}`` if an entry was deleted, ``{removed: False}``
    if no entry matched.
    """
    p = _path(path)
    schedules = _load(p)
    kept = [e for e in schedules if e["flow"] != flow]
    if len(kept) == len(schedules):
        return {"removed": False}
    _save(p, kept)
    return {"removed": True}
