from __future__ import annotations

import json
import time
from pathlib import Path

import jsonlines


LOGS_ROOT = Path("logs")
LOG_RETENTION_DAYS = 30

_REDACT_KEYS = {"password"}


def new_run_id() -> str:
    import os
    return os.urandom(4).hex()


def _log_path(flow: str | None) -> Path:
    today = time.strftime("%Y-%m-%d", time.gmtime())
    if flow:
        return LOGS_ROOT / "flows" / flow / f"{today}.jsonl"
    return LOGS_ROOT / "misc" / f"{today}.jsonl"


def log_event(
    treatment: str,
    status: str,
    input_dir: str,
    output_dir: str,
    duration_ms: float | None = None,
    error: str | None = None,
    time_from: str | None = None,
    time_to: str | None = None,
    params: dict | None = None,
    flow: str | None = None,
    run_id: str | None = None,
) -> None:
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "treatment": treatment,
        "status": status,
        "input_dir": input_dir,
        "output_dir": output_dir,
    }
    if run_id is not None:
        entry["run_id"] = run_id
    if flow is not None:
        entry["flow"] = flow
    if time_from is not None:
        entry["time_from"] = time_from
    if time_to is not None:
        entry["time_to"] = time_to
    if params is not None:
        entry["params"] = {k: ("***" if k in _REDACT_KEYS else v) for k, v in params.items()}
    if duration_ms is not None:
        entry["duration_ms"] = round(duration_ms, 1)
    if error is not None:
        entry["error"] = error
    path = _log_path(flow)
    path.parent.mkdir(parents=True, exist_ok=True)
    with jsonlines.open(path, mode="a") as writer:
        writer.write(entry)


def write_flow_summary(
    flow: str,
    run_id: str,
    status: str,
    ts_start: str,
    duration_ms: float,
    steps_total: int,
    steps_ok: int,
    steps_failed: int,
    error: str | None = None,
) -> None:
    """Write/overwrite logs/flows/<flow>/latest.json — the agent-readable triage layer."""
    summary = {
        "flow": flow,
        "run_id": run_id,
        "status": status,
        "ts_start": ts_start,
        "ts_end": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "duration_ms": round(duration_ms, 1),
        "steps_total": steps_total,
        "steps_ok": steps_ok,
        "steps_failed": steps_failed,
    }
    if error is not None:
        summary["error"] = error
    path = LOGS_ROOT / "flows" / flow / "latest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2))


def cleanup_old_logs(retention_days: int = LOG_RETENTION_DAYS) -> None:
    """Delete .jsonl files older than retention_days. Never touches latest.json."""
    import os
    cutoff = time.time() - retention_days * 86400
    for search_dir in (LOGS_ROOT / "flows", LOGS_ROOT / "misc"):
        if not search_dir.exists():
            continue
        for f in search_dir.rglob("*.jsonl"):
            try:
                if os.path.getmtime(f) < cutoff:
                    f.unlink()
            except OSError:
                pass
