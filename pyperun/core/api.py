"""Pure Python API for pyperun — returns dicts/lists, never prints.

Designed to be imported directly by Flask or other external tools
without going through the CLI subprocess.

Quick Flask integration example::

    from pyperun.core.api import get_status, list_flows, get_run_events, list_runs
    from pyperun.core.flow import run_flow

    @app.get("/api/status")
    def api_status():
        return jsonify(get_status())

    @app.get("/api/flows")
    def api_flows():
        return jsonify(list_flows())

    @app.post("/api/run/<flow_name>")
    def api_run(flow_name):
        # Run in a background thread and return run_id immediately
        from threading import Thread
        from pyperun.core.logger import new_run_id
        # run_id is printed to stdout; alternatively parse it from logs
        t = Thread(target=run_flow, args=(flow_name,), daemon=True)
        t.start()
        return jsonify({"status": "started"})

    @app.get("/api/runs")
    def api_runs():
        return jsonify(list_runs())

    @app.get("/api/runs/<run_id>")
    def api_run_events(run_id):
        return jsonify(get_run_events(run_id))
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------

def _get_flows_root() -> Path:
    from pyperun.core.flow import get_flows_root
    return get_flows_root()


def list_flows() -> list[dict]:
    """Return available flows as a list of dicts.

    Each dict: {name, description, dataset, n_steps}
    """
    result = []
    for fp in sorted(_get_flows_root().glob("*.json")):
        try:
            with open(fp) as f:
                flow = json.load(f)
            result.append({
                "name": flow.get("name", fp.stem),
                "description": flow.get("description", ""),
                "dataset": flow.get("dataset"),
                "n_steps": len(flow.get("steps", [])),
            })
        except Exception:
            result.append({"name": fp.stem, "description": "", "dataset": None, "n_steps": 0})
    return result


def list_steps(flow_name: str) -> list[dict]:
    """Return steps of a flow as a list of dicts.

    Each dict: {index, treatment, name, input, output, params}
    Raises FileNotFoundError if the flow does not exist.
    """
    from pyperun.core.flow import _find_flow
    flow_path = _find_flow(flow_name)
    with open(flow_path) as f:
        flow = json.load(f)

    result = []
    for i, s in enumerate(flow.get("steps", []), 1):
        result.append({
            "index": i,
            "treatment": s["treatment"],
            "name": s.get("name", s["treatment"]),
            "input": s.get("input", ""),
            "output": s.get("output", ""),
            "params": s.get("params", {}),
        })
    return result


# ---------------------------------------------------------------------------
# Treatments
# ---------------------------------------------------------------------------

def list_treatments() -> list[dict]:
    """Return available treatments as a list of dicts.

    Each dict: {name, description}
    """
    from pyperun.core.runner import TREATMENTS_ROOT
    result = []
    for d in sorted(TREATMENTS_ROOT.iterdir()):
        p = d / "treatment.json"
        if not p.exists():
            continue
        try:
            with open(p) as f:
                t = json.load(f)
            result.append({"name": d.name, "description": t.get("description", "")})
        except Exception:
            result.append({"name": d.name, "description": ""})
    return result


def describe_treatment(name: str) -> dict:
    """Return full description of a treatment.

    Returns a dict with keys: name, description, input_format, output_format, params.
    Each param: {name, type, default, description}
    Raises FileNotFoundError if the treatment does not exist.
    """
    from pyperun.core.runner import TREATMENTS_ROOT
    path = TREATMENTS_ROOT / name / "treatment.json"
    if not path.exists():
        raise FileNotFoundError(f"Treatment '{name}' not found")
    with open(path) as f:
        t = json.load(f)

    params = []
    for pname, pdef in t.get("params", {}).items():
        params.append({
            "name": pname,
            "type": pdef.get("type", ""),
            "default": pdef.get("default"),
            "description": pdef.get("description", ""),
        })

    return {
        "name": t.get("name", name),
        "description": t.get("description", ""),
        "input_format": t.get("input_format", ""),
        "output_format": t.get("output_format", ""),
        "params": params,
    }


# ---------------------------------------------------------------------------
# Run a flow
# ---------------------------------------------------------------------------

def run_flow(
    name: str,
    *,
    time_from: str | None = None,
    time_to: str | None = None,
    from_step: str | None = None,
    to_step: str | None = None,
    step: str | None = None,
    output_mode: str = "replace",
    params_override: dict | None = None,
) -> str:
    """Launch a flow synchronously and return the run_id.

    Blocks until the flow completes or raises on error.

    Parameters
    ----------
    name            : Flow name (e.g. "valvometry-daily")
    time_from       : ISO 8601 start of time window (e.g. "2026-01-01T00:00:00Z")
    time_to         : ISO 8601 end of time window
    from_step       : Start execution from this step (inclusive)
    to_step         : Stop execution at this step (inclusive)
    step            : Run a single step only
    output_mode     : "replace" (default) | "reset" (wipe all outputs)
    params_override : Dict of param overrides applied to every step

    Returns
    -------
    run_id : str — unique identifier for this run (use get_run_events to inspect)

    Raises
    ------
    FileNotFoundError : if the flow does not exist
    SystemExit        : if a step fails (mirrors CLI behaviour)
    ValueError        : on invalid step names or mutually exclusive options
    """
    from pyperun.core.flow import run_flow as _run_flow
    from pyperun.core.timefilter import parse_iso_utc

    if step and (from_step or to_step):
        raise ValueError("step is mutually exclusive with from_step/to_step")

    tf = parse_iso_utc(time_from) if time_from else None
    tt = parse_iso_utc(time_to) if time_to else None

    if tf and tt and tf > tt:
        raise ValueError("time_from must be before time_to")

    return _run_flow(
        name,
        time_from=tf,
        time_to=tt,
        from_step=from_step,
        to_step=to_step,
        step=step,
        output_mode=output_mode,
        params_override=params_override,
    )


def launch_flow(
    name: str,
    *,
    time_from: str | None = None,
    time_to: str | None = None,
    from_step: str | None = None,
    to_step: str | None = None,
    step: str | None = None,
    output_mode: str = "replace",
    params_override: dict | None = None,
) -> str:
    """Launch a flow as a detached subprocess and return its run_id immediately.

    Non-blocking (A1): the flow runs in its own `pyperun flow` child process,
    so different flows run in parallel and isolated. Track progress via
    get_flow_summary / get_run_events / list_running.

    Raises FileNotFoundError if the flow does not exist.
    """
    import subprocess
    from pyperun.core.flow import _find_flow
    from pyperun.core.logger import LOGS_ROOT, new_run_id

    _find_flow(name)  # validate up front (raises FileNotFoundError)

    if step and (from_step or to_step):
        raise ValueError("step is mutually exclusive with from_step/to_step")

    run_id = new_run_id()
    cmd = ["pyperun", "flow", name, "--run-id", run_id]
    if time_from:
        cmd += ["--from", time_from]
    if time_to:
        cmd += ["--to", time_to]
    if from_step:
        cmd += ["--from-step", from_step]
    if to_step:
        cmd += ["--to-step", to_step]
    if step:
        cmd += ["--step", step]
    if output_mode and output_mode != "replace":
        cmd += ["--output-mode", output_mode]
    if params_override:
        cmd += ["--params", json.dumps(params_override)]

    log_dir = LOGS_ROOT / "flows" / name
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = open(log_dir / "stdout.log", "ab")
    subprocess.Popen(cmd, stdout=log_file, stderr=log_file)
    log_file.close()
    return run_id


# ---------------------------------------------------------------------------
# Running flows — O7 (what's running) + A5 (stop), backed by the PID lockfile
# ---------------------------------------------------------------------------

def _pid_alive(pid: int) -> bool:
    """Return True if a process with this PID exists and is signalable."""
    import os
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # alive, owned by another user


def _read_lock_pid(lock_path: Path) -> int | None:
    try:
        return int(lock_path.read_text().strip())
    except (ValueError, OSError):
        return None


def list_running() -> list[dict]:
    """Return flows currently running, with live progress (O7).

    Source of truth is the per-flow PID lockfile (logs/flows/<flow>/.lock);
    stale lockfiles (dead PID) are cleaned up. Progress fields come from
    latest.json (written before each step).

    Each entry: {flow, pid, run_id, ts_start, step_index, steps_total, current_step}.
    """
    from pyperun.core.logger import LOGS_ROOT

    flows_dir = LOGS_ROOT / "flows"
    if not flows_dir.exists():
        return []

    running = []
    for lock in sorted(flows_dir.glob("*/.lock")):
        flow = lock.parent.name
        pid = _read_lock_pid(lock)
        if pid is None or not _pid_alive(pid):
            lock.unlink(missing_ok=True)
            continue
        entry = {"flow": flow, "pid": pid, "run_id": None, "ts_start": None,
                 "step_index": None, "steps_total": None, "current_step": None}
        summary = get_flow_summary(flow)
        if summary and summary.get("status") == "running":
            for k in ("run_id", "ts_start", "step_index", "steps_total", "current_step"):
                entry[k] = summary.get(k)
        running.append(entry)
    return running


def stop_flow(flow: str) -> dict:
    """Request a graceful stop of a running flow via SIGTERM (A5).

    The flow stops between steps (the in-progress step may finish); its `finally`
    removes the lockfile and writes a 'stopped' summary.

    Returns {flow, stopped: bool, pid?, reason?}.
    """
    import os
    import signal
    from pyperun.core.logger import LOGS_ROOT

    lock = LOGS_ROOT / "flows" / flow / ".lock"
    if not lock.exists():
        return {"flow": flow, "stopped": False, "reason": "not running"}
    pid = _read_lock_pid(lock)
    if pid is None:
        return {"flow": flow, "stopped": False, "reason": "invalid lockfile"}
    if not _pid_alive(pid):
        lock.unlink(missing_ok=True)
        return {"flow": flow, "stopped": False, "reason": "process not alive"}
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError as exc:
        return {"flow": flow, "stopped": False, "pid": pid, "reason": str(exc)}
    return {"flow": flow, "stopped": True, "pid": pid}


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def get_status() -> list[dict]:
    """Return pipeline status for all flows.

    Each entry: {flow, dataset, status, steps}
    Each step: {treatment, output, n_files, last_modified}
    status is 'up-to-date' | 'incomplete' | 'no-dataset'
    """
    from pyperun.core.flow import get_flows_root, _resolve_path
    from pyperun.core.pipeline import PIPELINE_STEPS, resolve_paths

    external = {s["treatment"] for s in PIPELINE_STEPS if s.get("external")}
    result = []

    for flow_path in sorted(get_flows_root().glob("*.json")):
        try:
            with open(flow_path) as f:
                flow = json.load(f)
        except Exception:
            continue

        name = flow.get("name", flow_path.stem)
        dataset = flow.get("dataset")

        if not dataset:
            result.append({"flow": name, "dataset": None, "status": "no-dataset", "steps": []})
            continue

        steps_out = []
        all_ok = True

        for s in flow.get("steps", []):
            treatment = s["treatment"]
            if "output" in s:
                out_dir = Path(_resolve_path(dataset, s["output"]))
            else:
                _, out_str = resolve_paths(dataset, treatment)
                out_dir = Path(out_str)

            if out_dir.exists():
                files = [f for f in out_dir.rglob("*") if f.is_file()]
                n_files = len(files)
                if files:
                    last_mod = max(f.stat().st_mtime for f in files)
                    last_modified = datetime.fromtimestamp(last_mod).strftime("%Y-%m-%d")
                else:
                    last_modified = None
            else:
                n_files = 0
                last_modified = None

            if n_files == 0 and treatment not in external:
                all_ok = False

            steps_out.append({
                "treatment": treatment,
                "output": out_dir.name,
                "n_files": n_files,
                "last_modified": last_modified,
                "external": treatment in external,
            })

        entry = {
            "flow": name,
            "dataset": dataset,
            "status": "up-to-date" if all_ok else "incomplete",
            "steps": steps_out,
            "last_run": get_flow_summary(name),
        }
        result.append(entry)

    return result


# ---------------------------------------------------------------------------
# Run history — agent triage layer (latest.json) + event search (daily logs)
# ---------------------------------------------------------------------------

def get_flow_summary(flow: str) -> dict | None:
    """Return the latest run summary for a flow, or None if never run.

    Reads logs/flows/<flow>/latest.json — O(1), agent-friendly triage.
    Fields: flow, run_id, status, ts_start, ts_end, duration_ms,
            steps_total, steps_ok, steps_failed, error?
    """
    from pyperun.core.logger import LOGS_ROOT
    path = LOGS_ROOT / "flows" / flow / "latest.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def list_flow_summaries() -> list[dict]:
    """Return latest run summaries for all flows that have ever run.

    Reads one latest.json per flow — single-pass, agent/MCP triage endpoint.
    Results sorted by ts_start descending (most recent first).
    """
    from pyperun.core.logger import LOGS_ROOT
    summaries = []
    flows_dir = LOGS_ROOT / "flows"
    if not flows_dir.exists():
        return []
    for f in sorted(flows_dir.glob("*/latest.json")):
        try:
            summaries.append(json.loads(f.read_text()))
        except Exception:
            pass
    summaries.sort(key=lambda s: s.get("ts_start", ""), reverse=True)
    return summaries


def list_runs(limit: int = 50) -> list[dict]:
    """Return recent flow runs as summary dicts (backed by latest.json).

    Each entry: run_id, flow, status, ts_start, ts_end, duration_ms,
                steps_total, steps_ok, steps_failed, error?
    """
    return list_flow_summaries()[:limit]


# ---------------------------------------------------------------------------
# Schedule management — façade over core.schedules (shared by REST/CLI/MCP)
# ---------------------------------------------------------------------------

def list_schedules() -> list[dict]:
    """Return all cron schedule entries from schedules.json ([] if none)."""
    from pyperun.core import schedules
    return schedules.list_schedules()


def upsert_schedule(
    flow: str, schedule: str, timezone: str = "UTC", enabled: bool = True
) -> dict:
    """Add or update a flow's schedule. Raises ValueError on invalid cron/tz."""
    from pyperun.core import schedules
    return schedules.upsert_schedule(flow, schedule, timezone, enabled)


def remove_schedule(flow: str) -> dict:
    """Remove a flow's schedule. Returns {removed: bool}."""
    from pyperun.core import schedules
    return schedules.remove_schedule(flow)


def get_run_events(run_id: str, flow: str | None = None) -> list[dict]:
    """Return all treatment-level log events for a run_id.

    Searches daily .jsonl logs. Provide flow for a faster targeted search;
    omit to search across all flows and misc logs.

    Each event: ts, treatment, status, input_dir, output_dir, duration_ms?, error?, ...
    Returns [] if not found.
    """
    from pyperun.core.logger import LOGS_ROOT

    def _search_dir(d: Path) -> list[dict]:
        events = []
        for log_file in sorted(d.glob("*.jsonl"), reverse=True):
            try:
                with open(log_file) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            e = json.loads(line)
                            if e.get("run_id") == run_id:
                                events.append(e)
                        except json.JSONDecodeError:
                            pass
            except OSError:
                pass
        return events

    if flow:
        return _search_dir(LOGS_ROOT / "flows" / flow)

    events = []
    flows_dir = LOGS_ROOT / "flows"
    if flows_dir.exists():
        for flow_dir in sorted(flows_dir.iterdir()):
            if flow_dir.is_dir():
                events.extend(_search_dir(flow_dir))
    misc_dir = LOGS_ROOT / "misc"
    if misc_dir.exists():
        events.extend(_search_dir(misc_dir))
    events.sort(key=lambda e: e.get("ts", ""))
    return events


# ---------------------------------------------------------------------------
# Dataset lifecycle — init, delete, presets
# ---------------------------------------------------------------------------

def list_presets(project_dir: str | None = None) -> list[dict]:
    """Return available presets (built-in + project-level presets.json).

    Each entry: {name, description, steps}  (steps is None for 'full' = all steps)
    """
    _BUILTIN_PRESETS = {
        "csv": {
            "description": "Core pipeline → exportcsv",
            "steps": ["parse", "clean", "resample", "transform", "normalize", "aggregate", "exportcsv"],
        },
        "parquet": {
            "description": "Core pipeline → exportparquet",
            "steps": ["parse", "clean", "resample", "transform", "normalize", "aggregate", "exportparquet"],
        },
        "duckdb": {
            "description": "Core pipeline → exportduckdb (analytical DuckDB database)",
            "steps": ["parse", "clean", "resample", "transform", "normalize", "aggregate", "exportduckdb"],
        },
        "full": {
            "description": "Full pipeline (all steps)",
            "steps": None,
        },
    }

    presets = dict(_BUILTIN_PRESETS)
    base = Path(project_dir).resolve() if project_dir else Path.cwd()
    project_file = base / "presets.json"
    if project_file.exists():
        try:
            with open(project_file) as f:
                for name, spec in json.load(f).items():
                    if isinstance(spec, list):
                        spec = {"steps": spec, "description": ""}
                    presets[name] = spec
        except Exception:
            pass

    return [
        {"name": name, "description": spec.get("description", ""), "steps": spec.get("steps")}
        for name, spec in presets.items()
    ]


def init_dataset(
    dataset: str,
    preset: str = "full",
    flow_name: str | None = None,
    raw: str | None = None,
    force: bool = False,
    project_dir: str | None = None,
) -> dict:
    """Scaffold a new dataset: create stage directories and generate a flow JSON.

    Parameters
    ----------
    dataset     : Dataset name (e.g. "MY-EXPERIMENT")
    preset      : Preset name — see list_presets() (default: "full")
    flow_name   : Flow file name; defaults to dataset.lower()
    raw         : Path to existing raw CSV directory (creates a symlink as 00_raw)
    force       : If True, overwrite an existing flow without prompting
    project_dir : Project root directory (default: cwd)

    Returns
    -------
    {
        "dataset":      "MY-EXPERIMENT",
        "flow":         "my-experiment",
        "flow_path":    "flows/my-experiment.json",
        "action":       "created" | "regenerated",
        "created_dirs": ["datasets/MY-EXPERIMENT/00_raw", ...],
        "raw_symlink":  "/abs/path/to/raw" | null,
    }

    Raises
    ------
    ValueError       if preset is unknown
    FileExistsError  if flow already exists and force=False
    FileNotFoundError if raw path does not exist
    """
    import os
    from pyperun.core.pipeline import PIPELINE_STEPS
    from pyperun.core.runner import TREATMENTS_ROOT

    presets = {p["name"]: p for p in list_presets(project_dir)}
    if preset not in presets:
        raise ValueError(f"Unknown preset '{preset}'. Available: {', '.join(presets)}")

    allowed = presets[preset]["steps"]  # None = all steps
    base = Path(project_dir).resolve() if project_dir else Path.cwd()

    datasets_dir = base / "datasets" / dataset
    flows_dir    = base / "flows"
    treatments_dir = base / "treatments"

    steps = [s for s in PIPELINE_STEPS if allowed is None or s["treatment"] in allowed]

    _flow_name = flow_name or dataset.lower()
    flow_path  = flows_dir / f"{_flow_name}.json"
    flow_exists = flow_path.exists()

    if flow_exists and not force:
        raise FileExistsError(
            f"Flow '{_flow_name}.json' already exists. Use force=True to overwrite."
        )

    # Create pipeline stage directories (idempotent)
    created_dirs = []
    for s in steps:
        for key in ("input", "output"):
            if key in s and not s.get("external"):
                d = datasets_dir / s[key]
                d.mkdir(parents=True, exist_ok=True)
                rel = str(d.relative_to(base))
                if rel not in created_dirs:
                    created_dirs.append(rel)

    # 00_raw symlink
    raw_symlink = None
    if raw:
        raw_src = Path(raw).resolve()
        if not raw_src.exists():
            raise FileNotFoundError(f"--raw path does not exist: {raw_src}")
        raw_dir = datasets_dir / "00_raw"
        if raw_dir.exists() and not raw_dir.is_symlink():
            raw_dir.rmdir()
        if raw_dir.is_symlink():
            raw_dir.unlink()
        os.symlink(raw_src, raw_dir)
        raw_symlink = str(raw_src)

    # treatments/ placeholder
    treatments_dir.mkdir(parents=True, exist_ok=True)

    # Build flow JSON with all treatment defaults explicit
    def _step_entry(s):
        entry = {"treatment": s["treatment"], "input": s["input"]}
        if "output" in s:
            entry["output"] = s["output"]
        t_path = TREATMENTS_ROOT / s["treatment"] / "treatment.json"
        if t_path.exists():
            with open(t_path) as f:
                t = json.load(f)
            params = {k: v["default"] for k, v in t.get("params", {}).items()}
            if params:
                entry["params"] = params
        return entry

    flow_json = {
        "name": _flow_name,
        "description": f"Pipeline for dataset {dataset}",
        "dataset": dataset,
        "params": {},
        "steps": [_step_entry(s) for s in steps],
    }
    flows_dir.mkdir(parents=True, exist_ok=True)
    flow_path.write_text(json.dumps(flow_json, indent=4) + "\n")

    return {
        "dataset":      dataset,
        "flow":         _flow_name,
        "flow_path":    str(flow_path.relative_to(base)),
        "action":       "regenerated" if flow_exists else "created",
        "created_dirs": created_dirs,
        "raw_symlink":  raw_symlink,
    }


def delete_dataset(
    dataset: str,
    project_dir: str | None = None,
) -> dict:
    """Delete a dataset directory and all flow files that reference it.

    Parameters
    ----------
    dataset     : Dataset name (e.g. "MY-EXPERIMENT")
    project_dir : Project root directory (default: cwd)

    Returns
    -------
    {
        "deleted_dataset": "MY-EXPERIMENT",
        "deleted_dirs":    ["datasets/MY-EXPERIMENT"],
        "deleted_flows":   ["flows/my-experiment.json"],
        "raw_symlink_kept": "/abs/path" | null,
    }

    Raises
    ------
    FileNotFoundError if neither dataset directory nor flow files are found
    """
    import shutil
    from pyperun.core.flow import get_flows_root

    base = Path(project_dir).resolve() if project_dir else Path.cwd()
    dataset_dir = base / "datasets" / dataset
    flows_root  = get_flows_root()

    # Find flows referencing this dataset
    flow_files = []
    for fp in sorted(flows_root.glob("*.json")):
        try:
            with open(fp) as f:
                flow = json.load(f)
            if flow.get("dataset") == dataset:
                flow_files.append(fp)
        except Exception:
            pass

    if not dataset_dir.exists() and not flow_files:
        raise FileNotFoundError(f"Nothing found for dataset '{dataset}'")

    deleted_dirs  = []
    deleted_flows = []
    raw_symlink_kept = None

    if dataset_dir.exists():
        raw_dir = dataset_dir / "00_raw"
        if raw_dir.is_symlink():
            raw_symlink_kept = str(raw_dir.resolve())
            raw_dir.unlink()
        shutil.rmtree(dataset_dir)
        deleted_dirs.append(str(dataset_dir.relative_to(base)))

    for fp in flow_files:
        fp.unlink()
        deleted_flows.append(str(fp.relative_to(base)))

    return {
        "deleted_dataset":  dataset,
        "deleted_dirs":     deleted_dirs,
        "deleted_flows":    deleted_flows,
        "raw_symlink_kept": raw_symlink_kept,
    }
