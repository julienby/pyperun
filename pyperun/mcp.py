"""Pyperun MCP server — exposes pyperun as tools for Claude / LLM agents.

Requires the `mcp` package:
    pip install mcp

Run:
    python -m pyperun.mcp          # stdio transport (default, for Claude Code)
    python -m pyperun.mcp --sse    # SSE transport on port 5001

Configure in Claude Code (~/.claude/claude_desktop_config.json or .mcp.json):
    {
        "mcpServers": {
            "pyperun": {
                "command": "python",
                "args": ["-m", "pyperun.mcp"],
                "cwd": "/path/to/your/project"
            }
        }
    }
"""
from __future__ import annotations

import json
import os

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

import pyperun.core.api as api


def _transport_security() -> TransportSecuritySettings:
    """DNS-rebinding protection policy for the SSE transport.

    FastMCP otherwise auto-restricts the Host header to localhost, which breaks
    the server behind a reverse proxy (returns "Invalid Host header"). The real
    auth is PYPERUN_TOKEN, so:

      - PYPERUN_ALLOWED_HOSTS set (comma-separated, e.g. "pyperun.example.org")
        → keep protection on and allow those hosts (+ localhost).
      - unset → disable protection (token-gated, typically behind a proxy).
    """
    raw = os.environ.get("PYPERUN_ALLOWED_HOSTS", "").strip()
    if not raw:
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    hosts = [h.strip() for h in raw.split(",") if h.strip()]
    hosts += ["127.0.0.1:*", "localhost:*", "[::1]:*"]
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True, allowed_hosts=hosts
    )


mcp = FastMCP(
    name="pyperun",
    instructions=(
        "You have access to a pyperun IoT time-series pipeline. "
        "Use list_flows and get_status first to understand what is available. "
        "run_flow is non-blocking — it launches the flow and returns a run_id immediately. "
        "Poll get_flow_summary / list_running to watch progress (step k/N), "
        "get_run_events to inspect results, and stop_flow to request a graceful stop."
    ),
    transport_security=_transport_security(),
)


# ---------------------------------------------------------------------------
# Read-only tools
# ---------------------------------------------------------------------------

@mcp.tool()
def list_flows() -> list[dict]:
    """List all available flows.

    Returns a list of dicts: {name, description, dataset, n_steps}.
    """
    return api.list_flows()


@mcp.tool()
def get_status() -> list[dict]:
    """Return the current pipeline status for all flows.

    For each flow: {flow, dataset, status, steps}.
    status is 'up-to-date' | 'incomplete' | 'no-dataset'.
    Each step includes: treatment, output dir, n_files, last_modified.
    """
    return api.get_status()


@mcp.tool()
def list_steps(flow_name: str) -> list[dict]:
    """Return the steps of a flow with their params.

    Each step: {index, treatment, name, input, output, params}.
    """
    return api.list_steps(flow_name)


@mcp.tool()
def describe_treatment(name: str) -> dict:
    """Return the full description of a treatment: params, input/output format.

    Useful to understand what a step does and which params it accepts.
    """
    return api.describe_treatment(name)


@mcp.tool()
def list_flow_summaries() -> list[dict]:
    """Return the last run summary for every flow (O(1) triage, agent-friendly).

    Reads logs/flows/*/latest.json — one file per flow.
    Each entry: {flow, run_id, status, ts_start, ts_end, duration_ms, steps_total, steps_ok, steps_failed, error?}.
    status is 'success' | 'error'. Sorted by ts_start descending.
    Use this as the first call to assess pipeline health without parsing event logs.
    """
    return api.list_flow_summaries()


@mcp.tool()
def get_flow_summary(flow: str) -> dict | None:
    """Return the last run summary for a single flow, or null if never run.

    Reads logs/flows/<flow>/latest.json — O(1).
    Fields: flow, run_id, status, ts_start, ts_end, duration_ms, steps_total, steps_ok, steps_failed, error?.
    """
    return api.get_flow_summary(flow)


@mcp.tool()
def get_run_events(run_id: str, flow: str | None = None) -> list[dict]:
    """Return all log events for a specific run (drill-down after triage).

    Each event: {ts, treatment, status, input_dir, output_dir, duration_ms, error, ...}
    Provide flow name for a faster targeted search; omit to search all flows.
    Use list_flow_summaries / get_flow_summary first, then call this only for failures.
    """
    return api.get_run_events(run_id, flow=flow)


# ---------------------------------------------------------------------------
# Write tools
# ---------------------------------------------------------------------------

@mcp.tool()
def run_flow(
    name: str,
    time_from: str | None = None,
    time_to: str | None = None,
    from_step: str | None = None,
    to_step: str | None = None,
    step: str | None = None,
    output_mode: str = "replace",
    params_override: str | None = None,
) -> dict:
    """Launch a flow in the background and return its run_id immediately.

    This is NON-BLOCKING — the flow runs as a detached subprocess. Long pipelines
    keep running after this returns. Watch progress with get_flow_summary(name)
    (status 'running' + step_index/steps_total) or list_running(), inspect results
    with get_run_events(run_id), and stop it early with stop_flow(name).

    Parameters
    ----------
    name            : Flow name, e.g. "valvometry-daily" (use list_flows to discover)
    time_from       : ISO 8601 start filter, e.g. "2026-01-01T00:00:00Z"
    time_to         : ISO 8601 end filter
    from_step       : Run from this step onwards (inclusive)
    to_step         : Run up to this step (inclusive)
    step            : Run a single step only
    output_mode     : "replace" (default) | "reset" (wipe all outputs then replace)
    params_override : JSON string of params applied to every step,
                      e.g. '{"freq": "1s"}' — pass null to use flow defaults

    Returns
    -------
    {run_id, status, error} — status is "started" on success.
    """
    overrides = None
    if params_override:
        try:
            overrides = json.loads(params_override)
        except json.JSONDecodeError as exc:
            return {"error": f"params_override is not valid JSON: {exc}"}

    try:
        run_id = api.launch_flow(
            name,
            time_from=time_from,
            time_to=time_to,
            from_step=from_step,
            to_step=to_step,
            step=step,
            output_mode=output_mode,
            params_override=overrides,
        )
    except (FileNotFoundError, ValueError) as exc:
        return {"error": str(exc), "run_id": None, "status": "error"}

    return {"run_id": run_id, "status": "started", "error": None}


@mcp.tool()
def list_running() -> list[dict]:
    """Return the flows currently running, with live progress (O7).

    Each entry: {flow, pid, run_id, ts_start, step_index, steps_total, current_step}.
    step_index/steps_total give "step k/N" progress. Returns [] if nothing is running.
    Source of truth is the per-flow lockfile; stale locks are cleaned up automatically.
    """
    return api.list_running()


@mcp.tool()
def stop_flow(flow: str) -> dict:
    """Request a graceful stop of a running flow (A5).

    Sends SIGTERM to the flow process; it stops between steps (the step in progress
    may finish) and writes a 'stopped' summary. Returns {flow, stopped, pid?, reason?}.
    """
    return api.stop_flow(flow)


@mcp.tool()
def init_dataset(
    dataset: str,
    preset: str = "full",
    flow_name: str | None = None,
) -> dict:
    """Scaffold a new dataset: create stage directories and generate a flow JSON.

    Parameters
    ----------
    dataset   : Dataset name in UPPERCASE, e.g. "MY-EXPERIMENT"
    preset    : "full" (all steps) | "csv" | "parquet" (use list_presets to discover)
    flow_name : Custom flow file name; defaults to dataset.lower()

    Returns
    -------
    {dataset, flow, flow_path, action, created_dirs}
    """
    return api.init_dataset(dataset, preset=preset, flow_name=flow_name)


@mcp.tool()
def list_treatments() -> list[dict]:
    """List all available treatments with their descriptions.

    Returns [{name, description}]. Use describe_treatment(name) for full param details.
    """
    return api.list_treatments()


# ---------------------------------------------------------------------------
# Flow config read/write
# ---------------------------------------------------------------------------

@mcp.tool()
def get_flow_config(flow_name: str) -> dict:
    """Read the raw JSON config of a flow file.

    Returns the full flow dict: {name, description, dataset, params, steps}.
    Use this before set_flow_config to read-modify-write.
    """
    from pyperun.core.flow import _find_flow
    try:
        path = _find_flow(flow_name)
    except FileNotFoundError as exc:
        return {"error": str(exc)}
    with open(path) as f:
        return json.load(f)


@mcp.tool()
def set_flow_config(flow_name: str, config: str) -> dict:
    """Write (overwrite) a flow JSON file.

    Parameters
    ----------
    flow_name : Flow name, e.g. "my-experiment" — writes to flows/<flow_name>.json
    config    : Full flow config as a JSON string.
                Must contain at least: {"steps": [...]}
                Include "dataset" to use relative stage paths.

    Returns
    -------
    {flow_path, n_steps} on success, {error} on failure.

    Workflow: call get_flow_config first, modify the dict, pass it back as JSON string.
    """
    from pathlib import Path
    try:
        cfg = json.loads(config)
    except json.JSONDecodeError as exc:
        return {"error": f"config is not valid JSON: {exc}"}
    if "steps" not in cfg or not isinstance(cfg["steps"], list):
        return {"error": "config must contain a 'steps' array"}

    flows_dir = Path("flows")
    flows_dir.mkdir(exist_ok=True)
    flow_path = flows_dir / f"{flow_name}.json"
    flow_path.write_text(json.dumps(cfg, indent=4, ensure_ascii=False) + "\n")
    return {"flow_path": str(flow_path), "n_steps": len(cfg["steps"])}


# ---------------------------------------------------------------------------
# Schedule management — thin façade over core.schedules
# ---------------------------------------------------------------------------


@mcp.tool()
def list_schedules() -> list[dict]:
    """Return the current contents of schedules.json.

    Each entry: {flow, schedule (cron expression), timezone, enabled}.
    Returns [] if schedules.json does not exist yet.
    """
    from pyperun.core import schedules
    return schedules.list_schedules()


@mcp.tool()
def upsert_schedule(
    flow: str,
    schedule: str,
    timezone: str = "UTC",
    enabled: bool = True,
) -> dict:
    """Add or update a schedule entry in schedules.json.

    Parameters
    ----------
    flow      : Flow name to schedule, e.g. "my-experiment"
    schedule  : Standard cron expression, e.g. "0 6 * * *" (daily at 06:00)
    timezone  : IANA timezone name, e.g. "Europe/Paris" (default: UTC)
    enabled   : Set to false to pause without deleting

    Returns
    -------
    {flow, schedule, timezone, enabled, action} where action is "created" or "updated".
    On invalid cron/timezone: {error: "..."}.
    """
    from pyperun.core import schedules
    try:
        return schedules.upsert_schedule(flow, schedule, timezone, enabled)
    except ValueError as e:
        return {"error": str(e)}


@mcp.tool()
def remove_schedule(flow: str) -> dict:
    """Remove a flow from schedules.json.

    Returns {removed: true} if the entry existed, {removed: false} if it was not found.
    """
    from pyperun.core import schedules
    return schedules.remove_schedule(flow)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    if "--sse" in sys.argv:
        mcp.run(transport="sse")
    else:
        mcp.run(transport="stdio")
