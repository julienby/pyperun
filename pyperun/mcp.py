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
from mcp.server.fastmcp import FastMCP

import pyperun.core.api as api

mcp = FastMCP(
    name="pyperun",
    instructions=(
        "You have access to a pyperun IoT time-series pipeline. "
        "Use list_flows and get_status first to understand what is available. "
        "run_flow is blocking — it returns only after the pipeline completes. "
        "Use get_run_events with the returned run_id to inspect results or diagnose errors."
    ),
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
    """Launch a flow and wait for it to complete. Returns a run summary.

    This is a BLOCKING call — it returns only once the flow finishes (or fails).
    For long pipelines this can take several minutes.

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
    {run_id, status, n_steps_done, error}
    """
    overrides = None
    if params_override:
        try:
            overrides = json.loads(params_override)
        except json.JSONDecodeError as exc:
            return {"error": f"params_override is not valid JSON: {exc}"}

    try:
        run_id = api.run_flow(
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
    except RuntimeError as exc:
        summary = api.get_flow_summary(name)
        return {
            "run_id": summary["run_id"] if summary else None,
            "status": "error",
            "n_steps_done": summary["steps_ok"] if summary else 0,
            "error": str(exc),
        }

    events = api.get_run_events(run_id) if run_id else []
    return {
        "run_id": run_id,
        "status": "success",
        "n_steps_done": sum(1 for e in events if e.get("status") == "success"),
        "error": None,
    }


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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    if "--sse" in sys.argv:
        mcp.run(transport="sse")
    else:
        mcp.run(transport="stdio")
