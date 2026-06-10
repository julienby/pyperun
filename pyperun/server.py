"""Pyperun unified ASGI server — one process, three façades + scheduler.

Folds everything into a single uvicorn process:

    /            → static UI (SPA, the ui_tests/ mockups for now)
    /api/*       → REST API (wraps pyperun.core.api)
    /mcp         → MCP server (SSE transport, for LLM agents)
    + in-process scheduler tick (replaces the standalone `pyperun tick` loop)

Run:
    pyperun serve                      # 0.0.0.0:8000
    pyperun serve --port 9000
    PYPERUN_TOKEN=secret pyperun serve # require auth on every façade

Auth (optional): if PYPERUN_TOKEN is set, every request to /api/*, /mcp and the
UI must carry the token via one of:
    Authorization: Bearer <token>      (agents, curl, the SPA's fetch calls)
    X-Pyperun-Token: <token>           (header alternative)
    ?token=<token>                     (browser first hit — sets a cookie)
    Cookie: pyperun_token=<token>      (browser subsequent navigation)
Refused attempts return 401 and are logged with the client IP so an external
fail2ban filter can ban the address. /health is always open (liveness probe).

Postgres/Grafana stay EXTERNAL — this server never bundles them.
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

import pyperun.core.api as api

log = logging.getLogger("pyperun.server")

UI_DIR = Path(os.environ.get("PYPERUN_UI_DIR", "ui_tests/version2"))
TOKEN = os.environ.get("PYPERUN_TOKEN")
EMAIL = os.environ.get("PYPERUN_EMAIL", "")
TICK_INTERVAL_S = int(os.environ.get("PYPERUN_TICK_INTERVAL", "60"))
COOKIE_NAME = "pyperun_token"

_REDACT_PARAMS = {"password"}


def _redact(params: dict) -> dict:
    return {k: ("***" if k in _REDACT_PARAMS else v) for k, v in params.items()}


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

_401_PAGE = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<title>401 — Unauthorized</title><style>
body{{font-family:system-ui,sans-serif;background:#0f172a;color:#e2e8f0;
display:flex;align-items:center;justify-content:center;height:100vh;margin:0}}
.card{{text-align:center;max-width:28rem;padding:2rem}}
h1{{font-size:4rem;margin:0;color:#f43f5e}}
p{{color:#94a3b8;line-height:1.5}}
code{{background:#1e293b;padding:.15rem .4rem;border-radius:.3rem}}
</style></head><body><div class="card">
<h1>401</h1><p>This pyperun instance is protected. Provide a valid token via
<code>?token=…</code> or <code>Authorization: Bearer …</code>.</p>
{contact}</div></body></html>"""


def _client_ip(request: Request) -> str:
    # Honour reverse-proxy headers (Caddy/nginx) then fall back to socket peer.
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "?"


def _extract_token(request: Request) -> str | None:
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    hdr = request.headers.get("x-pyperun-token")
    if hdr:
        return hdr.strip()
    q = request.query_params.get("token")
    if q:
        return q.strip()
    return request.cookies.get(COOKIE_NAME)


class AuthMiddleware(BaseHTTPMiddleware):
    """Single shared-token gate over UI + REST + MCP. No-op if PYPERUN_TOKEN unset."""

    async def dispatch(self, request: Request, call_next):
        if not TOKEN or request.url.path == "/health":
            return await call_next(request)

        supplied = _extract_token(request)
        if supplied == TOKEN:
            response = await call_next(request)
            # Browser arrived with a valid ?token=… → (re)persist it so navigation
            # keeps working. Always overwrite: cookies are scoped by host, not port,
            # so a stale cookie from another instance on the same host (e.g. another
            # localhost:PORT) would otherwise shadow this instance's token and 401.
            if request.query_params.get("token"):
                response.set_cookie(COOKIE_NAME, TOKEN, httponly=True, samesite="lax")
            return response

        ip = _client_ip(request)
        log.warning("AUTH FAIL ip=%s path=%s method=%s", ip, request.url.path, request.method)
        wants_html = "text/html" in request.headers.get("accept", "")
        if wants_html:
            contact = f"<p>Contact <code>{EMAIL}</code>.</p>" if EMAIL else ""
            return HTMLResponse(_401_PAGE.format(contact=contact), status_code=401)
        return JSONResponse({"error": "Unauthorized"}, status_code=401)


# ---------------------------------------------------------------------------
# REST API  (/api/*)
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api")


@router.get("/flows")
def rest_list_flows():
    return api.list_flows()


@router.get("/flows/{flow_name}/steps")
def rest_list_steps(flow_name: str):
    try:
        steps = api.list_steps(flow_name)
    except FileNotFoundError:
        return JSONResponse({"error": f"Flow '{flow_name}' not found"}, status_code=404)
    for s in steps:
        if s.get("params"):
            s["params"] = _redact(s["params"])
    return steps


@router.get("/treatments")
def rest_list_treatments():
    return api.list_treatments()


@router.get("/treatments/{name}")
def rest_describe_treatment(name: str):
    try:
        return api.describe_treatment(name)
    except FileNotFoundError:
        return JSONResponse({"error": f"Treatment '{name}' not found"}, status_code=404)


@router.get("/presets")
def rest_list_presets():
    return api.list_presets()


@router.get("/status")
def rest_status():
    return api.get_status()


@router.get("/running")
def rest_running():
    """Flows currently running, with live step k/N progress (O7)."""
    return api.list_running()


@router.post("/datasets")
async def rest_init_dataset(request: Request):
    body = await request.json()
    dataset = body.get("dataset")
    if not dataset:
        return JSONResponse({"error": "'dataset' is required"}, status_code=400)
    try:
        result = api.init_dataset(
            dataset=dataset,
            preset=body.get("preset", "full"),
            flow_name=body.get("flow_name"),
            raw=body.get("raw"),
            force=body.get("force", False),
        )
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except FileExistsError as e:
        return JSONResponse({"error": str(e)}, status_code=409)
    except FileNotFoundError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return JSONResponse(result, status_code=201)


@router.delete("/datasets/{dataset}")
def rest_delete_dataset(dataset: str):
    try:
        return api.delete_dataset(dataset=dataset)
    except FileNotFoundError as e:
        return JSONResponse({"error": str(e)}, status_code=404)


@router.post("/run/{flow_name}")
async def rest_run_flow(request: Request, flow_name: str):
    """Launch a flow in the background; return run_id immediately (202)."""
    body = {}
    if await request.body():
        body = await request.json()
    if body.get("step") and (body.get("from_step") or body.get("to_step")):
        return JSONResponse(
            {"error": "'step' is mutually exclusive with 'from_step'/'to_step'"},
            status_code=400,
        )
    try:
        run_id = api.launch_flow(
            flow_name,
            time_from=body.get("from"),
            time_to=body.get("to"),
            step=body.get("step"),
            from_step=body.get("from_step"),
            to_step=body.get("to_step"),
            output_mode=body.get("output_mode", "replace"),
            params_override=body.get("params"),
        )
    except FileNotFoundError:
        return JSONResponse({"error": f"Flow '{flow_name}' not found"}, status_code=404)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return JSONResponse(
        {"run_id": run_id, "flow": flow_name, "status": "started"}, status_code=202
    )


@router.post("/stop/{flow_name}")
def rest_stop_flow(flow_name: str):
    """Request a graceful stop (SIGTERM) of a running flow (A5)."""
    result = api.stop_flow(flow_name)
    code = 200 if result.get("stopped") else 409
    return JSONResponse(result, status_code=code)


@router.get("/runs")
def rest_list_runs(limit: int = 50):
    return api.list_runs(limit=limit)


@router.get("/runs/{run_id}")
def rest_run_events(run_id: str):
    events = api.get_run_events(run_id)
    if not events:
        return JSONResponse({"error": f"Run '{run_id}' not found"}, status_code=404)
    for e in events:
        if e.get("params"):
            e["params"] = _redact(e["params"])
    flow_name = events[0].get("flow")
    has_error = any(e["status"] == "error" for e in events)
    n_done = sum(1 for e in events if e["status"] == "success")
    n_steps = 0
    if flow_name:
        try:
            n_steps = len(api.list_steps(flow_name))
        except FileNotFoundError:
            pass
    if has_error:
        run_status = "error"
    elif n_steps > 0 and n_done >= n_steps:
        run_status = "success"
    else:
        run_status = "running"
    return {
        "run_id": run_id,
        "flow": flow_name,
        "status": run_status,
        "n_steps_total": n_steps,
        "n_steps_done": n_done,
        "events": events,
    }


@router.get("/summaries")
def rest_summaries():
    return api.list_flow_summaries()


@router.get("/summaries/{flow}")
def rest_summary(flow: str):
    summary = api.get_flow_summary(flow)
    if summary is None:
        return JSONResponse({"error": f"No run for flow '{flow}'"}, status_code=404)
    return summary


@router.get("/schedules")
def rest_list_schedules():
    """List cron schedules (schedules.json) driving the internal scheduler."""
    return api.list_schedules()


@router.put("/schedules/{flow}")
async def rest_upsert_schedule(request: Request, flow: str):
    """Create or update a flow's cron schedule.

    Body: {schedule (cron), timezone?='UTC', enabled?=true}.
    """
    body = {}
    if await request.body():
        body = await request.json()
    schedule = body.get("schedule")
    if not schedule:
        return JSONResponse({"error": "'schedule' (cron) is required"}, status_code=400)
    try:
        result = api.upsert_schedule(
            flow,
            schedule,
            timezone=body.get("timezone", "UTC"),
            enabled=body.get("enabled", True),
        )
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    code = 201 if result["action"] == "created" else 200
    return JSONResponse(result, status_code=code)


@router.delete("/schedules/{flow}")
def rest_remove_schedule(flow: str):
    """Remove a flow's cron schedule."""
    result = api.remove_schedule(flow)
    if not result["removed"]:
        return JSONResponse({"error": f"No schedule for flow '{flow}'"}, status_code=404)
    return result


# ---------------------------------------------------------------------------
# Scheduler tick — folded into the ASGI process
# ---------------------------------------------------------------------------

async def _scheduler_loop():
    """Run the cron tick every TICK_INTERVAL_S without blocking the event loop."""
    try:
        from pyperun.core.scheduler import tick
    except ImportError:
        log.warning("croniter not installed — scheduler disabled (pip install pyperun[scheduler])")
        return
    log.info("scheduler started (every %ss)", TICK_INTERVAL_S)
    while True:
        try:
            await asyncio.to_thread(tick)
        except Exception:
            log.exception("scheduler tick failed")
        await asyncio.sleep(TICK_INTERVAL_S)


# ---------------------------------------------------------------------------
# App assembly
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _lifespan(app: FastAPI):
    task = asyncio.create_task(_scheduler_loop())
    try:
        yield
    finally:
        task.cancel()


def create_app() -> FastAPI:
    app = FastAPI(
        title="pyperun",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
        lifespan=_lifespan,
    )
    app.add_middleware(AuthMiddleware)

    @app.get("/health")
    def health():
        return {"status": "ok"}

    app.include_router(router)

    # MCP at /mcp (SSE). Imported lazily so a missing `mcp` package only breaks /mcp.
    try:
        from pyperun.mcp import mcp as mcp_server
        app.mount("/mcp", mcp_server.sse_app())
    except Exception as exc:  # pragma: no cover
        log.warning("MCP not mounted: %s", exc)

    @app.get("/", include_in_schema=False)
    def index():
        return RedirectResponse("/login.html")

    # Static UI last so /api and /mcp take precedence over the catch-all mount.
    if UI_DIR.is_dir():
        app.mount("/", StaticFiles(directory=str(UI_DIR), html=True), name="ui")

    return app


app = create_app()


def serve(host: str = "0.0.0.0", port: int = 8000) -> None:
    import uvicorn
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Pyperun unified ASGI server")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8000)
    args = p.parse_args()
    serve(host=args.host, port=args.port)
