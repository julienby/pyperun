# Pyperun

> Minimal IoT time-series pipeline — from raw sensor CSV to aggregated parquet, PostgreSQL, and CSV/Parquet/DuckDB exports.

```
  raw CSV
     │
     ▼
  ┌───────┐  ┌───────┐  ┌──────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐
  │ parse │─▶│ clean │─▶│ resample │─▶│ transform │─▶│ normalize │─▶│ aggregate │─┐
  └───────┘  └───────┘  └──────────┘  └───────────┘  └───────────┘  └───────────┘ │
                                                                                   │
                                              ┌──────────────┬─────────┬──────────┼──────────┐
                                              ▼              ▼         ▼          ▼          ▼
                                          to_postgres    exportcsv  exportparquet  exportduckdb
                                          (Grafana)        (CSV)     (parquet)      (DuckDB)
```

Pyperun is a **framework**: install it once, then describe your experiment as a **flow** — a plain JSON file that sequences treatments, maps directories, and sets parameters. No code to write for standard pipelines.

**New here?** Follow the 5-minute [Getting Started](GETTING_STARTED.md) guide.

---

## Installation

### Docker (recommended) — one-liner, multi-instance, data-safe

```bash
curl -fsSL https://raw.githubusercontent.com/julienby/pyperun/master/install.sh | bash -s -- my-instance
```

Builds a shared `pyperun:latest` image once and starts an instance (UI + REST +
MCP + scheduler) at an auto-picked free port. Each instance lives in
`~/.pyperun/<instance>/` with its own `data/` and `.env`. **Re-running updates
the code and restarts; your `flows/`, `datasets/`, `logs/` are never
overwritten.** It prints the URL and access token at the end.

```bash
# Non-interactive overrides
PORT=8080 TOKEN=secret EMAIL=admin@example.org \
  curl -fsSL https://raw.githubusercontent.com/julienby/pyperun/master/install.sh | bash -s -- my-instance
```

### From source (CLI only)

```bash
git clone https://github.com/julienby/pyperun ~/pyperun
cd ~/pyperun
pip install -e .
```

```bash
# With dev tools (pytest, ruff)
pip install -e ".[dev]"
```

> **`pyperun` not found?** Add `~/.local/bin` to your PATH:
> ```bash
> echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc && source ~/.bashrc
> ```

All `pyperun` commands must be run from your **project directory** (the one containing `flows/` and `datasets/`).

---

## Quick start

**1. Initialize a dataset**

```bash
pyperun init MY-EXPERIMENT
```

Creates `datasets/MY-EXPERIMENT/00_raw/` and a flow template at `flows/my-experiment.json`.

> **On a Docker instance?** Don't run `pyperun init` inside the container — it
> mounts `flows/` read-only. Scaffold from the host with the `pyperun-init`
> helper (see [Authoring datasets on a Docker instance](#authoring-datasets-on-a-docker-instance)):
> ```bash
> pyperun-init my-instance MY-EXPERIMENT
> ```

**2. Drop your raw CSV files in**

```bash
cp /path/to/data/*.csv datasets/MY-EXPERIMENT/00_raw/
```

Expected format — no header, semicolon-delimited, key:value pairs:

```
2026-01-20T09:07:58.142308Z;m0:10;m1:12;outdoor_temp:18.94
2026-01-20T09:07:59.142308Z;m0:11;m1:13;outdoor_temp:18.95
```

**3. Run the pipeline**

```bash
pyperun flow my-experiment
```

**4. Check what's been processed**

```bash
pyperun status
```

```
my-experiment (MY-EXPERIMENT)
  parse       10_parsed      84 files   last: 2026-02-17
  clean       20_clean       84 files   last: 2026-02-17
  resample    25_resampled   84 files   last: 2026-02-17
  ...
  -> up-to-date
```

---

## Demo / reference dataset

`pyperun seed-demo` seeds **DEMO**, pyperun's canonical end-to-end test
fixture: a small, deterministic synthetic dataset (2 devices × 3 days, 1 Hz)
with injected spikes, duplicate timestamps and short gaps so every step does
real work. Use it to smoke-test an install or populate the UI.

```bash
pyperun seed-demo               # seeds datasets/DEMO/ + flows/demo.json
pyperun flow demo               # runs the full on-disk pipeline (postgres skipped)
```

For a Docker instance, `flows/` is read-only in the container — so seed from the
host with the `pyperun-seed-demo` helper (no local Python; runs `seed-demo` in a
throwaway container as you), then run inside:

```bash
cp scripts/pyperun-seed-demo ~/.local/bin/ && chmod +x ~/.local/bin/pyperun-seed-demo  # install once
pyperun-seed-demo my-instance
docker exec pyperun-my-instance pyperun flow demo
```

> The seeder ships inside the image, so `pyperun seed-demo` also works directly
> via `docker exec` — but that writes the demo flow to the read-only `flows/`
> mount and would fail; `pyperun-seed-demo` runs it host-side instead.

The old `python scripts/seed_demo.py [--target DIR]` still works from a source
checkout (it now forwards to `pyperun seed-demo`).

This is **the** reference dataset — grow it over time (more devices, edge cases)
to widen coverage.

---

## CLI reference

### `pyperun flow <name>` — run a pipeline

```bash
pyperun flow my-experiment                            # full run
pyperun flow my-experiment --step clean               # single step
pyperun flow my-experiment --from-step resample       # from a step to the end
pyperun flow my-experiment --from-step clean --to-step aggregate  # range

pyperun flow my-experiment --from 2026-02-01T00:00:00Z --to 2026-02-10T00:00:00Z  # time filter
pyperun flow my-experiment --from 2026-02-01T00:00:00Z             # from a date to the latest

pyperun flow my-experiment --output-mode replace                    # delete files in window then write (default)
pyperun flow my-experiment --output-mode reset                      # wipe all outputs then reprocess
pyperun flow my-experiment --dry-run                                # preview without running
```

Each run prints a `run_id` (8-char hex) that can be used to retrieve its events from the log:

```
[flow] Starting 'my-experiment' (6 steps)  run_id=a3f9b2c1
[flow] Step 1/6: parse
  ...
[flow] Completed 'my-experiment' successfully  run_id=a3f9b2c1
```

### `pyperun run <treatment>` — run a single treatment

```bash
pyperun run parse \
    --input  datasets/MY-EXPERIMENT/00_raw \
    --output datasets/MY-EXPERIMENT/10_parsed

pyperun run aggregate \
    --input  datasets/MY-EXPERIMENT/30_transform \
    --output datasets/MY-EXPERIMENT/40_aggregated \
    --params '{"windows": ["30s", "5min"], "metrics": ["mean", "median"]}'
```

### Query commands — `--format json`

All read-only commands accept `--format json` for machine-readable output:

```bash
pyperun list flows                         # human text
pyperun list flows --format json           # JSON array

pyperun list treatments --format json
pyperun list steps --flow my-flow --format json
pyperun describe aggregate --format json
pyperun status --format json
```

`--format json` outputs valid JSON on stdout, suitable for piping, scripting, and API wrappers.

### Other commands

```bash
pyperun init MY-EXPERIMENT          # scaffold a new dataset
pyperun status                      # show processing state for all datasets
pyperun list flows                  # list available flows
pyperun list treatments             # list available treatments
pyperun list steps --flow my-flow   # list steps in a flow
pyperun describe <treatment>        # show params and formats for a treatment
pyperun export MY-EXPERIMENT        # export dataset to a portable archive
pyperun import my-archive.tar.gz    # import on another server
pyperun delete MY-EXPERIMENT        # delete dataset and its flows
pyperun serve                       # run the unified server (UI + REST + MCP + scheduler)
pyperun upgrade                     # pull latest pyperun and reinstall
```

---

## Flow format

A flow is a JSON file in `flows/`. Each step declares its treatment, input directory, and output directory.

```json
{
    "name": "my-experiment",
    "description": "Full pipeline for MY-EXPERIMENT",
    "dataset": "MY-EXPERIMENT",
    "params": {
        "from": "2026-02-01T00:00:00Z"
    },
    "steps": [
        {"treatment": "parse",     "input": "00_raw",       "output": "10_parsed"},
        {"treatment": "clean",     "input": "10_parsed",    "output": "20_clean"},
        {"treatment": "resample",  "input": "20_clean",     "output": "25_resampled"},
        {"treatment": "transform", "input": "25_resampled", "output": "30_transform"},
        {"treatment": "aggregate", "input": "30_transform", "output": "40_aggregated"},
        {
            "treatment": "to_postgres",
            "input": "40_aggregated",
            "output": "60_postgres",
            "params": {
                "host": "my-server",
                "dbname": "mydb",
                "user": "myuser",
                "password": "mypass",
                "table_template": "MY_EXPERIMENT__AGGREGATED__{aggregation}"
            }
        },
        {
            "treatment": "exportcsv",
            "input": "40_aggregated",
            "output": "61_exportcsv",
            "params": {
                "columns": {
                    "m0__raw__mean": "c0",
                    "m1__raw__mean": "c1",
                    "outdoor_temp__raw__mean": "temperature"
                }
            }
        }
    ]
}
```

When `dataset` is set, `input`/`output` paths are relative to `datasets/<DATASET>/`. Without it, use absolute paths.

### Params hierarchy

| Priority | Source | Scope |
|----------|--------|-------|
| lowest | `treatment.json` defaults | treatment built-in |
| | `flow.params` | all steps |
| | `step.params` | that step only |
| highest | CLI `--params` / `--from` / `--to` | runtime override |

`from`/`to` in `flow.params` set the default time range for all steps; CLI `--from`/`--to` always win.

---

## Pipeline steps

| Treatment | Input → Output | What it does |
|-----------|---------------|--------------|
| `parse` | `00_raw` → `10_parsed` | Parse key:value CSV → typed parquet, split by domain and day |
| `clean` | `10_parsed` → `20_clean` | Drop duplicates, clamp to min/max, remove spikes (rolling median) |
| `resample` | `20_clean` → `25_resampled` | Regular 1s grid, forward-fill short gaps |
| `transform` | `25_resampled` → `30_transform` | Apply column transforms: `sqrt_inv`, `cbrt_inv`, `log` (add or replace) |
| `normalize` | `30_transform` → `35_normalized` | Per-device percentile normalization (fit once, apply incrementally) |
| `aggregate` | `35_normalized` → `40_aggregated` | Multi-window aggregation (1s, 10s, 60s, 5min, 1h) |
| `to_postgres` | `40_aggregated` → PostgreSQL | Export to wide PostgreSQL tables (e.g. for Grafana) |
| `exportcsv` | `40_aggregated` → `61_exportcsv` | Export per-device CSV with column renaming and timezone conversion |
| `exportparquet` | `40_aggregated` → `62_exportparquet` | Export selected aggregation windows to parquet |
| `exportduckdb` | `40_aggregated` → `63_exportduckdb` | Export all devices into one DuckDB file (one table per window). Needs `pip install pyperun[duckdb]` |

---

## Treatment configuration

Each treatment exposes typed params with defaults, overridable in the flow or via `--params`.

<details>
<summary><strong>parse</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `delimiter` | `";"` | CSV delimiter |
| `tz` | `"UTC"` | Timezone of raw timestamps |
| `timestamp_column` | `"ts"` | Name of the timestamp field |
| `domains` | bio_signal + environment | Domain split: prefix-based or explicit columns, with dtype |
| `file_name_substitute` | `[]` | Filename substitutions for source name extraction |

</details>

<details>
<summary><strong>clean</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `drop_duplicates` | `true` | Remove duplicate timestamps |
| `domains` | per-domain | `min_value`, `max_value`, `spike_window`, `spike_threshold` per domain |

</details>

<details>
<summary><strong>resample</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `freq` | `"1s"` | Resample frequency |
| `max_gap_fill_s` | `20` | Max gap (seconds) to forward-fill |
| `agg_method` | per-domain | Aggregation method when flooring to `freq` |

</details>

<details>
<summary><strong>transform</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `transforms` | `[]` | List of `{function, target, mode}` — functions: `sqrt_inv`, `cbrt_inv`, `log`; mode: `add` or `replace` |

</details>

<details>
<summary><strong>aggregate</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `windows` | `["1s","10s","60s","5min","1h"]` | Time windows |
| `metrics` | `["mean","std"]` | Aggregation functions |

</details>

<details>
<summary><strong>to_postgres</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `host` | `"localhost"` | PostgreSQL host |
| `port` | `5432` | PostgreSQL port |
| `dbname` | *(required)* | Database name |
| `user` | *(required)* | User |
| `password` | *(required)* | Password |
| `table_template` | `"{source}__{domain}__{aggregation}"` | Table naming pattern |
| `table_prefix` | `""` | Prefix prepended to table names |
| `mode` | `"append"` | `append`, `replace`, or `reset` |

</details>

<details>
<summary><strong>exportcsv</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `aggregation` | `"10s"` | Which aggregation window to export |
| `domain` | `"bio_signal"` | Domain to export |
| `tz` | `"Europe/Paris"` | Output timezone |
| `from` / `to` | none | Optional date range filter |
| `columns` | m0–m11 as int | `source_col → export_name` or `{"name":…,"dtype":"int","decimals":N}` |

</details>

---

## External integration — Python API, REST & MCP

Pyperun exposes a clean Python API in `pyperun.core.api` so external tools (the server, scripts, AI agents) can query state and trigger runs **without going through the CLI**.

### Python API

```python
from pyperun.core.api import (
    # Discovery
    list_flows,           # → [{name, description, dataset, n_steps}]
    list_steps,           # list_steps("my-flow") → [{index, treatment, name, input, output, params}]
    list_treatments,      # → [{name, description}]
    describe_treatment,   # describe_treatment("parse") → {name, description, params: [...]}
    list_presets,         # → [{name, description, steps}]
    # State
    get_status,           # → [{flow, dataset, status, steps: [{treatment, n_files, last_modified}]}]
    # Dataset lifecycle
    init_dataset,         # init_dataset("MY-EXP", preset="full") → {dataset, flow, created_dirs, ...}
    delete_dataset,       # delete_dataset("MY-EXP") → {deleted_dirs, deleted_flows, ...}
    # Run control (non-blocking)
    launch_flow,          # launch_flow("my-flow", time_from=...) → run_id  (subprocess, returns immediately)
    list_running,         # → [{flow, run_id, step_index, steps_total, current_step, pid}]
    stop_flow,            # stop_flow("my-flow") → {stopped: bool, ...}  (graceful SIGTERM between steps)
    # Run history & summaries
    list_runs,            # list_runs(limit=50) → [{run_id, flow, started_at, status, n_steps_done}]
    get_run_events,       # get_run_events("a3f9b2c1") → [{ts, treatment, status, duration_ms, ...}]
    get_flow_summary,     # get_flow_summary("my-flow") → latest.json triage (O(1)) or None
    list_flow_summaries,  # → [{flow, status, ts_start, ...}]  sorted by ts_start desc
)
```

All functions return plain dicts/lists — no printing, no side effects. Import them directly.

### Unified server — `pyperun serve`

One process serves three façades plus the scheduler (no separate containers):

```
/            → web UI (SPA)
/api/*       → REST API
/mcp         → MCP server (SSE, for LLM agents)
+ in-process scheduler tick
```

```bash
pip install -e ".[server]"      # fastapi + uvicorn + mcp + croniter
pyperun serve                   # 0.0.0.0:8000
pyperun serve --port 9000
```

Flows run as **isolated subprocesses** — launching one returns a `run_id`
immediately (non-blocking); poll for progress. Different flows run in parallel;
the same flow can't overlap (per-flow lockfile). PostgreSQL/Grafana stay
**external** — the server never bundles them.

**Auth (optional).** Set `PYPERUN_TOKEN` to gate every façade. The token can be
supplied via `Authorization: Bearer <token>`, `X-Pyperun-Token: <token>`,
`?token=<token>` (browsers — sets a cookie), or the cookie. Refused requests
return 401 and log the client IP (honouring `X-Forwarded-For`) so an external
fail2ban filter can ban it. Set `PYPERUN_EMAIL` to show a contact on the 401
page. `/health` is always open.

```bash
PYPERUN_TOKEN=secret PYPERUN_EMAIL=admin@example.org pyperun serve
```

**Endpoints:**

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check (always open) |
| `GET` | `/api/flows` | List flows |
| `GET` | `/api/flows/<flow>/steps` | Steps of a flow (passwords masked) |
| `GET` | `/api/treatments` · `/api/treatments/<name>` | List / describe treatments |
| `GET` | `/api/presets` | List available presets |
| `GET` | `/api/status` | Pipeline state for all datasets |
| `POST` | `/api/datasets` | Create a new dataset (init) |
| `DELETE` | `/api/datasets/<dataset>` | Delete a dataset and its flows |
| `POST` | `/api/run/<flow>` | Launch a flow → returns `run_id` immediately (202) |
| `POST` | `/api/stop/<flow>` | Graceful stop (SIGTERM between steps) |
| `GET` | `/api/running` | Flows running now, with live step k/N progress |
| `GET` | `/api/runs?limit=50` · `/api/runs/<run_id>` | Run history / events (for polling) |
| `GET` | `/api/summaries` · `/api/summaries/<flow>` | Latest-run triage (`latest.json`) |
| `*` | `/mcp` | MCP server (SSE) — same tools, for LLM agents |

```bash
# Launch a flow (auth header only needed if PYPERUN_TOKEN is set)
curl -X POST http://localhost:8000/api/run/my-experiment \
     -H "Authorization: Bearer secret" -H "Content-Type: application/json" \
     -d '{"from": "2026-04-01T00:00:00Z", "to": "2026-04-27T00:00:00Z"}'
# → 202 {"run_id": "a3f9b2c1", "flow": "my-experiment", "status": "started"}

# Poll progress until status = success|error
curl http://localhost:8000/api/runs/a3f9b2c1
# → {"status": "running", "n_steps_done": 3, "n_steps_total": 6, "events": [...]}

# What's running right now?
curl http://localhost:8000/api/running
# → [{"flow": "my-experiment", "run_id": "a3f9b2c1", "step_index": 3, "steps_total": 6, ...}]

# Stop it
curl -X POST http://localhost:8000/api/stop/my-experiment   # → {"stopped": true, ...}
```

Optional `POST /api/run/<flow>` body fields:

| Field | Type | Description |
|-------|------|-------------|
| `from` / `to` | ISO 8601 string | Time window |
| `step` | string | Run a single named step |
| `from_step` / `to_step` | string | Start / stop at this step |
| `output_mode` | string | `replace` (default) or `reset` |
| `params` | object | Per-run param overrides |

### MCP (for LLM agents)

The same operations are exposed as MCP tools at `/mcp` (SSE). Point an MCP client at
`http://<host>:8000/mcp/sse`. Standalone (no web UI): `python -m pyperun.mcp --sse`
(needs `pip install pyperun[mcp]`).

### Docker

One container runs everything (UI + REST + MCP + scheduler). PostgreSQL/Grafana stay external.

The easiest path is the [one-liner installer](#docker-recommended--one-liner-multi-instance-data-safe)
(handles build, port, token, multi-instance, data-safe updates). To drive
Compose yourself instead:

```bash
PYPERUN_TOKEN=secret PYPERUN_EMAIL=admin@example.org docker compose up -d
# → http://localhost:8000/  ·  /api/*  ·  /mcp/sse
```

`flows/` is mounted read-only (it may hold credentials); `datasets/` and `logs/`
are read-write. Put a reverse proxy (Caddy/nginx) in front for TLS.

#### Authoring datasets on a Docker instance

Because `flows/` is read-only inside the service container, `pyperun init` can't
run there. The `pyperun-init` helper (in `scripts/`) scaffolds a dataset on the
**host** instead — it runs `init` in a throwaway container *as your user*,
mounting the instance's data dir as the working directory, so the flow and
dataset land on the host owned by you and editable. The long-running service
just reads them.

```bash
# Install once (from a source checkout)
cp scripts/pyperun-init ~/.local/bin/ && chmod +x ~/.local/bin/pyperun-init

# Scaffold a dataset on an installed instance
pyperun-init my-instance MY-EXPERIMENT
pyperun-init my-instance MY-EXPERIMENT --preset csv   # extra args pass through to `pyperun init`
```

It prints where to drop your raw CSV, the flow file to edit, and the
`docker exec … pyperun flow …` command to run the pipeline. Override the
instances root with `PYPERUN_HOME` (default `~/.pyperun`) or the image with
`PYPERUN_IMAGE` (default `pyperun:latest`).

---

## Custom treatments

Pyperun discovers treatments from two locations — **local takes priority**:

1. `./treatments/<name>/` — your project (custom or overrides)
2. `pyperun/treatments/<name>/` — built-in fallback

To add a custom treatment:

```
my-project/
  treatments/
    my_treatment/
      treatment.json    # param schema + defaults
      run.py            # def run(input_dir, output_dir, params): ...
```

Scaffold with: `pyperun new my_treatment`

---

## Project layout

```
pyperun/                          ← framework (this repo)
  cli.py                          ← pyperun command entry point
  server.py                       ← unified ASGI server (UI + REST + MCP + scheduler)
  mcp.py                          ← MCP tools (LLM-agent interface)
  core/
    flow.py                       ← runs a flow sequentially, returns run_id
    runner.py                     ← runs a single treatment
    pipeline.py                   ← treatment → directory registry
    validator.py                  ← param validation + merging
    timefilter.py                 ← time range filtering by date range
    filename.py                   ← parquet naming conventions
    logger.py                     ← 2-layer event log (latest.json + daily .jsonl)
    scheduler.py                  ← cron tick (croniter)
    api.py                        ← Python API (list_flows, launch_flow, list_running, ...)
  treatments/                     ← built-in treatments
    parse/ clean/ resample/ transform/ normalize/
    aggregate/ to_postgres/ exportcsv/ exportparquet/ exportduckdb/
scripts/
  hourly_sync.sh                  ← cron: incremental run for one flow
  run_scheduled_flows.sh          ← cron: run all flows in scheduled_flows.txt
  run_flow_hourly.sh              ← loop: run a flow every N seconds
  update.sh                       ← git pull + pip install -e .
```

```
my-project/                       ← your experiment repo
  flows/                          ← flow definitions (JSON)
  datasets/                       ← data (gitignored)
    MY-EXPERIMENT/
      00_raw/                     ← raw CSV input
      10_parsed/
      20_clean/
      25_resampled/
      30_transform/
      35_normalized/
      40_aggregated/
      61_exportcsv/  62_exportparquet/  63_exportduckdb/
  treatments/                     ← optional custom treatments
  logs/
    flows/<flow>/
      latest.json                 ← O(1) triage of the last run
      2026-05-20.jsonl            ← per-day treatment events
    misc/2026-05-20.jsonl         ← runs launched without a flow
```

---

## Production — incremental cron

Run the pipeline automatically every hour with a rolling 2-hour window:

```bash
crontab -e
# Add:
0 * * * * /path/to/pyperun/scripts/hourly_sync.sh my-flow >> /var/log/pyperun.log 2>&1
```

Or run multiple flows from a list:

```bash
# scripts/scheduled_flows.txt
my-flow-streaming
my-flow-daily

# crontab
0 * * * * /path/to/pyperun/scripts/run_scheduled_flows.sh
```

Both scripts compute explicit `--from`/`--to` date windows at runtime to avoid reprocessing old data.

---

## Development

```bash
pip install -e ".[dev]"

pytest tests/ -v                                      # full test suite
pytest tests/test_runner.py::test_run_with_defaults   # single test
ruff check .                                          # lint
```

---

## Data conventions

- **Parquet filenames**: `<source>__<domain>__<YYYY-MM-DD>.parquet`
- **Aggregated filenames**: `<source>__<domain>__<YYYY-MM-DD>__<window>.parquet`
- **Default domains**: `bio_signal` (m0–m11, Int64) · `environment` (outdoor_temp, Float64)
- **Logs** (2-layer): `logs/flows/<flow>/latest.json` for O(1) triage of the last run, plus daily `logs/flows/<flow>/<YYYY-MM-DD>.jsonl` with one event per treatment step (grouped by `run_id`). `.jsonl` files older than 30 days are pruned automatically.
