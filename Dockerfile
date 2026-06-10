FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
COPY pyperun/ pyperun/

RUN pip install --no-cache-dir -e ".[server,duckdb]"

# Bake the static UI into the image so a shared pyperun:latest is self-contained
# (each instance then only mounts its own data — no source clone needed at runtime).
COPY ui_tests/ ui_tests/

# Volumes mounted at runtime: flows/ datasets/ logs/ schedules.json
EXPOSE 8000

# One process: web UI (/) + REST (/api/*) + MCP (/mcp) + in-process scheduler.
CMD ["pyperun", "serve", "--host", "0.0.0.0", "--port", "8000"]
