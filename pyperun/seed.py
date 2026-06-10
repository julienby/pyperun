"""Seed the **DEMO reference dataset** — pyperun's canonical test fixture.

Generates a small, deterministic, synthetic valvometric dataset and the matching
`demo` flow, so any pyperun install can be exercised end-to-end. Importable as a
library (`from pyperun.seed import run_seed`) and exposed on the CLI as
`pyperun seed-demo` — so it ships inside the Docker image and needs no local
Python.

The data is intentionally *imperfect* so every pipeline step has something to do:
  • bio_signal (m0..m11, int 0..800) — diurnal sine + noise
  • injected SPIKES   (> clean.spike_threshold)  → exercises clean's spike removal
  • injected DUPLICATE timestamps                → exercises clean.drop_duplicates
  • injected short GAPS (< resample.max_gap_fill) → exercises resample ffill
  • environment (outdoor_temp, float) — slow diurnal drift + occasional spike

It is deterministic (fixed seed): re-seeding reproduces byte-identical raw files,
so the DEMO dataset is a stable regression baseline. Grow it over time (more
devices, more edge cases) to widen coverage — that is the whole point of having
one reference dataset.

Raw filename convention (see pyperun.core.filename.parse_raw_stem):
    DEMO_<device>_<YYYY-MM-DD>.csv     → experience=DEMO, device_id=<device>
Raw line format (kv_csv, semicolon, no header):
    2026-01-20T09:07:58.000000Z;m0:312;m1:288;...;outdoor_temp:18.94
"""
from __future__ import annotations

import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATASET = "DEMO"
FLOW = "demo"
# On-disk pipeline only — to_postgres is external and needs a live DB, so we skip
# it in the reference flow. Every other step writes to disk and is fully testable.
DEMO_STEPS = [
    "parse", "clean", "resample", "transform", "normalize", "aggregate",
    "exportcsv", "exportparquet", "exportduckdb",
]
N_CHANNELS = 12  # m0..m11

DEFAULT_DEVICES = ["valve01", "valve02"]
DEFAULT_DAYS = 3
DEFAULT_HOURS = 1
DEFAULT_START_DATE = "2026-01-20"
DEFAULT_SEED = 42


def _treatments_root() -> Path:
    from pyperun.core.runner import TREATMENTS_ROOT
    return TREATMENTS_ROOT


def gen_device_day(device: str, day: datetime, hours: int, rng: random.Random) -> list[str]:
    """One day of 1 Hz raw lines for a device, with injected imperfections."""
    lines: list[str] = []
    phases = [rng.uniform(0, 2 * math.pi) for _ in range(N_CHANNELS)]
    bases = [rng.randint(250, 380) for _ in range(N_CHANNELS)]
    total_s = hours * 3600
    t = 0
    while t < total_s:
        # ~0.4% of seconds dropped → short gaps (resample max_gap_fill_s default 20)
        if rng.random() < 0.004:
            t += rng.randint(2, 8)
            continue
        ts = day + timedelta(seconds=t)
        ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S.000000Z")

        kv = []
        for i in range(N_CHANNELS):
            val = bases[i] + 200 * math.sin(2 * math.pi * t / 3600 + phases[i])
            val += rng.gauss(0, 12)
            # ~0.2%/channel spike well above clean.spike_threshold (100)
            if rng.random() < 0.002:
                val += rng.choice((-1, 1)) * rng.randint(180, 300)
            kv.append(f"m{i}:{max(0, min(800, round(val)))}")

        temp = 18 + 6 * math.sin(2 * math.pi * t / 86400)
        temp += rng.gauss(0, 0.25)
        if rng.random() < 0.0015:  # spike > environment.spike_threshold (5.0)
            temp += rng.choice((-1, 1)) * rng.uniform(8, 14)
        kv.append(f"outdoor_temp:{round(temp, 2)}")

        line = ts_str + ";" + ";".join(kv)
        lines.append(line)
        # ~0.3% duplicate timestamps → exercises clean.drop_duplicates
        if rng.random() < 0.003:
            lines.append(line)
        t += 1
    return lines


def build_flow(target: Path) -> dict:
    """Build flows/demo.json from each treatment's declared defaults."""
    from pyperun.core.pipeline import PIPELINE_STEPS
    troot = _treatments_root()
    by_name = {s["treatment"]: s for s in PIPELINE_STEPS}

    steps = []
    for name in DEMO_STEPS:
        s = by_name[name]
        entry = {"treatment": name, "input": s["input"]}
        if "output" in s:
            entry["output"] = s["output"]
        tj = troot / name / "treatment.json"
        if tj.exists():
            t = json.loads(tj.read_text())
            params = {k: v["default"] for k, v in t.get("params", {}).items()}
            # The reference flow re-fits normalize on every run so it is
            # reproducible from scratch (no carried-over fit state).
            if name == "normalize":
                params["fit"] = True
            if params:
                entry["params"] = params
        steps.append(entry)

    return {
        "name": FLOW,
        "description": "Pyperun reference DEMO dataset — canonical end-to-end test fixture.",
        "dataset": DATASET,
        "params": {},
        "steps": steps,
    }


def run_seed(
    target: str | Path = ".",
    *,
    devices: list[str] | None = None,
    days: int = DEFAULT_DAYS,
    hours: int = DEFAULT_HOURS,
    start_date: str = DEFAULT_START_DATE,
    seed: int = DEFAULT_SEED,
    force: bool = False,
) -> dict:
    """Seed the DEMO dataset + flow into ``target`` (a dir holding flows/ + datasets/).

    Returns a summary dict: ``{raw_dir, flow_path, n_devices, n_days, n_hours,
    n_lines, n_steps}``. Raises ``FileExistsError`` if raw files already exist and
    ``force`` is False.
    """
    devices = devices or list(DEFAULT_DEVICES)
    target = Path(target).resolve()
    rng = random.Random(seed)
    start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    raw_dir = target / "datasets" / DATASET / "00_raw"
    if raw_dir.exists() and any(raw_dir.glob("*.csv")) and not force:
        raise FileExistsError(f"Refusing to overwrite existing raw in {raw_dir} (use force=True).")
    raw_dir.mkdir(parents=True, exist_ok=True)

    n_lines = 0
    for d in range(days):
        day = start + timedelta(days=d)
        for device in devices:
            lines = gen_device_day(device, day, hours, rng)
            fname = f"{DATASET}_{device}_{day:%Y-%m-%d}.csv"
            (raw_dir / fname).write_text("\n".join(lines) + "\n")
            n_lines += len(lines)

    flows_dir = target / "flows"
    flows_dir.mkdir(parents=True, exist_ok=True)
    flow = build_flow(target)
    flow_path = flows_dir / f"{FLOW}.json"
    flow_path.write_text(json.dumps(flow, indent=4) + "\n")

    return {
        "raw_dir": raw_dir,
        "flow_path": flow_path,
        "n_devices": len(devices),
        "n_days": days,
        "n_hours": hours,
        "n_lines": n_lines,
        "n_steps": len(flow["steps"]),
    }
