#!/usr/bin/env python3
"""Seed the **DEMO reference dataset** — pyperun's canonical test fixture.

Generates a small, deterministic, synthetic valvometric dataset and the matching
`demo` flow, so any pyperun install can be exercised end-to-end with one command:

    python scripts/seed_demo.py                 # seed into the current repo
    python scripts/seed_demo.py --target DIR     # seed into an instance data dir
    pyperun flow demo                            # run the pipeline on it

The data is intentionally *imperfect* so every pipeline step has something to do:
  • bio_signal (m0..m11, int 0..800) — diurnal sine + noise
  • injected SPIKES   (> clean.spike_threshold)  → exercises clean's spike removal
  • injected DUPLICATE timestamps                → exercises clean.drop_duplicates
  • injected short GAPS (< resample.max_gap_fill) → exercises resample ffill
  • environment (outdoor_temp, float) — slow diurnal drift + occasional spike

It is deterministic (fixed --seed): re-seeding reproduces byte-identical raw files,
so the DEMO dataset is a stable regression baseline. Grow it over time (more
devices, more edge cases) to widen coverage — that is the whole point of having one
reference dataset.

Raw filename convention (see pyperun.core.filename.parse_raw_stem):
    DEMO_<device>_<YYYY-MM-DD>.csv     → experience=DEMO, device_id=<device>
Raw line format (kv_csv, semicolon, no header):
    2026-01-20T09:07:58.000000Z;m0:312;m1:288;...;outdoor_temp:18.94
"""
from __future__ import annotations

import argparse
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


def main() -> None:
    ap = argparse.ArgumentParser(description="Seed the DEMO reference dataset + flow.")
    ap.add_argument("--target", default=".",
                    help="Project/instance dir containing flows/ and datasets/ (default: cwd)")
    ap.add_argument("--devices", nargs="+", default=["valve01", "valve02"])
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--hours", type=int, default=1, help="Hours of 1 Hz data per device per day")
    ap.add_argument("--start-date", default="2026-01-20")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--force", action="store_true",
                    help="Overwrite existing DEMO raw files (default: refuse if present)")
    args = ap.parse_args()

    target = Path(args.target).resolve()
    rng = random.Random(args.seed)
    start = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    raw_dir = target / "datasets" / DATASET / "00_raw"
    if raw_dir.exists() and any(raw_dir.glob("*.csv")) and not args.force:
        raise SystemExit(f"Refusing to overwrite existing raw in {raw_dir} (use --force).")
    raw_dir.mkdir(parents=True, exist_ok=True)

    n_lines = 0
    for d in range(args.days):
        day = start + timedelta(days=d)
        for device in args.devices:
            lines = gen_device_day(device, day, args.hours, rng)
            fname = f"{DATASET}_{device}_{day:%Y-%m-%d}.csv"
            (raw_dir / fname).write_text("\n".join(lines) + "\n")
            n_lines += len(lines)

    # Flow
    flows_dir = target / "flows"
    flows_dir.mkdir(parents=True, exist_ok=True)
    flow = build_flow(target)
    (flows_dir / f"{FLOW}.json").write_text(json.dumps(flow, indent=4) + "\n")

    print(f"✓ DEMO seeded → {raw_dir}")
    print(f"  {len(args.devices)} device(s) × {args.days} day(s) × {args.hours}h  = {n_lines} raw lines")
    print(f"✓ flow → {flows_dir / (FLOW + '.json')}  ({len(flow['steps'])} steps, postgres skipped)")
    print(f"\nRun it:  pyperun flow {FLOW}")


if __name__ == "__main__":
    main()
