#!/usr/bin/env python3
"""Show row counts per day and per step for a dataset within a date range.

Stats are broken down by sensor (one table per sensor file).

Usage:
    python scripts/rowcount.py DATASET --from 2026-04-10 --to 2026-04-23
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

STEPS = [
    "00_raw",
    "10_parsed",
    "20_clean",
    "25_resampled",
    "30_transform",
    "35_normalized",
    "40_aggregated",
]

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def count_rows(path: Path) -> int:
    if path.suffix == ".parquet":
        return pq.read_metadata(path).num_rows
    if path.suffix == ".csv":
        with open(path, "rb") as f:
            return sum(1 for _ in f)
    return 0


def extract_sensor(stem: str, dataset_name: str) -> str | None:
    """Extract the sensor name from a file stem.

    Handles two naming conventions:
      - new:  DATASET__SENSOR__step__date  (double-underscore separated)
      - old:  DATASET_SENSOR_step_date     (single-underscore separated)
    """
    if "__" in stem:
        parts = stem.split("__")
        if len(parts) >= 2 and parts[0] == dataset_name:
            return parts[1]
    else:
        prefix = dataset_name + "_"
        if stem.startswith(prefix):
            remainder = stem[len(prefix):]
            return remainder.split("_")[0]
    return None


def collect(
    dataset_path: Path, step: str, dataset_name: str, date_from: str, date_to: str
) -> dict[str, dict[str, int]]:
    """Return rows_by_sensor[sensor][day] = row_count."""
    step_path = dataset_path / step
    if not step_path.exists():
        return {}

    rows: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for f in step_path.rglob("*"):
        if f.suffix not in (".parquet", ".csv"):
            continue
        m = DATE_RE.search(f.name)
        if not m:
            continue
        day = m.group(1)
        if day < date_from or day > date_to:
            continue
        sensor = extract_sensor(f.stem, dataset_name)
        if sensor is None:
            sensor = "(unknown)"
        rows[sensor][day] += count_rows(f)

    return {s: dict(d) for s, d in rows.items()}


def print_sensor_table(
    sensor: str,
    steps: list[str],
    step_labels: list[str],
    data: dict[str, dict[str, dict[str, int]]],
    col_w: int,
) -> None:
    all_days: set[str] = set()
    for step in steps:
        all_days |= data[step].get(sensor, {}).keys()

    if not all_days:
        return

    print(f"\n=== {sensor} ===")
    header = f"{'Day':<12}" + "".join(f"{lbl:>{col_w}}" for lbl in step_labels)
    print(header)
    print("-" * len(header))

    for day in sorted(all_days):
        row = f"{day:<12}"
        for step in steps:
            n = data[step].get(sensor, {}).get(day)
            row += f"{n if n is not None else '-':>{col_w}}"
        print(row)


def main():
    parser = argparse.ArgumentParser(description="Row counts per sensor, day, and step")
    parser.add_argument("dataset", help="Dataset name (e.g. Expo_pre_GRACE_2)")
    parser.add_argument("--from", dest="date_from", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--steps", nargs="+", default=None, help="Steps to show (default: all)")
    parser.add_argument("--sensor", help="Show only this sensor")
    args = parser.parse_args()

    root = Path(__file__).parent.parent / "datasets" / args.dataset
    if not root.exists():
        print(f"Dataset not found: {root}", file=sys.stderr)
        sys.exit(1)

    steps = args.steps or STEPS
    steps = [s for s in steps if (root / s).exists()]

    # Collect all data: step -> sensor -> day -> rows
    data: dict[str, dict[str, dict[str, int]]] = {}
    all_sensors: set[str] = set()
    for step in steps:
        data[step] = collect(root, step, args.dataset, args.date_from, args.date_to)
        all_sensors |= data[step].keys()

    if not all_sensors:
        print("No data found for this date range.")
        sys.exit(0)

    sensors = sorted(all_sensors)
    if args.sensor:
        sensors = [s for s in sensors if s == args.sensor]
        if not sensors:
            print(f"Sensor '{args.sensor}' not found.", file=sys.stderr)
            sys.exit(1)

    step_labels = [s.split("_", 1)[-1] if "_" in s else s for s in steps]
    col_w = max(10, *(len(lbl) for lbl in step_labels)) + 2

    for sensor in sensors:
        print_sensor_table(sensor, steps, step_labels, data, col_w)


if __name__ == "__main__":
    main()
