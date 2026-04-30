from collections import defaultdict
from pathlib import Path

import pandas as pd

from pyperun.core.filename import list_parquet_files, parse_parquet_path


def _find_existing_csvs(out_path: Path, experience: str, device_id: str, aggregation: str) -> list[Path]:
    pattern = f"{experience}_{device_id}_aggregated_{aggregation}_*.csv"
    return sorted(out_path.glob(pattern))


def run(input_dir: str, output_dir: str, params: dict) -> None:
    in_path = Path(input_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    aggregation = params["aggregation"]
    domain = params["domain"]
    tz = params["tz"]
    from_dt = pd.Timestamp(params["from"], tz="UTC") if params.get("from") else None
    to_dt = pd.Timestamp(params["to"], tz="UTC") if params.get("to") else None
    columns_raw = params["columns"]
    # Support both str and dict values: {"name": "c0", "dtype": "int"} or "c0"
    col_names = {src: (v["name"] if isinstance(v, dict) else v) for src, v in columns_raw.items()}
    col_dtypes = {(v["name"] if isinstance(v, dict) else v): v["dtype"] for src, v in columns_raw.items() if isinstance(v, dict) and "dtype" in v}
    col_decimals = {(v["name"] if isinstance(v, dict) else v): v["decimals"] for src, v in columns_raw.items() if isinstance(v, dict) and "decimals" in v}

    # Find matching parquet files (right domain + aggregation), grouped by device
    parquet_files = list_parquet_files(in_path)
    by_device = defaultdict(list)
    experience = None

    for pf in parquet_files:
        parts = parse_parquet_path(pf)
        if parts.domain != domain:
            continue
        if parts.aggregation != aggregation:
            continue
        by_device[parts.device_id].append(pf)
        if experience is None:
            experience = parts.experience

    if not by_device:
        print(f"  [exportcsv] No files matching domain={domain}, aggregation={aggregation}, skipping")
        return

    total_files = sum(len(v) for v in by_device.values())
    print(f"  [exportnour] Found {total_files} files, {len(by_device)} devices (domain={domain}, agg={aggregation})")

    for device_id, files in sorted(by_device.items()):
        frames = []
        for pf in files:
            df = pd.read_parquet(pf)
            if not df.empty:
                frames.append(df)

        if not frames:
            continue

        merged = pd.concat(frames, ignore_index=True)
        merged = merged.sort_values("ts").reset_index(drop=True)

        if from_dt is not None:
            merged = merged[merged["ts"] >= from_dt]
        if to_dt is not None:
            merged = merged[merged["ts"] < to_dt]

        if merged.empty:
            print(f"  [exportcsv] {device_id}: no data in range, skipping")
            continue

        # Select only columns that exist (warn about missing ones)
        missing = [c for c in col_names if c not in merged.columns]
        if missing:
            print(f"  [exportcsv] {device_id}: skipping missing columns: {missing}")
        available = {k: v for k, v in col_names.items() if k in merged.columns}
        if not available:
            print(f"  [exportcsv] {device_id}: no columns available, skipping")
            continue

        result = merged[["ts"] + list(available.keys())].copy()
        result = result.rename(columns=available)
        col_names_used = available

        # Cast columns with dtype specified (only if present)
        for col, dtype in col_dtypes.items():
            if col in result.columns and dtype == "int":
                result[col] = result[col].round().astype("Int64")

        # Round columns with decimals specified (only if present)
        for col, decimals in col_decimals.items():
            if col in result.columns:
                result[col] = result[col].round(decimals)

        # Convert timezone and format Time column
        result["ts"] = result["ts"].dt.tz_convert(tz)
        result["Time"] = result["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
        result = result.drop(columns=["ts"])

        # Reorder: Time first, then columns in declared order (only available ones)
        output_cols = ["Time"] + list(col_names_used.values())
        result = result[output_cols]

        # Merge with existing CSVs when running in a time-scoped window
        # __time_range is injected by the runner when --from/--to are set
        tr = params.get("__time_range") or {}
        merge_from = pd.Timestamp(tr["from"], tz="UTC").tz_convert(tz).tz_localize(None) if tr.get("from") else None
        merge_to = pd.Timestamp(tr["to"], tz="UTC").tz_convert(tz).tz_localize(None) if tr.get("to") else None

        existing_files = _find_existing_csvs(out_path, experience, device_id, aggregation)
        if existing_files and (merge_from is not None or merge_to is not None):
            frames_old = []
            for ef in existing_files:
                old = pd.read_csv(ef, sep=";", dtype=str)
                old_ts = pd.to_datetime(old["Time"])
                mask = pd.Series([True] * len(old), index=old.index)
                if merge_from is not None:
                    mask &= old_ts >= merge_from
                if merge_to is not None:
                    mask &= old_ts < merge_to
                kept = old[~mask]
                if not kept.empty:
                    frames_old.append(kept)
            if frames_old:
                kept_all = pd.concat(frames_old, ignore_index=True).drop_duplicates("Time")
                result = pd.concat([kept_all, result], ignore_index=True)
                result = result.sort_values("Time").reset_index(drop=True)

        # Build output filename from actual data range
        first_date = result["Time"].iloc[0][:10]
        last_date = result["Time"].iloc[-1][:10]
        filename = f"{experience}_{device_id}_aggregated_{aggregation}_{first_date}_{last_date}.csv"
        out_file = out_path / filename

        # Write first, then remove stale old files
        result.to_csv(out_file, sep=";", index=False)
        for ef in existing_files:
            if ef != out_file and ef.exists():
                ef.unlink()
        print(f"  [exportcsv] {device_id}: {len(result)} rows -> {out_file.name}")
