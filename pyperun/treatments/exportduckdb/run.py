"""Export aggregated parquet data to a single DuckDB analytical database.

One table per aggregation window (data_10s, data_60s, etc.) in long format:
rows = (ts TIMESTAMPTZ, device_id VARCHAR, <all sensor columns DOUBLE...>)
Columns = union of all devices — NULLs where a device has no such sensor.
A `devices` metadata table is created/updated alongside.
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

try:
    import duckdb
except ImportError as _e:
    raise ImportError(
        "duckdb is required for exportduckdb: pip install pyperun[duckdb]"
    ) from _e
import pandas as pd

from pyperun.core.filename import list_parquet_files, parse_parquet_path


def _discover(in_path: Path, aggregations: list[str], from_dt, to_dt):
    """Return {window: {device_id: [Path, ...]}} for the requested windows."""
    by_window: dict[str, dict[str, list[Path]]] = {w: defaultdict(list) for w in aggregations}
    for pf in list_parquet_files(in_path):
        try:
            parts = parse_parquet_path(pf)
        except ValueError:
            continue
        if parts.aggregation not in aggregations:
            continue
        # Date-level pre-filter (avoid loading entire parquets just to drop them)
        if from_dt and parts.day < from_dt.strftime("%Y-%m-%d"):
            continue
        if to_dt and parts.day > to_dt.strftime("%Y-%m-%d"):
            continue
        by_window[parts.aggregation][parts.device_id].append(pf)
    return by_window


def _load_device(files: list[Path], from_dt, to_dt) -> pd.DataFrame | None:
    frames = [pd.read_parquet(f) for f in files]
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True).sort_values("ts").reset_index(drop=True)
    if from_dt:
        df = df[df["ts"] >= from_dt]
    if to_dt:
        df = df[df["ts"] < to_dt]
    if df.empty:
        return None
    # Multiple domain files for the same device produce duplicate ts rows with
    # complementary columns — coalesce them into one row per timestamp.
    if df.duplicated("ts").any():
        df = df.groupby("ts", as_index=False).first().sort_values("ts").reset_index(drop=True)
    return df


def _build_table(device_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Merge all devices into a long DataFrame with a union of columns."""
    # Discover all sensor columns across devices (exclude ts)
    all_cols: list[str] = []
    seen: set[str] = set()
    for df in device_data.values():
        for c in df.columns:
            if c != "ts" and c not in seen:
                all_cols.append(c)
                seen.add(c)

    frames = []
    for device_id, df in sorted(device_data.items()):
        row = df[["ts"]].copy()
        row["device_id"] = device_id
        for col in all_cols:
            row[col] = df[col].values if col in df.columns else pd.NA
        frames.append(row)

    merged = pd.concat(frames, ignore_index=True).sort_values(["ts", "device_id"]).reset_index(drop=True)
    # Reorder columns: ts, device_id, then sensors
    return merged[["ts", "device_id"] + all_cols]


def _write_window(con: duckdb.DuckDBPyConnection, table_name: str, df: pd.DataFrame) -> None:
    """Replace window table with fresh data (full replace — not incremental)."""
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    con.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_ts_dev ON {table_name}(ts, device_id)")


def _upsert_devices(con: duckdb.DuckDBPyConnection, device_ids: set[str], metadata: dict) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            device_id VARCHAR PRIMARY KEY,
            name      VARCHAR,
            location  VARCHAR,
            type      VARCHAR,
            notes     VARCHAR
        )
    """)
    for dev in sorted(device_ids):
        meta = metadata.get(dev, {})
        name     = meta.get("name")
        location = meta.get("location")
        dtype    = meta.get("type")
        notes    = meta.get("notes")
        con.execute("""
            INSERT INTO devices (device_id, name, location, type, notes)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (device_id) DO UPDATE SET
                name     = COALESCE(excluded.name,     devices.name),
                location = COALESCE(excluded.location, devices.location),
                type     = COALESCE(excluded.type,     devices.type),
                notes    = COALESCE(excluded.notes,    devices.notes)
        """, [dev, name, location, dtype, notes])


def run(input_dir: str, output_dir: str, params: dict) -> None:
    in_path  = Path(input_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    aggregations: list[str] = params.get("aggregations") or ["10s", "60s", "5min", "1h"]
    from_dt = pd.Timestamp(params["from"], tz="UTC") if params.get("from") else None
    to_dt   = pd.Timestamp(params["to"],   tz="UTC") if params.get("to")   else None
    metadata: dict = params.get("metadata") or {}

    by_window = _discover(in_path, aggregations, from_dt, to_dt)

    # Determine db_name from params or from first parquet filename
    db_name = params.get("db_name")
    if not db_name:
        for window_data in by_window.values():
            for files in window_data.values():
                if files:
                    try:
                        parts = parse_parquet_path(files[0])
                        db_name = parts.experience.lower()
                    except ValueError:
                        pass
                    break
            if db_name:
                break
    if not db_name:
        db_name = "dataset"

    db_path = out_path / f"{db_name}.duckdb"
    all_device_ids: set[str] = set()

    with duckdb.connect(str(db_path)) as con:
        for window in aggregations:
            window_data_raw = by_window.get(window, {})
            if not window_data_raw:
                print(f"  [exportduckdb] window={window}: no files found, skipping table")
                continue

            device_data: dict[str, pd.DataFrame] = {}
            for device_id, files in window_data_raw.items():
                df = _load_device(files, from_dt, to_dt)
                if df is not None:
                    device_data[device_id] = df
                    all_device_ids.add(device_id)

            if not device_data:
                print(f"  [exportduckdb] window={window}: no data in range, skipping")
                continue

            table = _build_table(device_data)
            table_name = f"data_{window.replace('-', '_')}"
            _write_window(con, table_name, table)
            n_rows = len(table)
            n_dev  = len(device_data)
            n_cols = len(table.columns) - 2  # exclude ts, device_id
            print(f"  [exportduckdb] window={window}: {n_rows} rows, {n_dev} devices, {n_cols} sensor columns -> {table_name}")

        if all_device_ids:
            _upsert_devices(con, all_device_ids, metadata)
            print(f"  [exportduckdb] devices table: {len(all_device_ids)} entries")

    print(f"  [exportduckdb] -> {db_path}")
