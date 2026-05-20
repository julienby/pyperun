"""Tests for exportduckdb treatment."""
import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest

import pyperun.core.logger as logger_mod
from pyperun.core.runner import run_treatment
import pyperun.core.runner as runner_mod


@pytest.fixture(autouse=True)
def isolated_logs(tmp_path, monkeypatch):
    monkeypatch.setattr(logger_mod, "LOGS_ROOT", tmp_path / "logs")
    yield


@pytest.fixture()
def treatment_dir(tmp_path, monkeypatch):
    """Point TREATMENTS_ROOT to built-in treatments."""
    import pyperun.treatments as t_pkg
    real_root = Path(t_pkg.__file__).parent
    monkeypatch.setattr(runner_mod, "TREATMENTS_ROOT", real_root)
    return real_root


def _write_agg_parquet(out_dir: Path, experience: str, device_id: str,
                        aggregation: str, day: str, domain: str,
                        data: dict) -> None:
    d = out_dir / f"domain={domain}"
    d.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(data)
    ts = pd.to_datetime(df["ts"])
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize("UTC")
    else:
        ts = ts.dt.tz_convert("UTC")
    df["ts"] = ts
    path = d / f"{experience}__{device_id}__aggregated__{aggregation}__{day}.parquet"
    df.to_parquet(path, index=False)


class TestExportDuckDB:
    def test_creates_duckdb_file(self, tmp_path, treatment_dir):
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        _write_agg_parquet(in_dir, "EXP", "pil-90", "10s", "2026-01-25", "bio_signal",
                           {"ts": ["2026-01-25T12:00:00Z"], "m0__raw__mean": [10.0]})
        run_treatment("exportduckdb", str(in_dir), str(out_dir),
                      params={"aggregations": ["10s"]})
        assert (out_dir / "exp.duckdb").exists()

    def test_table_per_window(self, tmp_path, treatment_dir):
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        for window in ["10s", "60s"]:
            _write_agg_parquet(in_dir, "EXP", "pil-90", window, "2026-01-25", "bio_signal",
                               {"ts": ["2026-01-25T12:00:00Z"], "m0__raw__mean": [1.0]})
        run_treatment("exportduckdb", str(in_dir), str(out_dir),
                      params={"aggregations": ["10s", "60s"]})
        db_path = out_dir / "exp.duckdb"
        with duckdb.connect(str(db_path)) as con:
            tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        assert "data_10s" in tables
        assert "data_60s" in tables

    def test_long_format_with_device_id(self, tmp_path, treatment_dir):
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        _write_agg_parquet(in_dir, "EXP", "pil-90", "10s", "2026-01-25", "bio_signal",
                           {"ts": ["2026-01-25T12:00:00Z", "2026-01-25T12:00:10Z"],
                            "m0__raw__mean": [10.0, 11.0]})
        run_treatment("exportduckdb", str(in_dir), str(out_dir),
                      params={"aggregations": ["10s"]})
        with duckdb.connect(str(out_dir / "exp.duckdb")) as con:
            df = con.execute("SELECT * FROM data_10s").df()
        assert "ts" in df.columns
        assert "device_id" in df.columns
        assert (df["device_id"] == "pil-90").all()
        assert len(df) == 2

    def test_multiple_devices_null_padding(self, tmp_path, treatment_dir):
        """Device pil-90 has m0, device pil-91 has m1 only — should get NULLs for the other."""
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        _write_agg_parquet(in_dir, "EXP", "pil-90", "10s", "2026-01-25", "bio_signal",
                           {"ts": ["2026-01-25T12:00:00Z"], "m0__raw__mean": [10.0]})
        _write_agg_parquet(in_dir, "EXP", "pil-91", "10s", "2026-01-25", "bio_signal",
                           {"ts": ["2026-01-25T12:00:00Z"], "m1__raw__mean": [20.0]})
        run_treatment("exportduckdb", str(in_dir), str(out_dir),
                      params={"aggregations": ["10s"]})
        with duckdb.connect(str(out_dir / "exp.duckdb")) as con:
            df = con.execute("SELECT * FROM data_10s ORDER BY device_id").df()
        assert len(df) == 2
        assert "m0__raw__mean" in df.columns
        assert "m1__raw__mean" in df.columns
        row_90 = df[df["device_id"] == "pil-90"].iloc[0]
        row_91 = df[df["device_id"] == "pil-91"].iloc[0]
        assert row_90["m0__raw__mean"] == pytest.approx(10.0)
        assert pd.isna(row_90["m1__raw__mean"])
        assert row_91["m1__raw__mean"] == pytest.approx(20.0)
        assert pd.isna(row_91["m0__raw__mean"])

    def test_time_filter(self, tmp_path, treatment_dir):
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        _write_agg_parquet(in_dir, "EXP", "pil-90", "10s", "2026-01-25", "bio_signal",
                           {"ts": ["2026-01-25T06:00:00Z", "2026-01-25T07:00:00Z"],
                            "m0__raw__mean": [1.0, 2.0]})
        run_treatment("exportduckdb", str(in_dir), str(out_dir),
                      params={"aggregations": ["10s"],
                              "from": "2026-01-25T06:30:00Z",
                              "to":   "2026-01-25T07:30:00Z"})
        with duckdb.connect(str(out_dir / "exp.duckdb")) as con:
            df = con.execute("SELECT * FROM data_10s").df()
        assert len(df) == 1
        assert df["m0__raw__mean"].iloc[0] == pytest.approx(2.0)

    def test_devices_table_created(self, tmp_path, treatment_dir):
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        _write_agg_parquet(in_dir, "EXP", "pil-90", "10s", "2026-01-25", "bio_signal",
                           {"ts": ["2026-01-25T12:00:00Z"], "m0__raw__mean": [1.0]})
        run_treatment("exportduckdb", str(in_dir), str(out_dir),
                      params={"aggregations": ["10s"],
                              "metadata": {"pil-90": {"name": "Pilote 90", "location": "Brest"}}})
        with duckdb.connect(str(out_dir / "exp.duckdb")) as con:
            df = con.execute("SELECT * FROM devices").df()
        assert len(df) == 1
        assert df["device_id"].iloc[0] == "pil-90"
        assert df["name"].iloc[0] == "Pilote 90"
        assert df["location"].iloc[0] == "Brest"

    def test_custom_db_name(self, tmp_path, treatment_dir):
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        _write_agg_parquet(in_dir, "EXP", "pil-90", "10s", "2026-01-25", "bio_signal",
                           {"ts": ["2026-01-25T12:00:00Z"], "m0__raw__mean": [1.0]})
        run_treatment("exportduckdb", str(in_dir), str(out_dir),
                      params={"aggregations": ["10s"], "db_name": "mydb"})
        assert (out_dir / "mydb.duckdb").exists()

    def test_skips_missing_window(self, tmp_path, treatment_dir, capsys):
        """Requesting a window that has no parquet files should skip gracefully."""
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        _write_agg_parquet(in_dir, "EXP", "pil-90", "10s", "2026-01-25", "bio_signal",
                           {"ts": ["2026-01-25T12:00:00Z"], "m0__raw__mean": [1.0]})
        run_treatment("exportduckdb", str(in_dir), str(out_dir),
                      params={"aggregations": ["10s", "1h"]})
        with duckdb.connect(str(out_dir / "exp.duckdb")) as con:
            tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        assert "data_10s" in tables
        assert "data_1h" not in tables

    def test_multi_domain_merged(self, tmp_path, treatment_dir):
        """bio_signal and environment domains from the same device are merged."""
        in_dir = tmp_path / "input"
        out_dir = tmp_path / "output"
        _write_agg_parquet(in_dir, "EXP", "pil-90", "10s", "2026-01-25", "bio_signal",
                           {"ts": ["2026-01-25T12:00:00Z"], "m0__raw__mean": [5.0]})
        _write_agg_parquet(in_dir, "EXP", "pil-90", "10s", "2026-01-25", "environment",
                           {"ts": ["2026-01-25T12:00:00Z"], "outdoor_temp__raw__mean": [18.0]})
        run_treatment("exportduckdb", str(in_dir), str(out_dir),
                      params={"aggregations": ["10s"]})
        with duckdb.connect(str(out_dir / "exp.duckdb")) as con:
            df = con.execute("SELECT * FROM data_10s").df()
        # Same device_id, but two domain files → device_id appears for each file's row
        # (merge is by timestamp+device_id — here same ts so we get a unified row if same ts)
        assert "m0__raw__mean" in df.columns
        assert "outdoor_temp__raw__mean" in df.columns
