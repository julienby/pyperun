import json
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from pyperun.core.timefilter import (
    extract_date_from_filename,
    filter_files_by_date_range,
    parse_iso_utc,
)


# --- parse_iso_utc ---

class TestParseIsoUtc:
    def test_with_z_suffix(self):
        dt = parse_iso_utc("2026-01-25T00:00:00Z")
        assert dt == datetime(2026, 1, 25, tzinfo=timezone.utc)

    def test_with_offset(self):
        dt = parse_iso_utc("2026-01-25T02:00:00+02:00")
        assert dt == datetime(2026, 1, 25, 0, 0, 0, tzinfo=timezone.utc)

    def test_naive_becomes_utc(self):
        dt = parse_iso_utc("2026-01-25T12:30:00")
        assert dt.tzinfo == timezone.utc
        assert dt.hour == 12

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            parse_iso_utc("not-a-date")


# --- extract_date_from_filename ---

class TestExtractDate:
    def test_new_convention(self):
        assert extract_date_from_filename("PREMANIP-GRACE__pil-90__parsed__2026-01-25.parquet") == date(2026, 1, 25)

    def test_csv(self):
        assert extract_date_from_filename("PREMANIP_GRACE_pil-90_2026-01-25.csv") == date(2026, 1, 25)

    def test_no_date(self):
        assert extract_date_from_filename("readme.txt") is None

    def test_aggregated_with_window(self):
        assert extract_date_from_filename("PREMANIP-GRACE__pil-90__aggregated__60s__2026-01-25.parquet") == date(2026, 1, 25)


# --- filter_files_by_date_range ---

class TestFilterFiles:
    def _make_files(self, tmp_path, names):
        paths = []
        for n in names:
            p = tmp_path / n
            p.parent.mkdir(parents=True, exist_ok=True)
            p.touch()
            paths.append(p)
        return paths

    def test_full_range(self, tmp_path):
        files = self._make_files(tmp_path, [
            "domain=bio/EXP__dev__parsed__2026-01-24.parquet",
            "domain=bio/EXP__dev__parsed__2026-01-25.parquet",
            "domain=bio/EXP__dev__parsed__2026-01-26.parquet",
            "domain=bio/EXP__dev__parsed__2026-01-27.parquet",
        ])
        tf = datetime(2026, 1, 25, tzinfo=timezone.utc)
        tt = datetime(2026, 1, 26, 23, 59, 59, tzinfo=timezone.utc)
        result = filter_files_by_date_range(files, tf, tt)
        days = [f.name.split("__")[-1].replace(".parquet", "") for f in result]
        assert "2026-01-25" in days
        assert "2026-01-26" in days
        assert len(result) == 2

    def test_from_only(self, tmp_path):
        files = self._make_files(tmp_path, [
            "domain=bio/EXP__dev__parsed__2026-01-24.parquet",
            "domain=bio/EXP__dev__parsed__2026-01-25.parquet",
        ])
        tf = datetime(2026, 1, 25, tzinfo=timezone.utc)
        result = filter_files_by_date_range(files, tf, None)
        assert len(result) == 1
        assert "2026-01-25" in result[0].name

    def test_to_only(self, tmp_path):
        files = self._make_files(tmp_path, [
            "domain=bio/EXP__dev__parsed__2026-01-25.parquet",
            "domain=bio/EXP__dev__parsed__2026-01-26.parquet",
        ])
        tt = datetime(2026, 1, 25, 23, 59, 59, tzinfo=timezone.utc)
        result = filter_files_by_date_range(files, None, tt)
        assert len(result) == 1
        assert "2026-01-25" in result[0].name

    def test_no_date_in_filename_excluded(self, tmp_path):
        files = self._make_files(tmp_path, [
            "readme.txt",
            "domain=bio/EXP__dev__parsed__2026-01-25.parquet",
        ])
        result = filter_files_by_date_range(files, None, None)
        assert len(result) == 1

    def test_no_range_returns_all_dated(self, tmp_path):
        files = self._make_files(tmp_path, [
            "domain=bio/EXP__dev__parsed__2026-01-24.parquet",
            "domain=bio/EXP__dev__parsed__2026-01-25.parquet",
        ])
        result = filter_files_by_date_range(files, None, None)
        assert len(result) == 2


# --- Integration: runner with --from/--to ---

class TestRunnerTimeFilter:
    def test_run_with_time_filter(self, tmp_path, monkeypatch):
        """run_treatment with time_from/time_to only sees files in range."""
        import pyperun.core.runner as runner_mod
        from pyperun.core.runner import run_treatment
        from pyperun.core.logger import _log_path
        _misc_log = _log_path(None)
        if _misc_log.exists():
            _misc_log.unlink()

        # Create a treatment that lists input parquet files (in domain= subdirs)
        treatment_dir = tmp_path / "treatments" / "lister"
        treatment_dir.mkdir(parents=True)
        treatment_json = {
            "name": "lister",
            "description": "Lists input parquet files",
            "params": {},
        }
        (treatment_dir / "treatment.json").write_text(json.dumps(treatment_json))
        run_py = '''
import json
from pathlib import Path

def run(input_dir, output_dir, params):
    files = sorted(f.name for f in Path(input_dir).rglob("*.parquet"))
    Path(output_dir).joinpath("files.json").write_text(json.dumps(files))
'''
        (treatment_dir / "run.py").write_text(run_py)

        monkeypatch.setattr(runner_mod, "TREATMENTS_ROOT", tmp_path / "treatments")

        # Create input with 3 days of parquet files in domain= subdirs
        input_dir = tmp_path / "input"
        for day in ["2026-01-24", "2026-01-25", "2026-01-26"]:
            d = input_dir / "domain=bio"
            d.mkdir(parents=True, exist_ok=True)
            df = pd.DataFrame({"timestamp": [f"{day}T12:00:00Z"], "val": [1]})
            df.to_parquet(d / f"EXP__dev__parsed__{day}.parquet", index=False)

        output_dir = tmp_path / "output"

        tf = datetime(2026, 1, 25, tzinfo=timezone.utc)
        tt = datetime(2026, 1, 25, 23, 59, 59, tzinfo=timezone.utc)
        run_treatment("lister", str(input_dir), str(output_dir),
                      time_from=tf, time_to=tt)

        result = json.loads((output_dir / "files.json").read_text())
        assert result == ["EXP__dev__parsed__2026-01-25.parquet"]

        if _misc_log.exists():
            _misc_log.unlink()

    def test_replace_mode_scoped(self, tmp_path, monkeypatch):
        """replace mode with time filter only deletes files in range (including subdirs)."""
        import pyperun.core.runner as runner_mod
        from pyperun.core.runner import run_treatment
        from pyperun.core.logger import _log_path
        _misc_log = _log_path(None)
        if _misc_log.exists():
            _misc_log.unlink()

        treatment_dir = tmp_path / "treatments" / "noop"
        treatment_dir.mkdir(parents=True)
        (treatment_dir / "treatment.json").write_text(json.dumps({
            "name": "noop", "description": "noop", "params": {},
        }))
        (treatment_dir / "run.py").write_text("def run(i, o, p): pass\n")

        monkeypatch.setattr(runner_mod, "TREATMENTS_ROOT", tmp_path / "treatments")

        input_dir = tmp_path / "input"
        d_in = input_dir / "domain=bio"
        d_in.mkdir(parents=True)
        (d_in / "EXP__dev__parsed__2026-01-25.parquet").touch()

        output_dir = tmp_path / "output"
        d_out = output_dir / "domain=bio"
        d_out.mkdir(parents=True)
        (d_out / "EXP__dev__clean__2026-01-24.parquet").touch()
        (d_out / "EXP__dev__clean__2026-01-25.parquet").touch()

        tf = datetime(2026, 1, 25, tzinfo=timezone.utc)
        tt = datetime(2026, 1, 25, 23, 59, 59, tzinfo=timezone.utc)
        run_treatment("noop", str(input_dir), str(output_dir),
                      time_from=tf, time_to=tt, output_mode="replace")

        remaining = [f.name for f in output_dir.rglob("*.parquet")]
        assert "EXP__dev__clean__2026-01-24.parquet" in remaining
        assert "EXP__dev__clean__2026-01-25.parquet" not in remaining

        if _misc_log.exists():
            _misc_log.unlink()

    def test_no_files_in_range_skips(self, tmp_path, monkeypatch):
        """When --from is after all data, treatment is skipped without error."""
        import pyperun.core.runner as runner_mod
        from pyperun.core.runner import run_treatment
        from pyperun.core.logger import _log_path
        _misc_log = _log_path(None)
        if _misc_log.exists():
            _misc_log.unlink()

        treatment_dir = tmp_path / "treatments" / "noop"
        treatment_dir.mkdir(parents=True)
        (treatment_dir / "treatment.json").write_text(json.dumps({
            "name": "noop", "description": "noop", "params": {},
        }))
        (treatment_dir / "run.py").write_text("def run(i, o, p): pass\n")

        monkeypatch.setattr(runner_mod, "TREATMENTS_ROOT", tmp_path / "treatments")

        input_dir = tmp_path / "input"
        d = input_dir / "domain=bio"
        d.mkdir(parents=True)
        (d / "EXP__dev__parsed__2026-01-20.parquet").touch()

        output_dir = tmp_path / "output"

        # --from after all data
        tf = datetime(2026, 3, 1, tzinfo=timezone.utc)
        run_treatment("noop", str(input_dir), str(output_dir), time_from=tf)
        # Should not raise — just skip

        if _misc_log.exists():
            _misc_log.unlink()
