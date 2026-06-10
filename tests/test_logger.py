"""Tests for pyperun.core.logger — 2-layer log system."""
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import jsonlines
import pytest

from pyperun.core.logger import (
    _log_path,
    log_event,
    write_flow_summary,
    cleanup_old_logs,
    new_run_id,
)


@pytest.fixture(autouse=True)
def isolated_logs(tmp_path, monkeypatch):
    """Redirect LOGS_ROOT to a temp dir for every test."""
    import pyperun.core.logger as logger_mod
    monkeypatch.setattr(logger_mod, "LOGS_ROOT", tmp_path / "logs")
    yield tmp_path / "logs"


# ---------------------------------------------------------------------------
# _log_path routing
# ---------------------------------------------------------------------------

class TestLogPath:
    def test_misc_path(self, isolated_logs):
        p = _log_path(None)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert p == isolated_logs / "misc" / f"{today}.jsonl"

    def test_flow_path(self, isolated_logs):
        p = _log_path("my-flow")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert p == isolated_logs / "flows" / "my-flow" / f"{today}.jsonl"


# ---------------------------------------------------------------------------
# log_event — writes jsonlines
# ---------------------------------------------------------------------------

class TestLogEvent:
    def test_creates_parent_dirs(self, isolated_logs):
        log_event("parse", "start", "/in", "/out", flow="my-flow", run_id="abc123")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert (isolated_logs / "flows" / "my-flow" / f"{today}.jsonl").exists()

    def test_start_event_fields(self, isolated_logs):
        log_event("parse", "start", "/in", "/out", flow="my-flow", run_id="abc123",
                  params={"k": "v"})
        p = _log_path("my-flow")
        with jsonlines.open(p) as r:
            events = list(r)
        assert len(events) == 1
        e = events[0]
        assert e["treatment"] == "parse"
        assert e["status"] == "start"
        assert e["input_dir"] == "/in"
        assert e["output_dir"] == "/out"
        assert e["flow"] == "my-flow"
        assert e["run_id"] == "abc123"
        assert e["params"] == {"k": "v"}
        assert "ts" in e

    def test_success_event_has_duration(self, isolated_logs):
        log_event("parse", "success", "/in", "/out", duration_ms=123.4)
        p = _log_path(None)
        with jsonlines.open(p) as r:
            events = list(r)
        assert events[0]["duration_ms"] == 123.4

    def test_error_event_has_error_field(self, isolated_logs):
        log_event("parse", "error", "/in", "/out", error="boom")
        p = _log_path(None)
        with jsonlines.open(p) as r:
            events = list(r)
        assert events[0]["error"] == "boom"

    def test_appends_multiple_events(self, isolated_logs):
        log_event("parse", "start", "/in", "/out", flow="f", run_id="r1")
        log_event("parse", "success", "/in", "/out", flow="f", run_id="r1", duration_ms=10.0)
        p = _log_path("f")
        with jsonlines.open(p) as r:
            events = list(r)
        assert len(events) == 2
        assert [e["status"] for e in events] == ["start", "success"]


# ---------------------------------------------------------------------------
# write_flow_summary — writes latest.json
# ---------------------------------------------------------------------------

class TestWriteFlowSummary:
    def test_creates_latest_json(self, isolated_logs):
        write_flow_summary("my-flow", "run1", "success",
                           "2026-05-20T06:00:00Z", 5000.0, 3, 3, 0)
        p = isolated_logs / "flows" / "my-flow" / "latest.json"
        assert p.exists()

    def test_success_fields(self, isolated_logs):
        write_flow_summary("my-flow", "run1", "success",
                           "2026-05-20T06:00:00Z", 5000.0, 3, 3, 0)
        data = json.loads((isolated_logs / "flows" / "my-flow" / "latest.json").read_text())
        assert data["flow"] == "my-flow"
        assert data["run_id"] == "run1"
        assert data["status"] == "success"
        assert data["ts_start"] == "2026-05-20T06:00:00Z"
        assert data["duration_ms"] == 5000.0
        assert data["steps_total"] == 3
        assert data["steps_ok"] == 3
        assert data["steps_failed"] == 0
        assert "error" not in data
        assert "ts_end" in data

    def test_error_includes_error_field(self, isolated_logs):
        write_flow_summary("my-flow", "run1", "error",
                           "2026-05-20T06:00:00Z", 1000.0, 3, 1, 1,
                           error="step failed")
        data = json.loads((isolated_logs / "flows" / "my-flow" / "latest.json").read_text())
        assert data["status"] == "error"
        assert data["error"] == "step failed"

    def test_overwrites_previous_summary(self, isolated_logs):
        write_flow_summary("my-flow", "run1", "error",
                           "2026-05-20T06:00:00Z", 1000.0, 3, 1, 1, error="oops")
        write_flow_summary("my-flow", "run2", "success",
                           "2026-05-20T07:00:00Z", 2000.0, 3, 3, 0)
        data = json.loads((isolated_logs / "flows" / "my-flow" / "latest.json").read_text())
        assert data["run_id"] == "run2"
        assert data["status"] == "success"
        assert "error" not in data


# ---------------------------------------------------------------------------
# cleanup_old_logs — deletes stale .jsonl, never latest.json
# ---------------------------------------------------------------------------

class TestCleanupOldLogs:
    def _make_old_jsonl(self, logs_root: Path, name: str, days_ago: int) -> Path:
        """Create a .jsonl file and backdate its mtime."""
        p = logs_root / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text('{"status":"start"}\n')
        old_mtime = time.time() - days_ago * 86400
        import os
        os.utime(p, (old_mtime, old_mtime))
        return p

    def test_deletes_old_jsonl(self, isolated_logs):
        old = self._make_old_jsonl(isolated_logs, "flows/f/2026-01-01.jsonl", days_ago=40)
        cleanup_old_logs(retention_days=30)
        assert not old.exists()

    def test_keeps_recent_jsonl(self, isolated_logs):
        recent = self._make_old_jsonl(isolated_logs, "flows/f/2026-05-19.jsonl", days_ago=5)
        cleanup_old_logs(retention_days=30)
        assert recent.exists()

    def test_never_deletes_latest_json(self, isolated_logs):
        # latest.json is not a .jsonl — must never be deleted
        latest = isolated_logs / "flows" / "f" / "latest.json"
        latest.parent.mkdir(parents=True, exist_ok=True)
        latest.write_text('{"status":"success"}')
        cleanup_old_logs(retention_days=0)  # even with 0-day retention
        assert latest.exists()

    def test_misc_logs_cleaned(self, isolated_logs):
        old = self._make_old_jsonl(isolated_logs, "misc/2026-01-01.jsonl", days_ago=40)
        cleanup_old_logs(retention_days=30)
        assert not old.exists()

    def test_swallows_errors_silently(self, isolated_logs):
        # Should not raise even if logs dir doesn't exist
        cleanup_old_logs(retention_days=30)


# ---------------------------------------------------------------------------
# new_run_id
# ---------------------------------------------------------------------------

def test_run_id_is_8_hex_chars():
    rid = new_run_id()
    assert len(rid) == 8
    assert all(c in "0123456789abcdef" for c in rid)

def test_run_ids_are_unique():
    ids = {new_run_id() for _ in range(100)}
    assert len(ids) == 100
