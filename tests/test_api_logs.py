"""Tests for the log-related API: get_flow_summary, list_flow_summaries, get_run_events."""
import json

import pytest

from pyperun.core.logger import write_flow_summary, log_event
import pyperun.core.api as api_mod
import pyperun.core.logger as logger_mod


@pytest.fixture(autouse=True)
def isolated_logs(tmp_path, monkeypatch):
    logs = tmp_path / "logs"
    monkeypatch.setattr(logger_mod, "LOGS_ROOT", logs)
    yield logs


# ---------------------------------------------------------------------------
# get_flow_summary
# ---------------------------------------------------------------------------

class TestGetFlowSummary:
    def test_returns_none_when_never_run(self, isolated_logs):
        assert api_mod.get_flow_summary("unknown-flow") is None

    def test_returns_summary_after_run(self, isolated_logs):
        write_flow_summary("my-flow", "run1", "success",
                           "2026-05-20T06:00:00Z", 3000.0, 3, 3, 0)
        s = api_mod.get_flow_summary("my-flow")
        assert s is not None
        assert s["flow"] == "my-flow"
        assert s["status"] == "success"
        assert s["run_id"] == "run1"

    def test_returns_latest_after_multiple_runs(self, isolated_logs):
        write_flow_summary("my-flow", "run1", "error",
                           "2026-05-20T06:00:00Z", 1000.0, 3, 1, 1, error="oops")
        write_flow_summary("my-flow", "run2", "success",
                           "2026-05-20T07:00:00Z", 2000.0, 3, 3, 0)
        s = api_mod.get_flow_summary("my-flow")
        assert s["run_id"] == "run2"
        assert s["status"] == "success"


# ---------------------------------------------------------------------------
# list_flow_summaries
# ---------------------------------------------------------------------------

class TestListFlowSummaries:
    def test_empty_when_no_logs(self, isolated_logs):
        assert api_mod.list_flow_summaries() == []

    def test_returns_all_flows(self, isolated_logs):
        write_flow_summary("flow-a", "r1", "success",
                           "2026-05-20T06:00:00Z", 1000.0, 2, 2, 0)
        write_flow_summary("flow-b", "r2", "error",
                           "2026-05-20T07:00:00Z", 500.0, 2, 0, 1, error="fail")
        summaries = api_mod.list_flow_summaries()
        assert len(summaries) == 2
        flows = {s["flow"] for s in summaries}
        assert flows == {"flow-a", "flow-b"}

    def test_sorted_by_ts_start_desc(self, isolated_logs):
        write_flow_summary("flow-a", "r1", "success",
                           "2026-05-20T06:00:00Z", 1000.0, 1, 1, 0)
        write_flow_summary("flow-b", "r2", "success",
                           "2026-05-20T08:00:00Z", 1000.0, 1, 1, 0)
        summaries = api_mod.list_flow_summaries()
        assert summaries[0]["flow"] == "flow-b"
        assert summaries[1]["flow"] == "flow-a"

    def test_skips_malformed_latest_json(self, isolated_logs):
        # Valid flow
        write_flow_summary("good-flow", "r1", "success",
                           "2026-05-20T06:00:00Z", 1000.0, 1, 1, 0)
        # Malformed latest.json
        bad = isolated_logs / "flows" / "bad-flow" / "latest.json"
        bad.parent.mkdir(parents=True, exist_ok=True)
        bad.write_text("not-json{{{")

        summaries = api_mod.list_flow_summaries()
        assert len(summaries) == 1
        assert summaries[0]["flow"] == "good-flow"


# ---------------------------------------------------------------------------
# get_run_events
# ---------------------------------------------------------------------------

class TestGetRunEvents:
    def test_returns_empty_when_no_logs(self, isolated_logs):
        assert api_mod.get_run_events("nonexistent") == []

    def test_finds_events_by_run_id(self, isolated_logs):
        log_event("parse", "start", "/in", "/out", flow="f1", run_id="abc123")
        log_event("parse", "success", "/in", "/out", flow="f1", run_id="abc123",
                  duration_ms=500.0)
        events = api_mod.get_run_events("abc123", flow="f1")
        assert len(events) == 2
        assert events[0]["status"] == "start"
        assert events[1]["status"] == "success"

    def test_ignores_other_run_ids(self, isolated_logs):
        log_event("parse", "start", "/in", "/out", flow="f1", run_id="abc123")
        log_event("parse", "start", "/in", "/out", flow="f1", run_id="xyz999")
        events = api_mod.get_run_events("abc123", flow="f1")
        assert all(e["run_id"] == "abc123" for e in events)
        assert len(events) == 1

    def test_flow_param_limits_search(self, isolated_logs):
        log_event("parse", "start", "/in", "/out", flow="flow-a", run_id="run1")
        log_event("parse", "start", "/in", "/out", flow="flow-b", run_id="run1")
        # With flow="flow-a", should only search flow-a
        events = api_mod.get_run_events("run1", flow="flow-a")
        assert len(events) == 1
        assert events[0]["flow"] == "flow-a"

    def test_searches_misc_when_no_flow(self, isolated_logs):
        log_event("parse", "start", "/in", "/out", run_id="misc-run")
        events = api_mod.get_run_events("misc-run")
        assert len(events) == 1


# ---------------------------------------------------------------------------
# get_status includes last_run
# ---------------------------------------------------------------------------

class TestGetStatusLastRun:
    def _setup_flows_dir(self, tmp_path, monkeypatch):
        flows_dir = tmp_path / "flows"
        flows_dir.mkdir()
        flow = {
            "name": "test-flow",
            "dataset": "TEST",
            "steps": [{"treatment": "parse", "input": "00_raw", "output": "10_parsed"}],
        }
        (flows_dir / "test-flow.json").write_text(json.dumps(flow))
        import pyperun.core.flow as flow_mod
        monkeypatch.setattr(flow_mod, "get_flows_root", lambda: flows_dir)
        return flows_dir

    def test_last_run_is_none_when_no_log(self, tmp_path, monkeypatch):
        self._setup_flows_dir(tmp_path, monkeypatch)
        data = api_mod.get_status()
        entry = next(e for e in data if e["flow"] == "test-flow")
        assert entry["last_run"] is None

    def test_last_run_populated_after_write(self, tmp_path, monkeypatch):
        self._setup_flows_dir(tmp_path, monkeypatch)
        write_flow_summary("test-flow", "r1", "success",
                           "2026-05-20T06:00:00Z", 5000.0, 1, 1, 0)
        data = api_mod.get_status()
        entry = next(e for e in data if e["flow"] == "test-flow")
        assert entry["last_run"] is not None
        assert entry["last_run"]["status"] == "success"
        assert entry["last_run"]["run_id"] == "r1"
