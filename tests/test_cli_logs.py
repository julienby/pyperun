"""Tests for `pyperun logs` CLI command."""
import json
from unittest.mock import patch

import pytest

import pyperun.core.logger as logger_mod
from pyperun.core.logger import write_flow_summary, log_event
from pyperun.cli import main


@pytest.fixture(autouse=True)
def isolated_logs(tmp_path, monkeypatch):
    monkeypatch.setattr(logger_mod, "LOGS_ROOT", tmp_path / "logs")
    yield tmp_path / "logs"


def _run(args: list[str], capsys) -> tuple[str, str]:
    with pytest.raises(SystemExit):
        main.__wrapped__(*args) if hasattr(main, "__wrapped__") else _run_main(args)
    return capsys.readouterr()


def _run_main(args):
    with patch("sys.argv", ["pyperun"] + args):
        try:
            main()
        except SystemExit:
            raise


def _cli(args: list[str], capsys):
    """Run CLI and return (stdout, stderr). Accepts any exit code."""
    with patch("sys.argv", ["pyperun"] + args):
        try:
            main()
        except SystemExit:
            pass
    return capsys.readouterr()


# ---------------------------------------------------------------------------
# pyperun logs — all flows table
# ---------------------------------------------------------------------------

class TestLogsAll:
    def test_empty_message_when_no_logs(self, capsys, isolated_logs):
        out, _ = _cli(["logs"], capsys)
        assert "No flow logs found" in out

    def test_shows_all_flows(self, capsys, isolated_logs):
        write_flow_summary("flow-a", "r1", "success",
                           "2026-05-20T06:00:00Z", 2000.0, 3, 3, 0)
        write_flow_summary("flow-b", "r2", "error",
                           "2026-05-20T07:00:00Z", 500.0, 2, 0, 1, error="boom")
        out, _ = _cli(["logs"], capsys)
        assert "flow-a" in out
        assert "flow-b" in out

    def test_json_format(self, capsys, isolated_logs):
        write_flow_summary("flow-a", "r1", "success",
                           "2026-05-20T06:00:00Z", 2000.0, 3, 3, 0)
        out, _ = _cli(["logs", "--format", "json"], capsys)
        data = json.loads(out)
        assert isinstance(data, list)
        assert data[0]["flow"] == "flow-a"


# ---------------------------------------------------------------------------
# pyperun logs <flow> — single flow summary
# ---------------------------------------------------------------------------

class TestLogsSingleFlow:
    def test_not_found_message(self, capsys, isolated_logs):
        out, _ = _cli(["logs", "unknown-flow"], capsys)
        assert "No log found" in out

    def test_shows_summary(self, capsys, isolated_logs):
        write_flow_summary("my-flow", "abc123", "success",
                           "2026-05-20T06:00:00Z", 5000.0, 3, 3, 0)
        out, _ = _cli(["logs", "my-flow"], capsys)
        assert "my-flow" in out
        assert "abc123" in out
        assert "success" in out
        assert "5.0s" in out

    def test_shows_error_field(self, capsys, isolated_logs):
        write_flow_summary("my-flow", "r1", "error",
                           "2026-05-20T06:00:00Z", 1000.0, 3, 1, 1, error="step failed")
        out, _ = _cli(["logs", "my-flow"], capsys)
        assert "step failed" in out

    def test_json_format(self, capsys, isolated_logs):
        write_flow_summary("my-flow", "r1", "success",
                           "2026-05-20T06:00:00Z", 2000.0, 2, 2, 0)
        out, _ = _cli(["logs", "my-flow", "--format", "json"], capsys)
        data = json.loads(out)
        assert data["flow"] == "my-flow"
        assert data["status"] == "success"


# ---------------------------------------------------------------------------
# pyperun logs <flow> --run <id> — event drill-down
# ---------------------------------------------------------------------------

class TestLogsRunEvents:
    def test_not_found_message(self, capsys, isolated_logs):
        out, _ = _cli(["logs", "my-flow", "--run", "nonexistent"], capsys)
        assert "No events found" in out

    def test_shows_events(self, capsys, isolated_logs):
        log_event("parse", "start", "/in", "/out", flow="my-flow", run_id="abc123")
        log_event("parse", "success", "/in", "/out", flow="my-flow", run_id="abc123",
                  duration_ms=300.0)
        out, _ = _cli(["logs", "my-flow", "--run", "abc123"], capsys)
        assert "parse" in out
        assert "start" in out
        assert "success" in out
        assert "300ms" in out

    def test_json_format(self, capsys, isolated_logs):
        log_event("parse", "start", "/in", "/out", flow="my-flow", run_id="r1")
        out, _ = _cli(["logs", "my-flow", "--run", "r1", "--format", "json"], capsys)
        data = json.loads(out)
        assert isinstance(data, list)
        assert data[0]["treatment"] == "parse"
        assert data[0]["run_id"] == "r1"
