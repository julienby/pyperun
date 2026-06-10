"""Tests for schedule management: core.schedules store + REST endpoints."""
import json

import pytest

from pyperun.core import schedules


@pytest.fixture(autouse=True)
def isolated_cwd(tmp_path, monkeypatch):
    """Run each test in a clean cwd so schedules.json is isolated."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


# ---------------------------------------------------------------------------
# core.schedules — CRUD
# ---------------------------------------------------------------------------

class TestStore:
    def test_list_empty_when_no_file(self):
        assert schedules.list_schedules() == []

    def test_create_then_list(self):
        result = schedules.upsert_schedule("demo", "0 6 * * *", "Europe/Paris")
        assert result["action"] == "created"
        entries = schedules.list_schedules()
        assert entries == [
            {"flow": "demo", "schedule": "0 6 * * *", "timezone": "Europe/Paris", "enabled": True}
        ]

    def test_upsert_updates_in_place(self):
        schedules.upsert_schedule("demo", "0 6 * * *")
        result = schedules.upsert_schedule("demo", "30 2 * * *", enabled=False)
        assert result["action"] == "updated"
        entries = schedules.list_schedules()
        assert len(entries) == 1
        assert entries[0]["schedule"] == "30 2 * * *"
        assert entries[0]["enabled"] is False

    def test_remove_existing(self):
        schedules.upsert_schedule("demo", "0 6 * * *")
        assert schedules.remove_schedule("demo") == {"removed": True}
        assert schedules.list_schedules() == []

    def test_remove_missing(self):
        assert schedules.remove_schedule("nope") == {"removed": False}

    def test_persisted_file_is_valid_json(self, tmp_path):
        schedules.upsert_schedule("demo", "0 6 * * *")
        data = json.loads((tmp_path / "schedules.json").read_text())
        assert data[0]["flow"] == "demo"


# ---------------------------------------------------------------------------
# core.schedules — validation
# ---------------------------------------------------------------------------

class TestValidation:
    def test_invalid_timezone_rejected(self):
        with pytest.raises(ValueError, match="unknown timezone"):
            schedules.upsert_schedule("demo", "0 6 * * *", "Mars/Olympus")

    def test_invalid_cron_rejected(self):
        pytest.importorskip("croniter")
        with pytest.raises(ValueError, match="invalid cron"):
            schedules.upsert_schedule("demo", "not a cron")

    def test_rejected_upsert_does_not_write(self):
        with pytest.raises(ValueError):
            schedules.upsert_schedule("demo", "0 6 * * *", "Mars/Olympus")
        assert schedules.list_schedules() == []


# ---------------------------------------------------------------------------
# REST — /api/schedules
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from pyperun.server import app
    return TestClient(app)


class TestRest:
    def test_list_empty(self, client):
        assert client.get("/api/schedules").json() == []

    def test_put_creates_then_lists(self, client):
        r = client.put("/api/schedules/demo", json={"schedule": "0 6 * * *", "timezone": "UTC"})
        assert r.status_code == 201
        assert r.json()["action"] == "created"
        assert client.get("/api/schedules").json()[0]["flow"] == "demo"

    def test_put_update_returns_200(self, client):
        client.put("/api/schedules/demo", json={"schedule": "0 6 * * *"})
        r = client.put("/api/schedules/demo", json={"schedule": "30 2 * * *"})
        assert r.status_code == 200
        assert r.json()["action"] == "updated"

    def test_put_missing_schedule_400(self, client):
        r = client.put("/api/schedules/demo", json={"timezone": "UTC"})
        assert r.status_code == 400

    def test_put_invalid_tz_400(self, client):
        r = client.put("/api/schedules/demo", json={"schedule": "0 6 * * *", "timezone": "X/Y"})
        assert r.status_code == 400

    def test_delete_existing(self, client):
        client.put("/api/schedules/demo", json={"schedule": "0 6 * * *"})
        r = client.delete("/api/schedules/demo")
        assert r.status_code == 200
        assert r.json() == {"removed": True}

    def test_delete_missing_404(self, client):
        assert client.delete("/api/schedules/nope").status_code == 404
