import asyncio
import threading
import tomllib
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from server import db
from server.app import app as cost_obs_app
from server.routers import aws_actual, azure_actual, gcp_actual, settings, setup, users_groups


class _Request:
    def __init__(self, body=None, headers=None):
        self._body = body or {}
        self.headers = headers or {}

    async def json(self):
        return self._body


def test_query_started_before_clear_cannot_late_write_stale_result():
    query_started = threading.Event()
    release_query = threading.Event()

    class Cursor:
        description = [("value",)]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, *_args):
            query_started.set()
            assert release_query.wait(timeout=2)

        def fetchall(self):
            return [(1,)]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def cursor(self):
            return Cursor()

    with db._query_cache_lock:
        db._query_cache.clear()
        db._query_cache_pattern_generations.clear()
        db._query_cache_global_generation = 0
        db._query_cache_sequence = 0

    @contextmanager
    def connection():
        yield Connection()

    with patch.object(db, "get_connection", connection):
        with ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(
                db.execute_query, "SELECT 1", None, cache_tag="tab:dbu"
            )
            assert query_started.wait(timeout=1)
            db.clear_query_cache("tab:dbu")
            release_query.set()
            assert pending.result(timeout=2) == [{"value": 1}]

    cache_key = db._get_cache_key("SELECT 1", None, tag="tab:dbu")
    with db._query_cache_lock:
        assert cache_key not in db._query_cache


@pytest.mark.parametrize(
    ("router_module", "status_name", "availability_key"),
    [
        (aws_actual, "get_cur_status", "cur_available"),
        (azure_actual, "get_azure_status", "azure_available"),
        (gcp_actual, "get_gcp_status", "gcp_available"),
    ],
)
def test_local_cloud_exports_are_excluded_from_shared_only_scope(
    router_module, status_name, availability_key
):
    token = db.set_source_labels(["shared-west"])
    try:
        with (
            patch.object(db, "get_local_source_label", return_value="local-workspace"),
            patch.object(router_module, "execute_query") as execute,
        ):
            result = asyncio.run(getattr(router_module, status_name)())
    finally:
        db.reset_source_labels(token)

    assert result[availability_key] is False
    assert result["available"] is False
    assert result["scoped_out"] is True
    assert result["reason"] == "source_scope_excludes_local"
    execute.assert_not_called()


@pytest.mark.parametrize(
    ("router_module", "status_name", "availability_key", "cache_name"),
    [
        (aws_actual, "get_cur_status", "cur_available", "_cur_status_cache"),
        (azure_actual, "get_azure_status", "azure_available", "_azure_status_cache"),
        (gcp_actual, "get_gcp_status", "gcp_available", "_gcp_status_cache"),
    ],
)
def test_local_cloud_exports_operate_when_local_source_is_selected(
    router_module, status_name, availability_key, cache_name
):
    getattr(router_module, cache_name).update({"available": None, "checked_at": 0})
    token = db.set_source_labels(["local-workspace"])
    try:
        with (
            patch.object(db, "get_local_source_label", return_value="local-workspace"),
            patch.object(router_module, "execute_query", return_value=[{"ok": 1}]),
        ):
            result = asyncio.run(getattr(router_module, status_name)())
    finally:
        db.reset_source_labels(token)
    assert result[availability_key] is True


def test_permissions_replacement_is_one_atomic_statement():
    writes = []
    with (
        patch.object(settings, "_ensure_permissions_table"),
        patch.object(settings, "_permissions_table", return_value="permissions"),
        patch("server.db.execute_write", side_effect=lambda sql, params: writes.append((sql, params))),
        patch("server.db.clear_query_cache"),
    ):
        settings._save_user_permissions_to_table(
            ["admin@example.com"], ["consumer@example.com"]
        )

    assert len(writes) == 1
    assert "INSERT OVERWRITE" in writes[0][0]
    assert "DELETE FROM" not in writes[0][0]
    assert set(writes[0][1].values()) == {
        "admin",
        "admin@example.com",
        "consumer",
        "consumer@example.com",
    }


def test_permissions_replacement_rejects_zero_admins_before_writing():
    with patch("server.db.execute_write") as write:
        with pytest.raises(ValueError):
            settings._save_user_permissions_to_table([], ["consumer@example.com"])
    write.assert_not_called()


def test_concurrent_unified_schedule_partial_updates_do_not_overwrite(tmp_path):
    state = {
        "enabled": True,
        "frequency": "nightly",
        "hour_utc": 5,
        "lookback_days": 180,
    }
    state_lock = threading.Lock()

    def load():
        with state_lock:
            snapshot = dict(state)
        return snapshot

    def save(updated):
        with state_lock:
            state.clear()
            state.update(updated)

    requests = (
        _Request({"schedule": {"hour_utc": 11}}),
        _Request({"schedule": {"frequency": "weekly"}}),
    )
    with (
        patch.object(settings, "_require_admin"),
        patch.object(settings, "SETTINGS_DIR", str(tmp_path)),
        patch.object(settings, "SCHEDULE_SETTINGS_FILE", str(tmp_path / "schedule.json")),
        patch.object(settings, "load_schedule_settings", side_effect=load),
        patch.object(settings, "_save_schedule_to_table", side_effect=save),
    ):
        with ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(lambda request: asyncio.run(settings.put_unified_settings(request)), requests))

    assert state["hour_utc"] == 11
    assert state["frequency"] == "weekly"


def test_legacy_azure_connection_routes_call_guarded_handlers():
    app = FastAPI()
    app.include_router(settings.router, prefix="/api/settings")
    client = TestClient(app)
    stored = []

    with (
        patch.object(settings, "_load_user_permissions", return_value={"admins": []}),
        patch.object(settings, "_load_connections", side_effect=lambda: [dict(item) for item in stored]),
        patch.object(settings, "_upsert_connection_to_table"),
        patch.object(settings, "_delete_connection_from_table"),
        patch.object(settings, "_save_connections_to_file", side_effect=lambda rows: stored.__setitem__(slice(None), rows)),
    ):
        created = client.post(
            "/api/settings/azure-connections",
            json={
                "name": "legacy",
                "provider": "aws",
                "tenant_id": "tenant",
                "subscription_id": "subscription",
                "client_id": "client",
                "client_secret": "secret",
            },
        )
        assert created.status_code == 200
        assert created.json()["provider"] == "azure"
        connection_id = created.json()["id"]
        deleted = client.delete(f"/api/settings/azure-connections/{connection_id}")
        assert deleted.status_code == 200
        assert deleted.json() == {"status": "deleted", "id": connection_id}


def test_costly_and_mutating_routes_reject_consumers_but_reads_remain_available():
    app = FastAPI()
    app.include_router(setup.router, prefix="/api/setup")
    app.include_router(settings.router, prefix="/api/settings")
    app.include_router(users_groups.router, prefix="/api/users-groups")
    client = TestClient(app)
    headers = {"X-Forwarded-Email": "consumer@example.com"}
    permissions = {"admins": ["admin@example.com"], "consumers": ["consumer@example.com"]}

    with patch.object(settings, "_load_user_permissions", return_value=permissions):
        assert client.post("/api/setup/create-tables", headers=headers).status_code == 403
        assert client.delete("/api/setup/drop-materialized-views", headers=headers).status_code == 403
        assert client.post(
            "/api/settings/webhook/send-alert", headers=headers, json={}
        ).status_code == 403
        assert client.post(
            "/api/users-groups/report-config/weekly-report",
            headers=headers,
            json={"email": "target@example.com"},
        ).status_code == 403
        assert client.post(
            "/api/users-groups/send-test-report?email=target@example.com",
            headers=headers,
        ).status_code == 403
        assert client.get("/api/users-groups/report-config", headers=headers).status_code == 200


def test_fastapi_openapi_version_matches_project_version():
    project_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    with project_path.open("rb") as project_file:
        expected = tomllib.load(project_file)["project"]["version"]
    assert expected == "1.2.0"
    assert cost_obs_app.version == expected
    assert cost_obs_app.openapi()["info"]["version"] == expected
