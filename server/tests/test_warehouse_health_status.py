import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from server.routers import health


def _request():
    return SimpleNamespace(headers={})


@pytest.fixture(autouse=True)
def _reset_probe_state(monkeypatch, tmp_path):
    monkeypatch.setattr(health, "_warehouse_probe_inflight", None)
    monkeypatch.setattr(health, "_warehouse_probe_last_at", 0.0)
    monkeypatch.setattr(health, "_warehouse_probe_last_result", None)
    monkeypatch.setattr(
        health, "_WAREHOUSE_PROBE_LOCK_PATH", str(tmp_path / "probe.lock")
    )
    monkeypatch.setattr(
        health, "_WAREHOUSE_PROBE_STATE_PATH", str(tmp_path / "probe.json")
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "cluster_size", "serverless", "expected_status", "expected_type"),
    [
        ("RUNNING", "Small", True, "warm", "SERVERLESS"),
        ("STOPPED", "X-Small", False, "warming_up", "CLASSIC"),
        ("DELETED", "Small", False, "unavailable", "CLASSIC"),
    ],
)
async def test_sql_warehouse_status_includes_bound_size_and_type(
    state,
    cluster_size,
    serverless,
    expected_status,
    expected_type,
):
    warehouse = SimpleNamespace(
        state=SimpleNamespace(value=state),
        cluster_size=cluster_size,
        warehouse_type=SimpleNamespace(value="PRO"),
        enable_serverless_compute=serverless,
    )
    workspace_client = MagicMock()
    workspace_client.warehouses.get.return_value = warehouse

    with (
        patch.dict("os.environ", {"DATABRICKS_WAREHOUSE_ID": "warehouse-123"}),
        patch("server.db.get_workspace_client", return_value=workspace_client),
    ):
        result = await health.get_sql_warehouse_status(_request())

    assert result["status"] == expected_status
    assert result["warehouse_id"] == "warehouse-123"
    assert result["warehouse_size"] == cluster_size
    assert result["warehouse_type"] == expected_type


@pytest.mark.asyncio
async def test_sql_warehouse_status_marks_missing_binding_unavailable():
    with patch.dict(
        "os.environ",
        {"DATABRICKS_WAREHOUSE_ID": "", "DATABRICKS_HTTP_PATH": ""},
    ):
        result = await health.get_sql_warehouse_status(_request())

    assert result["status"] == "unavailable"
    assert result["state"] == "NOT_CONFIGURED"
    assert result["warehouse_id"] is None


@pytest.mark.asyncio
async def test_sql_warehouse_status_treats_rest_failure_as_transient():
    workspace_client = MagicMock()
    workspace_client.warehouses.get.side_effect = RuntimeError("temporary API failure")

    with (
        patch.dict("os.environ", {"DATABRICKS_WAREHOUSE_ID": "warehouse-123"}),
        patch("server.db.get_workspace_client", return_value=workspace_client),
    ):
        result = await health.get_sql_warehouse_status(_request())

    assert result["status"] == "warming_up"
    assert result["state"] == "REST_UNVERIFIED"
    assert result["warehouse_id"] == "warehouse-123"


@pytest.mark.asyncio
async def test_sql_warehouse_status_probe_recovers_after_rest_failure():
    workspace_client = MagicMock()
    workspace_client.warehouses.get.side_effect = RuntimeError("temporary API failure")

    with (
        patch.dict("os.environ", {"DATABRICKS_WAREHOUSE_ID": "warehouse-123"}),
        patch("server.db.get_workspace_client", return_value=workspace_client),
        patch("server.db.execute_query", return_value=[{"warehouse_ready": 1}]) as execute,
        patch("server.routers.settings._require_admin"),
    ):
        result = await health.get_sql_warehouse_status(_request(), probe=True)

    assert result["status"] == "warm"
    assert result["state"] == "SQL_PROBE_SUCCEEDED"
    assert result["warehouse_id"] == "warehouse-123"
    assert result["latency_ms"] is not None
    execute.assert_called_once_with("SELECT 1 AS warehouse_ready", no_cache=True)


@pytest.mark.asyncio
async def test_sql_warehouse_status_probe_wakes_a_stopped_warehouse():
    warehouse = SimpleNamespace(
        state=SimpleNamespace(value="STOPPED"),
        cluster_size="Medium",
        enable_serverless_compute=True,
    )
    workspace_client = MagicMock()
    workspace_client.warehouses.get.return_value = warehouse

    with (
        patch.dict("os.environ", {"DATABRICKS_WAREHOUSE_ID": "warehouse-123"}),
        patch("server.db.get_workspace_client", return_value=workspace_client),
        patch("server.db.execute_query", return_value=[{"warehouse_ready": 1}]),
        patch("server.routers.settings._require_admin"),
    ):
        result = await health.get_sql_warehouse_status(_request(), probe=True)

    assert result["status"] == "warm"
    assert result["state"] == "SQL_PROBE_SUCCEEDED"
    assert result["warehouse_size"] == "Medium"


@pytest.mark.asyncio
async def test_sql_warehouse_status_never_probes_a_missing_binding():
    with (
        patch.dict(
            "os.environ",
            {"DATABRICKS_WAREHOUSE_ID": "", "DATABRICKS_HTTP_PATH": ""},
        ),
        patch("server.db.execute_query") as execute,
        patch("server.routers.settings._require_admin"),
    ):
        result = await health.get_sql_warehouse_status(_request(), probe=True)

    assert result["status"] == "unavailable"
    assert result["state"] == "NOT_CONFIGURED"
    execute.assert_not_called()


@pytest.mark.asyncio
async def test_sql_warehouse_probe_requires_admin():
    with (
        patch.dict("os.environ", {"DATABRICKS_WAREHOUSE_ID": "warehouse-123"}),
        patch(
            "server.routers.settings._require_admin",
            side_effect=HTTPException(status_code=403, detail="Admin required"),
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            await health.get_sql_warehouse_status(_request(), probe=True)
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_sql_warehouse_probe_is_single_flight_and_rate_limited():
    workspace_client = MagicMock()
    workspace_client.warehouses.get.side_effect = RuntimeError("REST unavailable")
    started = __import__("threading").Event()
    release = __import__("threading").Event()

    def probe(*_args, **_kwargs):
        started.set()
        assert release.wait(timeout=2)
        return [{"warehouse_ready": 1}]

    with (
        patch.dict("os.environ", {"DATABRICKS_WAREHOUSE_ID": "warehouse-123"}),
        patch("server.db.get_workspace_client", return_value=workspace_client),
        patch("server.db.execute_query", side_effect=probe) as execute,
        patch("server.routers.settings._require_admin"),
    ):
        first = asyncio.create_task(
            health.get_sql_warehouse_status(_request(), probe=True)
        )
        assert await asyncio.to_thread(started.wait, 1)
        second = asyncio.create_task(
            health.get_sql_warehouse_status(_request(), probe=True)
        )
        release.set()
        first_result, second_result = await asyncio.gather(first, second)
        third_result = await health.get_sql_warehouse_status(_request(), probe=True)

    assert first_result["state"] == "SQL_PROBE_SUCCEEDED"
    assert second_result["state"] == "SQL_PROBE_SUCCEEDED"
    assert third_result["state"] == "SQL_PROBE_SUCCEEDED"
    execute.assert_called_once()


@pytest.mark.asyncio
async def test_sql_warehouse_probe_honors_cross_worker_rate_state():
    cached = {
        "status": "warm",
        "state": "SQL_PROBE_SUCCEEDED",
        "latency_ms": 10,
        "warehouse_id": "warehouse-123",
        "warehouse_size": "Small",
        "warehouse_type": "SERVERLESS",
    }
    Path(health._WAREHOUSE_PROBE_STATE_PATH).write_text(json.dumps({
        "completed_at": time.time(),
        "result": cached,
    }))
    workspace_client = MagicMock()
    workspace_client.warehouses.get.side_effect = RuntimeError("REST unavailable")
    with (
        patch.dict("os.environ", {"DATABRICKS_WAREHOUSE_ID": "warehouse-123"}),
        patch("server.db.get_workspace_client", return_value=workspace_client),
        patch("server.db.execute_query") as execute,
        patch("server.routers.settings._require_admin"),
    ):
        result = await health.get_sql_warehouse_status(_request(), probe=True)
    assert result == cached
    execute.assert_not_called()
