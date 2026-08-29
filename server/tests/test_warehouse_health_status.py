from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from server.routers.health import get_sql_warehouse_status


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
        result = await get_sql_warehouse_status()

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
        result = await get_sql_warehouse_status()

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
        result = await get_sql_warehouse_status()

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
    ):
        result = await get_sql_warehouse_status(probe=True)

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
    ):
        result = await get_sql_warehouse_status(probe=True)

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
    ):
        result = await get_sql_warehouse_status(probe=True)

    assert result["status"] == "unavailable"
    assert result["state"] == "NOT_CONFIGURED"
    execute.assert_not_called()
