from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from server import db
from server.routers import billing, health


def _request():
    return SimpleNamespace(headers={})


@pytest.fixture(autouse=True)
def _allow_admin():
    with patch(
        "server.auth.require_admin",
        new=AsyncMock(return_value="admin@example.com"),
    ):
        yield


@pytest.mark.asyncio
async def test_dbu_cache_clear_is_scoped_to_dbu_patterns():
    with (
        patch("server.db.clear_query_cache", return_value=1) as clear_query_cache,
        patch("server.db.delta_cache_invalidate") as delta_cache_invalidate,
    ):
        result = await health.clear_cache(_request(), "dbu")

    assert result["tab"] == "dbu"
    assert result["durable_response_cache_invalidated"] is True
    assert "shared response caches were invalidated" in result["message"]
    cleared_patterns = [call.args[0] for call in clear_query_cache.call_args_list]
    assert "tab:dbu" in cleared_patterns
    assert "dashboard-bundle-fast" in cleared_patterns
    assert "sku-breakdown" in cleared_patterns
    assert "pipeline-objects" in cleared_patterns
    assert "interactive-breakdown" in cleared_patterns
    assert "tab:infra" not in cleared_patterns
    delta_patterns = [call.args[0] for call in delta_cache_invalidate.call_args_list]
    assert "billing:dashboard-bundle-fast" in delta_patterns
    assert "billing:sku-breakdown" in delta_patterns


@pytest.mark.asyncio
async def test_infra_cache_clear_covers_all_actual_cost_providers():
    with (
        patch("server.db.clear_query_cache", return_value=0) as clear_query_cache,
        patch("server.db.delta_cache_invalidate"),
    ):
        result = await health.clear_cache(_request(), "infra")

    assert result["tab"] == "infra"
    patterns = [call.args[0] for call in clear_query_cache.call_args_list]
    for pattern in ("tab:infra", "aws-actual", "azure-actual", "gcp-actual"):
        assert pattern in patterns


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tab",
    ["dbu", "sql", "infra", "kpis", "aiml", "apps", "tagging", "users-groups"],
)
async def test_tab_cache_clear_invalidates_only_its_owned_trends(tab):
    with (
        patch("server.db.clear_query_cache", return_value=0),
        patch("server.db.delta_cache_invalidate") as delta_cache_invalidate,
    ):
        await health.clear_cache(_request(), tab)

    patterns = [call.args[0] for call in delta_cache_invalidate.call_args_list]
    assert f"trend:{tab}:" in patterns
    assert all(
        not pattern.startswith("trend:") or pattern.startswith(f"trend:{tab}:")
        for pattern in patterns
    )


@pytest.mark.asyncio
async def test_user_trends_are_cleared_only_with_users_groups():
    with (
        patch("server.db.clear_query_cache", return_value=0) as clear_query_cache,
        patch("server.db.delta_cache_invalidate") as delta_cache_invalidate,
    ):
        await health.clear_cache(_request(), "dbu")

    assert "tab:users-groups" not in [
        call.args[0] for call in clear_query_cache.call_args_list
    ]
    assert "trend:users-groups:" not in [
        call.args[0] for call in delta_cache_invalidate.call_args_list
    ]

    with (
        patch("server.db.clear_query_cache", return_value=0) as clear_query_cache,
        patch("server.db.delta_cache_invalidate") as delta_cache_invalidate,
    ):
        await health.clear_cache(_request(), "users-groups")

    assert "tab:users-groups" in [
        call.args[0] for call in clear_query_cache.call_args_list
    ]
    assert "trend:users-groups:" in [
        call.args[0] for call in delta_cache_invalidate.call_args_list
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "kpi", "cache_endpoint"),
    [
        (billing.get_kpi_trend, "user_spend", "trend:users-groups:billing-kpi"),
        (
            billing.get_platform_kpi_trend,
            "total_queries",
            "trend:users-groups:platform-kpi",
        ),
    ],
)
async def test_billing_trend_endpoints_honor_users_groups_owner(
    endpoint, kpi, cache_endpoint
):
    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put") as cache_put,
        patch.object(
            billing,
            "capture_cache_generation",
            return_value=db.CacheGeneration(cache_endpoint, 0),
        ) as capture_generation,
        patch.object(billing, "_check_mv_available", return_value=False),
        patch.object(
            billing,
            "execute_query",
            return_value=[{"date": "2026-08-01", "value": 1}],
        ),
    ):
        await endpoint(
            kpi=kpi,
            start_date="2026-08-01",
            end_date="2026-08-28",
            granularity="daily",
            workspace_ids=None,
            tab="users-groups",
        )

    capture_generation.assert_called_once_with(cache_endpoint)
    assert cache_put.call_args.args[1] == cache_endpoint


@pytest.mark.asyncio
async def test_optimizer_cache_clear_resets_router_caches():
    from server.routers import warehouse_health

    warehouse_health._health_cache = {"available": True}
    warehouse_health._health_cache_ts = 123
    warehouse_health._idle_time_cache = {"available": True}
    warehouse_health._idle_time_cache_ts = 456

    with (
        patch("server.db.clear_query_cache", return_value=0) as clear_query_cache,
        patch("server.db.delta_cache_invalidate"),
    ):
        result = await health.clear_cache(_request(), "optimizer")

    assert result["tab"] == "optimizer"
    assert "tab:optimizer" in [call.args[0] for call in clear_query_cache.call_args_list]
    assert warehouse_health._health_cache is None
    assert warehouse_health._health_cache_ts == 0.0
    assert warehouse_health._idle_time_cache is None
    assert warehouse_health._idle_time_cache_ts == 0.0


def test_delta_l1_invalidation_keeps_unrelated_tabs():
    db._delta_l1.clear()
    db._delta_l1_endpoints.clear()
    db._delta_l1["dbu-key"] = {"tab": "dbu"}
    db._delta_l1["infra-key"] = {"tab": "infra"}
    db._delta_l1_endpoints.update({
        "dbu-key": "billing:dashboard-bundle-fast",
        "infra-key": "billing:infra-bundle",
    })

    with (
        patch.object(db, "get_catalog_schema", return_value=(None, None)),
    ):
        db.delta_cache_invalidate("billing:infra-bundle")

    assert "dbu-key" in db._delta_l1
    assert "infra-key" not in db._delta_l1
