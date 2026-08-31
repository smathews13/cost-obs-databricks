"""Required/optional infrastructure-failure contracts for dashboard bundles."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import patch

from server import db
from server.routers import aiml, apps, billing, dbsql_base, users_groups

START = "2026-02-01"
END = "2026-02-28"
PARAMS = {"start_date": START, "end_date": END}


def _failure(name: str, partial: dict) -> db.SQLTimeoutError:
    exc = db.SQLTimeoutError(f"{name} timed out")
    exc.query_name = name
    exc.partial_results = partial
    exc.infrastructure_failures = {name: exc.code}
    return exc


def _dbsql_compute():
    router = dbsql_base.create_dbsql_router("dbsql_cost_per_query")
    endpoint = next(
        route.endpoint for route in router.routes if route.path == "/dashboard-bundle"
    )
    return next(
        cell.cell_contents
        for cell in endpoint.__closure__ or ()
        if callable(cell.cell_contents)
        and getattr(cell.cell_contents, "__name__", "") == "_compute_dbsql_bundle"
    )


def test_apps_required_failure_is_not_cached():
    partial = {
        "summary": None,
        "apps": [],
        "timeseries": [],
        "avg_cost_per_app": [],
        "sku_breakdown": [],
        "workspaces": [],
        "service_principals": [],
    }
    with (
        patch.object(apps, "_get_app_registry", return_value={}),
        patch.object(apps, "_check_mv_available", return_value=False),
        patch.object(
            apps,
            "execute_queries_parallel",
            side_effect=_failure("summary", partial),
        ),
        patch.object(apps, "delta_cache_put") as cache_put,
    ):
        apps._compute_apps_bundle(
            PARAMS,
            None,
            False,
            "apps-key",
            db.CacheGeneration("apps:dashboard-bundle:all", 0),
        )
    cache_put.assert_not_called()


def test_apps_optional_failure_is_partial_and_short_cached():
    partial = {
        "summary": [],
        "apps": [],
        "timeseries": [],
        "avg_cost_per_app": [],
        "sku_breakdown": [],
        "workspaces": None,
        "service_principals": [],
    }
    with (
        patch.object(apps, "_get_app_registry", return_value={}),
        patch.object(apps, "_check_mv_available", return_value=False),
        patch.object(
            apps,
            "execute_queries_parallel",
            side_effect=_failure("workspaces", partial),
        ),
        patch.object(apps, "delta_cache_put") as cache_put,
    ):
        apps._compute_apps_bundle(
            PARAMS,
            None,
            False,
            "apps-key",
            db.CacheGeneration("apps:dashboard-bundle:all", 0),
        )
    payload = cache_put.call_args.args[2]
    assert payload["availability"] == "partial"
    assert payload["partial_reasons"] == {"workspaces": "SQL_TIMEOUT"}
    assert cache_put.call_args.kwargs["ttl_seconds"] == 60


def test_aiml_required_failure_is_not_cached():
    partial = {name: [] for name in (
        "summary",
        "providers",
        "endpoints",
        "categories",
        "timeseries",
        "models",
        "ml_clusters",
        "agent_bricks",
    )}
    partial["summary"] = None
    with (
        patch.object(
            aiml,
            "execute_queries_parallel",
            side_effect=_failure("summary", partial),
        ),
        patch.object(aiml, "delta_cache_put") as cache_put,
    ):
        aiml._compute_aiml_bundle(
            PARAMS,
            None,
            "",
            "aiml-key",
            db.CacheGeneration("aiml:dashboard-bundle", 0),
        )
    cache_put.assert_not_called()


def test_aiml_optional_failure_is_partial_and_short_cached():
    partial = {name: [] for name in (
        "summary",
        "providers",
        "endpoints",
        "categories",
        "timeseries",
        "models",
        "ml_clusters",
        "agent_bricks",
    )}
    partial["models"] = None
    with (
        patch.object(
            aiml,
            "execute_queries_parallel",
            side_effect=_failure("models", partial),
        ),
        patch.object(aiml, "delta_cache_put") as cache_put,
    ):
        aiml._compute_aiml_bundle(
            PARAMS,
            None,
            "",
            "aiml-key",
            db.CacheGeneration("aiml:dashboard-bundle", 0),
        )
    payload = cache_put.call_args.args[2]
    assert payload["availability"] == "partial"
    assert cache_put.call_args.kwargs["ttl_seconds"] == 60


def test_users_required_failure_returns_error_without_cache_write():
    partial = {
        "summary": None,
        "top_users": [],
        "product_breakdown": [],
        "timeseries": [],
        "by_workspace": [],
        "spend_growth": [],
        "user_growth": [],
    }
    with (
        patch.object(users_groups, "delta_cache_get", return_value=None),
        patch.object(users_groups, "capture_cache_generation"),
        patch.object(
            users_groups,
            "execute_queries_parallel",
            side_effect=_failure("summary", partial),
        ),
        patch.object(users_groups, "delta_cache_put") as cache_put,
    ):
        response = asyncio.run(
            users_groups.get_users_groups_bundle(
                start_date=START,
                end_date=END,
                workspace_ids=None,
                source_labels=None,
            )
        )
    assert response["availability"] == "error"
    assert response["error_code"] == "SQL_TIMEOUT"
    cache_put.assert_not_called()


def test_users_optional_failure_is_partial_and_short_cached():
    partial = {
        "summary": [],
        "top_users": [],
        "product_breakdown": None,
        "timeseries": [],
        "by_workspace": [],
        "spend_growth": [],
        "user_growth": [],
    }
    with (
        patch.object(users_groups, "delta_cache_get", return_value=None),
        patch.object(
            users_groups,
            "capture_cache_generation",
            return_value=db.CacheGeneration("users:dashboard-bundle", 0),
        ),
        patch.object(
            users_groups,
            "execute_queries_parallel",
            side_effect=_failure("product_breakdown", partial),
        ),
        patch.object(users_groups, "delta_cache_put") as cache_put,
    ):
        response = asyncio.run(
            users_groups.get_users_groups_bundle(
                start_date=START,
                end_date=END,
                workspace_ids=None,
                source_labels=None,
            )
        )
    assert response["availability"] == "partial"
    assert response["partial_reasons"] == {"product_breakdown": "SQL_TIMEOUT"}
    assert cache_put.call_args.kwargs["ttl_seconds"] == 60


def test_dbsql_required_failure_is_not_cached():
    partial = {
        "summary": None,
        "by_source": [],
        "by_user": [],
        "by_warehouse": [],
        "timeseries": [],
    }
    compute = _dbsql_compute()
    with (
        patch.object(
            dbsql_base,
            "execute_queries_parallel",
            side_effect=_failure("summary", partial),
        ),
        patch.object(dbsql_base, "delta_cache_put") as cache_put,
    ):
        compute(
            START,
            END,
            None,
            None,
            "dbsql-key",
            db.CacheGeneration("dbsql:dbsql_cost_per_query:dashboard-bundle", 0),
        )
    cache_put.assert_not_called()


def test_billing_fast_required_failure_returns_error_without_cache_write():
    partial = {
        "summary": None,
        "products": [],
        "workspaces": [],
        "timeseries": [],
        "etl_breakdown": [],
        "workspace_count": [],
    }
    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "capture_cache_generation"),
        patch.object(billing, "_check_mv_available", return_value=False),
        patch.object(
            billing,
            "execute_queries_parallel",
            side_effect=_failure("summary", partial),
        ),
        patch.object(billing, "delta_cache_put") as cache_put,
    ):
        response = asyncio.run(
            billing.get_dashboard_bundle_fast(
                start_date=START,
                end_date=END,
                workspace_ids=None,
            )
        )
    assert response["availability"] == "error"
    cache_put.assert_not_called()


def test_billing_fast_optional_failure_is_partial_and_short_cached():
    partial = {
        "summary": [],
        "products": [],
        "workspaces": [],
        "timeseries": [],
        "etl_breakdown": None,
        "workspace_count": [],
    }
    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "capture_cache_generation"),
        patch.object(billing, "_check_mv_available", return_value=False),
        patch.object(
            billing,
            "execute_queries_parallel",
            side_effect=_failure("etl_breakdown", partial),
        ),
        patch.object(billing, "delta_cache_put") as cache_put,
    ):
        response = asyncio.run(
            billing.get_dashboard_bundle_fast(
                start_date=START,
                end_date=END,
                workspace_ids=None,
            )
        )
    assert response["availability"] == "partial"
    assert response["partial_reasons"] == {"etl_breakdown": "SQL_TIMEOUT"}
    assert cache_put.call_args.kwargs["ttl_seconds"] == 60


def test_billing_fast_bounds_optional_queries_without_delaying_core_results():
    observed: dict[str, object] = {}

    def run_optional(queries, *, required, timeout):
        observed["required"] = required
        observed["timeout"] = timeout
        results = {name: [] for name, _query in queries}
        for name, query in queries:
            if name not in required:
                query()
        return results, {}

    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put"),
        patch.object(billing, "capture_cache_generation", return_value=1),
        patch.object(billing, "_check_mv_available", return_value=False),
        patch.object(billing, "_run_bundle_parallel", side_effect=run_optional),
        patch.object(billing, "execute_query", return_value=[]) as execute,
    ):
        response = asyncio.run(
            billing.get_dashboard_bundle_fast(
                start_date=START,
                end_date=END,
                workspace_ids=None,
            )
        )

    assert response["availability"] == "available"
    assert observed == {
        "required": {"summary", "products", "timeseries"},
        "timeout": 20.0,
    }
    assert execute.call_count == 3
    for call in execute.call_args_list:
        assert call.kwargs["timeout"] == 15


def test_dbsql_optional_failure_is_partial_and_short_cached():
    partial = {
        "summary": [],
        "by_source": [],
        "by_user": [],
        "by_warehouse": [],
        "timeseries": [],
        "wh_meta": None,
        "wh_type_billing": [],
        "region_billing_ws": [],
        "region_compute_ws": [],
    }
    compute = _dbsql_compute()
    dbsql_base._mv_status_cache["dbsql_cost_per_query"] = (
        time.monotonic(),
        {"mv_available": True},
    )
    try:
        with (
            patch.object(dbsql_base, "get_catalog_schema", return_value=("c", "s")),
            patch.object(
                dbsql_base,
                "execute_queries_parallel",
                side_effect=_failure("wh_meta", partial),
            ),
            patch.object(dbsql_base, "delta_cache_put") as cache_put,
        ):
            compute(
                START,
                END,
                None,
                None,
                "dbsql-key",
                db.CacheGeneration(
                    "dbsql:dbsql_cost_per_query:dashboard-bundle", 0
                ),
            )
        payload = cache_put.call_args.args[2]
        assert payload["availability"] == "partial"
        assert cache_put.call_args.kwargs["ttl_seconds"] == 60
    finally:
        dbsql_base._mv_status_cache.clear()
