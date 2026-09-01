"""Required/optional infrastructure-failure contracts for dashboard bundles."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import patch

import pytest

from server import db
from server.routers import aiml, apps, billing, dbsql_base, tagging, users_groups

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


def test_apps_required_failure_is_typed_and_short_cached_for_pollers():
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
        patch.object(apps, "_app_name_cache", {}),
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
            db.CacheGeneration("apps:dashboard-bundle:v5:all", 0),
        )
    payload = cache_put.call_args.args[2]
    assert payload["availability"] == "unavailable"
    assert payload["retryable"] is True
    assert payload["error_code"] == "SQL_TIMEOUT"
    assert "timed out" not in repr(payload).lower()
    assert cache_put.call_args.kwargs["ttl_seconds"] == 15


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
        patch.object(
            apps,
            "_app_name_cache",
            {"app-id": {"name": "app", "url": "", "metadata": {}}},
        ),
        patch.object(
            apps,
            "_app_details_cache",
            {"app-id": {"metadata": {}, "resources": []}},
        ),
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
            db.CacheGeneration("apps:dashboard-bundle:v5:all", 0),
        )
    payload = cache_put.call_args.args[2]
    assert payload["availability"] == "partial"
    assert payload["partial_reasons"] == {"workspaces": "SQL_TIMEOUT"}
    assert cache_put.call_args.kwargs["ttl_seconds"] == 60


def test_apps_large_account_aggregate_remains_usable():
    raw_apps = [
        {
            "app_id": f"app-{index}",
            "total_spend": 1,
            "total_dbus": 1,
            "workspace_count": 1,
            "days_active": 30,
            "last_usage_date": END,
        }
        for index in range(3549)
    ]
    registry = {
        row["app_id"]: {"name": row["app_id"], "url": ""}
        for row in raw_apps
    }

    response = apps._process_apps(raw_apps, False, START, END, registry)

    assert len(response["apps"]) == 3549
    assert response["total_app_count"] == 3549
    assert response["total_spend"] == 3549


@pytest.mark.parametrize(
    "cache_outcome",
    [False, RuntimeError("remote cache unavailable")],
    ids=["false-return", "exception"],
)
def test_apps_shared_cache_failure_keeps_the_successful_local_result(cache_outcome):
    partial = {
        "summary": [],
        "apps": [],
        "timeseries": [],
        "avg_cost_per_app": [],
        "sku_breakdown": [],
        "workspaces": None,
        "service_principals": [],
    }
    cache_key = "apps-durable-failure"
    with (
        patch.object(apps, "_app_name_cache", {}),
        patch.object(apps, "_app_details_cache", {}),
        patch.object(apps, "_check_mv_available", return_value=False),
        patch.object(
            apps,
            "execute_queries_parallel",
            side_effect=_failure("workspaces", partial),
        ),
        patch.object(apps, "delta_cache_put", side_effect=cache_outcome),
    ):
        apps._compute_apps_bundle(
            PARAMS,
            None,
            False,
            cache_key,
            db.CacheGeneration("apps:dashboard-bundle:v5:all", 0),
        )

    with apps._apps_bundle_status_lock:
        status = dict(apps._apps_bundle_status[cache_key])
        failure = apps._apps_bundle_failures.get(cache_key)
        apps._apps_bundle_status.pop(cache_key, None)
        apps._apps_bundle_failures.pop(cache_key, None)
    assert status["state"] == "complete"
    assert failure is None


@pytest.mark.parametrize(
    "cache_outcome",
    [False, RuntimeError("remote cache unavailable")],
    ids=["false-return", "exception"],
)
def test_apps_shared_cache_failure_releases_lease_as_successful(
    monkeypatch, tmp_path, cache_outcome
):
    partial = {
        "summary": [],
        "apps": [],
        "timeseries": [],
        "avg_cost_per_app": [],
        "sku_breakdown": [],
        "workspaces": None,
        "service_principals": [],
    }
    cache_key = f"apps-lease-{type(cache_outcome).__name__}"
    monkeypatch.setattr(db, "_BUNDLE_LEASE_DIR", str(tmp_path))
    with (
        patch.object(db, "delta_cache_get", return_value=None),
        patch.object(apps, "_app_name_cache", {}),
        patch.object(apps, "_app_details_cache", {}),
        patch.object(apps, "_check_mv_available", return_value=False),
        patch.object(
            apps,
            "execute_queries_parallel",
            side_effect=_failure("workspaces", partial),
        ),
        patch.object(
            apps,
            "delta_cache_put",
            side_effect=cache_outcome,
        ),
    ):
        assert db.start_bundle_compute(
            cache_key,
            lambda: apps._compute_apps_bundle(
                PARAMS,
                None,
                False,
                cache_key,
                db.CacheGeneration("apps:dashboard-bundle:v5:all", 0),
            ),
            lease_seconds=30,
            hard_deadline_seconds=30,
        )
        deadline = time.monotonic() + 2
        state = db.get_bundle_compute_state(cache_key)
        while time.monotonic() < deadline:
            state = db.get_bundle_compute_state(cache_key)
            if state is None:
                break
            time.sleep(0.01)

    assert state is None


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
        with pytest.raises(aiml._AimlProducerError) as exc_info:
            aiml._compute_aiml_bundle(
                PARAMS,
                None,
                "",
                "aiml-key",
                db.CacheGeneration("aiml:dashboard-bundle:v3", 0),
            )
    assert exc_info.value.code == "SQL_TIMEOUT"
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
            db.CacheGeneration("aiml:dashboard-bundle:v3", 0),
        )
    payload = cache_put.call_args.args[2]
    assert payload["availability"] == "partial"
    assert cache_put.call_args.kwargs["ttl_seconds"] == 60


@pytest.mark.parametrize(
    "cache_outcome",
    [False, RuntimeError("remote cache unavailable")],
    ids=["false-return", "exception"],
)
def test_aiml_durable_cache_failure_is_typed(cache_outcome):
    results = {
        name: []
        for name in (
            "summary",
            "providers",
            "endpoints",
            "categories",
            "timeseries",
            "models",
            "ml_clusters",
            "agent_bricks",
        )
    }
    cache_exception = cache_outcome if isinstance(cache_outcome, BaseException) else None
    with (
        patch.object(aiml, "execute_queries_parallel", return_value=results),
        patch.object(
            aiml,
            "delta_cache_put",
            return_value=False,
            side_effect=cache_exception,
        ),
    ):
        with pytest.raises(aiml._AimlProducerError) as exc_info:
            aiml._compute_aiml_bundle(
                PARAMS,
                None,
                "",
                "aiml-cache-failure",
                db.CacheGeneration("aiml:dashboard-bundle:v3", 0),
            )

    assert exc_info.value.code == "AIML_CACHE_WRITE_FAILED"


def _tagging_results() -> dict:
    return {
        "summary": [],
        "clusters": [],
        "jobs": [],
        "pipelines": [],
        "warehouses": [],
        "endpoints": [],
        "cost_by_tag": [],
        "tag_stats": [{"avg_cost_per_tag": 10, "total_tag_count": 4}],
        "timeseries": [{
            "usage_date": "2026-02-01",
            "tagged_spend": 75,
            "untagged_spend": 25,
        }],
    }


def test_tagging_required_timeout_returns_typed_unavailable_without_fake_zero():
    partial = _tagging_results()
    partial["timeseries"] = None
    with (
        patch.object(tagging, "delta_cache_get", return_value=None),
        patch.object(
            tagging,
            "execute_queries_parallel",
            side_effect=_failure("timeseries", partial),
        ) as execute,
        patch.object(tagging, "delta_cache_put") as cache_put,
    ):
        result = asyncio.run(
            tagging.get_tagging_dashboard_bundle(START, END, None)
        )

    assert result["available"] is False
    assert result["availability"] == "unavailable"
    assert result["retryable"] is True
    assert result["error_code"] == "SQL_TIMEOUT"
    assert result["summary"] == {}
    assert "error" not in result
    assert execute.call_args.args[1] == 27.0
    assert execute.call_args.kwargs["required_names"] == {"summary", "timeseries"}
    assert execute.call_args.kwargs["max_concurrency"] == 2
    cache_put.assert_not_called()


def test_tagging_optional_timeout_returns_positive_partial_core():
    partial = _tagging_results()
    partial["jobs"] = None
    with (
        patch.object(tagging, "delta_cache_get", return_value=None),
        patch.object(
            tagging,
            "execute_queries_parallel",
            side_effect=_failure("jobs", partial),
        ),
        patch.object(tagging, "delta_cache_put") as cache_put,
    ):
        result = asyncio.run(
            tagging.get_tagging_dashboard_bundle(START, END, None)
        )

    assert result["available"] is True
    assert result["availability"] == "partial"
    assert result["summary"]["total_spend"] == 100
    assert result["summary"]["tagged_percentage"] == 75
    assert result["partial_reasons"] == {"jobs": "SQL_TIMEOUT"}
    assert cache_put.call_args.kwargs["ttl_seconds"] == 60


def test_users_required_failure_returns_typed_unavailable_without_cache_write():
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
            users_groups._compute_users_groups_bundle(
                start_date=START,
                end_date=END,
                workspace_ids=None,
                source_labels=None,
            )
        )
    assert response["availability"] == "unavailable"
    assert response["retryable"] is True
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
            return_value=db.CacheGeneration("users:dashboard-bundle:v4", 0),
        ),
        patch.object(
            users_groups,
            "execute_queries_parallel",
            side_effect=_failure("product_breakdown", partial),
        ),
        patch.object(users_groups, "delta_cache_put") as cache_put,
    ):
        response = asyncio.run(
            users_groups._compute_users_groups_bundle(
                start_date=START,
                end_date=END,
                workspace_ids=None,
                source_labels=None,
            )
        )
    assert response["availability"] == "partial"
    assert response["partial_reasons"] == {"product_breakdown": "SQL_TIMEOUT"}
    assert cache_put.call_args.kwargs["ttl_seconds"] == 60


def test_users_large_aggregate_is_usable_but_detail_payload_is_capped():
    rows = [
        {
            "user_email": f"user-{index}@example.com",
            "total_spend": 1,
            "total_dbus": 1,
            "active_days": 1,
            "workspace_count": 1,
        }
        for index in range(1001)
    ]
    results = {
        "summary": [{
            "user_count": len(rows),
            "workspace_count": 1,
            "total_spend": len(rows),
            "total_dbus": len(rows),
            "avg_spend_per_user": 1,
        }],
        "top_users": rows,
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
            return_value=results,
        ),
        patch.object(users_groups, "delta_cache_put"),
    ):
        response = asyncio.run(
            users_groups._compute_users_groups_bundle(
                start_date=START,
                end_date=END,
                workspace_ids=None,
                source_labels=None,
            )
        )

    assert response["availability"] == "available"
    assert response["summary"]["user_count"] == 1001
    assert len(response["top_users"]) == 1000
    assert response["top_users_limits"]["truncated"] is True


def test_dbsql_required_failure_is_short_cached_as_typed_unavailable():
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
            db.CacheGeneration("dbsql:dbsql_cost_per_query:dashboard-bundle:v2", 0),
        )
    payload = cache_put.call_args.args[2]
    assert payload["available"] is False
    assert payload["error_code"] == "SQL_TIMEOUT"
    assert cache_put.call_args.kwargs["ttl_seconds"] == 60


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


def test_billing_fast_workspace_kpi_reports_selected_filter_count():
    results = {
        "summary": [{
            "total_dbus": 10,
            "total_spend": 20,
            "workspace_count": 1,
            "days_in_range": 1,
        }],
        "products": [],
        "workspaces": [],
        "timeseries": [],
        "etl_breakdown": [],
        "workspace_count": [{"workspace_count": 1}],
    }
    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put"),
        patch.object(billing, "capture_cache_generation"),
        patch.object(billing, "_check_mv_available", return_value=False),
        patch.object(billing, "_run_bundle_parallel", return_value=(results, {})),
    ):
        response = asyncio.run(
            billing.get_dashboard_bundle_fast(
                start_date=START,
                end_date=END,
                workspace_ids="1,2,3,4,5",
            )
        )

    assert response["summary"]["workspace_count"] == 5


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
                    "dbsql:dbsql_cost_per_query:dashboard-bundle:v2", 0
                ),
            )
        payload = cache_put.call_args.args[2]
        assert payload["availability"] == "partial"
        assert cache_put.call_args.kwargs["ttl_seconds"] == 60
    finally:
        dbsql_base._mv_status_cache.clear()
