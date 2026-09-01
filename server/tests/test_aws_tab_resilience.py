import asyncio
import json
import time
from unittest.mock import patch

import pytest

from server import db
from server.routers import aiml, apps, billing, users_groups


@pytest.mark.parametrize(
    ("endpoint", "collection", "args"),
    [
        (billing.get_sku_breakdown, "skus", ("2026-08-01", "2026-08-30", None)),
        (billing.get_pipeline_objects, "objects", ("2026-08-01", "2026-08-30", None)),
        (billing.get_interactive_breakdown, "items", ("2026-08-01", "2026-08-30", None)),
    ],
)
def test_optional_dbu_breakdowns_settle_as_typed_200_on_timeout(
    endpoint, collection, args
):
    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(
            billing,
            "execute_query",
            side_effect=db.SQLTimeoutError("private SQL text"),
        ),
        patch.object(billing, "delta_cache_put") as cache_put,
    ):
        result = asyncio.run(endpoint(*args))

    assert result["available"] is False
    assert result["availability"] == "unavailable"
    assert result["retryable"] is True
    assert result["reason"] == "query_timeout"
    assert result["total_spend"] is None
    assert result[collection] == []
    assert "private SQL text" not in repr(result)
    cache_put.assert_not_called()


def test_users_bundle_submits_one_bounded_background_producer():
    captured = {}

    def start(cache_key, producer, **kwargs):
        captured.update(cache_key=cache_key, producer=producer, kwargs=kwargs)
        return True

    with (
        patch.object(users_groups, "get_bundle_compute_state", return_value=None),
        patch.object(users_groups, "bundle_compute_is_pending", return_value=False),
        patch.object(users_groups, "delta_cache_get", return_value=None),
        patch.object(users_groups, "start_bundle_compute", side_effect=start),
    ):
        response = asyncio.run(
            users_groups.get_users_groups_bundle(
                "2026-08-01", "2026-08-30", None, None
            )
        )

    assert response.status_code == 202
    assert json.loads(response.body)["status"] == "pending"
    assert captured["kwargs"]["lease_seconds"] == 90
    assert captured["kwargs"]["hard_deadline_seconds"] == 90
    assert captured["kwargs"]["name"] == "users-bundle"


def test_users_required_queries_are_prioritized_and_bounded():
    observed = {}

    def execute_parallel(queries, timeout, **kwargs):
        observed.update(
            names=[name for name, _ in queries],
            timeout=timeout,
            required=kwargs["required_names"],
            concurrency=kwargs["max_concurrency"],
        )
        return {name: [] for name, _ in queries}

    with (
        patch.object(users_groups, "delta_cache_get", return_value=None),
        patch.object(users_groups, "delta_cache_put"),
        patch.object(users_groups, "execute_queries_parallel", side_effect=execute_parallel),
    ):
        asyncio.run(
            users_groups._compute_users_groups_bundle(
                "2026-08-01", "2026-08-30", None, None
            )
        )

    assert observed["timeout"] == 45.0
    assert observed["concurrency"] == 3
    assert observed["required"] == {"summary", "top_users"}
    assert len(observed["names"]) == 7


@pytest.mark.parametrize(
    ("kind", "cache_key", "endpoint", "args", "error_code"),
    [
        (
            "apps",
            db.bundle_cache_key(
                "apps:dashboard-bundle:v5:all", "2026-08-01", "2026-08-30", None
            ),
            apps.get_apps_dashboard_bundle,
            ("2026-08-01", "2026-08-30", False, None),
            "APPS_PRODUCER_DEADLINE",
        ),
        (
            "users",
            db.bundle_cache_key(
                "users:dashboard-bundle:v4", "2026-08-01", "2026-08-30", None
            ),
            users_groups.get_users_groups_bundle,
            ("2026-08-01", "2026-08-30", None, None),
            "USERS_PRODUCER_DEADLINE",
        ),
        (
            "aiml",
            db.bundle_cache_key(
                "aiml:dashboard-bundle:v3", "2026-08-01", "2026-08-30", None
            ),
            aiml.get_aiml_dashboard_bundle,
            ("2026-08-01", "2026-08-30", None),
            "AIML_PRODUCER_DEADLINE",
        ),
    ],
)
def test_second_worker_sees_shared_deadline_and_allows_bounded_retry(
    monkeypatch, tmp_path, kind, cache_key, endpoint, args, error_code
):
    monkeypatch.setattr(db, "_BUNDLE_LEASE_DIR", str(tmp_path))
    lease = db.try_acquire_bundle_lease(
        cache_key,
        lease_seconds=90,
        hard_deadline_seconds=90,
    )
    assert lease is not None
    with open(lease.state_path) as state_file:
        state = json.load(state_file)
    state.update(
        {
            "state": "running",
            "started_at": time.time() - 91,
            "deadline_at": time.time() - 1,
        }
    )
    with open(lease.state_path, "w") as state_file:
        json.dump(state, state_file)

    # Simulate worker B: no process-local producer/status, only the shared lease.
    with db._bundle_inflight_lock:
        db._bundle_inflight.pop(cache_key, None)
    if kind == "apps":
        with apps._apps_bundle_status_lock:
            apps._apps_bundle_status.pop(cache_key, None)

    module = {
        "apps": apps,
        "users": users_groups,
        "aiml": aiml,
    }[kind]
    with (
        patch.object(module, "delta_cache_get") as cache_get,
        patch.object(module, "start_bundle_compute") as start,
    ):
        response = asyncio.run(endpoint(*args))
    assert response.status_code == 503
    payload = json.loads(response.body)
    assert payload["availability"] == "unavailable"
    assert payload["retryable"] is True
    assert payload["error_code"] == error_code
    cache_get.assert_not_called()
    start.assert_not_called()

    # Terminal failure remains visible to concurrent pollers, then expires so a
    # later explicit retry can claim a fresh owner.
    with open(lease.state_path) as state_file:
        failed_state = json.load(state_file)
    assert failed_state["state"] == "failed"
    failed_state["terminal_expires_at"] = time.time() - 1
    with open(lease.state_path, "w") as state_file:
        json.dump(failed_state, state_file)
    with (
        patch.object(module, "delta_cache_get", return_value=None),
        patch.object(module, "start_bundle_compute", return_value=True) as restart,
    ):
        retry = asyncio.run(endpoint(*args))
    assert retry.status_code == 202
    restart.assert_called_once()


@pytest.mark.parametrize(
    ("module", "endpoint", "args", "payload"),
    [
        (
            apps,
            apps.get_apps_dashboard_bundle,
            ("2026-08-01", "2026-08-30", False, None),
            {
                "availability": "available",
                "summary": {"active_app_count": 0},
                "apps": {"active_count": 0, "active_window": {}},
            },
        ),
        (
            users_groups,
            users_groups.get_users_groups_bundle,
            ("2026-08-01", "2026-08-30", None, None),
            {
                "availability": "available",
                "summary": {"user_count": 1},
                "top_users": [],
            },
        ),
    ],
)
def test_apps_and_users_pollers_return_shared_terminal_payload(
    module, endpoint, args, payload
):
    with (
        patch.object(module, "get_bundle_compute_state", return_value=None),
        patch.object(module, "bundle_compute_is_pending", return_value=False),
        patch.object(module, "delta_cache_get", return_value=payload),
        patch.object(module, "start_bundle_compute") as start,
    ):
        result = asyncio.run(endpoint(*args))

    assert result == payload
    start.assert_not_called()


def test_aiml_submit_and_poll_transitions_from_202_to_200():
    payload = {
        "availability": "available",
        "summary": {"total_spend": 12, "total_dbus": 4},
        "providers": {"providers": [], "total_spend": 0},
    }
    with (
        patch.object(aiml, "get_bundle_compute_state", return_value=None),
        patch.object(aiml, "bundle_compute_is_pending", return_value=False),
        patch.object(aiml, "delta_cache_get", side_effect=[None, payload]),
        patch.object(aiml, "start_bundle_compute", return_value=True) as start,
    ):
        submitted = asyncio.run(
            aiml.get_aiml_dashboard_bundle("2026-08-01", "2026-08-30", None)
        )
        completed = asyncio.run(
            aiml.get_aiml_dashboard_bundle("2026-08-01", "2026-08-30", None)
        )

    assert submitted.status_code == 202
    assert completed == payload
    assert start.call_count == 1
    assert start.call_args.kwargs["hard_deadline_seconds"] == 90


@pytest.mark.parametrize(
    "kpi",
    ["aiml_spend", "aiml_dbus", "aiml_endpoints", "aiml_avg_endpoint_cost"],
)
def test_positive_aiml_kpis_return_nonempty_consistent_trends(kpi):
    captured = []

    def execute(sql, *_args, **kwargs):
        captured.append((sql, kwargs))
        return [
            {"date": "2026-08-01", "value": 10},
            {"date": "2026-08-02", "value": 20},
        ]

    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put"),
        patch.object(billing, "_check_mv_available", return_value=False),
        patch.object(billing, "get_catalog_schema", return_value=("c", "s")),
        patch.object(billing, "execute_query", side_effect=execute),
    ):
        result = asyncio.run(
            billing.get_kpi_trend(
                kpi,
                "2026-08-01",
                "2026-08-30",
                "daily",
                None,
                "aiml",
            )
        )

    assert len(result["data_points"]) == 2
    sql, kwargs = captured[0]
    assert "VECTOR_SEARCH" in sql
    assert "FINE_TUNING" in sql
    assert kwargs["timeout"] == 25
    if kpi in {"aiml_spend", "aiml_avg_endpoint_cost"}:
        join_marker = (
            "LEFT JOIN system.billing.list_prices p"
            if "LEFT JOIN system.billing.list_prices p" in sql
            else "/* TEMPORAL_LIST_PRICE_JOIN */"
        )
        assert sql.index("WITH filtered_usage") < sql.index(join_marker)


@pytest.mark.parametrize("kpi", ["infra_cost", "avg_cost_per_cluster"])
def test_positive_cloud_kpis_return_authoritative_daily_trends(kpi):
    captured = {}

    def execute(sql, *_args, **_kwargs):
        captured["sql"] = sql
        return [
            {"date": "2026-08-01", "value": 20.0},
            {"date": "2026-08-02", "value": 10.0},
        ]

    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put"),
        patch.object(billing, "_check_mv_available", return_value=False),
        patch.object(billing, "execute_query", side_effect=execute),
    ):
        result = asyncio.run(
            billing.get_kpi_trend(
                kpi,
                "2026-08-01",
                "2026-08-02",
                "daily",
                "123",
                "infra",
            )
        )

    assert result["available"] is True
    assert [point["value"] for point in result["data_points"]] == [20.0, 10.0]
    assert "filtered_usage AS" in captured["sql"]
    assert "u.usage_metadata.cluster_id IS NOT NULL" in captured["sql"]
    assert "u.sku_name NOT LIKE '%SERVERLESS%'" in captured["sql"]
    assert "CAST(workspace_id AS STRING) IN ('123')" in captured["sql"]
    if kpi == "avg_cost_per_cluster":
        assert "COUNT(DISTINCT cluster_id)" in captured["sql"]
        assert "/ NULLIF(" in captured["sql"]


def test_live_price_queries_prefilter_usage_before_current_join():
    for sql in (
        users_groups.USERS_SUMMARY,
        users_groups.USERS_TOP_SPEND,
        apps.APPS_SUMMARY,
        aiml.AIML_SUMMARY,
    ):
        assert sql.index("WITH filtered_usage") < sql.index(
            "LEFT JOIN system.billing.list_prices p"
        )
        assert "LEFT JOIN LATERAL" not in sql
