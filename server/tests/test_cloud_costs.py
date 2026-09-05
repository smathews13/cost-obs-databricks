"""Focused regressions for honest cloud infrastructure cost states."""

import asyncio
import threading
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from server import db
from server.cloud_pricing import get_instance_family
from server.queries import INFRA_COST_ESTIMATE
from server.routers import aws_actual, azure_actual, billing, gcp_actual


def _ok(rows):
    return {"rows": rows, "error": None, "error_kind": None}


def _run_bundle(query_results, workspace_ids=None):
    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put") as cache_put,
        patch.object(billing, "get_host_url", return_value="https://example.gcp.databricks.com"),
        patch.object(billing, "execute_queries_parallel", return_value=query_results),
    ):
        result = asyncio.run(
            billing.get_infra_bundle(
                start_date="2026-08-01",
                end_date="2026-08-28",
                workspace_ids=workspace_ids,
            )
        )
    return result, cache_put


@pytest.mark.parametrize(
    ("instance_type", "cloud", "expected"),
    [
        ("m6i.xlarge", "AWS", "m6i"),
        ("Standard_D8s_v5", "AZURE", "Standard_D"),
        ("n2-standard-4", "GCP", "n2"),
    ],
)
def test_instance_family_matches_three_cloud_contract(
    instance_type,
    cloud,
    expected,
):
    assert get_instance_family(instance_type, cloud) == expected


def test_infra_bundle_does_not_treat_silent_cluster_failure_as_zero():
    result, cache_put = _run_bundle(
        {
            "clusters": None,
            "timeseries": _ok([]),
            "billing_summary": _ok([]),
            "usage_scope": _ok(
                [{"usage_rows": 12, "cluster_usage_rows": 4, "serverless_usage_rows": 8}]
            ),
        }
    )

    costs = result["infra_costs"]
    assert costs["available"] is False
    assert costs["availability"] == "unavailable"
    assert costs["error_kind"] == "query_failure"
    assert costs["reason"] == "query_failed"
    assert "zero usage cannot be confirmed" in costs["reason_detail"]
    cache_put.assert_not_called()


def test_infra_bundle_distinguishes_serverless_only_from_query_failure():
    result, cache_put = _run_bundle(
        {
            "clusters": _ok([]),
            "timeseries": _ok([]),
            "billing_summary": _ok([]),
            "usage_scope": _ok(
                [{"usage_rows": 20, "cluster_usage_rows": 0, "serverless_usage_rows": 20}]
            ),
        }
    )

    costs = result["infra_costs"]
    assert costs["available"] is True
    assert costs["availability"] == "empty"
    assert costs["reason"] == "serverless_only"
    assert costs["error"] is None
    cache_put.assert_called_once()


def test_infra_bundle_remains_local_when_a_shared_cost_source_is_selected():
    token = db.set_source_labels(["shared-only"])
    try:
        result, _ = _run_bundle(
            {
                "clusters": _ok([]),
                "timeseries": _ok([]),
                "billing_summary": _ok([]),
                "usage_scope": _ok(
                    [{"usage_rows": 5, "cluster_usage_rows": 0, "serverless_usage_rows": 5}]
                ),
            }
        )
    finally:
        db.reset_source_labels(token)

    assert result["infra_costs"]["availability"] == "empty"
    assert result["infra_costs"]["reason"] == "serverless_only"


def test_infra_bundle_explains_empty_filtered_date_range():
    result, _ = _run_bundle(
        {
            "clusters": _ok([]),
            "timeseries": _ok([]),
            "billing_summary": _ok([]),
            "usage_scope": _ok(
                [{"usage_rows": 0, "cluster_usage_rows": 0, "serverless_usage_rows": 0}]
            ),
        },
        workspace_ids="123",
    )

    costs = result["infra_costs"]
    assert costs["availability"] == "empty"
    assert costs["reason"] == "no_usage_for_filter_or_date"
    assert "workspace filter and date range" in costs["reason_detail"]


def test_infra_bundle_keeps_dbus_but_never_invents_vm_currency_cost():
    result, cache_put = _run_bundle(
        {
            "clusters": _ok(
                [
                    {
                        "cluster_id": "complete",
                        "cloud": "GCP",
                        "driver_instance_type": "future-driver-type",
                        "worker_instance_type": "future-worker-type",
                        "total_dbu_hours": 10,
                        "databricks_spend": 20,
                        "days_active": 2,
                    },
                    {
                        "cluster_id": "missing-driver",
                        "cloud": "GCP",
                        "driver_instance_type": None,
                        "worker_instance_type": "n2-standard-4",
                        "total_dbu_hours": 5,
                        "databricks_spend": 10,
                        "days_active": 1,
                    },
                ]
            ),
            "timeseries": _ok(
                [
                    {
                        "usage_date": "2026-08-01",
                        "cloud": "GCP",
                        "total_dbu_hours": 15,
                    },
                ]
            ),
            "billing_summary": _ok([{"total_cost": 30, "days_in_range": 1}]),
            "usage_scope": _ok(
                [{"usage_rows": 2, "cluster_usage_rows": 2, "serverless_usage_rows": 0}]
            ),
        }
    )

    costs = result["infra_costs"]
    assert costs["available"] is True
    assert costs["availability"] == "available"
    assert costs["reason"] is None
    assert [row["cluster_id"] for row in costs["clusters"]] == [
        "complete",
        "missing-driver",
    ]
    assert costs["total_dbu_hours"] == 15
    assert costs["total_databricks_spend"] == 30
    assert costs["total_estimated_cost"] is None
    assert costs["currency_estimate_available"] is False
    assert all(row["estimated_cost"] is None for row in costs["clusters"])
    assert [row["databricks_spend"] for row in costs["clusters"]] == [20, 10]
    assert [row["percentage"] for row in costs["clusters"]] == pytest.approx([
        100 * 20 / 30,
        100 * 10 / 30,
    ])
    assert costs["metadata_quality"] == {
        "total_rows": 2,
        "complete_rows": 1,
        "incomplete_rows": 1,
        "incomplete_dbu_hours": 5.0,
    }

    timeseries = result["infra_timeseries"]
    assert timeseries["available"] is True
    assert timeseries["availability"] == "available"
    assert timeseries["reason"] is None
    assert timeseries["timeseries"] == [
        {"date": "2026-08-01", "total_dbu_hours": 15.0},
    ]
    assert timeseries["currency_estimate_available"] is False
    cache_put.assert_called_once()


def test_infra_bundle_keeps_cluster_dbus_when_instance_metadata_is_missing():
    result, _ = _run_bundle(
        {
            "clusters": _ok(
                [
                    {
                        "cluster_id": "missing-metadata",
                        "cloud": "GCP",
                        "driver_instance_type": None,
                        "worker_instance_type": None,
                        "total_dbu_hours": 8,
                        "databricks_spend": 12,
                    }
                ]
            ),
            "timeseries": _ok(
                [
                    {
                        "usage_date": "2026-08-01",
                        "cloud": "GCP",
                        "total_dbu_hours": 8,
                    }
                ]
            ),
            "billing_summary": _ok([{"total_cost": 25, "days_in_range": 1}]),
            "usage_scope": _ok(
                [{"usage_rows": 1, "cluster_usage_rows": 1, "serverless_usage_rows": 0}]
            ),
        }
    )

    costs = result["infra_costs"]
    assert costs["available"] is True
    assert costs["availability"] == "available"
    assert costs["error_kind"] is None
    assert costs["reason"] is None
    assert len(costs["clusters"]) == 1
    assert costs["clusters"][0]["total_dbu_hours"] == 8
    assert costs["clusters"][0]["databricks_spend"] == 12
    assert costs["total_databricks_spend"] == 12
    assert costs["total_estimated_cost"] is None

    timeseries = result["infra_timeseries"]
    assert timeseries["available"] is True
    assert timeseries["availability"] == "available"
    assert timeseries["error_kind"] is None
    assert timeseries["reason"] is None
    assert timeseries["timeseries"] == [
        {"date": "2026-08-01", "total_dbu_hours": 8.0}
    ]


def test_infra_summary_detail_and_timeseries_share_classic_dlt_scope():
    captured_queries = []

    def capture_query(query, _params):
        captured_queries.append(query)
        if "COUNT(*) AS usage_rows" in query:
            return [{"usage_rows": 0, "cluster_usage_rows": 0, "serverless_usage_rows": 0}]
        return []

    def run_sequential(queries, timeout):
        assert timeout == 90.0
        return {name: query() for name, query in queries}

    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put"),
        patch.object(billing, "get_host_url", return_value="https://example.cloud.databricks.com"),
        patch.object(billing, "execute_query", side_effect=capture_query),
        patch.object(billing, "execute_queries_parallel", side_effect=run_sequential),
    ):
        asyncio.run(
            billing.get_infra_bundle(
                start_date="2026-08-01",
                end_date="2026-08-28",
                workspace_ids=None,
            )
        )

    detail_query = next(query for query in captured_queries if "MAX(ci.cluster_name)" in query)
    timeseries_query = next(query for query in captured_queries if "GROUP BY u.usage_date" in query)
    summary_query = next(query for query in captured_queries if "daily_stats AS" in query)

    assert "billing_origin_product = 'DLT'" in summary_query
    assert "LIKE '%DLT%'" in summary_query
    for query in (detail_query, timeseries_query):
        assert "NOT IN ('SQL', 'DLT')" not in query
        assert "billing_origin_product <> 'SQL'" in query
    for query in (summary_query, detail_query, timeseries_query):
        assert "sku_name NOT LIKE '%SERVERLESS%'" in query


def test_cluster_detail_failure_keeps_authoritative_usage_totals():
    result, cache_put = _run_bundle(
        {
            "clusters": None,
            "timeseries": _ok(
                [{"usage_date": "2026-08-01", "total_dbu_hours": 75}]
            ),
            "billing_summary": _ok(
                [{
                    "total_cost": 120,
                    "total_dbu_hours": 75,
                    "total_cluster_count": 42,
                    "avg_clusters_per_day": 6,
                    "avg_cost_per_cluster": 20,
                    "days_in_range": 1,
                }]
            ),
            "usage_scope": _ok(
                [{"usage_rows": 1000, "cluster_usage_rows": 800, "serverless_usage_rows": 200}]
            ),
        }
    )

    costs = result["infra_costs"]
    assert result["availability"] == "partial"
    assert costs["available"] is True
    assert costs["availability"] == "partial"
    assert costs["reason"] == "cluster_detail_unavailable"
    assert costs["total_databricks_spend"] == 120
    assert costs["total_dbu_hours"] == 75
    assert costs["total_cluster_count"] == 42
    assert costs["clusters"] == []
    cache_put.assert_not_called()


def test_cloud_bundle_keeps_core_when_actual_provider_is_overloaded():
    infra = {
        "availability": "available",
        "infra_costs": {"available": True},
        "infra_timeseries": {"available": True},
    }
    unavailable = {
        "available": False,
        "start_date": "2026-08-01",
        "end_date": "2026-08-28",
    }
    with (
        patch.object(billing, "get_infra_bundle", new=AsyncMock(return_value=infra)),
        patch.object(
            aws_actual,
            "get_aws_actual_dashboard_bundle",
            new=AsyncMock(side_effect=billing.SQLExecutionError("capacity")),
        ),
        patch.object(
            azure_actual,
            "get_azure_actual_dashboard_bundle",
            new=AsyncMock(return_value=unavailable),
        ),
        patch.object(
            gcp_actual,
            "get_gcp_actual_dashboard_bundle",
            new=AsyncMock(return_value=unavailable),
        ),
    ):
        result = asyncio.run(
            billing._compute_cloud_costs_bundle(
                {"start_date": "2026-08-01", "end_date": "2026-08-28"},
                None,
            )
        )

    assert result["availability"] == "partial"
    assert result["infra_bundle"] is infra
    assert result["partial_reasons"]["aws_actual"] == "SQL_EXECUTION_ERROR"
    assert result["aws_actual"]["transient_error"] is True


def test_cloud_bundle_keeps_core_when_azure_actual_is_overloaded():
    infra = {
        "availability": "available",
        "infra_costs": {"available": True},
        "infra_timeseries": {"available": True},
    }
    unavailable = {
        "available": False,
        "start_date": "2026-08-01",
        "end_date": "2026-08-28",
    }
    with (
        patch.object(billing, "get_infra_bundle", new=AsyncMock(return_value=infra)),
        patch.object(
            aws_actual,
            "get_aws_actual_dashboard_bundle",
            new=AsyncMock(return_value=unavailable),
        ),
        patch.object(
            azure_actual,
            "get_azure_actual_dashboard_bundle",
            new=AsyncMock(side_effect=billing.SQLExecutionError("capacity")),
        ),
        patch.object(
            gcp_actual,
            "get_gcp_actual_dashboard_bundle",
            new=AsyncMock(return_value=unavailable),
        ),
    ):
        result = asyncio.run(
            billing._compute_cloud_costs_bundle(
                {"start_date": "2026-08-01", "end_date": "2026-08-28"},
                None,
            )
        )

    assert result["availability"] == "partial"
    assert result["infra_bundle"] is infra
    assert result["partial_reasons"]["azure_actual"] == "SQL_EXECUTION_ERROR"
    assert result["azure_actual"]["transient_error"] is True


def test_gcp_status_does_not_cache_capacity_as_integration_absence():
    gcp_actual._gcp_status_cache.update({"available": None, "checked_at": 0})
    with patch.object(
        gcp_actual,
        "execute_query",
        side_effect=billing.SQLExecutionError("SQL capacity is full"),
    ):
        with pytest.raises(billing.SQLExecutionError):
            asyncio.run(gcp_actual.get_gcp_status())

    assert gcp_actual._gcp_status_cache["available"] is None
    assert gcp_actual._gcp_status_cache["checked_at"] == 0


def test_gcp_config_does_not_reuse_the_app_storage_catalog(monkeypatch):
    monkeypatch.setenv("COST_OBS_CATALOG", "customer_app_storage")
    monkeypatch.delenv("GCP_COST_CATALOG", raising=False)
    monkeypatch.delenv("GCP_COST_SCHEMA", raising=False)
    monkeypatch.delenv("GCP_COST_TABLE", raising=False)

    assert gcp_actual.get_catalog_schema_table() == ("billing", "gcp", "")


def test_gcp_status_discovers_one_suffixed_standard_export(monkeypatch):
    monkeypatch.setenv("GCP_COST_CATALOG", "gcp_billing")
    monkeypatch.setenv("GCP_COST_SCHEMA", "billing_export")
    monkeypatch.delenv("GCP_COST_TABLE", raising=False)
    gcp_actual._gcp_status_cache.update({
        "available": None,
        "checked_at": 0,
        "table": None,
        "reason": None,
        "message": None,
    })
    rows = [{"table_name": "gcp_billing_export_v1_000000_000000_000000"}]

    with patch.object(gcp_actual, "execute_query", return_value=rows) as query:
        status = asyncio.run(gcp_actual.get_gcp_status())

    assert status["gcp_available"] is True
    assert status["table"] == "gcp_billing_export_v1_000000_000000_000000"
    assert "`gcp_billing`.information_schema.tables" in query.call_args.args[0]
    assert query.call_args.args[1] == {"schema": "billing_export"}


def test_gcp_status_requires_explicit_choice_when_multiple_exports_exist(monkeypatch):
    monkeypatch.delenv("GCP_COST_TABLE", raising=False)
    gcp_actual._gcp_status_cache.update({
        "available": None,
        "checked_at": 0,
        "table": None,
        "reason": None,
        "message": None,
    })
    with patch.object(gcp_actual, "execute_query", return_value=[
        {"table_name": "gcp_billing_export_v1_first"},
        {"table_name": "gcp_billing_export_v1_second"},
    ]):
        status = asyncio.run(gcp_actual.get_gcp_status())

    assert status["gcp_available"] is False
    assert status["reason"] == "multiple_export_tables"
    assert "Set GCP_COST_TABLE" in status["message"]


def test_gcp_status_treats_a_missing_default_catalog_as_not_configured():
    gcp_actual._gcp_status_cache.update({
        "available": None,
        "checked_at": 0,
        "table": None,
        "reason": None,
        "message": None,
    })
    missing = billing.SQLExecutionError(
        "[TABLE_OR_VIEW_NOT_FOUND] billing.information_schema.tables cannot be found"
    )

    with patch.object(gcp_actual, "execute_query", side_effect=missing):
        status = asyncio.run(gcp_actual.get_gcp_status())

    assert status["gcp_available"] is False
    assert status["reason"] == "not_configured"
    assert "GCP_COST_CATALOG" in status["message"]


def test_gcp_actual_queries_use_net_cost_including_credits():
    queries = (
        gcp_actual.GCP_ACTUAL_SUMMARY,
        gcp_actual.GCP_COSTS_BY_SERVICE,
        gcp_actual.GCP_COSTS_BY_PROJECT,
        gcp_actual.GCP_COSTS_BY_SKU,
        gcp_actual.GCP_COSTS_TIMESERIES,
    )
    for query in queries:
        assert "AGGREGATE(" in query
        assert "credit.amount" in query
        assert "cost > 0" not in query


def test_gcp_actual_rejects_flat_gold_contract(monkeypatch):
    monkeypatch.setenv("GCP_COST_TABLE", "actuals_gold")
    gcp_actual._gcp_status_cache.update({
        "available": None,
        "checked_at": 0,
        "table": None,
        "reason": None,
        "message": None,
    })

    status = asyncio.run(gcp_actual.get_gcp_status())

    assert status["gcp_available"] is False
    assert status["reason"] == "unsupported_table_contract"
    assert "raw standard BigQuery billing export" in status["message"]


@pytest.mark.parametrize(
    ("module", "endpoint_name", "cache_name", "availability_key"),
    (
        (aws_actual, "get_cur_status", "_cur_status_cache", "cur_available"),
        (
            azure_actual,
            "get_azure_status",
            "_azure_status_cache",
            "azure_available",
        ),
        (gcp_actual, "get_gcp_status", "_gcp_status_cache", "gcp_available"),
    ),
)
def test_provider_status_transient_errors_are_not_cached_and_retry_recovers(
    module,
    endpoint_name,
    cache_name,
    availability_key,
):
    status_cache = getattr(module, cache_name)
    status_cache.update({"available": None, "checked_at": 0})
    secret_error = RuntimeError(
        "permission denied host=https://private.example SQL=SELECT secret_value"
    )
    gcp_env = (
        {"GCP_COST_TABLE": "gcp_billing_export_v1_000000_000000_000000"}
        if module is gcp_actual
        else {}
    )
    with (
        patch.dict("os.environ", gcp_env, clear=False),
        patch.object(module, "execute_query", side_effect=[secret_error, [{}]]) as query,
    ):
        with pytest.raises(RuntimeError):
            asyncio.run(getattr(module, endpoint_name)())

        assert status_cache["available"] is None
        assert status_cache["checked_at"] == 0
        recovered = asyncio.run(getattr(module, endpoint_name)())

    assert recovered[availability_key] is True
    assert status_cache["available"] is True
    assert status_cache["checked_at"] > 0
    assert query.call_count == 2


@pytest.mark.parametrize(
    ("module", "endpoint_name", "cache_name", "availability_key"),
    (
        (aws_actual, "get_cur_status", "_cur_status_cache", "cur_available"),
        (
            azure_actual,
            "get_azure_status",
            "_azure_status_cache",
            "azure_available",
        ),
        (gcp_actual, "get_gcp_status", "_gcp_status_cache", "gcp_available"),
    ),
)
def test_provider_status_caches_absence_only_after_successful_existence_query(
    module,
    endpoint_name,
    cache_name,
    availability_key,
):
    status_cache = getattr(module, cache_name)
    status_cache.update({"available": None, "checked_at": 0})
    with (
        patch.object(module, "execute_query", return_value=[]) as query,
    ):
        first = asyncio.run(getattr(module, endpoint_name)())
        second = asyncio.run(getattr(module, endpoint_name)())

    assert first[availability_key] is False
    assert second[availability_key] is False
    assert status_cache["available"] is False
    query.assert_called_once()


def test_gcp_actual_optional_detail_failure_is_partial():
    summary = {
        "available": True,
        "total_cost": 100,
        "start_date": "2026-08-01",
        "end_date": "2026-08-28",
    }
    empty = {
        "available": True,
        "start_date": "2026-08-01",
        "end_date": "2026-08-28",
    }
    with (
        patch.object(
            gcp_actual,
            "get_gcp_status",
            new=AsyncMock(return_value={"gcp_available": True}),
        ),
        patch.object(
            gcp_actual,
            "get_gcp_actual_summary",
            new=AsyncMock(return_value=summary),
        ),
        patch.object(
            gcp_actual,
            "get_gcp_costs_by_service",
            new=AsyncMock(side_effect=billing.SQLExecutionError("capacity")),
        ),
        patch.object(
            gcp_actual,
            "get_gcp_costs_by_project",
            new=AsyncMock(return_value=empty),
        ),
        patch.object(
            gcp_actual,
            "get_gcp_costs_by_sku",
            new=AsyncMock(return_value=empty),
        ),
        patch.object(
            gcp_actual,
            "get_gcp_costs_timeseries",
            new=AsyncMock(return_value=empty),
        ),
    ):
        result = asyncio.run(
            gcp_actual.get_gcp_actual_dashboard_bundle(
                start_date="2026-08-01",
                end_date="2026-08-28",
            )
        )

    assert result["available"] is True
    assert result["availability"] == "partial"
    assert result["summary"] is summary
    assert result["by_service"] is None
    assert result["partial_reasons"] == {"by_service": "SQL_EXECUTION_ERROR"}


def test_azure_actual_optional_detail_failure_is_partial():
    summary = {
        "available": True,
        "total_cost": 100,
        "start_date": "2026-08-01",
        "end_date": "2026-08-28",
    }
    empty = {
        "available": True,
        "start_date": "2026-08-01",
        "end_date": "2026-08-28",
    }
    with (
        patch.object(
            azure_actual,
            "get_azure_status",
            new=AsyncMock(return_value={"azure_available": True}),
        ),
        patch.object(
            azure_actual,
            "get_azure_actual_summary",
            new=AsyncMock(return_value=summary),
        ),
        patch.object(
            azure_actual,
            "get_azure_costs_by_cluster",
            new=AsyncMock(side_effect=billing.SQLExecutionError("capacity")),
        ),
        patch.object(
            azure_actual,
            "get_azure_costs_by_charge_type",
            new=AsyncMock(return_value=empty),
        ),
        patch.object(
            azure_actual,
            "get_azure_costs_timeseries",
            new=AsyncMock(return_value=empty),
        ),
    ):
        result = asyncio.run(
            azure_actual.get_azure_actual_dashboard_bundle(
                start_date="2026-08-01",
                end_date="2026-08-28",
            )
        )

    assert result["available"] is True
    assert result["availability"] == "partial"
    assert result["summary"] is summary
    assert result["by_cluster"] is None
    assert result["partial_reasons"] == {"by_cluster": "SQL_EXECUTION_ERROR"}


def test_cloud_poll_does_not_query_cache_while_producer_is_pending():
    with (
        patch.object(billing, "bundle_compute_is_pending", return_value=True),
        patch.object(billing, "delta_cache_get") as cache_get,
    ):
        response = asyncio.run(
            billing.get_cloud_costs_bundle(
                start_date="2026-08-01",
                end_date="2026-08-28",
                workspace_ids=None,
            )
        )

    assert response.status_code == 202
    assert response.headers["retry-after"] == "1"
    cache_get.assert_not_called()


def test_cloud_payload_normalizes_connector_native_dates_before_caching():
    payload = billing._json_safe_cloud_payload(
        {"availability": "available", "infra_bundle": {"start_date": date(2026, 8, 1)}}
    )

    assert payload["infra_bundle"]["start_date"] == "2026-08-01"


def test_sku_breakdown_has_a_bounded_optional_query_deadline():
    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put"),
        patch.object(billing, "capture_cache_generation", return_value=1),
        patch.object(billing, "execute_query", return_value=[]) as execute,
    ):
        result = asyncio.run(
            billing.get_sku_breakdown(
                start_date="2026-08-01",
                end_date="2026-08-30",
                workspace_ids=None,
            )
        )

    assert result["skus"] == []
    assert execute.call_args.kwargs["timeout"] == 22
    assert execute.call_args.kwargs["max_rows"] == 100


def test_cloud_producer_failure_returns_safe_typed_retryable_error(
    caplog,
):
    raw_error = billing.SQLExecutionError(
        "host=https://private.example SQL=SELECT secret_value token=super-secret"
    )
    with billing._cloud_bundle_failures_lock:
        billing._cloud_bundle_failures.clear()

    def run_inline(_key, operation, **_kwargs):
        worker = threading.Thread(target=operation)
        worker.start()
        worker.join()

    with (
        patch.object(billing, "bundle_compute_is_pending", return_value=False),
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(
            billing,
            "_compute_cloud_costs_bundle",
            new=AsyncMock(side_effect=raw_error),
        ),
        patch.object(billing, "start_bundle_compute", side_effect=run_inline),
        patch("server.app.current_request_id", return_value="request-123"),
    ):
        first = asyncio.run(
            billing.get_cloud_costs_bundle(
                start_date="2026-08-01",
                end_date="2026-08-28",
                workspace_ids=None,
            )
        )
        assert first.status_code == 202

        with pytest.raises(HTTPException) as raised:
            asyncio.run(
                billing.get_cloud_costs_bundle(
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    workspace_ids=None,
                )
            )

    detail = raised.value.detail
    assert detail == {
        "message": "Cloud cost data is temporarily unavailable. Retry shortly.",
        "error_code": "SQL_EXECUTION_ERROR",
        "retryable": True,
        "request_id": "request-123",
    }
    assert raised.value.headers == {"Retry-After": "2"}
    exposed = repr(detail).lower()
    assert "private.example" not in exposed
    assert "select" not in exposed
    assert "secret_value" not in exposed
    assert "super-secret" not in exposed
    assert "request_id=request-123" in caplog.text
    assert "private.example" in caplog.text

    with billing._cloud_bundle_failures_lock:
        billing._cloud_bundle_failures.clear()


def test_cluster_query_uses_billing_list_prices_for_dbu_spend_only():
    assert "system.billing.list_prices" in INFRA_COST_ESTIMATE
    assert "u.usage_quantity * COALESCE(p.pricing.default, 0) AS databricks_spend" in INFRA_COST_ESTIMATE
    assert "u.sku_name = p.sku_name" in INFRA_COST_ESTIMATE
    assert "u.cloud = p.cloud" in INFRA_COST_ESTIMATE
    assert "p.price_end_time IS NULL" in INFRA_COST_ESTIMATE
    assert "LEFT JOIN LATERAL" not in INFRA_COST_ESTIMATE
    assert "SUM(uf.databricks_spend)     AS databricks_spend" in INFRA_COST_ESTIMATE
    assert "COUNT(*) OVER ()" in INFRA_COST_ESTIMATE
    assert "SUM(cr.total_dbu_hours) OVER ()" in INFRA_COST_ESTIMATE
    assert "SUM(cr.databricks_spend) OVER ()" in INFRA_COST_ESTIMATE
    assert "LIMIT 100" in INFRA_COST_ESTIMATE
    assert "driver_hourly_cost" not in INFRA_COST_ESTIMATE
    assert "worker_hourly_cost" not in INFRA_COST_ESTIMATE


def test_infra_bundle_uses_full_window_totals_when_detail_is_limited():
    detail_rows = [
        {
            "cluster_id": f"cluster-{index}",
            "cloud": "AWS",
            "driver_instance_type": "m5.xlarge",
            "worker_instance_type": "m5.xlarge",
            "total_dbu_hours": 10,
            "databricks_spend": 20,
            "days_active": 1,
            "full_cluster_count": 150,
            "full_total_dbu_hours": 2_500,
            "full_databricks_spend": 5_000,
            "full_first_usage_date": "2026-08-01",
            "full_last_usage_date": "2026-08-28",
        }
        for index in range(100)
    ]
    result, _ = _run_bundle(
        {
            "clusters": _ok(detail_rows),
            "timeseries": _ok([]),
            "billing_summary": _ok(
                [{
                    "total_cost": 5_000,
                    "avg_clusters_per_day": 50,
                    "avg_cost_per_cluster": 100,
                    "days_in_range": 28,
                }]
            ),
            "usage_scope": _ok(
                [{"usage_rows": 150, "cluster_usage_rows": 150, "serverless_usage_rows": 0}]
            ),
        }
    )

    costs = result["infra_costs"]
    assert len(costs["clusters"]) == 100
    assert costs["detail_limit"] == 100
    assert costs["detail_truncated"] is True
    assert costs["total_cluster_count"] == 150
    assert costs["total_dbu_hours"] == 2_500
    assert costs["total_databricks_spend"] == 5_000
    assert costs["full_first_usage_date"] == "2026-08-01"
    assert costs["full_last_usage_date"] == "2026-08-28"
    assert costs["clusters"][0]["percentage"] == pytest.approx(0.4)


def test_infra_query_errors_classify_permission_and_metadata_failures():
    assert (
        billing._classify_infra_query_error("[INSUFFICIENT_PERMISSIONS] denied")
        == "permission"
    )
    assert (
        billing._classify_infra_query_error(
            "[TABLE_OR_VIEW_NOT_FOUND] system.compute.clusters"
        )
        == "metadata"
    )


def test_actual_cost_queries_include_the_selected_end_date():
    aws_queries = [
        aws_actual.AWS_ACTUAL_SUMMARY,
        aws_actual.AWS_COSTS_BY_CLUSTER,
        aws_actual.AWS_COSTS_BY_CHARGE_TYPE,
        aws_actual.AWS_COSTS_TIMESERIES,
    ]
    azure_queries = [
        azure_actual.AZURE_ACTUAL_SUMMARY,
        azure_actual.AZURE_COSTS_BY_CLUSTER,
        azure_actual.AZURE_COSTS_BY_CHARGE_TYPE,
        azure_actual.AZURE_COSTS_TIMESERIES,
    ]
    gcp_queries = [
        gcp_actual.GCP_ACTUAL_SUMMARY,
        gcp_actual.GCP_COSTS_BY_SERVICE,
        gcp_actual.GCP_COSTS_BY_PROJECT,
        gcp_actual.GCP_COSTS_BY_SKU,
        gcp_actual.GCP_COSTS_TIMESERIES,
    ]

    assert all("usage_date <= :end_date" in sql for sql in aws_queries + azure_queries)
    assert all(
        "DATE(usage_start_time) <= :end_date" in sql for sql in gcp_queries
    )
