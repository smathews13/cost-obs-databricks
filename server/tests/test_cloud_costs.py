"""Focused regressions for honest cloud infrastructure cost states."""

import asyncio
from unittest.mock import patch

import pytest

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


def test_gcp_instance_family_matches_backend_contract():
    assert get_instance_family("n2-standard-4", "GCP") == "n2"


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
                        "driver_instance_type": "future-driver-type",
                        "worker_instance_type": "future-worker-type",
                        "total_dbu_hours": 10,
                    },
                    {
                        "usage_date": "2026-08-01",
                        "cloud": "GCP",
                        "driver_instance_type": None,
                        "worker_instance_type": "n2-standard-4",
                        "total_dbu_hours": 5,
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
    assert costs["availability"] == "partial"
    assert costs["reason"] == "metadata_partial"
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
    assert timeseries["availability"] == "partial"
    assert timeseries["reason"] == "metadata_partial"
    assert timeseries["timeseries"] == [
        {"date": "2026-08-01", "total_dbu_hours": 15.0}
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
                        "driver_instance_type": None,
                        "worker_instance_type": None,
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
    assert costs["availability"] == "partial"
    assert costs["error_kind"] == "metadata"
    assert costs["reason"] == "metadata_partial"
    assert len(costs["clusters"]) == 1
    assert costs["clusters"][0]["total_dbu_hours"] == 8
    assert costs["clusters"][0]["databricks_spend"] == 12
    assert costs["total_databricks_spend"] == 12
    assert costs["total_estimated_cost"] is None

    timeseries = result["infra_timeseries"]
    assert timeseries["available"] is True
    assert timeseries["availability"] == "partial"
    assert timeseries["error_kind"] == "metadata"
    assert timeseries["reason"] == "metadata_partial"
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
    timeseries_query = next(
        query for query in captured_queries if "GROUP BY uf.usage_date" in query
    )
    summary_query = next(query for query in captured_queries if "daily_stats AS" in query)

    assert "billing_origin_product = 'DLT'" in summary_query
    assert "LIKE '%DLT%'" in summary_query
    for query in (detail_query, timeseries_query):
        assert "NOT IN ('SQL', 'DLT')" not in query
        assert "billing_origin_product <> 'SQL'" in query
    for query in (summary_query, detail_query, timeseries_query):
        assert "sku_name NOT LIKE '%SERVERLESS%'" in query


def test_cluster_query_uses_billing_list_prices_for_dbu_spend_only():
    assert "system.billing.list_prices" in INFRA_COST_ESTIMATE
    assert "u.usage_quantity * COALESCE(p.pricing.default, 0) AS databricks_spend" in INFRA_COST_ESTIMATE
    assert "u.usage_start_time >= p.price_start_time" in INFRA_COST_ESTIMATE
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
