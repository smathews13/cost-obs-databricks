"""Regression coverage for the August dashboard feedback fixes."""

import asyncio
from unittest.mock import patch

from server.queries import (
    BILLING_KPIS_FAST,
    INFRA_COST_ESTIMATE,
    INFRA_COST_TIMESERIES,
    LAKEFLOW_JOB_STATS,
)
from server.routers import billing, dbsql_base, settings, tagging


def test_tag_key_drilldown_aggregates_all_values():
    with patch.object(tagging, "execute_query", return_value=[]) as execute:
        result = asyncio.run(
            tagging.get_top_objects_by_tag(
                tag_key="DataClassification",
                tag_value=None,
                start_date="2026-08-01",
                end_date="2026-08-28",
            )
        )

    params = execute.call_args.args[1]
    assert params["tag_key"] == "DataClassification"
    assert params["tag_value"] == ""
    assert result["tag_value"] is None
    assert ":tag_value = '' OR tv = :tag_value" in tagging.TOP_OBJECTS_BY_TAG


def test_tag_value_drilldown_remains_scoped():
    with patch.object(tagging, "execute_query", return_value=[]) as execute:
        asyncio.run(
            tagging.get_top_objects_by_tag(
                tag_key="DataClassification",
                tag_value="Confidential",
                start_date="2026-08-01",
                end_date="2026-08-28",
            )
        )

    assert execute.call_args.args[1]["tag_value"] == "Confidential"


def test_compute_kpi_query_counts_sql_warehouses():
    assert "COUNT(DISTINCT usage_metadata.cluster_id) as total_clusters" in BILLING_KPIS_FAST
    assert "usage_metadata.warehouse_id" in BILLING_KPIS_FAST
    assert "as sql_warehouses" in BILLING_KPIS_FAST


def test_warehouse_type_classification_prefers_compute_then_billing_metadata():
    classify = dbsql_base._classify_warehouse_type

    assert classify(compute_type="PRO", is_serverless=True, warehouse_id="w1") == "PRO"
    assert classify(is_serverless=True, warehouse_id="w2") == "SERVERLESS"
    assert classify(sql_tier="PREMIUM", warehouse_id="w3") == "PRO"
    assert classify(sku_name="STANDARD_SQL_COMPUTE", warehouse_id="w4") == "CLASSIC"
    assert classify(warehouse_id="w5") == "UNCLASSIFIED"
    assert classify() == "UNCLASSIFIED"


def test_classic_dlt_is_included_in_infra_detail_and_timeseries_queries():
    for query in (INFRA_COST_ESTIMATE, INFRA_COST_TIMESERIES):
        assert "billing_origin_product NOT IN ('SQL', 'DLT')" not in query
        assert "billing_origin_product <> 'SQL'" in query
        assert "sku_name NOT LIKE '%SERVERLESS%'" in query


def test_lakeflow_kpi_query_reports_result_state_availability():
    assert "COUNT(result_state) > 0 as result_state_available" in LAKEFLOW_JOB_STATS


def test_kpis_bundle_preserves_true_zero_successful_runs():
    query_results = {
        "billing_kpis": [{
            "total_jobs": 2,
            "total_job_runs": 4,
            "unique_job_owners": 1,
            "active_workspaces": 1,
        }],
        "lakeflow_kpis": [{
            "result_state_available": True,
            "successful_runs": 0,
            "total_run_hours": 1,
        }],
        "anomalies": [],
        "delta_query_stats": [],
        "user_count": [],
        "total_workspaces": [],
        "avg_daily_models": [],
        "stickiness_pct": [],
    }
    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put"),
        patch.object(billing, "_check_mv_available", return_value=False),
        patch.object(billing, "get_catalog_schema", return_value=("catalog", "schema")),
        patch.object(billing, "execute_queries_parallel", return_value=query_results),
    ):
        result = asyncio.run(billing.get_kpis_bundle(
            start_date="2026-08-01",
            end_date="2026-08-28",
            workspace_ids=None,
        ))

    assert result["kpis"]["successful_runs"] == 0
    assert result["kpis"]["successful_runs_available"] is True


def test_kpis_bundle_marks_missing_result_state_data_unavailable():
    query_results = {
        "billing_kpis": [{"total_jobs": 1, "total_job_runs": 1, "active_workspaces": 1}],
        "lakeflow_kpis": [],
        "anomalies": [],
    }
    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put"),
        patch.object(billing, "_check_mv_available", return_value=False),
        patch.object(billing, "get_catalog_schema", return_value=("catalog", "schema")),
        patch.object(billing, "execute_queries_parallel", return_value=query_results),
    ):
        result = asyncio.run(billing.get_kpis_bundle(
            start_date="2026-07-01",
            end_date="2026-07-31",
            workspace_ids=None,
        ))

    assert result["kpis"]["successful_runs_available"] is False


def test_removed_use_case_settings_are_discarded():
    cleaned = settings._sanitize_app_settings({
        "enable_use_case_tracking": True,
        "enable_accuracy_checks": True,
        "anonymize_users": True,
        "tab_visibility": {"dbu": True, "use-cases": True},
    })

    assert "enable_use_case_tracking" not in cleaned
    assert "enable_accuracy_checks" not in cleaned
    assert "use-cases" not in cleaned["tab_visibility"]
    assert cleaned["tab_visibility"]["dbu"] is True
    assert cleaned["anonymize_users"] is True
