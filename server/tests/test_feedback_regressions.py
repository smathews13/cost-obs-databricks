"""Regression coverage for the August dashboard feedback fixes."""

import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from server import db, workspace_filter
from server.queries import (
    BILLING_KPIS_FAST,
    INFRA_COST_ESTIMATE,
    INFRA_COST_TIMESERIES,
    LAKEFLOW_JOB_STATS,
)
from server.routers import billing, dbsql_base, settings, tagging, warehouse_health


@pytest.mark.parametrize(
    "host",
    [
        "https://" + ("8" * 16) + ".7.gcp.databricks.com",
        "https://adb-" + ("8" * 16) + ".7.azuredatabricks.net",
    ],
)
def test_current_workspace_id_is_parsed_from_provider_host(monkeypatch, host):
    monkeypatch.delenv("DATABRICKS_WORKSPACE_ID", raising=False)
    workspace = SimpleNamespace(config=SimpleNamespace(host=host))
    with patch.object(db, "get_workspace_client", return_value=workspace):
        assert db.get_current_workspace_id() == "8" * 16


def test_numeric_workspace_host_is_not_used_as_account_display_name(monkeypatch):
    monkeypatch.delenv("DATABRICKS_ACCOUNT_NAME", raising=False)
    numeric_workspace_id = "8" * 16
    with (
        patch.object(
            billing,
            "get_host_url",
            return_value=f"https://{numeric_workspace_id}.gcp.databricks.com",
        ),
        patch.object(
            billing,
            "get_current_workspace_id",
            return_value=numeric_workspace_id,
        ),
    ):
        result = asyncio.run(billing.get_account_info())

    assert result["account_name"] is None
    assert result["workspace_id"] == numeric_workspace_id


def test_account_details_resolve_the_current_workspace_display_name(monkeypatch):
    monkeypatch.delenv("DATABRICKS_ACCOUNT_NAME", raising=False)
    monkeypatch.setenv("DATABRICKS_WORKSPACE_ID", "workspace-gcp-1")
    with (
        patch.object(
            billing,
            "execute_query",
            return_value=[{"account_id": "account-1", "cloud": "GCP"}],
        ),
        patch.object(
            billing,
            "_get_current_workspace_display_label",
            return_value="fevm-cmegdemos",
        ),
    ):
        result = asyncio.run(billing.get_account_details())

    assert result["account_name"] == "fevm-cmegdemos"


def test_account_label_prefers_deployment_name_over_workspace_name():
    with (
        patch.object(
            billing,
            "_get_account_workspace_names",
            return_value={"workspace-gcp-1": "cmegdemos"},
        ),
        patch.object(
            billing,
            "_account_ws_deployment_names",
            {"workspace-gcp-1": "fevm-cmegdemos"},
        ),
    ):
        assert (
            billing._get_current_workspace_display_label("workspace-gcp-1")
            == "fevm-cmegdemos"
        )


def test_current_workspace_scope_excludes_historical_rows_without_large_id_lists():
    token = workspace_filter.set_include_historical_workspaces(False)
    try:
        clause = workspace_filter.build_ws_filter_clause(col="u.workspace_id")
    finally:
        workspace_filter.reset_include_historical_workspaces(token)

    assert "system.access.workspaces_latest" in clause
    assert "current_ws.workspace_id" in clause
    assert "u.workspace_id" in clause


def test_explicit_workspace_selection_takes_precedence_over_history_scope():
    token = workspace_filter.set_include_historical_workspaces(False)
    try:
        clause = workspace_filter.build_ws_filter_clause(
            col="workspace_id",
            id_list=["historical-123"],
        )
    finally:
        workspace_filter.reset_include_historical_workspaces(token)

    assert clause == "AND CAST(workspace_id AS STRING) IN ('historical-123')"


def test_empty_source_workspace_intersection_is_an_explicit_empty_scope():
    clause = workspace_filter.build_ws_filter_clause(
        id_list=[workspace_filter.EMPTY_WORKSPACE_SCOPE_ID],
    )

    assert clause == "AND 1 = 0"


def test_workspace_picker_uses_the_selected_managed_source():
    captured: list[str] = []

    def execute(sql, *_args, **_kwargs):
        captured.append(sql)
        return [{"workspace_id": "current-1", "workspace_name": "Current one"}]

    source_token = db.set_source_labels(["shared-west"])
    try:
        with (
            patch.object(billing, "execute_query", side_effect=execute),
            patch.object(billing, "get_catalog_schema", return_value=("main", "cost_obs")),
            patch.object(settings, "workspace_names_enabled", return_value=True),
            patch.object(
                billing,
                "source_label_filter_clause",
                return_value="AND source_label IN ('shared-west')",
            ),
            patch.object(billing, "apply_mv_overrides", side_effect=lambda sql, *_: sql),
        ):
            result = asyncio.run(
                billing.get_workspace_list(
                    start_date="2026-08-01",
                    end_date="2026-08-31",
                )
            )
    finally:
        db.reset_source_labels(source_token)

    assert result["workspaces"][0]["id"] == "current-1"
    assert "daily_workspace_breakdown" in captured[0]
    assert "source_label IN ('shared-west')" in captured[0]
    assert "AS ws" in captured[0]
    assert "MAX(ws.workspace_name)" in captured[0]
    assert "GROUP BY ws.workspace_id" in captured[0]


@pytest.mark.parametrize(
    ("cloud", "workspace_name", "expected_region"),
    [
        ("GCP", "west4-serverless", "west4"),
        ("AWS", "customer-us-east-1-prod", "us-east-1"),
        ("AZURE", "customer-eastus2-prod", "eastus2"),
    ],
)
def test_workspace_breakdown_includes_cloud_and_region(
    cloud,
    workspace_name,
    expected_region,
):
    with (
        patch.object(billing, "_account_ws_cloud_metadata", {}),
        patch.object(settings, "workspace_names_enabled", return_value=True),
    ):
        result = billing._format_workspaces(
            [{
                "workspace_id": "123",
                "workspace_name": workspace_name,
                "cloud": cloud,
                "total_spend": 10,
                "total_dbus": 2,
                "top_products": [],
                "top_users": [],
            }],
            {"start_date": "2026-08-01", "end_date": "2026-08-31"},
        )

    assert result["workspaces"][0]["cloud"] == cloud.lower()
    assert result["workspaces"][0]["region"] == expected_region


@pytest.fixture(autouse=True)
def declared_shared_source_tables(monkeypatch):
    tables = list(db.MV_UNIFIED_TABLE_NAMES)
    monkeypatch.setattr(
        db,
        "get_mv_sources",
        lambda: [
            {"label": label, "tables": tables}
            for label in ("shared", "shared-west", "shared-east", "shared-central")
        ],
    )


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


def test_tagging_bundle_excludes_local_resource_details_when_source_filter_excludes_local():
    def run_queries(query_funcs, *_args, **_kwargs):
        return {name: func() for name, func in query_funcs}

    source_token = db.set_source_labels(["shared-west"])
    try:
        with (
            patch.object(tagging, "delta_cache_get", return_value=None),
            patch.object(tagging, "delta_cache_put"),
            patch.object(tagging, "capture_cache_generation"),
            patch.object(tagging, "_check_mv_available", return_value=False),
            patch.object(tagging, "local_source_is_selected", return_value=False),
            patch.object(
                db,
                "get_mv_sources",
                return_value=[{
                    "label": "shared-west",
                    "tables": ["daily_usage_summary"],
                }],
            ),
            patch.object(tagging, "execute_query", return_value=[]) as execute,
            patch.object(tagging, "execute_queries_parallel", side_effect=run_queries),
        ):
            result = asyncio.run(
                tagging.get_tagging_dashboard_bundle(
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    workspace_ids=None,
                )
            )
    finally:
        db.reset_source_labels(source_token)

    assert execute.call_count == 0
    assert result["availability"] == "unavailable"
    assert result["error_code"] == "SOURCE_SCOPE_UNSUPPORTED"


def test_compute_kpi_query_counts_sql_warehouses():
    assert "COUNT(DISTINCT usage_metadata.cluster_id) as total_clusters" in BILLING_KPIS_FAST
    assert "usage_metadata.warehouse_id" in BILLING_KPIS_FAST
    assert "as sql_warehouses" in BILLING_KPIS_FAST


def test_rightsizing_queries_apply_the_workspace_filter_to_every_table():
    captured: list[str] = []

    def run_queries(query_funcs, *_args, **_kwargs):
        for _name, query in query_funcs:
            query()
        return {}

    with (
        patch.object(
            warehouse_health,
            "execute_queries_parallel",
            side_effect=run_queries,
        ),
        patch.object(
            warehouse_health,
            "execute_query",
            side_effect=lambda sql, **_kwargs: captured.append(sql) or [],
        ),
    ):
        warehouse_health._run_health_queries(["123", "456"])

    assert len(captured) == 3
    assert all(
        "CAST(workspace_id AS STRING) IN ('123', '456')" in sql
        for sql in captured
    )


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
        patch.dict(
            billing.get_kpis_bundle.__globals__,
            {"_get_mv_query": lambda sql, *_args: sql},
        ),
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
        patch.dict(
            billing.get_kpis_bundle.__globals__,
            {"_get_mv_query": lambda sql, *_args: sql},
        ),
        patch.object(billing, "execute_queries_parallel", return_value=query_results),
    ):
        result = asyncio.run(billing.get_kpis_bundle(
            start_date="2026-07-01",
            end_date="2026-07-31",
            workspace_ids=None,
        ))

    assert result["kpis"]["successful_runs_available"] is False


def test_shared_only_kpis_never_reuse_unfiltered_stale_fallback():
    """Regression: 100/9/10/50% unfiltered KPIs must not replace shared 3/0/2/25%."""
    billing._kpis_stale.clear()

    def scoped_results(query_funcs, timeout=None):
        del query_funcs, timeout
        if db.selected_source_labels():
            return {
                "billing_kpis": [],
                "lakeflow_kpis": [],
                "anomalies": [],
                "mv_kpis": [{"total_queries": 3}],
                "avg_daily_ws": [{"avg_daily_workspaces": 2}],
                "avg_daily_query_users": [],
                "user_count": [],
                "total_workspaces": [],
                "avg_daily_models": [],
                "stickiness_pct": [{"stickiness_pct": 25.0}],
            }
        return {
            "billing_kpis": [{
                "total_jobs": 1,
                "total_job_runs": 1,
                "active_workspaces": 1,
            }],
            "lakeflow_kpis": [],
            "anomalies": [],
            "mv_kpis": [{"total_queries": 100}],
            "avg_daily_ws": [{"avg_daily_workspaces": 10}],
            "avg_daily_query_users": [],
            "user_count": [{"unique_query_users": 9}],
            "total_workspaces": [],
            "avg_daily_models": [],
            "stickiness_pct": [{"stickiness_pct": 50.0}],
        }

    common_patches = (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put"),
        patch.object(billing, "_check_mv_available", return_value=True),
        patch.object(billing, "get_catalog_schema", return_value=("catalog", "schema")),
        patch.object(billing, "get_local_source_label", return_value="local"),
        patch.object(
            db,
            "_list_existing_source_row_views",
            return_value=[
                "daily_usage_summary",
                "daily_query_stats",
                "daily_workspace_breakdown",
                "dbsql_cost_per_query",
            ],
        ),
        patch.object(db, "get_mv_table_overrides", return_value={}),
        patch.object(
            billing,
            "execute_queries_parallel",
            side_effect=scoped_results,
        ),
    )

    try:
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], \
                common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            unfiltered = asyncio.run(billing.get_kpis_bundle(
                start_date="2026-08-01",
                end_date="2026-08-28",
                workspace_ids="123",
            ))

            source_token = db.set_source_labels(["shared-west"])
            try:
                shared = asyncio.run(billing.get_kpis_bundle(
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    workspace_ids="123",
                ))
            finally:
                db.reset_source_labels(source_token)
    finally:
        billing._kpis_stale.clear()

    assert (
        unfiltered["kpis"]["total_queries"],
        unfiltered["kpis"]["unique_query_users"],
        unfiltered["kpis"]["avg_daily_workspaces"],
        unfiltered["kpis"]["stickiness_pct"],
    ) == (100, 9, 10, 50.0)
    assert (
        shared["kpis"]["total_queries"],
        shared["kpis"]["unique_query_users"],
        shared["kpis"]["avg_daily_workspaces"],
        shared["kpis"]["stickiness_pct"],
    ) == (3, 0, 2, 25.0)
    assert shared["kpis"].get("data_stale") is not True


PLATFORM_CARD_TREND_KEYS = [
    "total_queries",
    "total_rows_read",
    "total_bytes_read",
    "total_compute_seconds",
    "total_jobs",
    "total_job_runs",
    "successful_runs",
    "active_notebooks",
    "active_workspaces",
    "models_served",
    "total_users",
    "stickiness",
]


@pytest.mark.parametrize("kpi", PLATFORM_CARD_TREND_KEYS)
def test_every_clickable_platform_kpi_returns_daily_points(kpi):
    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put"),
        patch.object(
            billing,
            "capture_cache_generation",
            return_value=db.CacheGeneration("trend:kpis:platform-kpi", 0),
        ),
        patch.object(billing, "_check_mv_available", return_value=False),
        patch.dict(
            billing.get_platform_kpi_trend.__globals__,
            {"_get_mv_query": lambda sql, *_args: sql},
        ),
        patch.object(
            billing,
            "execute_query",
            return_value=[{"date": "2026-08-01", "value": 3}],
        ),
    ):
        result = asyncio.run(
            billing.get_platform_kpi_trend(
                kpi=kpi,
                start_date="2026-08-01",
                end_date="2026-08-28",
                granularity="daily",
                workspace_ids="123",
                tab="kpis",
            )
        )

    assert result["kpi"] == kpi
    assert result["data_points"] == [{"date": "2026-08-01", "value": 3.0}]


@pytest.mark.parametrize(
    ("kpi", "table_name"),
    [
        ("total_queries", "daily_query_stats"),
        ("total_rows_read", "daily_query_stats"),
        ("total_bytes_read", "daily_query_stats"),
        ("total_compute_seconds", "daily_query_stats"),
        ("active_workspaces", "daily_workspace_breakdown"),
        ("total_users", "dbsql_cost_per_query"),
        ("stickiness", "dbsql_cost_per_query"),
    ],
)
def test_source_capable_platform_trends_match_card_tables_and_scope(kpi, table_name):
    token = db.set_source_labels(["shared-west"])
    try:
        with (
            patch.object(billing, "delta_cache_get", return_value=None),
            patch.object(billing, "delta_cache_put"),
            patch.object(
                billing,
                "capture_cache_generation",
                return_value=db.CacheGeneration("trend:kpis:platform-kpi", 0),
            ),
            patch.object(billing, "_check_mv_available", return_value=True),
            patch.object(billing, "get_catalog_schema", return_value=("catalog", "schema")),
            patch.object(db, "_list_existing_source_row_views", return_value=[table_name]),
            patch.object(db, "get_mv_table_overrides", return_value={}),
            patch.object(
                billing,
                "execute_query",
                return_value=[{"date": "2026-08-01", "value": 3}],
            ) as execute,
        ):
            result = asyncio.run(
                billing.get_platform_kpi_trend(
                    kpi=kpi,
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    granularity="daily",
                    workspace_ids="123",
                    tab="kpis",
                )
            )

        sql = execute.call_args.args[0]
        assert f"`{table_name}__source_rows`" in sql
        assert "source_label IN ('shared-west')" in sql
        assert "CAST(workspace_id AS STRING) IN ('123')" in sql
        assert result["summary"]["avg_value"] == 3.0
    finally:
        db.reset_source_labels(token)


@pytest.mark.parametrize("kpi", ["total_users", "stickiness"])
def test_query_user_card_and_trend_use_identical_managed_scope(kpi):
    token = db.set_source_labels(["shared-west"])
    card_sql: list[str] = []

    def execute_card(sql, *_args, **_kwargs):
        if (
            "as unique_query_users" in sql.lower()
            and "dbsql_cost_per_query" in sql
        ):
            card_sql.append(sql)
            return [{"unique_query_users": 4}]
        if "as stickiness_pct" in sql.lower():
            card_sql.append(sql)
            return [{"stickiness_pct": 50.0}]
        return []

    def execute_sequential(query_funcs, timeout=None):
        del timeout
        return {name: fn() for name, fn in query_funcs}

    try:
        with (
            patch.object(billing, "delta_cache_get", return_value=None),
            patch.object(billing, "delta_cache_put"),
            patch.object(billing, "_check_mv_available", return_value=True),
            patch.object(billing, "get_catalog_schema", return_value=("catalog", "schema")),
            patch.object(
                db,
                "_list_existing_source_row_views",
                return_value=[
                    "dbsql_cost_per_query",
                    "daily_usage_summary",
                    "daily_query_stats",
                    "daily_workspace_breakdown",
                ],
            ),
            patch.object(
                db,
                "_list_existing_unified_views",
                return_value=[
                    "daily_usage_summary",
                    "dbsql_cost_per_query",
                    "daily_query_stats",
                    "daily_workspace_breakdown",
                ],
            ),
            patch.object(db, "get_mv_table_overrides", return_value={}),
            patch.object(billing, "execute_query", side_effect=execute_card),
            patch.object(
                billing,
                "execute_queries_parallel",
                side_effect=execute_sequential,
            ),
        ):
            bundle = asyncio.run(
                billing.get_kpis_bundle(
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    workspace_ids="123",
                )
            )

        trend_sql: list[str] = []
        with (
            patch.object(billing, "delta_cache_get", return_value=None),
            patch.object(billing, "delta_cache_put"),
            patch.object(
                billing,
                "capture_cache_generation",
                return_value=db.CacheGeneration("trend:kpis:platform-kpi", 0),
            ),
            patch.object(billing, "get_catalog_schema", return_value=("catalog", "schema")),
            patch.object(
                db,
                "_list_existing_source_row_views",
                return_value=["dbsql_cost_per_query"],
            ),
            patch.object(db, "get_mv_table_overrides", return_value={}),
            patch.object(
                billing,
                "execute_query",
                side_effect=lambda sql, *_args, **_kwargs: (
                    trend_sql.append(sql)
                    or [{"date": "2026-08-01", "value": 50}]
                ),
            ),
        ):
            trend = asyncio.run(
                billing.get_platform_kpi_trend(
                    kpi=kpi,
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    granularity="daily",
                    workspace_ids="123",
                    tab="kpis",
                )
            )

        alias = "as unique_query_users" if kpi == "total_users" else "as stickiness_pct"
        scoped_card_sql = next(sql for sql in card_sql if alias in sql.lower())
        scoped_trend_sql = trend_sql[0]
        for sql in (scoped_card_sql, scoped_trend_sql):
            assert "`dbsql_cost_per_query__source_rows`" in sql
            assert "source_label IN ('shared-west')" in sql
            assert "CAST(workspace_id AS STRING) IN ('123')" in sql
            assert "start_time >= :start_date" in sql
            assert "start_time < DATE_ADD(CAST(:end_date AS DATE), 1)" in sql
            assert "system.query.history" not in sql
        assert bundle["kpis"]["query_users_available"] is True
        assert bundle["kpis"]["stickiness_available"] is True
        assert trend["data_points"] == [{"date": "2026-08-01", "value": 50.0}]
    finally:
        db.reset_source_labels(token)


@pytest.mark.parametrize(
    ("labels", "expected_filter"),
    [
        (["local"], "source_label IN ('local')"),
        ([], None),
        (["shared-west"], "source_label IN ('shared-west')"),
    ],
)
def test_kpis_anomalies_use_source_aware_daily_summary(labels, expected_filter):
    token = db.set_source_labels(labels)
    anomaly_sql: list[str] = []

    def execute(sql, *_args, **_kwargs):
        anomaly_sql.append(sql)
        return []

    def execute_anomaly_only(query_funcs, timeout=None):
        del timeout
        results = {}
        for name, fn in query_funcs:
            results[name] = fn() if name == "anomalies" else []
        return results

    try:
        with (
            patch.object(billing, "delta_cache_get", return_value=None),
            patch.object(billing, "delta_cache_put"),
            patch.object(billing, "_check_mv_available", return_value=True),
            patch.object(billing, "get_catalog_schema", return_value=("catalog", "schema")),
            patch.object(
                db,
                "_list_existing_source_row_views",
                return_value=[
                    "daily_usage_summary",
                    "dbsql_cost_per_query",
                    "daily_query_stats",
                    "daily_workspace_breakdown",
                ],
            ),
            patch.object(
                db,
                "_list_existing_unified_views",
                return_value=[
                    "daily_usage_summary",
                    "dbsql_cost_per_query",
                    "daily_query_stats",
                    "daily_workspace_breakdown",
                ],
            ),
            patch.object(db, "get_mv_table_overrides", return_value={}),
            patch.object(billing, "execute_query", side_effect=execute),
            patch.object(
                billing,
                "execute_queries_parallel",
                side_effect=execute_anomaly_only,
            ),
        ):
            result = asyncio.run(
                billing.get_kpis_bundle(
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    workspace_ids="123",
                )
            )

        assert len(anomaly_sql) == 1
        expected_view = (
            "`daily_usage_summary__source_rows`"
            if labels
            else "`daily_usage_summary__unified`"
        )
        assert expected_view in anomaly_sql[0]
        assert "CAST(workspace_id AS STRING) IN ('123')" in anomaly_sql[0]
        assert "system.billing.usage" not in anomaly_sql[0]
        if expected_filter:
            assert expected_filter in anomaly_sql[0]
        else:
            assert "source_label IN" not in anomaly_sql[0]
        assert result["anomalies"]["available"] is True
    finally:
        db.reset_source_labels(token)


def test_kpis_anomalies_do_not_query_local_when_shared_scope_cannot_be_routed():
    token = db.set_source_labels(["shared-west"])
    query_names: list[str] = []

    def record_queries(query_funcs, timeout=None):
        del timeout
        query_names.extend(name for name, _fn in query_funcs)
        return {name: [] for name, _fn in query_funcs}

    try:
        with (
            patch.object(billing, "delta_cache_get", return_value=None),
            patch.object(billing, "delta_cache_put"),
            patch.object(billing, "_check_mv_available", return_value=True),
            patch.object(billing, "get_catalog_schema", return_value=("catalog", "schema")),
            patch.object(
                db,
                "_list_existing_source_row_views",
                return_value=[
                    "dbsql_cost_per_query",
                    "daily_query_stats",
                    "daily_workspace_breakdown",
                ],
            ),
            patch.object(db, "get_mv_table_overrides", return_value={}),
            patch.object(
                billing,
                "execute_queries_parallel",
                side_effect=record_queries,
            ),
        ):
            result = asyncio.run(
                billing.get_kpis_bundle(
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    workspace_ids=None,
                )
            )

        assert "anomalies" not in query_names
        assert result["anomalies"]["anomalies"] == []
        assert result["anomalies"]["available"] is False
    finally:
        db.reset_source_labels(token)


def test_query_user_cards_are_unavailable_when_managed_source_cannot_be_routed():
    token = db.set_source_labels(["shared-west"])
    query_names: list[str] = []

    def record_queries(query_funcs, timeout=None):
        del timeout
        query_names.extend(name for name, _fn in query_funcs)
        return {name: [] for name, _fn in query_funcs}

    try:
        with (
            patch.object(billing, "delta_cache_get", return_value=None),
            patch.object(billing, "delta_cache_put"),
            patch.object(billing, "_check_mv_available", return_value=True),
            patch.object(billing, "get_catalog_schema", return_value=("catalog", "schema")),
            patch.object(
                db,
                "_list_existing_source_row_views",
                return_value=[
                    "daily_usage_summary",
                    "daily_query_stats",
                    "daily_workspace_breakdown",
                ],
            ),
            patch.object(db, "get_mv_table_overrides", return_value={}),
            patch.object(
                billing,
                "execute_queries_parallel",
                side_effect=record_queries,
            ),
        ):
            result = asyncio.run(
                billing.get_kpis_bundle(
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    workspace_ids="123",
                )
            )

        assert "user_count" not in query_names
        assert "stickiness_pct" not in query_names
        assert result["kpis"]["query_users_available"] is False
        assert result["kpis"]["stickiness_available"] is False
    finally:
        db.reset_source_labels(token)


@pytest.mark.parametrize(
    ("kpi", "card_sql", "trend_sql"),
    [
        ("total_jobs", "COUNT(DISTINCT CASE WHEN usage_metadata.job_id IS NOT NULL", "COUNT(DISTINCT usage_metadata.job_id)"),
        ("total_job_runs", "SUM(CASE WHEN usage_metadata.job_id IS NOT NULL THEN 1 ELSE 0 END)", "COUNT(*) as value"),
        ("active_notebooks", "COUNT(DISTINCT usage_metadata.cluster_id)", "COUNT(DISTINCT usage_metadata.cluster_id)"),
        ("models_served", "COUNT(DISTINCT CASE WHEN sku_name LIKE '%INFERENCE%'", "COUNT(DISTINCT usage_metadata.endpoint_name)"),
    ],
)
def test_local_platform_trends_use_the_card_metric_source(kpi, card_sql, trend_sql):
    assert card_sql in BILLING_KPIS_FAST
    captured = []
    with (
        patch.object(billing, "delta_cache_get", return_value=None),
        patch.object(billing, "delta_cache_put"),
        patch.object(
            billing,
            "capture_cache_generation",
            return_value=db.CacheGeneration("trend:kpis:platform-kpi", 0),
        ),
        patch.object(billing, "_check_mv_available", return_value=False),
        patch.object(
            billing,
            "execute_query",
            side_effect=lambda sql, *_args: captured.append(sql) or [{"date": "2026-08-01", "value": 1}],
        ),
    ):
        asyncio.run(
            billing.get_platform_kpi_trend(
                kpi=kpi,
                start_date="2026-08-01",
                end_date="2026-08-28",
                granularity="daily",
                workspace_ids="123",
                tab="kpis",
            )
        )

    assert trend_sql in captured[0]
    assert "system.billing.usage" in captured[0]
    assert "CAST(workspace_id AS STRING) IN ('123')" in captured[0]


def test_local_only_platform_trend_is_empty_when_local_source_is_excluded():
    token = db.set_source_labels(["shared-west"])
    try:
        with (
            patch.object(billing, "delta_cache_get", return_value=None),
            patch.object(billing, "delta_cache_put"),
            patch.object(
                billing,
                "capture_cache_generation",
                return_value=db.CacheGeneration("trend:kpis:platform-kpi", 0),
            ),
            patch.object(billing, "_check_mv_available", return_value=False),
            patch.object(billing, "get_local_source_label", return_value="local"),
            patch.object(billing, "execute_query") as execute,
        ):
            result = asyncio.run(
                billing.get_platform_kpi_trend(
                    kpi="total_jobs",
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    granularity="daily",
                    workspace_ids=None,
                    tab="kpis",
                )
            )

        execute.assert_not_called()
        assert result["data_points"] == []
    finally:
        db.reset_source_labels(token)


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
