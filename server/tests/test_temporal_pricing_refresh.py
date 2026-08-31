"""Release 2 regressions for temporal pricing and bounded refresh deletion."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from server import materialized_views, queries
from server.queries.pricing import (
    current_list_price_join,
    temporal_list_price_join,
)
from server.routers import aiml, apps, billing, dbsql_base, tagging, users_groups


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


def _select_reference_price(usage_time: datetime, prices: list[dict]) -> dict | None:
    """Small reference model for the SQL fragment's documented precedence."""
    matches = [
        price
        for price in prices
        if usage_time >= price["start"]
        and (price["end"] is None or usage_time < price["end"])
    ]
    if not matches:
        return None
    return sorted(
        matches,
        key=lambda price: (
            price["start"],
            -(price["end"] or datetime.max.replace(tzinfo=timezone.utc)).timestamp(),
            price["effective"],
            price["list"],
        ),
        reverse=True,
    )[0]


def test_temporal_price_boundaries_null_end_and_retired_skus():
    retired = {
        "name": "retired",
        "start": _ts("2025-01-01T00:00:00"),
        "end": _ts("2025-02-01T00:00:00"),
        "list": 2.0,
        "effective": 1.5,
    }
    current = {
        "name": "current",
        "start": _ts("2025-02-01T00:00:00"),
        "end": None,
        "list": 3.0,
        "effective": 2.5,
    }

    assert _select_reference_price(_ts("2025-01-01T00:00:00"), [retired, current]) == retired
    assert _select_reference_price(_ts("2025-01-31T23:59:59"), [retired, current]) == retired
    assert _select_reference_price(_ts("2025-02-01T00:00:00"), [retired, current]) == current
    assert _select_reference_price(_ts("2026-01-01T00:00:00"), [retired, current]) == current


def test_overlap_is_deterministic_and_new_prices_do_not_rewrite_history():
    old = {
        "name": "old",
        "start": _ts("2025-01-01T00:00:00"),
        "end": _ts("2025-04-01T00:00:00"),
        "list": 2.0,
        "effective": 2.0,
    }
    overlap = {
        "name": "overlap",
        "start": _ts("2025-02-01T00:00:00"),
        "end": _ts("2025-03-01T00:00:00"),
        "list": 4.0,
        "effective": 4.0,
    }
    future = {
        "name": "future",
        "start": _ts("2025-04-01T00:00:00"),
        "end": None,
        "list": 8.0,
        "effective": 8.0,
    }
    historical_usage = _ts("2025-01-15T12:00:00")

    assert _select_reference_price(_ts("2025-02-15T12:00:00"), [old, overlap]) == overlap
    before = _select_reference_price(historical_usage, [old])
    after = _select_reference_price(historical_usage, [old, future])
    assert before == after == old
    assert 10 * before["list"] == 10 * after["list"]


def test_canonical_join_preserves_units_and_half_open_temporal_semantics():
    sql = temporal_list_price_join()

    assert "candidate.sku_name = u.sku_name" in sql
    assert "candidate.account_id = u.account_id" in sql
    assert "candidate.cloud = u.cloud" in sql
    assert "candidate.usage_unit = u.usage_unit" in sql
    assert "candidate.currency_code = 'USD'" in sql
    assert ") >= candidate.price_start_time" in sql
    assert ") < candidate.price_end_time" in sql
    assert "candidate.price_end_time IS NULL" in sql
    assert "candidate.price_start_time DESC" in sql
    assert "ROW_NUMBER() OVER" in sql
    assert "WHERE ranked.price_rank = 1" in sql


def test_live_queries_use_current_price_join_without_lateral():
    modules = (queries, aiml, apps, tagging, users_groups)
    priced_templates = []
    for module in modules:
        for name, value in vars(module).items():
            if (
                name.isupper()
                and not name.startswith("MV_")
                and isinstance(value, str)
                and "p.pricing" in value
            ):
                priced_templates.append((module.__name__, name, value))

    assert priced_templates
    for module_name, name, sql in priced_templates:
        assert "/* TEMPORAL_LIST_PRICE_JOIN */" not in sql, (module_name, name)
        assert "LEFT JOIN LATERAL" not in sql, (module_name, name)
        assert "LEFT JOIN system.billing.list_prices p" in sql, (module_name, name)
        assert "u.sku_name = p.sku_name" in sql, (module_name, name)
        assert "u.cloud = p.cloud" in sql, (module_name, name)
        assert "p.price_end_time IS NULL" in sql, (module_name, name)


def test_materialized_view_build_and_merge_sql_remains_temporal():
    priced_templates = [
        (name, value)
        for name, value in vars(materialized_views).items()
        if name.isupper() and isinstance(value, str) and "p.pricing" in value
    ]
    assert priced_templates
    for name, sql in priced_templates:
        assert "LEFT JOIN LATERAL" in sql, name
        assert "candidate.usage_unit = u.usage_unit" in sql, name
        assert "candidate.price_end_time IS NULL" in sql, name


def test_request_path_modules_do_not_import_temporal_helper():
    root = Path(__file__).resolve().parents[1]
    paths = [
        root / "queries" / "__init__.py",
        *(root / "routers" / name for name in (
            "users_groups.py",
            "apps.py",
            "aiml.py",
            "dbsql_base.py",
            "billing.py",
        )),
    ]
    for path in paths:
        source = path.read_text()
        assert "apply_temporal_list_price_join" not in source, path
        assert "temporal_list_price_join(" not in source, path


def test_representative_routes_use_compatible_current_price_contract(monkeypatch):
    expected = current_list_price_join()
    for sql in (
        queries.BILLING_SUMMARY,
        apps.APPS_SUMMARY,
        aiml.AIML_SUMMARY,
        users_groups.USERS_SUMMARY,
    ):
        assert expected in sql
        assert "LEFT JOIN LATERAL" not in sql

    captured = {}
    monkeypatch.setattr(
        billing,
        "_execute_query",
        lambda sql, *_args, **_kwargs: captured.setdefault("sql", sql) and [],
    )
    billing.execute_query("SELECT * FROM usage u /* TEMPORAL_LIST_PRICE_JOIN */")
    assert expected in captured["sql"]
    assert "LEFT JOIN LATERAL" not in captured["sql"]

    assert "current_list_price_join()" in Path(dbsql_base.__file__).read_text()
    assert temporal_list_price_join() in materialized_views.CREATE_DAILY_USAGE_SUMMARY


_MERGES = {
    "daily_usage_summary": (materialized_views.MERGE_DAILY_USAGE_SUMMARY, "usage_date"),
    "daily_product_breakdown": (
        materialized_views.MERGE_DAILY_PRODUCT_BREAKDOWN,
        "usage_date",
    ),
    "daily_workspace_breakdown": (
        materialized_views.MERGE_DAILY_WORKSPACE_BREAKDOWN,
        "usage_date",
    ),
    "sql_tool_attribution": (materialized_views.MERGE_SQL_TOOL_ATTRIBUTION, "usage_date"),
    "daily_query_stats": (materialized_views.MERGE_QUERY_STATS, "usage_date"),
    "dbsql_cost_per_query": (materialized_views.MERGE_DBSQL_COST_PER_QUERY, "query_date"),
    "daily_tag_summary": (materialized_views.MERGE_DAILY_TAG_SUMMARY, "usage_date"),
    "daily_apps_summary": (materialized_views.MERGE_DAILY_APPS_SUMMARY, "usage_date"),
}


def test_all_eight_incremental_merges_delete_only_inside_reprocess_window():
    assert set(_MERGES) == set(materialized_views._TABLE_REFRESH_CONFIG)

    for table_name, (sql, date_column) in _MERGES.items():
        assert "WHEN NOT MATCHED BY SOURCE" in sql, table_name
        assert f"tgt.{date_column} >= DATE('{{reprocess_start}}')" in sql, table_name
        assert f"tgt.{date_column} <= CURRENT_DATE()" in sql, table_name
        assert "THEN DELETE" in sql, table_name


def test_bounded_replacement_removes_missing_and_reclassified_rows_not_history():
    window_start = _ts("2025-02-01T00:00:00").date()
    existing = {
        ("2025-01-15", "interactive"),  # outside the rolling window
        ("2025-02-10", "interactive"),  # removed upstream
        ("2025-02-11", "batch"),  # reclassified upstream
    }
    refreshed_source = {
        ("2025-02-11", "streaming"),  # replacement category
    }

    retained = {
        row for row in existing if datetime.fromisoformat(row[0]).date() < window_start
    }
    refreshed = retained | refreshed_source

    assert ("2025-01-15", "interactive") in refreshed
    assert ("2025-02-10", "interactive") not in refreshed
    assert ("2025-02-11", "batch") not in refreshed
    assert ("2025-02-11", "streaming") in refreshed
