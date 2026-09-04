import asyncio
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from server import db
from server.routers import dbsql_base, users_groups


@pytest.fixture(autouse=True)
def declared_shared_source_tables(monkeypatch):
    tables = list(db.MV_UNIFIED_TABLE_NAMES)
    monkeypatch.setattr(
        db,
        "get_mv_sources",
        lambda: [{"label": "shared-west", "tables": tables}],
    )


def _endpoint(router, path: str):
    return next(route.endpoint for route in router.routes if route.path == path)


def test_request_scope_rejects_invalid_reversed_future_and_oversized_ranges():
    utc_today = datetime.now(timezone.utc).date()
    yesterday = utc_today - timedelta(days=1)
    future = utc_today.isoformat()

    with pytest.raises(HTTPException, match="ISO format"):
        dbsql_base.validate_request_scope("not-a-date", yesterday.isoformat())
    with pytest.raises(HTTPException, match="on or before"):
        dbsql_base.validate_request_scope(yesterday.isoformat(), "2020-01-01")
    with pytest.raises(HTTPException, match="yesterday or earlier"):
        dbsql_base.validate_request_scope(yesterday.isoformat(), future)
    with pytest.raises(HTTPException, match="six|6 calendar months"):
        dbsql_base.validate_request_scope("2020-01-01", yesterday.isoformat())


def test_request_scope_caps_workspace_and_source_label_counts():
    with pytest.raises(HTTPException, match="workspace_ids may contain at most"):
        dbsql_base.validate_request_scope(
            None,
            None,
            ",".join(f"ws-{index}" for index in range(dbsql_base.MAX_WORKSPACE_IDS + 1)),
        )
    with pytest.raises(HTTPException, match="source_labels may contain at most"):
        dbsql_base.validate_request_scope(
            None,
            None,
            source_labels=[
                f"source-{index}" for index in range(dbsql_base.MAX_SOURCE_LABELS + 1)
            ],
        )


def test_users_shared_only_scope_is_explicitly_unavailable():
    with (
        patch.object(users_groups, "get_local_source_label", return_value="local"),
        patch.object(users_groups, "execute_queries_parallel") as execute,
    ):
        result = asyncio.run(
            users_groups.get_users_groups_bundle(
                start_date="2026-08-01",
                end_date="2026-08-28",
                workspace_ids=None,
                source_labels=["shared-west"],
            )
        )

    assert result["available"] is False
    assert result["availability"] == "unavailable"
    assert result["reason"] == "identity_detail_unavailable_for_shared_sources"
    assert result["top_users"] == []
    execute.assert_not_called()


def test_users_mixed_scope_returns_only_filtered_local_identity_rows():
    captured_sql: list[str] = []

    def execute(sql, _params=None):
        captured_sql.append(sql)
        if "COUNT(DISTINCT user_email)" in sql:
            return [{
                "user_count": 1,
                "workspace_count": 1,
                "total_spend": 10,
                "total_dbus": 2,
                "avg_spend_per_user": 10,
            }]
        if "GROUP BY user_email\nORDER BY total_spend" in sql:
            return [{
                "user_email": "local@example.com",
                "total_spend": 10,
                "total_dbus": 2,
                "active_days": 1,
                "workspace_count": 1,
            }]
        return []

    def sequential(queries, timeout=None, **_kwargs):
        del timeout
        return {name: fn() for name, fn in queries}

    with (
        patch.object(users_groups, "get_local_source_label", return_value="local"),
        patch.object(users_groups, "delta_cache_get", return_value=None),
        patch.object(users_groups, "delta_cache_put"),
        patch.object(users_groups, "execute_query", side_effect=execute),
        patch.object(
            users_groups, "execute_queries_parallel", side_effect=sequential
        ),
    ):
        result = asyncio.run(
            users_groups._compute_users_groups_bundle(
                start_date="2026-08-01",
                end_date="2026-08-28",
                workspace_ids="123",
                source_labels=["local", "shared-west"],
            )
        )

    assert result["available"] is True
    assert result["availability"] == "partial"
    assert result["reason"] == "shared_identity_detail_omitted"
    assert [row["user_email"] for row in result["top_users"]] == [
        "local@example.com"
    ]
    assert captured_sql
    assert all(
        "CAST(u.workspace_id AS STRING) IN ('123')" in sql for sql in captured_sql
    )
    timeseries_sql = next(sql for sql in captured_sql if "daily AS (" in sql)
    assert timeseries_sql.count("CAST(u.workspace_id AS STRING) IN ('123')") == 1


def test_dbsql_source_drilldown_uses_unified_source_and_workspace_scope():
    router = dbsql_base.create_dbsql_router("dbsql_cost_per_query")
    endpoint = _endpoint(router, "/top-queries-by-source")
    dbsql_base._mv_status_cache["dbsql_cost_per_query"] = (
        time.monotonic(),
        {"mv_available": True},
    )
    captured: list[str] = []
    source_token = db.set_source_labels(["shared-west"])
    try:
        with (
            patch.object(dbsql_base, "get_catalog_schema", return_value=("c", "s")),
            patch.object(
                db, "_list_existing_source_row_views", return_value=["dbsql_cost_per_query"]
            ),
            patch.object(db, "get_mv_table_overrides", return_value={}),
            patch.object(
                dbsql_base,
                "execute_query",
                side_effect=lambda sql, _params=None: captured.append(sql) or [],
            ),
        ):
            result = asyncio.run(
                endpoint(
                    source_type="JOB",
                    start_date="2026-08-01",
                    end_date="2026-08-28",
                    limit=5,
                    workspace_ids="123",
                    source_labels=["shared-west"],
                )
            )
    finally:
        db.reset_source_labels(source_token)
        dbsql_base._mv_status_cache.clear()

    assert result["available"] is True
    assert len(captured) == 1
    assert "`c`.`s`.`dbsql_cost_per_query__source_rows`" in captured[0]
    assert "source_label IN ('shared-west')" in captured[0]
    assert "CAST(workspace_id AS STRING) IN ('123')" in captured[0]
