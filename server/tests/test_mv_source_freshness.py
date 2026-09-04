import asyncio
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from server.routers import settings


def test_shared_source_freshness_check_reprobes_and_rebuilds_views():
    sources = [{
        "label": "west",
        "catalog": "shared_catalog",
        "schema": "cost_obs",
        "tables": ["daily_usage_summary"],
    }]

    with (
        patch.object(settings, "_require_admin"),
        patch("server.db.get_mv_sources", return_value=sources),
        patch("server.db.get_catalog_schema", return_value=("local_catalog", "cost_obs")),
        patch("server.materialized_views._table_columns", return_value=["usage_date", "total_spend"]),
        patch("server.materialized_views._rebuild_unified_views_locked", return_value={"ok": True, "views": {}}) as rebuild,
        patch("server.materialized_views.unified_views_rebuild_lock"),
        patch.object(settings, "_refresh_shared_catalog", return_value={"ok": True}) as refresh,
        patch.object(settings, "_share_last_updated", return_value="2026-08-28T12:00:00Z"),
        patch.object(settings, "_invalidate_mv_caches") as invalidate,
    ):
        result = asyncio.run(settings.check_mv_source_freshness(None, "west"))

    assert result["ok"] is True
    assert result["matched"] == 1
    assert result["total"] == 1
    assert result["share_last_updated"] == "2026-08-28T12:00:00Z"
    assert result["catalog_refresh"] == {"ok": True}
    refresh.assert_called_once_with("shared_catalog")
    rebuild.assert_called_once()
    invalidate.assert_called_once()


def test_shared_source_freshness_check_rejects_unknown_label():
    with (
        patch.object(settings, "_require_admin"),
        patch("server.db.get_mv_sources", return_value=[]),
        patch("server.materialized_views.unified_views_rebuild_lock"),
    ):
        with pytest.raises(HTTPException) as exc:
            asyncio.run(settings.check_mv_source_freshness(None, "missing"))

    assert exc.value.status_code == 404


def test_shared_source_preview_refreshes_recipient_catalog_before_probing():
    columns = {
        "`local_catalog`.`cost_obs`.`daily_usage_summary`": ["usage_date", "total_spend"],
        "`shared_catalog`.`cost_obs`.`daily_usage_summary`": ["usage_date", "total_spend"],
    }
    with (
        patch.object(settings, "_require_admin_async"),
        patch("server.db.get_catalog_schema", return_value=("local_catalog", "cost_obs")),
        patch("server.materialized_views._MV_TABLES", ["daily_usage_summary"]),
        patch(
            "server.materialized_views._table_columns",
            side_effect=lambda table: columns.get(table),
        ),
        patch.object(settings, "_refresh_shared_catalog", return_value={"ok": True}) as refresh,
    ):
        result = asyncio.run(
            settings.preview_mv_source(None, "shared_catalog", "cost_obs")
        )

    assert result["matched"] == 1
    assert result["catalog_refresh"] == {"ok": True}
    refresh.assert_called_once_with("shared_catalog")


def test_refresh_shared_catalog_uses_recipient_refresh_ddl():
    with patch("server.db.execute_query") as execute:
        result = settings._refresh_shared_catalog("west4_share")

    assert result == {"ok": True}
    execute.assert_called_once_with(
        "ALTER CATALOG `west4_share` REFRESH",
        no_cache=True,
        timeout=60,
    )
