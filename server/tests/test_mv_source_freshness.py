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
        patch.object(settings, "_share_last_updated", return_value="2026-08-28T12:00:00Z"),
        patch.object(settings, "_invalidate_mv_caches") as invalidate,
    ):
        result = asyncio.run(settings.check_mv_source_freshness(None, "west"))

    assert result["ok"] is True
    assert result["matched"] == 1
    assert result["total"] == 1
    assert result["share_last_updated"] == "2026-08-28T12:00:00Z"
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
