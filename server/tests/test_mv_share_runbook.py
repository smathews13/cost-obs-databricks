"""Regression coverage for the standalone aggregate/share publisher runbook."""

import asyncio
import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

from server import db, materialized_views
from server.queries.pricing import apply_temporal_list_price_join
from server.routers import settings

ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "server" / "assets" / "cost_obs_mv_share_publisher.py"
REPOSITORY_RUNBOOK = ROOT / "notebooks" / "cost_obs_mv_share_publisher.py"


def test_runbook_is_generated_from_the_exact_runtime_table_contract():
    rendered = RUNBOOK.read_text()
    canonical_sql = {
        name: apply_temporal_list_price_join(template).strip()
        for name, template in materialized_views.CREATE_MV_TABLES.items()
    }
    contract_hash = hashlib.sha256(
        json.dumps(
            canonical_sql,
            sort_keys=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()

    assert list(materialized_views.CREATE_MV_TABLES) == list(db.MV_UNIFIED_TABLE_NAMES)
    assert f"Contract: `{contract_hash}`" in rendered
    for table_name in db.MV_UNIFIED_TABLE_NAMES:
        assert f"`{{catalog}}`.`{{schema}}`.`{table_name}`" in rendered
    assert rendered.count("CREATE OR REPLACE TABLE") == len(db.MV_UNIFIED_TABLE_NAMES)
    assert "CREATE SHARE IF NOT EXISTS" in rendered
    assert "SHOW ALL IN SHARE" in rendered
    assert "ADD TABLE" in rendered
    assert "# environment_version = \"5\"" in rendered
    assert '_dropdown("share_mode", "schema", ["schema", "table"], "Share mode")' in rendered
    assert 'ALTER SHARE `{SHARE}` ADD SCHEMA `{CATALOG}`.`{SCHEMA}`' in rendered
    assert 'ALTER SHARE `{SHARE}` REMOVE TABLE `{cat}`.`{sch}`.`{tbl}`' in rendered
    assert "SCHEMA ALREADY SHARED" in rendered
    assert '_widget("lookback_days", "180", "Billing/query lookback days")' in rendered
    for source_table in (
        "system.access.workspaces_latest",
        "system.billing.list_prices",
        "system.billing.usage",
        "system.query.history",
    ):
        assert source_table in rendered
    assert "SKIPPED_PERMISSION" in rendered
    assert "Continuing with partial capability" in rendered
    assert "Source-table permission preflight failed" not in rendered
    assert "GRANT SELECT ON TABLE" in rendered
    assert "GRANT SELECT ON SCHEMA `<recipient_catalog>`.`<shared_schema>`" in rendered
    assert REPOSITORY_RUNBOOK.read_text() == rendered
    compile(rendered, str(RUNBOOK), "exec")


def test_settings_download_serves_the_generated_notebook_to_admins():
    request = object()
    with patch.object(
        settings,
        "_require_admin_async",
        new=AsyncMock(return_value="admin@example.com"),
    ):
        response = asyncio.run(settings.download_materialized_view_runbook(request))

    assert Path(response.path) == RUNBOOK
    assert response.media_type == "text/x-python"
    assert "cost_obs_mv_share_publisher.py" in response.headers["content-disposition"]
