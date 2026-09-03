"""Regression coverage for the standalone aggregate/share publisher runbook."""

import asyncio
import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock, patch

from server import db, materialized_views
from server.routers import settings

ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "notebooks" / "cost_obs_mv_share_publisher.py"
GENERATOR = ROOT / "scripts" / "generate_mv_share_runbook.py"


def _generator_module():
    spec = importlib.util.spec_from_file_location("generate_mv_share_runbook", GENERATOR)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_runbook_is_generated_from_the_exact_runtime_table_contract():
    generator = _generator_module()
    rendered = generator.render()

    assert RUNBOOK.read_text() == rendered
    assert list(materialized_views.CREATE_MV_TABLES) == list(db.MV_UNIFIED_TABLE_NAMES)
    for table_name in db.MV_UNIFIED_TABLE_NAMES:
        assert f"`{{catalog}}`.`{{schema}}`.`{table_name}`" in rendered
    assert rendered.count("CREATE OR REPLACE TABLE") == len(db.MV_UNIFIED_TABLE_NAMES)
    assert "CREATE SHARE IF NOT EXISTS" in rendered
    assert "SHOW ALL IN SHARE" in rendered
    assert "ADD TABLE" in rendered
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
