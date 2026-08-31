"""Release 2 deployment configuration consistency checks."""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _manifest_text(name: str) -> str:
    return (ROOT / name).read_text()


def test_bound_warehouse_resource_does_not_require_http_path():
    for name in ("app.yaml", "app.yaml.example"):
        manifest = _manifest_text(name)
        assert re.search(
            r"name:\s*sql-warehouse.*?sql_warehouse:\s*\n"
            r"\s+permission:\s*CAN_USE",
            manifest,
            re.DOTALL,
        )
        assert re.search(
            r"name:\s*DATABRICKS_WAREHOUSE_ID\s*\n"
            r"\s+valueFrom:\s*sql-warehouse",
            manifest,
        )
        assert not re.search(
            r"^\s*-\s+name:\s*DATABRICKS_HTTP_PATH\s*$",
            manifest,
            re.MULTILINE,
        )

    deploy_script = (ROOT / "dba_deploy.sh").read_text()
    assert 'WAREHOUSE_ID="${DATABRICKS_WAREHOUSE_ID:-}"' in deploy_script
    assert "Resolved warehouse ID from existing app resource" in deploy_script
    assert "app.yaml has DATABRICKS_HTTP_PATH set to 'auto' or empty" not in deploy_script


def test_internal_and_example_manifests_keep_sp_only_auth_consistent():
    internal = _manifest_text("app.yaml")
    example = _manifest_text("app.yaml.example")

    assert internal.splitlines()[0] == example.splitlines()[0]
    assert "service-principal-only" in example
    assert "User authorization → + Add scope → sql" not in example


def test_committed_app_config_defers_warehouse_binding_to_app_yaml():
    config = json.loads((ROOT / "app_config.json").read_text())
    assert config["resources"] == []
    assert "id_source" not in (ROOT / "app_config.json").read_text()
    assert "valueFrom: sql-warehouse" in _manifest_text("app.yaml")
    assert "valueFrom: sql-warehouse" in _manifest_text("app.yaml.example")


def test_committed_deployment_configs_contain_no_live_warehouse_ids():
    live_warehouse_id = re.compile(r'(?<![<A-Za-z0-9])[0-9a-f]{16}(?![>A-Za-z0-9])')

    for name in ("app_config.json", "app.yaml", "app.yaml.example"):
        assert not live_warehouse_id.search(_manifest_text(name)), name
