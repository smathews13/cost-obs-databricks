from pathlib import Path

from server import db
from server.routers import settings

ROOT = Path(__file__).resolve().parents[2]


def test_dab_is_an_explicit_backup_not_the_primary_deployment():
    bundle = (ROOT / "databricks.yml").read_text()
    readme = (ROOT / "README.md").read_text()

    assert "backup:" in bundle
    assert "default: true" not in bundle
    assert "cost-observability-backup" in bundle
    assert "Deploy from Git remains the primary deployment path." in readme


def test_dab_reuses_the_runtime_and_injects_one_storage_location():
    app_resource = (ROOT / "resources" / "cost_obs.app.yml").read_text()

    assert "source_code_path: .." in app_resource
    assert "id: ${var.warehouse_id}" in app_resource
    assert "permission: CAN_USE" in app_resource
    assert "name: COST_OBS_CATALOG" in app_resource
    assert "value: ${var.catalog}" in app_resource
    assert "name: COST_OBS_SCHEMA" in app_resource
    assert "value: ${var.schema}" in app_resource
    assert "value_from: sql-warehouse" in app_resource


def test_git_manifest_remains_the_canonical_primary_contract():
    manifest = (ROOT / "app.yaml").read_text()

    assert "name: sql-warehouse" in manifest
    assert "valueFrom: sql-warehouse" in manifest
    assert "databricks.yml" not in manifest


def test_dab_upload_excludes_local_state_and_development_files():
    ignored = (ROOT / ".databricksignore").read_text().splitlines()

    for path in (
        ".env*",
        ".settings/",
        ".git/",
        "client/",
        "server/tests/",
        "databricks.yml",
        "resources/",
    ):
        assert path in ignored


def test_managed_table_inventory_covers_aggregates_state_and_cache():
    inventory = settings._managed_table_inventory()

    assert set(db.MV_UNIFIED_TABLE_NAMES).issubset(inventory)
    assert set(settings._APP_STATE_TABLES).issubset(inventory)
    assert settings._APP_RESPONSE_CACHE_TABLE in inventory
    assert len(inventory) == len(set(inventory))
