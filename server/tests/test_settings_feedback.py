"""Focused coverage for Settings feedback payloads."""

import asyncio
import json
import time
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from server import auth
from server.routers import settings, user


def test_history_is_classified_persisted_and_filtered(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "SETTINGS_DIR", str(tmp_path))
    entries = [
        {
            "timestamp": "2026-08-28T01:00:00Z",
            "status": "skipped",
            "duration_seconds": 0,
            "lookback_days": 180,
            "trigger": "startup",
            "note": "Managed data is fresh.",
        },
        {
            "timestamp": "2026-08-28T02:00:00Z",
            "status": "success",
            "duration_seconds": 42,
            "lookback_days": 180,
            "trigger": "scheduled",
        },
        {
            "timestamp": "2026-08-28T03:00:00Z",
            "status": "config",
            "duration_seconds": 0,
            "lookback_days": None,
            "trigger": "config",
            "note": "Added shared source 'west'",
        },
        {
            "timestamp": "2026-08-28T04:00:00Z",
            "status": "config",
            "duration_seconds": 0,
            "lookback_days": None,
            "trigger": "config",
            "note": "Removed shared source 'west'",
        },
    ]

    with (
        patch.object(settings, "save_refresh_log_to_delta"),
        patch.object(settings, "restore_refresh_log_from_delta", return_value=None),
    ):
        for entry in entries:
            settings.persist_refresh_log({}, entry)

    persisted = json.loads((tmp_path / "mv_refresh_log.json").read_text())
    assert [entry["operation"] for entry in persisted["refresh_history"]] == [
        "rebuild",
        "rebuild",
        "source_added",
        "source_removed",
    ]
    assert [
        (entry["operation"], entry["status"])
        for entry in settings.get_refresh_log_status()["refresh_history"]
    ] == [
        ("rebuild", "success"),
        ("source_added", "config"),
    ]


def test_catalog_explorer_link_uses_workspace_host_and_exact_fqn():
    with patch("server.db.get_host_url", return_value="https://dbc.example.com/"):
        links = settings._catalog_explorer_table_links(
            "west 4_share", "cost_obs", ["daily/usage"]
        )

    assert links == [{
        "fqn": "west 4_share.cost_obs.daily/usage",
        "url": (
            "https://dbc.example.com/explore/data/"
            "west%204_share/cost_obs/daily%2Fusage"
        ),
    }]


def test_shared_source_payload_marks_provider_managed_refresh():
    sources = [{
        "label": "west",
        "catalog": "west_share",
        "schema": "cost_obs",
        "tables": ["daily_usage_summary"],
        "cloud": "aws",
    }]

    @contextmanager
    def unlocked():
        yield

    with (
        patch("server.db.get_mv_sources", return_value=sources),
        patch("server.db.get_local_source_label", return_value="local"),
        patch("server.db.save_mv_sources"),
        patch("server.db.get_host_url", return_value="https://dbc.example.com"),
        patch(
            "server.materialized_views.unified_views_rebuild_lock",
            return_value=unlocked(),
        ),
        patch.object(
            settings,
            "_share_last_updated",
            return_value="2026-08-28T09:00:00Z",
        ),
    ):
        payload = asyncio.run(settings.get_mv_sources_endpoint(detail=True))

    assert payload["recipient_refresh"] == {
        "supported": False,
        "mode": "provider_managed",
        "check_action": "metadata_and_local_bindings_only",
    }
    assert payload["sources"][0]["catalog_explorer_tables"][0]["fqn"] == (
        "west_share.cost_obs.daily_usage_summary"
    )


def test_role_payloads_match_admin_route_policy():
    consumer_request = SimpleNamespace(
        headers={"X-Forwarded-Email": "viewer@example.com"}
    )
    with patch.object(user, "_get_user_role", return_value="consumer"):
        current_user = asyncio.run(user.get_current_user(consumer_request))
    assert current_user["capabilities"]["can_manage_settings"] is False
    assert current_user["capabilities"]["can_manage_data"] is False

    admin_request = SimpleNamespace(
        headers={"X-Forwarded-Email": "admin@example.com"}
    )
    with (
        patch.object(
            settings,
            "_load_user_permissions",
            return_value={
                "admins": ["admin@example.com"],
                "consumers": ["viewer@example.com"],
            },
        ),
        patch(
            "server.routers.health.deployment_metadata",
            new=AsyncMock(return_value={
                "deployer": "admin@example.com",
                "source": "databricks_apps_api",
                "available": True,
            }),
        ),
        patch("server.db.get_catalog_schema", return_value=("main", "cost_obs")),
    ):
        permissions = asyncio.run(settings.get_user_permissions(admin_request))
    assert permissions["current_role"] == "admin"
    assert permissions["owner"] == {
        "email": "admin@example.com",
        "source": "databricks_apps_api",
        "verified": True,
        "deployment_creator": "admin@example.com",
    }
    assert permissions["role_capabilities"]["admin"]["can_manage_users"] is True
    assert permissions["role_capabilities"]["consumer"]["can_manage_users"] is False


def test_empty_admin_list_is_bootstrap_only_not_implicit_admin():
    request = SimpleNamespace(headers={"X-Forwarded-Email": "new-user@example.com"})
    auth._permission_cache = auth.PermissionSnapshot(
        state=auth.PermissionState.BOOTSTRAPPABLE,
        admins=(),
        consumers=(),
        loaded_at=__import__("time").monotonic(),
    )
    with pytest.raises(HTTPException) as exc:
        settings._require_admin(request)
    assert exc.value.status_code == 403
    assert settings._is_admin(request) is False


def test_resource_inventory_is_bounded_and_redacts_connection_credentials(monkeypatch):
    from server import db

    connection_row = {
        "id": "connection-1",
        "name": "AWS CUR",
        "provider": "aws",
        "secret_access_key": "do-not-return",
        "access_key_id": "also-do-not-return",
    }
    queries = []
    monkeypatch.setattr(settings, "_config_table", lambda name: f"`catalog`.`schema`.`{name}`")
    monkeypatch.setattr(
        db,
        "execute_query",
        lambda query, *_args, **_kwargs: queries.append(query) or [connection_row],
    )
    monkeypatch.setattr(settings, "_resource_shared_source_metadata", lambda: [{
        "label": "west",
        "catalog": "west_share",
        "schema": "cost_obs",
        "tables": ["daily_usage_summary"],
    }])
    monkeypatch.setattr(settings, "_resource_unified_views", lambda: ["daily_usage_summary"])
    monkeypatch.setattr(
        "server.workspace_filter.get_configured_workspace_ids",
        lambda: ["100", "200"],
    )
    settings._tables_cache = None

    result = settings._resource_inventory_snapshot()

    assert result["inventory"]["aggregates"]["count"] == len(
        db.MV_UNIFIED_TABLE_NAMES
    )
    assert result["inventory"]["cache"]["process_max_entries"] == db._CACHE_MAX_SIZE
    assert result["workspace_filter"] == {
        "mode": "restricted",
        "count": 2,
    }
    assert result["cloud_cost_connections"] == [{
        "id": "connection-1",
        "name": "AWS CUR",
        "provider": "aws",
    }]
    assert "do-not-return" not in repr(result)
    assert "access_key_id" not in repr(result)
    assert "LIMIT 100" in queries[0]
    assert "secret" not in queries[0].lower()


def test_resource_source_and_view_inventory_queries_are_bounded(monkeypatch):
    from server import db

    queries = []
    monkeypatch.setattr(db, "get_catalog_schema", lambda: ("catalog", "schema"))
    monkeypatch.setattr(
        db,
        "execute_query",
        lambda query, *_args, **_kwargs: queries.append(query) or [],
    )

    assert settings._resource_shared_source_metadata() == []
    assert settings._resource_unified_views() == []
    assert len(queries) == 2
    assert all("LIMIT 100" in query for query in queries)


def test_resources_returns_other_sections_when_apps_api_times_out(monkeypatch):
    async def slow_links():
        # Deliberately block before any await to prove the endpoint isolates SDK
        # handlers whose async wrappers still contain synchronous work.
        time.sleep(0.08)
        return {"app_name": "too-late"}

    monkeypatch.setattr(settings, "_RESOURCES_SUBSECTION_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(settings, "_RESOURCES_ENDPOINT_DEADLINE_SECONDS", 0.08)
    monkeypatch.setenv("DATABRICKS_APP_NAME", "cost-obs")
    monkeypatch.setattr(settings, "get_app_config", AsyncMock(return_value={
        "storage_location": {"catalog": "catalog", "schema": "schema"},
        "warehouse": None,
        "version": {},
    }))
    monkeypatch.setattr(settings, "get_auth_status_endpoint", AsyncMock(return_value={}))
    monkeypatch.setattr(settings, "get_app_links", slow_links)
    monkeypatch.setattr(settings, "_resource_shared_source_metadata", lambda: [])
    monkeypatch.setattr(settings, "_resource_connection_metadata", lambda: [])
    monkeypatch.setattr(settings, "_resource_unified_views", lambda: [])
    monkeypatch.setattr(
        "server.workspace_filter.get_configured_workspace_ids",
        lambda: [],
    )
    monkeypatch.setattr(settings, "_resource_refresh_snapshot", lambda: {
        "schedule": {
            "enabled": True,
            "frequency": "nightly",
            "hour_utc": 5,
            "lookback_days": 180,
        },
        "status": {"status": "success"},
    })
    with patch(
        "server.routers.health.deployment_metadata",
        new=AsyncMock(return_value={
            "deployed_at": "2026-08-31T17:00:00Z",
            "deployer": "owner@example.com",
            "commit_sha": "abc123",
            "available": True,
            "source": "databricks_apps_api",
        }),
    ):
        result = asyncio.run(settings.get_resources())

    assert settings._RESOURCES_SUBSECTION_TIMEOUT_SECONDS <= 3
    assert settings._RESOURCES_ENDPOINT_DEADLINE_SECONDS <= 10
    assert result["subsections"]["app_links"] == {
        "available": False,
        "reason": "temporarily_unavailable",
    }
    assert result["subsections"]["config"] == {"available": True}
    assert result["storage"]["catalog"] == "catalog"
    assert result["app"]["name"] == "cost-obs"


def test_resources_keeps_fast_inventory_when_one_sql_subsection_times_out(
    monkeypatch,
):
    def slow_sources():
        time.sleep(0.2)
        return [{"label": "too-late"}]

    monkeypatch.setattr(settings, "_RESOURCES_SUBSECTION_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(settings, "_RESOURCES_ENDPOINT_DEADLINE_SECONDS", 0.15)
    monkeypatch.setattr(settings, "get_app_config", AsyncMock(return_value={
        "storage_location": {},
        "warehouse": None,
        "version": {},
    }))
    monkeypatch.setattr(settings, "get_auth_status_endpoint", AsyncMock(return_value={}))
    monkeypatch.setattr(settings, "get_app_links", AsyncMock(return_value={}))
    monkeypatch.setattr(settings, "_resource_shared_source_metadata", slow_sources)
    monkeypatch.setattr(
        settings,
        "_resource_connection_metadata",
        lambda: [{"name": "AWS CUR", "provider": "aws"}],
    )
    monkeypatch.setattr(settings, "_resource_unified_views", lambda: ["daily_usage_summary"])
    monkeypatch.setattr(
        "server.workspace_filter.get_configured_workspace_ids",
        lambda: ["100"],
    )
    monkeypatch.setattr(settings, "_resource_refresh_snapshot", lambda: {
        "schedule": {
            "enabled": False,
            "frequency": "nightly",
            "hour_utc": 5,
            "lookback_days": 180,
        },
        "status": None,
    })
    with patch(
        "server.routers.health.deployment_metadata",
        new=AsyncMock(return_value={
            "deployed_at": "2026-08-31T17:00:00Z",
            "deployer": None,
            "commit_sha": None,
            "available": True,
            "source": "process_start_approximate_restart",
        }),
    ):
        result = asyncio.run(settings.get_resources())

    assert result["subsections"]["shared_data_sources"] == {
        "available": False,
        "reason": "temporarily_unavailable",
    }
    assert result["subsections"]["cloud_cost_connections"] == {"available": True}
    assert result["shared_data_sources"] == []
    assert result["cloud_cost_connections"] == [{"name": "AWS CUR", "provider": "aws"}]
    assert result["inventory"]["unified_views"]["names"] == ["daily_usage_summary"]
    assert result["workspace_filter"] == {"mode": "restricted", "count": 1}


def test_auth_status_exposes_safe_authoritative_identity_link(monkeypatch):
    me = SimpleNamespace(
        id="sp-object-123",
        user_name="client-123",
        display_name="app-cost-obs",
    )
    monkeypatch.setenv("DATABRICKS_HOST", "https://dbc.example.com")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "client-123")
    with (
        patch("server.db.get_auth_status", return_value={}),
        patch(
            "server.db.get_workspace_client",
            return_value=SimpleNamespace(
                current_user=SimpleNamespace(me=lambda: me)
            ),
        ),
        patch("server.db.get_catalog_schema", return_value=("main", "cost_obs")),
    ):
        result = asyncio.run(settings.get_auth_status_endpoint())

    assert result["sp_object_id"] == "sp-object-123"
    assert result["sp_client_id"] == "client-123"
    assert result["effective_oauth_scopes"] == ["all-apis"]
    assert result["sp_identity_url"] == (
        "https://dbc.example.com/api/2.0/preview/scim/v2/"
        "ServicePrincipals/sp-object-123"
    )
    assert "secret" not in result


def test_resource_links_reject_credentials_and_strip_token_queries():
    assert settings._safe_resource_link(
        "https://user:secret@github.com/example/repo"
    ) == ""
    assert settings._safe_resource_link(
        "https://github.com/example/repo?access_token=secret#fragment"
    ) == "https://github.com/example/repo"
    assert settings._safe_resource_link(
        "https://dbc.example.com/apps-v2/app/cost-obs/overview?o=12345"
    ).endswith("?o=12345")


def test_gcp_app_links_use_app_level_git_repository_without_active_deployment(
    monkeypatch,
):
    app_body = {
        "name": "cost-obs-gcp",
        "url": "https://cost-obs-gcp-123456789.gcp.databricksapps.com",
        "update_time": "2026-08-31T16:42:03Z",
        "updater": "deployer@example.com",
        "git_repository": {
            "url": "https://github.com/example/cost-obs",
            "provider": "gitHub",
        },
    }
    workspace = SimpleNamespace(
        api_client=SimpleNamespace(
            do=lambda method, path, **_kwargs: app_body
            if (method, path) == ("GET", "/api/2.0/apps/cost-obs-gcp")
            else {}
        )
    )
    monkeypatch.setenv(
        "DATABRICKS_HOST",
        "https://1234567890123456.7.gcp.databricks.com",
    )
    monkeypatch.setenv("DATABRICKS_APP_NAME", "cost-obs-gcp")
    with patch("server.db.get_workspace_client", return_value=workspace):
        result = asyncio.run(settings.get_app_links())

    assert result["source_code_url"] == "https://github.com/example/cost-obs"
    assert result["app_page_url"].endswith(
        "/apps-v2/app/cost-obs-gcp/overview?o=123456789"
    )


def test_owner_cannot_be_demoted_when_other_admins_remain():
    request = SimpleNamespace(headers={"X-Forwarded-Email": "backup@example.com"})
    current = {
        "admins": ["owner@example.com", "backup@example.com"],
        "consumers": [],
        "owner": "owner@example.com",
    }
    with (
        patch.object(
            settings,
            "_require_admin_async",
            new=AsyncMock(return_value="backup@example.com"),
        ),
        patch.object(settings, "_load_user_permissions", return_value=current),
        patch(
            "server.routers.health.deployment_metadata",
            new=AsyncMock(return_value={
                "deployer": "owner@example.com",
                "source": "databricks_apps_api",
                "available": True,
            }),
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            asyncio.run(settings.save_user_permissions(
                request,
                settings.UserPermissionsModel(
                    admins=["backup@example.com"],
                    consumers=["owner@example.com"],
                ),
            ))

    assert exc.value.status_code == 400
    assert "Owner" in str(exc.value.detail)


def test_permission_owner_has_stable_store_fallback():
    with patch(
        "server.routers.health.deployment_metadata",
        new=AsyncMock(return_value={
            "deployer": None,
            "source": "unavailable",
            "available": False,
        }),
    ):
        owner = asyncio.run(settings._permission_owner({
            "admins": ["stored-owner@example.com", "backup@example.com"],
            "consumers": [],
            "owner": "stored-owner@example.com",
        }))

    assert owner == {
        "email": "stored-owner@example.com",
        "source": "permission_store",
        "verified": False,
        "deployment_creator": None,
    }
