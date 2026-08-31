"""Focused coverage for Settings feedback payloads."""

import asyncio
import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

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
        patch("server.db.get_catalog_schema", return_value=("main", "cost_obs")),
    ):
        permissions = asyncio.run(settings.get_user_permissions(admin_request))
    assert permissions["current_role"] == "admin"
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
