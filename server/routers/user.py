"""User endpoints."""

import asyncio
import logging
import os
import re
import threading
import time
from typing import Any
from urllib.parse import quote

from cachetools import TTLCache
from fastapi import APIRouter, Request

from server.feedback import (
    safe_feedback_slack_url,
    safe_https_url,
    safe_slack_deep_link,
    safe_slack_ids_target,
    safe_slack_profile_url,
)

router = APIRouter()
logger = logging.getLogger(__name__)

SETTINGS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", ".settings")
USER_PERMISSIONS_FILE = os.path.join(SETTINGS_DIR, "user_permissions.json")

# Service-principal display-name lookup — application_id -> display_name.
# Fetched via WorkspaceClient once per day since SPs rarely change; falls back
# to an empty map on SDK error so consumers can still render raw SP-<hex>.
# Failure path uses a short TTL so recovery (e.g. SCIM perms granted later)
# doesn't require a pod restart.
_sp_cache: dict[str, str] | None = None
_sp_cache_at: float = 0.0
_sp_cache_ok: bool = False
_SP_CACHE_TTL = 24 * 3600  # 24 hours on success
_SP_CACHE_FAIL_TTL = 300   # 5 minutes on failure — retry sooner

_DEFAULT_FEEDBACK_ISSUE_URL = (
    "https://github.com/smathews13/cost-obs-databricks-v1.0/issues/new"
)
_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_FEEDBACK_SETTINGS_TIMEOUT_SECONDS = 0.5
_feedback_settings_cache: TTLCache[str, str | None] = TTLCache(maxsize=1, ttl=30)
_feedback_settings_cache_lock = threading.Lock()
_feedback_settings_load_lock = threading.Lock()
_feedback_settings_generation = 0

ROLE_CAPABILITIES = {
    "admin": {
        "summary": (
            "View dashboards and manage shared app settings, users, data sources, "
            "rebuilds, alerts, setup, and experimental features."
        ),
        "can_view_dashboards": True,
        "can_view_settings": True,
        "can_manage_settings": True,
        "can_manage_users": True,
        "can_manage_data": True,
    },
    "consumer": {
        "summary": (
            "View dashboards and basic app information. Cannot change shared app "
            "settings or run administrative actions."
        ),
        "can_view_dashboards": True,
        "can_view_settings": True,
        "can_manage_settings": False,
        "can_manage_users": False,
        "can_manage_data": False,
    },
}


def get_role_capabilities(role: str) -> dict[str, Any]:
    """Return a copy of the route/UI capabilities for an app role."""
    return dict(ROLE_CAPABILITIES.get(role, ROLE_CAPABILITIES["consumer"]))


def _feedback_targets_from_env(
    persisted_slack_url: str | None = None,
) -> dict[str, Any]:
    """Return safe feedback targets, preferring runtime environment values."""
    configured_issue_url = safe_https_url(
        os.getenv("COST_OBS_FEEDBACK_GITHUB_URL", ""),
        host="github.com",
    )
    email = os.getenv("COST_OBS_FEEDBACK_EMAIL", "").strip()
    email_href = (
        f"mailto:{email}?subject={quote('cost-obs v1.2 feedback')}"
        if _EMAIL_PATTERN.fullmatch(email)
        else None
    )

    team_id = os.getenv("COST_OBS_FEEDBACK_SLACK_TEAM_ID", "").strip()
    member_id = os.getenv("COST_OBS_FEEDBACK_SLACK_MEMBER_ID", "").strip()
    slack_web_url = safe_slack_profile_url(
        os.getenv("COST_OBS_FEEDBACK_SLACK_WEB_URL", "")
    )
    configured_slack_url = os.getenv("COST_OBS_FEEDBACK_SLACK_URL", "")
    slack_deep_link = safe_slack_deep_link(configured_slack_url)
    configured_slack_web_url = safe_slack_profile_url(configured_slack_url)
    legacy_slack_deep_link = safe_slack_ids_target(team_id, member_id)
    slack = None
    if slack_deep_link:
        slack = {
            "url": slack_deep_link,
            "fallback_url": slack_web_url,
        }
    elif configured_slack_web_url:
        slack = {
            "url": configured_slack_web_url,
            "fallback_url": None,
        }
    elif legacy_slack_deep_link:
        slack = {
            "url": legacy_slack_deep_link,
            "fallback_url": slack_web_url,
        }
    elif slack_web_url:
        slack = {
            "url": slack_web_url,
            "fallback_url": None,
        }
    else:
        runtime_slack_url = (
            safe_feedback_slack_url(persisted_slack_url)
            if isinstance(persisted_slack_url, str)
            else None
        )
        if runtime_slack_url:
            slack = {
                "url": runtime_slack_url,
                "fallback_url": None,
            }

    return {
        "github_issue_url": configured_issue_url or _DEFAULT_FEEDBACK_ISSUE_URL,
        "email_href": email_href,
        "slack": slack,
    }


def invalidate_feedback_settings_cache() -> None:
    """Clear the bounded runtime target cache after an admin settings save."""
    global _feedback_settings_generation
    with _feedback_settings_cache_lock:
        _feedback_settings_generation += 1
        _feedback_settings_cache.clear()


def _load_persisted_feedback_slack_url() -> str | None:
    """Read and validate the durable target without allowing duplicate hung loads."""
    with _feedback_settings_cache_lock:
        if "slack_url" in _feedback_settings_cache:
            return _feedback_settings_cache["slack_url"]
        generation = _feedback_settings_generation

    if not _feedback_settings_load_lock.acquire(blocking=False):
        return None
    try:
        from server.routers.settings import get_app_settings

        settings = get_app_settings()
        raw_url = settings.get("feedback_slack_url")
        slack_url = (
            safe_feedback_slack_url(raw_url)
            if isinstance(raw_url, str)
            else None
        )
        with _feedback_settings_cache_lock:
            if generation == _feedback_settings_generation:
                _feedback_settings_cache["slack_url"] = slack_url
        return slack_url
    except Exception:
        logger.warning("Could not load durable feedback target")
        with _feedback_settings_cache_lock:
            if generation == _feedback_settings_generation:
                _feedback_settings_cache["slack_url"] = None
        return None
    finally:
        _feedback_settings_load_lock.release()


async def _persisted_feedback_slack_url() -> str | None:
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_load_persisted_feedback_slack_url),
            timeout=_FEEDBACK_SETTINGS_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning("Durable feedback target lookup timed out")
        return None


def _collect_sp_names(sp_iter) -> dict[str, str]:
    """Build application_id(lowercased) -> display_name from a SCIM SP iterator."""
    out: dict[str, str] = {}
    for sp in sp_iter:
        app_id = getattr(sp, "application_id", None)
        display = getattr(sp, "display_name", None)
        if app_id and display:
            out[str(app_id).lower()] = str(display)
    return out


def _list_service_principals_sync() -> dict[str, str]:
    """Build application_id -> display_name for service principals.

    Merges ACCOUNT-level SPs (the comprehensive source — `run_as` UUIDs in
    system.billing.usage are almost all account-wide SPs, which a workspace SCIM
    list does not see) with WORKSPACE-level SPs. Account resolution is best-effort:
    if no AccountClient can be built (no account admin, no account_id), we fall back
    to the workspace list alone so there is no regression from prior behavior.

    Keys are lowercased so lookups from the frontend (which sees SP UUIDs from
    system.billing.usage identity_metadata.run_as) match regardless of casing
    differences between SCIM and billing.

    Raises only if BOTH the account and workspace paths fail, so the caller's
    fail-TTL retry logic still engages on a total outage.
    """
    from server.db import get_account_client, get_workspace_client

    out: dict[str, str] = {}
    account_count = 0
    account_err: Exception | None = None
    workspace_err: Exception | None = None

    # 1) Account-level SPs first (comprehensive). Best-effort.
    try:
        a = get_account_client()
        if a is not None:
            out = _collect_sp_names(a.service_principals.list())
            account_count = len(out)
            logger.info("Fetched %d account-level service principals", account_count)
    except Exception as e:  # 403 (not account admin), transient SCIM error, etc.
        account_err = e
        logger.warning("Account service_principals.list() failed: %s", e)

    # 2) Overlay workspace-level SPs to fill any gaps (also the sole source when
    #    account access is unavailable). Workspace names win on conflict since a
    #    workspace-local SP display name is the more specific label.
    try:
        w = get_workspace_client()
        ws_names = _collect_sp_names(w.service_principals.list())
        out.update(ws_names)
        logger.info(
            "Merged %d workspace service principals (total %d, account contributed %d)",
            len(ws_names), len(out), account_count,
        )
    except Exception as e:
        workspace_err = e
        logger.warning("Workspace service_principals.list() failed: %s", e)

    # Only propagate failure if we got nothing from either source — that lets the
    # endpoint mark the result unavailable and retry on the short fail TTL.
    if not out and (account_err or workspace_err):
        raise (workspace_err or account_err)  # type: ignore[misc]
    return out


def _load_permissions() -> dict:
    """Compatibility wrapper for centralized permission loading."""
    from server.auth import get_permission_snapshot_sync

    return get_permission_snapshot_sync().as_dict()


def _get_user_role(email: str) -> str:
    """Return 'admin' or 'consumer' for the given email based on stored permissions."""
    try:
        perms = _load_permissions()
    except Exception:
        return "consumer"
    if email.strip().lower() in perms.get("admins", []):
        return "admin"
    return "consumer"



@router.get("/me")
async def get_current_user(request: Request):
    """Get current user information."""
    from server.auth import request_identity

    user_email = request_identity(request)
    user_name = request.headers.get("X-Forwarded-User", user_email.split("@")[0] if "@" in user_email else user_email)

    role = await asyncio.to_thread(_get_user_role, user_email)
    return {
        "email": user_email,
        "name": user_name,
        "role": role,
        "capabilities": get_role_capabilities(role),
    }


@router.get("/feedback-targets")
async def get_feedback_targets() -> dict[str, Any]:
    """Expose customer-safe feedback links configured at app runtime."""
    env_targets = _feedback_targets_from_env()
    if env_targets["slack"]:
        return env_targets
    persisted_slack_url = await _persisted_feedback_slack_url()
    return _feedback_targets_from_env(persisted_slack_url)


@router.get("/service-principals")
async def get_service_principals() -> dict[str, Any]:
    """Return application_id -> display_name map for service principals.

    Cached for 24 hours since SP identities rarely change. Falls back to an
    empty map if the SDK call fails (missing SCIM permission, SP-less
    workspace, etc.) so callers can still render the SP-<hex> shortening
    without erroring.
    """
    global _sp_cache, _sp_cache_at, _sp_cache_ok
    now = time.monotonic()
    ttl = _SP_CACHE_TTL if _sp_cache_ok else _SP_CACHE_FAIL_TTL
    if _sp_cache is not None and (now - _sp_cache_at) < ttl:
        return {"map": _sp_cache, "available": _sp_cache_ok, "cached": True}

    try:
        result = await asyncio.to_thread(_list_service_principals_sync)
        _sp_cache = result
        _sp_cache_at = now
        _sp_cache_ok = True
        logger.info("Resolved %d service principal display names (account + workspace)", len(result))
        return {"map": result, "available": True, "cached": False}
    except Exception as e:
        logger.warning("service_principals.list() failed: %s", e)
        # Cache empty briefly so we don't hammer the SDK — retry after _SP_CACHE_FAIL_TTL.
        _sp_cache = {}
        _sp_cache_at = now
        _sp_cache_ok = False
        return {"map": {}, "available": False, "error": str(e)}
