"""User endpoints."""

import asyncio
import logging
import os
import re
import time
from typing import Any
from urllib.parse import parse_qs, quote, urlencode, urlparse

from fastapi import APIRouter, Request

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
_SLACK_TEAM_PATTERN = re.compile(r"^T[A-Z0-9]{8,}$")
_SLACK_MEMBER_PATTERN = re.compile(r"^[UW][A-Z0-9]{8,}$")

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


def _safe_https_url(value: str, *, host: str | None = None) -> str | None:
    candidate = value.strip()
    if not candidate:
        return None
    parsed = urlparse(candidate)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or (host and parsed.hostname != host)
    ):
        return None
    return candidate


def _safe_slack_web_url(value: str) -> str | None:
    candidate = _safe_https_url(value)
    if not candidate:
        return None
    parsed = urlparse(candidate)
    hostname = parsed.hostname or ""
    if (
        not hostname.endswith(".slack.com")
        or hostname == "hooks.slack.com"
        or not parsed.path.startswith("/team/")
    ):
        return None
    return candidate


def _safe_slack_deep_link(value: str) -> str | None:
    candidate = value.strip()
    if not candidate:
        return None
    parsed = urlparse(candidate)
    if (
        parsed.scheme != "slack"
        or parsed.netloc != "user"
        or parsed.path not in ("", "/")
        or parsed.username
        or parsed.password
        or parsed.fragment
    ):
        return None
    try:
        query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError:
        return None
    if set(query) != {"team", "id"}:
        return None
    team_id = query["team"]
    member_id = query["id"]
    if (
        len(team_id) != 1
        or len(member_id) != 1
        or not _SLACK_TEAM_PATTERN.fullmatch(team_id[0])
        or not _SLACK_MEMBER_PATTERN.fullmatch(member_id[0])
    ):
        return None
    return f"slack://user?{urlencode({'team': team_id[0], 'id': member_id[0]})}"


def _feedback_targets_from_env() -> dict[str, Any]:
    """Return public feedback destinations only; never return webhook credentials."""
    configured_issue_url = _safe_https_url(
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
    slack_web_url = _safe_slack_web_url(
        os.getenv("COST_OBS_FEEDBACK_SLACK_WEB_URL", "")
    )
    configured_slack_url = os.getenv("COST_OBS_FEEDBACK_SLACK_URL", "")
    slack_deep_link = _safe_slack_deep_link(configured_slack_url)
    configured_slack_web_url = _safe_slack_web_url(configured_slack_url)
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
    elif (
        _SLACK_TEAM_PATTERN.fullmatch(team_id)
        and _SLACK_MEMBER_PATTERN.fullmatch(member_id)
    ):
        slack = {
            "url": f"slack://user?{urlencode({'team': team_id, 'id': member_id})}",
            "fallback_url": slack_web_url,
        }
    elif slack_web_url:
        slack = {
            "url": slack_web_url,
            "fallback_url": None,
        }

    return {
        "github_issue_url": configured_issue_url or _DEFAULT_FEEDBACK_ISSUE_URL,
        "email_href": email_href,
        "slack": slack,
    }


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
    return _feedback_targets_from_env()


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
