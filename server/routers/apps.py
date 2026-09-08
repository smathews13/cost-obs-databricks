"""Databricks Apps cost analysis API endpoints."""

import asyncio
import contextvars
import hashlib
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import httpx
from cachetools import TTLCache
from fastapi import APIRouter, Header, HTTPException, Query
from fastapi.responses import JSONResponse, Response

from server import cache_ttls
from server import workspace_filter as wf
from server.db import (
    BundleOverloadedError,
    CacheGeneration,
    SourceScopeUnsupportedError,
    SQLExecutionError,
    apply_mv_overrides,
    bundle_cache_key,
    bundle_compute_is_pending,
    capture_cache_generation,
    delta_cache_get,
    delta_cache_put,
    execute_queries_parallel,
    execute_query,
    get_bundle_compute_state,
    get_catalog_schema,
    get_current_workspace_id,
    get_workspace_client,
    local_source_is_selected,
    recover_optional_bundle_queries,
    selected_source_labels,
    source_label_filter_clause,
    start_bundle_compute,
)
from server.materialized_views import MV_APPS_AVG_COST_PER_APP
from server.queries.pricing import (
    apply_current_list_price_join,
    current_list_price_join,
)
from server.request_limits import (
    default_date_range,
    parse_workspace_ids,
    validate_date_range,
)
from server.routers.billing import _check_mv_available

# Workspace service-principal detail routes use the numeric SCIM object ID.
# Client/application UUIDs and display names are not interchangeable with it.
_SP_WORKSPACE_ID_RE = re.compile(r"^\d+$")

router = APIRouter()
logger = logging.getLogger(__name__)

_apps_bundle_inflight: set[str] = set()
_apps_bundle_inflight_lock = threading.Lock()
_apps_bundle_failures: TTLCache[str, dict[str, Any]] = TTLCache(maxsize=100, ttl=30)
_apps_bundle_status: dict[str, dict[str, Any]] = {}
_apps_bundle_status_lock = threading.Lock()


class _AppsCacheWriteError(RuntimeError):
    code = "APPS_CACHE_WRITE_FAILED"


def _apps_failure_payload(
    params: dict[str, str],
    code: str = "APPS_BUNDLE_FAILED",
) -> dict[str, Any]:
    source_unsupported = code == "SOURCE_SCOPE_UNSUPPORTED"
    return {
        "available": False,
        "availability": "unavailable",
        "retryable": not source_unsupported,
        "reason": "shared_scope_unsupported" if source_unsupported else "producer_failed",
        "reason_detail": (
            "The selected shared source does not publish Apps cost detail."
            if source_unsupported
            else "Apps cost data is temporarily unavailable. Retry shortly."
        ),
        "error_code": code,
        "summary": {},
        "apps": {},
        "timeseries": {"timeseries": [], "categories": []},
        "connected_artifacts": [],
        "workspaces": [],
        "start_date": params["start_date"],
        "end_date": params["end_date"],
    }


def _set_apps_producer_status(cache_key: str, state: str, **detail: Any) -> None:
    now = time.monotonic()
    with _apps_bundle_status_lock:
        current = dict(_apps_bundle_status.get(cache_key) or {})
        current.update(detail)
        current.update({"state": state, "heartbeat_at": now})
        current.setdefault("started_at", now)
        _apps_bundle_status[cache_key] = current


def _refresh_apps_metadata() -> None:
    """Best-effort optional SDK enrichment; never fail a billing producer."""
    try:
        _get_app_details(_get_app_registry())
    except BaseException:
        logger.warning("Optional Apps metadata refresh failed", exc_info=True)


def get_default_start_date() -> str:
    """Get default start date (last 30 days)."""
    return default_date_range()[0]


def get_default_end_date() -> str:
    """Get default end date (last complete UTC day)."""
    return default_date_range()[1]


# The active_days param controls the "active" filter: apps with usage in
# the last N days of the date range are considered active.
ACTIVE_DAYS = 7

_RUNNING_APP_STATES = {"ACTIVE", "RUNNING"}
_STOPPED_APP_STATES = {
    "CRASHED",
    "DELETED",
    "FAILED",
    "INACTIVE",
    "STOPPED",
    "UNAVAILABLE",
}

# ── App name resolution (UUID → human-readable name) ────────────────────

_app_name_cache: dict[str, dict[str, Any]] = {}  # uuid → safe list metadata
_app_name_cache_time: float = 0
APP_NAME_CACHE_TTL = 3600  # 1 hour - app list rarely changes


def _enum_value(value: Any) -> str:
    """Return the wire value for SDK enums without exposing object reprs."""
    if value is None:
        return ""
    return str(getattr(value, "value", value))


def _status_metadata(status: Any, instance_field: str | None = None) -> dict[str, Any] | None:
    if not status:
        return None
    result: dict[str, Any] = {
        "state": _enum_value(getattr(status, "state", None)),
        "message": str(getattr(status, "message", None) or ""),
    }
    if instance_field:
        instances = getattr(status, instance_field, None)
        if instances is not None:
            result["instances"] = int(instances)
    if not result["state"] and not result["message"] and "instances" not in result:
        return None
    return result


def _safe_repository_url(value: Any) -> str:
    """Allow clickable repository URLs while dropping credentials and query tokens."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlsplit(raw)
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return ""
    if parsed.username or parsed.password:
        return ""
    host = parsed.hostname
    try:
        if parsed.port:
            host = f"{host}:{parsed.port}"
    except ValueError:
        return ""
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def _safe_app_url(value: Any) -> str:
    raw = str(value or "").strip()
    if raw and "://" not in raw:
        raw = f"https://{raw}"
    safe = _safe_repository_url(raw)
    return safe if safe.startswith("https://") else ""


def _extract_app_metadata(app: Any, availability: str = "available") -> dict[str, Any]:
    """Select customer-safe Apps API fields.

    Deliberately excludes OAuth/client identifiers, environment variables,
    credentials, access-token configuration, and deployment artifacts.
    """
    active_deployment = getattr(app, "active_deployment", None)
    pending_deployment = getattr(app, "pending_deployment", None)
    deployment = pending_deployment or active_deployment
    deployment_status = _status_metadata(getattr(deployment, "status", None))

    git_source = (
        getattr(deployment, "git_source", None)
        or getattr(app, "git_source", None)
        or getattr(app, "default_git_source", None)
    )
    git_repository = (
        getattr(git_source, "git_repository", None)
        or getattr(app, "git_repository", None)
    )
    git_metadata: dict[str, str] | None = None
    if git_source or git_repository:
        git_metadata = {
            "repository_url": _safe_repository_url(getattr(git_repository, "url", None)),
            "branch": str(getattr(git_source, "branch", None) or ""),
            "tag": str(getattr(git_source, "tag", None) or ""),
            "commit": str(
                getattr(git_source, "resolved_commit", None)
                or getattr(git_source, "commit", None)
                or ""
            ),
            "source_code_path": str(getattr(git_source, "source_code_path", None) or ""),
        }
        if not any(git_metadata.values()):
            git_metadata = None

    deployment_metadata: dict[str, Any] | None = None
    if deployment:
        deployment_metadata = {
            "deployment_id": str(getattr(deployment, "deployment_id", None) or ""),
            "state": (deployment_status or {}).get("state", ""),
            "message": (deployment_status or {}).get("message", ""),
            "creator": str(getattr(deployment, "creator", None) or ""),
            "create_time": str(getattr(deployment, "create_time", None) or ""),
            "update_time": str(getattr(deployment, "update_time", None) or ""),
            "mode": _enum_value(getattr(deployment, "mode", None)),
            "pending": pending_deployment is not None,
        }

    thumbnail_url = str(getattr(app, "thumbnail_url", None) or "")
    return {
        "availability": availability,
        "description": str(getattr(app, "description", None) or ""),
        "creator": str(getattr(app, "creator", None) or ""),
        "updater": str(getattr(app, "updater", None) or ""),
        "create_time": str(getattr(app, "create_time", None) or ""),
        "update_time": str(getattr(app, "update_time", None) or ""),
        "compute_size": _enum_value(getattr(app, "compute_size", None)),
        "compute_min_instances": getattr(app, "compute_min_instances", None),
        "compute_max_instances": getattr(app, "compute_max_instances", None),
        "compute_status": _status_metadata(
            getattr(app, "compute_status", None), "active_instances"
        ),
        "app_status": _status_metadata(
            getattr(app, "app_status", None), "running_instances"
        ),
        "deployment": deployment_metadata,
        "source_code_path": str(
            getattr(deployment, "source_code_path", None)
            or getattr(app, "source_code_path", None)
            or getattr(app, "default_source_code_path", None)
            or ""
        ),
        "git": git_metadata,
        "space": str(getattr(app, "space", None) or ""),
        # Used only by the server-side image proxy; stripped from bundle output.
        "_thumbnail_source_url": thumbnail_url,
    }


def _public_app_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    return {
        key: value
        for key, value in (metadata or {}).items()
        if not key.startswith("_")
    }


def _public_thumbnail_url(
    app_id: str,
    is_registered: bool,
    metadata: dict[str, Any],
    registry_entry: dict[str, Any],
) -> str | None:
    """Return the one client-facing thumbnail contract.

    A registered app with either an explicit registry thumbnail or a trusted app
    endpoint can be resolved by the proxy. The proxy probes conventional static
    icon paths and returns a generated fallback instead of leaking a 404.
    """
    if not is_registered:
        return None
    if not (metadata.get("_thumbnail_source_url") or registry_entry.get("url")):
        return None
    return f"/api/apps/thumbnail?app_id={quote(str(app_id), safe='')}"


def _registry_app_is_running(entry: dict[str, Any]) -> bool | None:
    """Return live Apps API state when available, otherwise defer to billing."""
    metadata = entry.get("metadata") or {}
    app_state = str(
        (metadata.get("app_status") or {}).get("state") or ""
    ).upper()
    if app_state in _RUNNING_APP_STATES:
        return True
    if app_state in _STOPPED_APP_STATES:
        return False

    compute_state = str(
        (metadata.get("compute_status") or {}).get("state") or ""
    ).upper()
    if compute_state in _RUNNING_APP_STATES:
        return True
    if compute_state in _STOPPED_APP_STATES:
        return False
    return None


def _get_app_registry() -> dict[str, dict[str, Any]]:
    """Fetch and cache the UUID → {name, url} mapping from Databricks Apps API.

    Returns a dict keyed by app UUID with values like:
      {"name": "cost-observability", "url": "https://cost-observability-xxx.aws.databricksapps.com"}
    """
    global _app_name_cache, _app_name_cache_time

    now = time.time()
    if _app_name_cache and (now - _app_name_cache_time) < APP_NAME_CACHE_TTL:
        return _app_name_cache

    try:
        w = get_workspace_client()
        registry: dict[str, dict[str, Any]] = {}
        for app in w.apps.list():
            app_id = getattr(app, "id", None)
            app_name = getattr(app, "name", None)
            app_url = _safe_app_url(getattr(app, "url", None))
            app_description = getattr(app, "description", None) or ""
            if app_id and app_name:
                registry[app_id] = {
                    "name": app_name,
                    "url": app_url,
                    "description": app_description,
                    "metadata": _extract_app_metadata(app),
                }
        _app_name_cache = registry
        _app_name_cache_time = now
        logger.info("Refreshed app name cache: %d apps", len(registry))
        return registry
    except Exception as e:
        logger.warning("Failed to fetch app registry: %s", e)
        return _app_name_cache  # return stale cache on error


# ── App detail + connected artifacts cache ───────────────────────────
# One existing w.apps.get() per app supplies both metadata and resources. Keeping
# them together prevents the expanded UI from introducing a second N+1 path.
_app_details_cache: dict[str, dict[str, Any]] = {}
_app_details_cache_time: float = 0
APP_RESOURCES_CACHE_TTL = 1800  # 30 minutes — SDK calls are expensive
_app_details_refresh_lock = threading.Lock()
_app_details_refresh_inflight: threading.Event | None = None
_APP_DETAILS_MAX_WORKERS = 6


def _resource_binding(resource: Any) -> dict[str, str]:
    """Convert an SDK app resource to a display-safe binding."""
    res_name = str(getattr(resource, "name", None) or "")
    res_description = str(getattr(resource, "description", None) or "")
    resource_fields = (
        ("serving_endpoint", "SERVING_ENDPOINT", ("name", "endpoint_name")),
        ("sql_warehouse", "SQL_WAREHOUSE", ("name", "id")),
        ("job", "JOB", ("name", "id")),
        ("database", "LAKEBASE", ("database_name", "instance_name")),
        ("postgres", "POSTGRES", ("database", "branch")),
        ("genie_space", "GENIE_SPACE", ("name", "space_id")),
        ("experiment", "EXPERIMENT", ("name", "experiment_id")),
        ("app", "APP", ("name",)),
        ("uc_securable", "UC_SECURABLE", ("full_name", "name")),
    )
    for field, resource_type, name_fields in resource_fields:
        target = getattr(resource, field, None)
        if not target:
            continue
        if not res_name:
            res_name = next(
                (str(getattr(target, key, None)) for key in name_fields if getattr(target, key, None)),
                "",
            )
        permission = _enum_value(getattr(target, "permission", None))
        if not res_description and permission:
            res_description = permission.replace("_", " ").title()
        return {"name": res_name, "type": resource_type, "description": res_description}

    if getattr(resource, "secret", None):
        # Binding aliases are useful; secret scopes, keys and values are not sent.
        return {
            "name": res_name or "Secret binding",
            "type": "SECRET",
            "description": res_description or "Secret resource binding",
        }
    return {
        "name": res_name,
        "type": _enum_value(getattr(resource, "type", None)) or "UNKNOWN",
        "description": res_description,
    }


def _fetch_one_app_details(
    w: Any, app_id: str, registry_entry: dict[str, Any]
) -> tuple[str, dict[str, Any]]:
    """Fetch one app once and derive both metadata and resource bindings."""
    app_name = str(registry_entry.get("name") or app_id)
    try:
        app_detail = w.apps.get(app_name)
        resources = (
            getattr(app_detail, "effective_resources", None)
            or getattr(app_detail, "resources", None)
            or []
        )
        app_resources = [_resource_binding(resource) for resource in resources]

        # Only the workspace SCIM object ID can safely form an admin-console URL.
        # OAuth/client IDs remain intentionally omitted.
        sp_name = str(getattr(app_detail, "service_principal_name", None) or "")
        sp_id = str(getattr(app_detail, "service_principal_id", None) or "").strip()
        if sp_name:
            app_resources.append({
                "name": sp_name,
                "type": "SERVICE_PRINCIPAL",
                "description": "Run-as identity",
                "id": sp_id if _SP_WORKSPACE_ID_RE.fullmatch(sp_id) else "",
            })

        metadata = _extract_app_metadata(app_detail)
        registry_entry["url"] = _safe_app_url(
            getattr(app_detail, "url", None) or registry_entry.get("url")
        )
        registry_entry["description"] = metadata.get("description", "")
        registry_entry["metadata"] = metadata
        return app_id, {"metadata": metadata, "resources": app_resources}
    except Exception as e:
        logger.debug("Failed to get details for app %s: %s", app_name, e)
        metadata = dict(registry_entry.get("metadata") or {})
        metadata["availability"] = "partial"
        return app_id, {"metadata": metadata, "resources": []}


def _get_app_details(
    registry: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Fetch and cache full safe metadata/resources via the existing get calls.

    Uses bounded parallel fetches and one shared refresh flight. Concurrent
    requests either reuse the fresh cache or wait for the same refresh.
    """
    global _app_details_cache, _app_details_cache_time

    now = time.time()
    if _app_details_cache and (now - _app_details_cache_time) < APP_RESOURCES_CACHE_TTL:
        return _app_details_cache

    registry = registry or _get_app_registry()
    if not registry:
        return _app_details_cache

    global _app_details_refresh_inflight
    leader = False
    with _app_details_refresh_lock:
        now = time.time()
        if _app_details_cache and (now - _app_details_cache_time) < APP_RESOURCES_CACHE_TTL:
            return _app_details_cache
        if _app_details_refresh_inflight is None:
            _app_details_refresh_inflight = threading.Event()
            leader = True
        inflight = _app_details_refresh_inflight

    if not leader:
        inflight.wait(timeout=30.0)
        return _app_details_cache

    try:
        w = get_workspace_client()
        details_by_app: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(
            max_workers=min(_APP_DETAILS_MAX_WORKERS, max(1, len(registry))),
            thread_name_prefix="apps-detail",
        ) as executor:
            futures = {
                executor.submit(_fetch_one_app_details, w, app_id, entry): app_id
                for app_id, entry in registry.items()
            }
            for future in as_completed(futures):
                detail_id, detail = future.result()
                details_by_app[detail_id] = detail

        _app_details_cache = details_by_app
        _app_details_cache_time = time.time()
        logger.info("Refreshed app details cache: %d apps", len(details_by_app))
        return details_by_app
    except Exception as e:
        logger.warning("Failed to fetch app details: %s", e)
        return _app_details_cache
    finally:
        with _app_details_refresh_lock:
            if _app_details_refresh_inflight is not None:
                _app_details_refresh_inflight.set()
            _app_details_refresh_inflight = None


def _get_app_resources() -> dict[str, list[dict[str, str]]]:
    """Compatibility view keyed by app name for the connected-artifacts endpoint."""
    registry = _get_app_registry()
    details = _get_app_details(registry)
    return {
        str(entry.get("name") or app_id): list(details.get(app_id, {}).get("resources") or [])
        for app_id, entry in registry.items()
    }


def _resolve_app_name(app_id: str, registry: dict[str, dict[str, Any]]) -> str:
    """Resolve a billing app_id (UUID) to a human-readable name."""
    entry = registry.get(app_id)
    if entry:
        return entry["name"]
    return app_id  # fall back to raw UUID


# ── SQL Queries ──────────────────────────────────────────────────────────

APPS_SUMMARY = """
WITH filtered_usage AS (
  SELECT *
  FROM system.billing.usage u
  WHERE u.usage_date BETWEEN :start_date AND :end_date
    AND u.usage_quantity > 0
    AND u.billing_origin_product = 'APPS'
),
apps_usage AS (
  SELECT
    u.usage_date,
    u.workspace_id,
    u.sku_name,
    u.usage_quantity,
    u.usage_metadata,
    COALESCE(p.pricing.default, 0) as price_per_dbu
  FROM filtered_usage u
  /* TEMPORAL_LIST_PRICE_JOIN */
),
apps_by_day AS (
  SELECT
    usage_date,
    workspace_id,
    COALESCE(usage_metadata.app_id, 'unknown') as app_id,
    SUM(usage_quantity) as daily_dbus,
    SUM(usage_quantity * price_per_dbu) as daily_spend
  FROM apps_usage
  GROUP BY usage_date, workspace_id, COALESCE(usage_metadata.app_id, 'unknown')
),
apps_totals AS (
  SELECT
    SUM(daily_dbus) as total_dbus,
    SUM(daily_spend) as total_spend,
    COUNT(DISTINCT workspace_id) as workspace_count,
    COUNT(DISTINCT app_id) as app_count,
    COUNT(DISTINCT usage_date) as days_in_range,
    MIN(usage_date) as first_date,
    MAX(usage_date) as last_date
  FROM apps_by_day
),
apps_avg AS (
  SELECT COALESCE(AVG(daily_apps), 0) as avg_daily_apps
  FROM (
    SELECT usage_date, COUNT(DISTINCT app_id) as daily_apps
    FROM apps_by_day
    GROUP BY usage_date
  ) t
)
SELECT t.*, a.avg_daily_apps
FROM apps_totals t, apps_avg a
"""

# Returns per-app breakdown with last_usage_date for active filtering.
# app_id here is the raw UUID from billing; names are resolved in Python.
APPS_BY_APP_FULL = """
WITH filtered_usage AS (
  SELECT *
  FROM system.billing.usage u
  WHERE u.usage_date BETWEEN :start_date AND :end_date
    AND u.usage_quantity > 0
    AND u.billing_origin_product = 'APPS'
),
apps_usage AS (
  SELECT
    u.usage_date,
    u.workspace_id,
    u.usage_quantity,
    COALESCE(u.usage_metadata.app_id, 'Unknown') as app_id,
    COALESCE(p.pricing.default, 0) as price_per_dbu
  FROM filtered_usage u
  /* TEMPORAL_LIST_PRICE_JOIN */
)
SELECT
  app_id,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * price_per_dbu) as total_spend,
  COUNT(DISTINCT workspace_id) as workspace_count,
  COUNT(DISTINCT usage_date) as days_active,
  MAX(usage_date) as last_usage_date
FROM apps_usage
GROUP BY app_id
ORDER BY total_spend DESC
"""

# Distinct workspaces per app (for workspace filtering) with name resolution
APPS_WORKSPACES = """
WITH apps_usage AS (
  SELECT DISTINCT
    COALESCE(u.usage_metadata.app_id, 'Unknown') as app_id,
    u.workspace_id
  FROM system.billing.usage u
  WHERE u.usage_date BETWEEN :start_date AND :end_date
    AND u.usage_quantity > 0
    AND u.billing_origin_product = 'APPS'
)
SELECT
  a.app_id,
  a.workspace_id,
  COALESCE(ws.workspace_name, CAST(a.workspace_id AS STRING)) as workspace_name
FROM apps_usage a
LEFT JOIN system.access.workspaces_latest ws ON a.workspace_id = ws.workspace_id
"""

# Service principals used as run_as identity for each app
APPS_SERVICE_PRINCIPALS = """
SELECT
  COALESCE(u.usage_metadata.app_id, 'Unknown') as app_id,
  u.identity_metadata.run_as as run_as
FROM system.billing.usage u
WHERE u.usage_date BETWEEN :start_date AND :end_date
  AND u.usage_quantity > 0
  AND u.billing_origin_product = 'APPS'
  AND u.identity_metadata.run_as IS NOT NULL
GROUP BY 1, 2
"""

# Fallback: workspace IDs only (no name resolution)
APPS_WORKSPACES_FALLBACK = """
WITH apps_usage AS (
  SELECT DISTINCT
    COALESCE(u.usage_metadata.app_id, 'Unknown') as app_id,
    u.workspace_id
  FROM system.billing.usage u
  WHERE u.usage_date BETWEEN :start_date AND :end_date
    AND u.usage_quantity > 0
    AND u.billing_origin_product = 'APPS'
)
SELECT
  app_id,
  workspace_id,
  CAST(workspace_id AS STRING) as workspace_name
FROM apps_usage
"""


def _query_app_workspaces(params: dict[str, Any], ws_clause: str = "") -> list[dict[str, Any]]:
    """Query workspace info per app, falling back to IDs if name table is inaccessible."""
    sql = wf.inject_ws_filter(APPS_WORKSPACES, ws_clause)
    fallback = wf.inject_ws_filter(APPS_WORKSPACES_FALLBACK, ws_clause)
    try:
        rows = execute_query(sql, params)
        if rows:
            sample = rows[0]
            logger.info("App workspace sample: workspace_id=%s, workspace_name=%s",
                        sample.get("workspace_id"), sample.get("workspace_name"))
        return rows
    except Exception as e:
        logger.warning("Could not query workspace names (system.access.workspaces_latest may not be accessible): %s", e)
        try:
            return execute_query(fallback, params)
        except Exception:
            return []


APPS_TIMESERIES = """
WITH filtered_usage AS (
  SELECT *
  FROM system.billing.usage u
  WHERE u.usage_date BETWEEN :start_date AND :end_date
    AND u.usage_quantity > 0
    AND u.billing_origin_product = 'APPS'
)
SELECT
  u.usage_date,
  SUM(u.usage_quantity) as total_dbus,
  SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend
FROM filtered_usage u
/* TEMPORAL_LIST_PRICE_JOIN */
GROUP BY u.usage_date
ORDER BY u.usage_date
"""

# Per-app SKU breakdown — what cost categories make up each app's total
APPS_BY_APP_SKU = """
WITH filtered_usage AS (
  SELECT *
  FROM system.billing.usage u
  WHERE u.usage_date BETWEEN :start_date AND :end_date
    AND u.usage_quantity > 0
    AND u.billing_origin_product = 'APPS'
),
apps_usage AS (
  SELECT
    COALESCE(u.usage_metadata.app_id, 'Unknown') as app_id,
    u.sku_name,
    u.usage_quantity,
    COALESCE(p.pricing.default, 0) as price_per_dbu
  FROM filtered_usage u
  /* TEMPORAL_LIST_PRICE_JOIN */
)
SELECT
  app_id,
  sku_name,
  SUM(usage_quantity) as total_dbus,
  SUM(usage_quantity * price_per_dbu) as total_spend
FROM apps_usage
GROUP BY app_id, sku_name
ORDER BY app_id, total_spend DESC
"""


# ── MV fast-path SQL (uses daily_apps_summary, pre-aggregated) ──────────
# The raw APPS_* queries scan system.billing.usage on every request. When
# the daily_apps_summary MV is available these read from it directly. All
# four preserve the exact output shape of their raw counterparts so
# _process_apps and the frontend don't need to change.

MV_APPS_SUMMARY = """
WITH apps_by_day AS (
  SELECT usage_date, workspace_id, app_id,
    SUM(total_dbus) AS daily_dbus,
    SUM(total_spend) AS daily_spend
  FROM `{catalog}`.`{schema}`.`daily_apps_summary`
  WHERE usage_date BETWEEN :start_date AND :end_date
    {ws_filter}
  GROUP BY usage_date, workspace_id, app_id
),
apps_totals AS (
  SELECT
    SUM(daily_dbus) AS total_dbus,
    SUM(daily_spend) AS total_spend,
    COUNT(DISTINCT workspace_id) AS workspace_count,
    COUNT(DISTINCT app_id) AS app_count,
    COUNT(DISTINCT usage_date) AS days_in_range,
    MIN(usage_date) AS first_date,
    MAX(usage_date) AS last_date
  FROM apps_by_day
),
apps_avg AS (
  SELECT COALESCE(AVG(daily_apps), 0) AS avg_daily_apps
  FROM (
    SELECT usage_date, COUNT(DISTINCT app_id) AS daily_apps
    FROM apps_by_day
    GROUP BY usage_date
  ) t
)
SELECT t.*, a.avg_daily_apps
FROM apps_totals t, apps_avg a
"""

MV_APPS_BY_APP_FULL = """
SELECT
  app_id,
  SUM(total_dbus) AS total_dbus,
  SUM(total_spend) AS total_spend,
  COUNT(DISTINCT workspace_id) AS workspace_count,
  COUNT(DISTINCT usage_date) AS days_active,
  MAX(usage_date) AS last_usage_date
FROM `{catalog}`.`{schema}`.`daily_apps_summary`
WHERE usage_date BETWEEN :start_date AND :end_date
  {ws_filter}
GROUP BY app_id
ORDER BY total_spend DESC
"""

MV_APPS_TIMESERIES = """
SELECT
  usage_date,
  SUM(total_dbus) AS total_dbus,
  SUM(total_spend) AS total_spend
FROM `{catalog}`.`{schema}`.`daily_apps_summary`
WHERE usage_date BETWEEN :start_date AND :end_date
  {app_filter}
  {ws_filter}
GROUP BY usage_date
ORDER BY usage_date
"""

MV_APPS_BY_APP_SKU = """
SELECT
  app_id,
  sku_name,
  SUM(total_dbus) AS total_dbus,
  SUM(total_spend) AS total_spend
FROM `{catalog}`.`{schema}`.`daily_apps_summary`
WHERE usage_date BETWEEN :start_date AND :end_date
  {ws_filter}
GROUP BY app_id, sku_name
ORDER BY app_id, total_spend DESC
"""

MV_APPS_WORKSPACES = """
SELECT
  app_id,
  CAST(workspace_id AS STRING) AS workspace_id,
  CAST(workspace_id AS STRING) AS workspace_name
FROM `{catalog}`.`{schema}`.`daily_apps_summary`
WHERE usage_date BETWEEN :start_date AND :end_date
  {ws_filter}
GROUP BY app_id, workspace_id
ORDER BY app_id, workspace_id
"""

for _query_name, _query_value in list(globals().items()):
    if isinstance(_query_value, str) and "/* TEMPORAL_LIST_PRICE_JOIN */" in _query_value:
        globals()[_query_name] = apply_current_list_price_join(_query_value)


def _process_apps(
    raw_apps: list[dict[str, Any]],
    active_only: bool,
    start_date_str: str,
    end_date_str: str,
    registry: dict[str, dict[str, Any]],
    sku_rows: list[dict[str, Any]] | None = None,
    include_registry_only: bool = True,
) -> dict[str, Any]:
    """Split billing rows into registered apps + unregistered bucket.

    Strategy: show every app that exists in the Apps API registry as an
    individual tile (these are "real" deployed apps).  Billing rows whose
    UUID doesn't match any registered app are bucketed as
    "Unregistered apps" (likely synthetic data or deleted apps).

    Returns a dict with keys: apps, inactive_summary, total_app_count,
    active_count, inactive_count, total_spend, unregistered_summary.
    """
    selected_start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    selected_end = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    # Inclusive seven-day window: end date plus the six preceding dates.
    active_window_start = max(
        selected_start,
        selected_end - timedelta(days=ACTIVE_DAYS - 1),
    )

    active_rows: list[dict[str, Any]] = []
    inactive_rows: list[dict[str, Any]] = []

    for r in raw_apps:
        last = r.get("last_usage_date")
        if isinstance(last, datetime):
            last_date = last.date()
        elif isinstance(last, date):
            last_date = last
        elif last:
            try:
                last_date = datetime.strptime(str(last)[:10], "%Y-%m-%d").date()
            except ValueError:
                last_date = None
        else:
            last_date = None
        billing_active = last_date is not None and last_date >= active_window_start
        r["_billing_active"] = billing_active
        if billing_active:
            active_rows.append(r)
        else:
            inactive_rows.append(r)

    # Build the list we'll return as individual tiles. Live deployment state is
    # applied below for registered apps, so filtering raw rows here would drop a
    # running app whose billing record has not landed yet.
    source = raw_apps
    total_spend_all = sum(float(r.get("total_spend") or 0) for r in source)

    # Separate registered (real) apps from unregistered billing UUIDs
    registered_apps: list[dict[str, Any]] = []
    unregistered_rows: list[dict[str, Any]] = []

    for r in source:
        raw_id = r.get("app_id") or r.get("app_name") or "Unknown"
        if raw_id in registry:
            registered_apps.append(r)
        else:
            unregistered_rows.append(r)

    apps = []
    for r in registered_apps:
        raw_id = r.get("app_id") or r.get("app_name") or "Unknown"
        spend = float(r.get("total_spend") or 0)
        reg_entry = registry.get(raw_id, {})
        platform_active = _registry_app_is_running(reg_entry)
        is_active = (
            platform_active
            if platform_active is not None
            else bool(r.get("_billing_active"))
        )
        if active_only and not is_active:
            continue
        apps.append({
            "app_id": raw_id,
            "app_name": reg_entry.get("name", raw_id),
            "app_url": reg_entry.get("url", ""),
            "total_dbus": float(r.get("total_dbus") or 0),
            "total_spend": spend,
            "workspace_count": r.get("workspace_count") or 0,
            "days_active": r.get("days_active") or 0,
            "last_usage_date": str(r.get("last_usage_date")) if r.get("last_usage_date") else None,
            "percentage": (spend / total_spend_all * 100) if total_spend_all > 0 else 0,
            "is_registered": True,
            "status": "active" if is_active else "inactive",
        })

    # Sort registered apps by spend desc
    apps.sort(key=lambda a: a["total_spend"], reverse=True)

    apps_in_list = {a["app_id"] for a in apps}

    # Registry apps with no billing in the window still belong in the status
    # population. A running app must remain active even before its usage lands.
    for uid, entry in (registry.items() if include_registry_only else ()):
        if uid in apps_in_list:
            continue
        platform_active = _registry_app_is_running(entry)
        if active_only and platform_active is not True:
            continue
        apps.append({
            "app_id": uid,
            "app_name": entry.get("name", uid),
            "app_url": entry.get("url", ""),
            "total_dbus": 0,
            "total_spend": 0,
            "workspace_count": 0,
            "days_active": 0,
            "last_usage_date": None,
            "percentage": 0,
            "is_registered": True,
            "status": "active" if platform_active is True else "inactive",
        })
        apps_in_list.add(uid)

    # Unregistered billing rows are historical/deleted apps, not live status.
    if not active_only:
        for r in unregistered_rows:
            raw_id = r.get("app_id") or r.get("app_name") or "Unknown"
            if raw_id in apps_in_list:
                continue
            spend = float(r.get("total_spend") or 0)
            apps.append({
                "app_id": raw_id,
                "app_name": raw_id,
                "app_url": "",
                "total_dbus": float(r.get("total_dbus") or 0),
                "total_spend": spend,
                "workspace_count": r.get("workspace_count") or 0,
                "days_active": r.get("days_active") or 0,
                "last_usage_date": str(r.get("last_usage_date")) if r.get("last_usage_date") else None,
                "percentage": (spend / total_spend_all * 100) if total_spend_all > 0 else 0,
                "is_registered": False,
                "status": "historical",
            })
            apps_in_list.add(raw_id)

    # Unregistered apps summary
    unreg_spend = sum(float(r.get("total_spend") or 0) for r in unregistered_rows)
    unreg_dbus = sum(float(r.get("total_dbus") or 0) for r in unregistered_rows)

    # Inactive summary (when showing active only)
    inactive_spend = sum(float(r.get("total_spend") or 0) for r in inactive_rows)
    inactive_dbus = sum(float(r.get("total_dbus") or 0) for r in inactive_rows)

    registered_spend = sum(a["total_spend"] for a in apps)

    # Attach per-app SKU breakdown if available
    if sku_rows:
        sku_by_app: dict[str, list[dict[str, Any]]] = {}
        for row in sku_rows:
            aid = row.get("app_id") or "Unknown"
            if aid not in registry:
                continue  # skip unregistered
            sku_by_app.setdefault(aid, []).append({
                "sku_name": row.get("sku_name") or "Unknown",
                "total_dbus": float(row.get("total_dbus") or 0),
                "total_spend": float(row.get("total_spend") or 0),
            })
        for app in apps:
            breakdown = sku_by_app.get(app["app_id"], [])
            app_spend = app["total_spend"]
            for item in breakdown:
                item["percentage"] = (item["total_spend"] / app_spend * 100) if app_spend > 0 else 0
            app["sku_breakdown"] = breakdown

    return {
        "apps": apps,
        "total_spend": registered_spend,
        "total_app_count": len(apps),
        "active_count": sum(
            1
            for app in apps
            if app.get("is_registered") and app.get("status") == "active"
        ),
        "inactive_count": sum(
            1
            for app in apps
            if app.get("is_registered") and app.get("status") == "inactive"
        ),
        "active_window": {
            "start_date": active_window_start.isoformat(),
            "end_date": selected_end.isoformat(),
            "days": (selected_end - active_window_start).days + 1,
            "definition": (
                "Currently running registered apps; recent compute usage is used "
                "only when live Apps API status is unavailable"
            ),
        },
        "inactive_summary": {
            "count": len(inactive_rows),
            "total_spend": inactive_spend,
            "total_dbus": inactive_dbus,
            "percentage": (inactive_spend / (total_spend_all + inactive_spend) * 100)
            if (total_spend_all + inactive_spend) > 0
            else 0,
        },
        "unregistered_summary": {
            "count": len(unregistered_rows),
            "total_spend": unreg_spend,
            "total_dbus": unreg_dbus,
            "percentage": (unreg_spend / total_spend_all * 100) if total_spend_all > 0 else 0,
        },
    }


def _validate_active_count_contract(
    summary: dict[str, Any],
    apps_result: dict[str, Any],
) -> None:
    """Reject bundles whose KPI and status breakdown use different populations."""
    summary_count = summary.get("active_app_count")
    breakdown_count = apps_result.get("active_count")
    active_window = apps_result.get("active_window")
    if (
        not isinstance(summary_count, int)
        or not isinstance(breakdown_count, int)
        or summary_count != breakdown_count
        or not isinstance(active_window, dict)
    ):
        raise ValueError(
            "Apps active count contract mismatch: "
            f"summary={summary_count!r}, breakdown={breakdown_count!r}"
        )


@router.get("/summary")
async def get_apps_summary(
    start_date: str = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(default=None, description="End date (YYYY-MM-DD)"),
) -> dict[str, Any]:
    """Get Databricks Apps cost summary."""
    params = {
        "start_date": start_date or get_default_start_date(),
        "end_date": end_date or get_default_end_date(),
    }

    results = await asyncio.to_thread(execute_query, APPS_SUMMARY, params)

    if not results:
        return {
            "total_dbus": 0,
            "total_spend": 0,
            "workspace_count": 0,
            "app_count": 0,
            "days_in_range": 0,
            "avg_daily_spend": 0,
            "start_date": params["start_date"],
            "end_date": params["end_date"],
        }

    row = results[0]
    days = row.get("days_in_range") or 1
    total_spend = float(row.get("total_spend") or 0)

    return {
        "total_dbus": float(row.get("total_dbus") or 0),
        "total_spend": total_spend,
        "workspace_count": row.get("workspace_count") or 0,
        "app_count": row.get("app_count") or 0,
        "avg_daily_apps": round(float(row.get("avg_daily_apps") or 0)),
        "days_in_range": days,
        "avg_daily_spend": total_spend / days if days > 0 else 0,
        "start_date": params["start_date"],
        "end_date": params["end_date"],
        "first_date": str(row.get("first_date")) if row.get("first_date") else None,
        "last_date": str(row.get("last_date")) if row.get("last_date") else None,
    }


def _empty_bundle(params: dict, active_only: bool) -> dict[str, Any]:
    """Return a valid zero-data bundle — used when workspace filter finds no Apps rows."""
    selected_start = datetime.strptime(params["start_date"], "%Y-%m-%d").date()
    selected_end = datetime.strptime(params["end_date"], "%Y-%m-%d").date()
    active_window_start = max(
        selected_start,
        selected_end - timedelta(days=ACTIVE_DAYS - 1),
    )
    return {
        "summary": {"total_dbus": 0, "total_spend": 0, "workspace_count": 0, "app_count": 0, "active_app_count": 0, "days_in_range": 1, "avg_daily_spend": 0, "avg_cost_per_app": 0},
        "apps": {"apps": [], "total_spend": 0, "total_app_count": 0, "active_count": 0, "inactive_count": 0,
                 "active_window": {"start_date": active_window_start.isoformat(), "end_date": selected_end.isoformat(), "days": (selected_end - active_window_start).days + 1, "definition": "Currently running registered apps; recent compute usage is used only when live Apps API status is unavailable"},
                 "inactive_summary": {"count": 0, "total_spend": 0, "total_dbus": 0, "percentage": 0},
                 "unregistered_summary": {"count": 0, "total_spend": 0, "total_dbus": 0, "percentage": 0}},
        "timeseries": {"timeseries": [], "categories": ["Total"]},
        "connected_artifacts": [],
        "workspaces": [],
        "active_only": active_only,
        "start_date": params["start_date"],
        "end_date": params["end_date"],
    }


def _compute_apps_bundle(
    params: dict,
    id_list: list | None,
    active_only: bool,
    dkey: str,
    cache_generation: CacheGeneration,
) -> None:
    """Background worker: run all Apps queries, build response, write to Delta cache."""
    import time as _time
    _endpoint = f"apps:dashboard-bundle:v5:{'active' if active_only else 'all'}"
    _start = _time.time()
    _set_apps_producer_status(dkey, "running")

    try:
        source_labels = selected_source_labels()
        shared_only_scope = bool(source_labels) and not local_source_is_selected()
        local_workspace_id = get_current_workspace_id()
        selected_workspace_ids = {str(value) for value in (id_list or [])}
        workspace_scope_includes_local = (
            not selected_workspace_ids or local_workspace_id in selected_workspace_ids
        )
        allow_local_registry = not shared_only_scope and workspace_scope_includes_local
        metadata_thread: threading.Thread | None = None
        metadata_stale = allow_local_registry and (
            not _app_name_cache
            or (time.time() - _app_name_cache_time) >= APP_NAME_CACHE_TTL
            or not _app_details_cache
            or (time.time() - _app_details_cache_time) >= APP_RESOURCES_CACHE_TTL
        )
        if metadata_stale:
            metadata_context = contextvars.copy_context()
            metadata_thread = threading.Thread(
                target=metadata_context.run,
                args=(_refresh_apps_metadata,),
                daemon=True,
                name="apps-metadata-bg",
            )
            metadata_thread.start()

        ws_clause = wf.build_ws_filter_clause(id_list=id_list)

        def _ws(sql: str) -> str:
            return wf.inject_ws_filter(sql, ws_clause)

        # Billing totals are required; live Apps API metadata is optional. Never
        # hold the bundle behind an unbounded SDK list/get call.
        registry = dict(_app_name_cache) if allow_local_registry else {}
        app_filter = _build_app_id_filter(registry)

        filtered_timeseries = f"""
        WITH filtered_usage AS (
          SELECT *
          FROM system.billing.usage u
          WHERE u.usage_date BETWEEN :start_date AND :end_date
            AND u.usage_quantity > 0
            AND u.billing_origin_product = 'APPS'
            {ws_clause}
        )
        SELECT
          u.usage_date,
          SUM(u.usage_quantity) as total_dbus,
          SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)) as total_spend
        FROM filtered_usage u
        {current_list_price_join()}
        GROUP BY u.usage_date
        ORDER BY u.usage_date
        """

        # Mean-of-daily-ratios: matches the kpi-trend drilldown methodology exactly.
        avg_cost_per_app_query = f"""
        WITH filtered_usage AS (
          SELECT *
          FROM system.billing.usage u
          WHERE u.usage_date BETWEEN :start_date AND :end_date
            AND u.usage_quantity > 0
            AND u.billing_origin_product = 'APPS'
            {app_filter}
            {ws_clause}
        )
        SELECT COALESCE(AVG(daily_cost_per_app), 0) as avg_cost_per_app
        FROM (
          SELECT u.usage_date,
            SUM(u.usage_quantity * COALESCE(p.pricing.default, 0))
              / NULLIF(COUNT(DISTINCT u.usage_metadata.app_id), 0) as daily_cost_per_app
          FROM filtered_usage u
          {current_list_price_join()}
          GROUP BY u.usage_date
        ) t
        """

        # MV fast paths — daily_apps_summary avoids the raw system.billing.usage
        # scan for the four heavy slots when the MV is available. Each helper
        # falls back to its raw counterpart on any exception so a broken MV
        # never blocks a page load.
        _use_mv = _check_mv_available()
        if source_labels:
            if not _use_mv or not source_label_filter_clause(MV_APPS_SUMMARY):
                raise SourceScopeUnsupportedError(
                    "Selected Apps sources require a routed daily_apps_summary view."
                )
        _mv_ws_clause = wf.build_ws_filter_clause(col="workspace_id", id_list=id_list) if _use_mv else ""
        # MV column is bare `app_id`; the raw filter uses `u.usage_metadata.app_id`
        _mv_app_filter = ""
        if _use_mv and registry:
            _mv_app_filter = "AND app_id IN (" + ", ".join(f"'{uid}'" for uid in registry) + ")"

        def _mv_query(name: str, mv_sql_tpl: str, raw_fallback: Any, **fmt_kwargs: str) -> list[dict[str, Any]]:
            """Try the MV path; fall back to raw on any exception."""
            if _use_mv:
                try:
                    cat, sch = get_catalog_schema()
                    mv_sql = mv_sql_tpl.format(
                        catalog=cat,
                        schema=sch,
                        ws_filter=_mv_ws_clause + source_label_filter_clause(mv_sql_tpl),
                        **fmt_kwargs,
                    )
                    mv_sql = apply_mv_overrides(mv_sql, cat, sch)
                    return execute_query(mv_sql, params)
                except SQLExecutionError:
                    raise
                except Exception as e:
                    if selected_source_labels():
                        raise
                    logger.warning("apps %s MV path failed (%s); falling back to raw scan", name, type(e).__name__)
            if selected_source_labels():
                raise SourceScopeUnsupportedError(
                    "Selected Apps sources require daily_apps_summary routing; "
                    "local raw billing fallback is not source-safe."
                )
            return raw_fallback()

        queries = [
            ("summary", lambda: _mv_query("summary", MV_APPS_SUMMARY, lambda: execute_query(_ws(APPS_SUMMARY), params))),
            ("apps", lambda: _mv_query("apps", MV_APPS_BY_APP_FULL, lambda: execute_query(_ws(APPS_BY_APP_FULL), params))),
            ("timeseries", lambda: _mv_query("timeseries", MV_APPS_TIMESERIES, lambda: execute_query(filtered_timeseries, params), app_filter="")),
            ("avg_cost_per_app", lambda: _mv_query("avg_cost_per_app", MV_APPS_AVG_COST_PER_APP, lambda: execute_query(avg_cost_per_app_query, params), app_filter=_mv_app_filter)),
            ("sku_breakdown", lambda: _mv_query("sku_breakdown", MV_APPS_BY_APP_SKU, lambda: execute_query(_ws(APPS_BY_APP_SKU), params))),
            ("workspaces", lambda: _mv_query("workspaces", MV_APPS_WORKSPACES, lambda: _query_app_workspaces(params, ws_clause))),
            ("service_principals", lambda: [] if shared_only_scope else execute_query(_ws(APPS_SERVICE_PRINCIPALS), params)),
        ]

        required_queries = {"summary", "apps", "timeseries"}
        try:
            results = execute_queries_parallel(
                queries,
                55.0,
                required_names=required_queries,
                max_concurrency=2,
            )
            optional_failures: dict[str, str] = {}
        except SQLExecutionError as exc:
            results, optional_failures = recover_optional_bundle_queries(
                exc, required_queries
            )
        _set_apps_producer_status(dkey, "running", phase="formatting")
        if metadata_thread is not None:
            metadata_thread.join(timeout=2.0)
            registry = dict(_app_name_cache)
        if allow_local_registry and not registry:
            optional_failures["app_registry"] = "METADATA_REFRESH_PENDING"

        # Build workspace lookup per app_id
        workspace_rows = results.get("workspaces", []) or []
        app_workspace_map: dict[str, list[str]] = {}
        all_workspaces: dict[str, str] = {}  # ws_id -> ws_name
        for row in workspace_rows:
            app_id = row.get("app_id", "")
            ws_name = str(row.get("workspace_name", ""))
            ws_id = str(row.get("workspace_id", ""))
            if app_id not in app_workspace_map:
                app_workspace_map[app_id] = []
            if ws_id not in app_workspace_map[app_id]:
                app_workspace_map[app_id].append(ws_id)
            all_workspaces[ws_id] = ws_name

        summary_data = results.get("summary", []) or []
        # Calendar days so the KPI subtitle matches the selected range regardless of billing table lag
        _start_dt = datetime.strptime(params["start_date"], "%Y-%m-%d")
        _end_dt = datetime.strptime(params["end_date"], "%Y-%m-%d")
        days_in_range = (_end_dt - _start_dt).days + 1
        raw_apps = results.get("apps", []) or []
        sku_rows = results.get("sku_breakdown", []) or []
        apps_result = _process_apps(
            raw_apps,
            active_only,
            params["start_date"],
            params["end_date"],
            registry,
            sku_rows,
            include_registry_only=allow_local_registry,
        )

        for app in apps_result["apps"]:
            app["workspace_names"] = app_workspace_map.get(app["app_id"], [])

        summary_row = summary_data[0] if summary_data else {}
        total_spend_all = float(summary_row.get("total_spend") or 0)
        total_dbus_all = float(summary_row.get("total_dbus") or 0)
        avg_daily_spend = total_spend_all / days_in_range if days_in_range > 0 else 0

        avg_cost_per_app_data = results.get("avg_cost_per_app", []) or []
        avg_cost_per_app = float(avg_cost_per_app_data[0].get("avg_cost_per_app") or 0) if avg_cost_per_app_data else 0

        summary = {
            "total_dbus": total_dbus_all,
            "total_spend": total_spend_all,
            "workspace_count": len(all_workspaces),
            "app_count": int(apps_result.get("total_app_count") or 0),
            # Same processed rows, filters, registration scope, and trailing
            # activity window used by the status breakdown below.
            "active_app_count": int(apps_result.get("active_count") or 0),
            "days_in_range": days_in_range,
            "avg_daily_spend": avg_daily_spend,
            "avg_cost_per_app": avg_cost_per_app,
        }
        _validate_active_count_contract(summary, apps_result)

        timeseries_data = results.get("timeseries", []) or []
        timeseries = sorted(
            [{"date": str(row.get("usage_date")), "Total": float(row.get("total_spend") or 0)} for row in timeseries_data],
            key=lambda x: x["date"],
        )

        details_by_app = dict(_app_details_cache) if allow_local_registry else {}
        if registry and not details_by_app:
            optional_failures["app_details"] = "METADATA_REFRESH_PENDING"

        for app in apps_result["apps"]:
            app_id = app["app_id"]
            detail = details_by_app.get(app_id, {})
            if app.get("is_registered"):
                metadata = detail.get("metadata") or registry.get(app_id, {}).get("metadata") or {}
            else:
                metadata = {
                    "availability": "unavailable",
                    "description": "",
                    "creator": "",
                    "updater": "",
                    "compute_size": "",
                    "compute_status": None,
                    "app_status": None,
                    "deployment": None,
                    "source_code_path": "",
                    "git": None,
                    "thumbnail_url": None,
                }
            workspace_ids = app_workspace_map.get(app_id, [])
            public_metadata = _public_app_metadata(metadata)
            public_metadata["thumbnail_url"] = _public_thumbnail_url(
                str(app_id),
                bool(app.get("is_registered")),
                metadata,
                registry.get(app_id, {}),
            )
            app["metadata"] = public_metadata
            app["resource_bindings"] = list(detail.get("resources") or [])
            app["workspaces"] = [
                {"id": ws_id, "name": all_workspaces.get(ws_id, ws_id)}
                for ws_id in workspace_ids
            ]

        connected_artifacts: list[dict[str, Any]] = []
        for uid, entry in (registry.items() if allow_local_registry else ()):
            app_name = entry.get("name", uid)
            for res in details_by_app.get(uid, {}).get("resources", []):
                connected_artifacts.append({
                    "app_id": uid,
                    "app_name": app_name,
                    "artifact_name": str(res.get("name", "")),
                    "artifact_type": str(res.get("type", "")),
                    "artifact_description": str(res.get("description", "")),
                    "artifact_id": str(res.get("id", "")) or None,
                })

        sp_rows = results.get("service_principals", []) or []
        seen_sp: set[tuple[str, str]] = set()
        for row in sp_rows:
            app_id = str(row.get("app_id", ""))
            run_as = str(row.get("run_as", ""))
            if not app_id or not run_as:
                continue
            key = (app_id, run_as)
            if key in seen_sp:
                continue
            seen_sp.add(key)
            app_name = registry.get(app_id, {}).get("name", app_id)
            connected_artifacts.append({
                "app_id": app_id,
                "app_name": app_name,
                "artifact_name": run_as,
                "artifact_type": "SERVICE_PRINCIPAL",
                "artifact_description": "Run-as identity",
                "artifact_id": None,
            })

        _resp = {
            "availability": "partial" if optional_failures else "available",
            "partial_reasons": optional_failures,
            "summary": summary,
            "apps": apps_result,
            "timeseries": {"timeseries": timeseries, "categories": ["Total"]},
            "connected_artifacts": connected_artifacts,
            "connected_resources_available": allow_local_registry,
            "connected_resources_reason": (
                None
                if allow_local_registry
                else "Connected resources come from this workspace's Apps API and are unavailable for the selected remote workspace or shared-only source."
            ),
            "workspaces": [{"id": ws_id, "name": ws_name} for ws_id, ws_name in sorted(all_workspaces.items(), key=lambda x: x[1])],
            "active_only": active_only,
            "start_date": params["start_date"],
            "end_date": params["end_date"],
        }
        # Guard against locking in a zero-value bundle for 30 min when a critical
        # query timed out (execute_queries_parallel sets that slot to None).
        # A partial-timeout would otherwise cache the empty response with full TTL
        # and users see $0 on the KPIs until the cache expires. Short-cache
        # (60s) so the next request retries against a hopefully-warm warehouse.
        _summary_missing = results.get("summary") is None
        _apps_missing = results.get("apps") is None
        _timeseries_missing = results.get("timeseries") is None
        _bundle_empty = (
            total_spend_all == 0
            and total_dbus_all == 0
            and not apps_result.get("apps")
        )
        _degraded = (
            bool(optional_failures)
            or _summary_missing
            or _apps_missing
            or _timeseries_missing
            or _bundle_empty
        )
        if _degraded:
            cache_ttl = 60
            logger.info(
                "apps dashboard-bundle degraded response — short-caching (60s): "
                "summary_missing=%s apps_missing=%s timeseries_missing=%s empty=%s",
                _summary_missing, _apps_missing, _timeseries_missing, _bundle_empty,
            )
        else:
            cache_ttl = 60 if not registry else (600 if id_list else 1800)
        try:
            _set_apps_producer_status(dkey, "running", phase="cache_write")
            cache_written = delta_cache_put(
                dkey,
                _endpoint,
                _resp,
                ttl_seconds=cache_ttl,
                generation=cache_generation,
                wait_for_remote=False,
            )
        except Exception as _ce:
            logger.warning(
                "Apps shared cache write could not be queued; serving the local result: %s",
                _ce,
            )
            cache_written = True
        if not cache_written:
            logger.warning(
                "Apps shared cache rejected the payload; serving the local result"
            )
        logger.info(
            "apps dashboard-bundle background compute complete: %.1fs workspaces=%s apps=%d",
            _time.time() - _start, id_list or "all", apps_result.get("total_app_count", 0),
        )
        _set_apps_producer_status(dkey, "complete")

        # Refresh optional registry/resource metadata only after required billing
        # data is durable and visible to pollers.
        if allow_local_registry and metadata_thread is None and {
            "app_registry",
            "app_details",
        }.intersection(optional_failures):
            resource_context = contextvars.copy_context()
            threading.Thread(
                target=resource_context.run,
                args=(_refresh_apps_metadata,),
                daemon=True,
                name="apps-metadata-bg",
            ).start()
    except Exception as e:
        logger.error("apps dashboard-bundle background compute failed: %s", e, exc_info=True)
        code = str(getattr(e, "code", "APPS_BUNDLE_FAILED"))
        failure = _apps_failure_payload(params, code)
        with _apps_bundle_status_lock:
            _apps_bundle_failures[dkey] = failure
        _set_apps_producer_status(dkey, "failed", error_code=code)
        failure_written = False
        try:
            failure_written = delta_cache_put(
                dkey,
                _endpoint,
                failure,
                ttl_seconds=15,
                generation=cache_generation,
                wait_for_remote=True,
            )
        except Exception:
            logger.warning("Could not persist typed Apps producer failure", exc_info=True)
        if isinstance(e, _AppsCacheWriteError) or not failure_written:
            raise _AppsCacheWriteError(
                "Apps producer failed before terminal state became durable."
            ) from e


@router.get("/dashboard-bundle")
async def get_apps_dashboard_bundle(
    start_date: str = Query(default=None),
    end_date: str = Query(default=None),
    active_only: bool = Query(default=False, description="Show only apps active in last 7 days"),
    workspace_ids: str = Query(default=None),
) -> dict[str, Any]:
    """Get all Apps dashboard data in a single request (submit-and-poll: 202 on cache miss)."""
    validated_start, validated_end = validate_date_range(
        start_date,
        end_date,
        default_start=get_default_start_date(),
        default_end=get_default_end_date(),
    )
    params = {"start_date": validated_start, "end_date": validated_end}
    id_list = parse_workspace_ids(workspace_ids)
    _endpoint = f"apps:dashboard-bundle:v5:{'active' if active_only else 'all'}"
    _dkey = bundle_cache_key(_endpoint, params["start_date"], params["end_date"], id_list)

    producer_status = await asyncio.to_thread(get_bundle_compute_state, _dkey)
    if producer_status and producer_status.get("state") == "failed":
        shared_code = str(producer_status.get("error_code") or "")
        deadline_failure = shared_code in {
            "BUNDLE_PRODUCER_DEADLINE",
            "BUNDLE_PRODUCER_LEASE_EXPIRED",
        }
        if shared_code in {
            "BUNDLE_REMOTE_CACHE_WRITE_FAILED",
            "APPS_CACHE_WRITE_FAILED",
        }:
            public_error_code = "APPS_CACHE_WRITE_FAILED"
        elif deadline_failure:
            public_error_code = "APPS_PRODUCER_DEADLINE"
        else:
            public_error_code = "APPS_PRODUCER_FAILED"
        failure = _apps_failure_payload(params, public_error_code)
        if deadline_failure:
            failure["reason"] = "producer_deadline_exceeded"
            failure["reason_detail"] = (
                "Apps data exceeded its 90-second deadline. Retry shortly to start "
                "a fresh producer."
            )
        return JSONResponse(
            status_code=503,
            content=failure,
            headers={"Retry-After": "2"},
        )
    if bundle_compute_is_pending(_dkey):
        started_at = float(
            (producer_status or {}).get("started_at")
            or (producer_status or {}).get("created_at")
            or time.time()
        )
        age_seconds = max(0.0, time.time() - started_at)
        return JSONResponse(
            status_code=202,
            content={
                "status": "pending",
                "producer_state": producer_status.get("state", "claimed"),
                "age_seconds": round(age_seconds, 1),
            },
            headers={"Retry-After": "2"},
        )

    if (_dcached := await asyncio.to_thread(delta_cache_get, _dkey)) is not None:
        if isinstance(_dcached, dict) and "_error" in _dcached:
            return _apps_failure_payload(params)
        if isinstance(_dcached, dict) and _dcached.get("availability") == "unavailable":
            return _dcached
        try:
            _validate_active_count_contract(
                _dcached.get("summary", {}),
                _dcached.get("apps", {}),
            )
            return _dcached
        except (AttributeError, ValueError):
            # Ignore bundles written by the previous, mismatched contract and
            # recompute under the same date/workspace/source-scoped cache key.
            logger.info("Ignoring stale Apps bundle with legacy active-count contract")

    with _apps_bundle_status_lock:
        local_failure = _apps_bundle_failures.get(_dkey)
    if local_failure:
        return local_failure

    cache_generation = capture_cache_generation(_endpoint)
    try:
        with _apps_bundle_status_lock:
            if (_apps_bundle_status.get(_dkey) or {}).get("restart_ready"):
                _apps_bundle_status.pop(_dkey, None)
                _apps_bundle_failures.pop(_dkey, None)
        _set_apps_producer_status(_dkey, "queued")
        started = await asyncio.to_thread(
            start_bundle_compute,
            _dkey,
            lambda: _compute_apps_bundle(
                params,
                id_list,
                active_only,
                _dkey,
                cache_generation,
            ),
            name="apps-bundle",
            lease_seconds=90,
            hard_deadline_seconds=90,
        )
        if started:
            logger.info("apps dashboard-bundle: started background compute for %s", _dkey)
        else:
            logger.debug("apps dashboard-bundle: already claimed for %s", _dkey)
    except BundleOverloadedError as exc:
        raise HTTPException(
            status_code=503,
            detail={"message": str(exc), "error_code": exc.code},
            headers={"Retry-After": "2"},
        ) from exc

    return JSONResponse(
        status_code=202,
        content={"status": "pending", "producer_state": "queued"},
        headers={"Retry-After": "2"},
    )


# ── KPI Trend (registered-apps-only) ─────────────────────────────────

def _build_app_id_filter(
    registry: dict[str, dict[str, Any]],
    col: str = "u.usage_metadata.app_id",
) -> str:
    """Build a SQL IN-clause for registered app UUIDs.
    Returns empty string when registry is unavailable so we still show all apps data.
    """
    if not registry:
        return ""  # no registry → no filter, show all billing APPS rows
    ids = ", ".join(f"'{uid}'" for uid in registry)
    return f"AND {col} IN ({ids})"


@router.get("/kpi-trend")
async def get_apps_kpi_trend(
    kpi: str = Query(..., description="KPI: apps_spend, apps_dbus, apps_count"),
    start_date: str = Query(default=None),
    end_date: str = Query(default=None),
    granularity: str = Query("daily", description="daily, weekly, monthly"),
    workspace_ids: str = Query(default=None),
) -> dict[str, Any]:
    """KPI trend filtered to registered apps only."""
    params = {
        "start_date": start_date or get_default_start_date(),
        "end_date": end_date or get_default_end_date(),
    }
    id_list = [i.strip() for i in workspace_ids.split(",") if i.strip()] if workspace_ids else None
    cache_endpoint = "trend:apps:kpi"
    _dkey = bundle_cache_key(
        f"{cache_endpoint}:{kpi}:{granularity}",
        params["start_date"],
        params["end_date"],
        id_list,
    )
    if (_dcached := await asyncio.to_thread(delta_cache_get, _dkey)) is not None:
        return _dcached
    _cache_generation = capture_cache_generation(cache_endpoint)

    registry = _app_name_cache  # use stale cache — background refresh handled by dashboard-bundle
    if kpi == "apps_count" and not registry:
        return {
            "available": False,
            "retryable": True,
            "unavailable_reason": "Registered app metadata is still loading.",
            "kpi": kpi,
            "granularity": granularity,
            "data_points": [],
            "summary": {
                "period_start_value": 0,
                "period_end_value": 0,
                "change_amount": 0,
                "change_percent": 0,
                "min_value": 0,
                "max_value": 0,
                "avg_value": 0,
                "trend": "flat",
            },
        }
    raw_app_filter = _build_app_id_filter(registry)
    mv_app_filter = _build_app_id_filter(registry, col="app_id")
    raw_ws_filter = wf.build_ws_filter_clause(col="u.workspace_id", id_list=id_list)
    mv_ws_filter = wf.build_ws_filter_clause(col="workspace_id", id_list=id_list)

    if kpi == "apps_spend":
        raw_query = f"""
        WITH usage_with_price AS (
          SELECT
            u.usage_date,
            u.usage_quantity,
            COALESCE(p.pricing.default, 0) as price_per_dbu
          FROM system.billing.usage u
          {current_list_price_join()}
          WHERE u.usage_date BETWEEN :start_date AND :end_date
            AND u.usage_quantity > 0
            AND u.billing_origin_product = 'APPS'
            {raw_ws_filter}
        )
        SELECT usage_date as date, SUM(usage_quantity * price_per_dbu) as value
        FROM usage_with_price
        GROUP BY usage_date
        ORDER BY usage_date
        """
        mv_query_template = """
        SELECT usage_date AS date, SUM(total_spend) AS value
        FROM `{catalog}`.`{schema}`.`daily_apps_summary`
        WHERE usage_date BETWEEN :start_date AND :end_date
          {ws_filter}
        GROUP BY usage_date
        ORDER BY usage_date
        """
    elif kpi == "apps_dbus":
        raw_query = f"""
        SELECT u.usage_date as date, SUM(u.usage_quantity) as value
        FROM system.billing.usage u
        WHERE u.usage_date BETWEEN :start_date AND :end_date
          AND u.usage_quantity > 0
          AND u.billing_origin_product = 'APPS'
          {raw_ws_filter}
        GROUP BY u.usage_date
        ORDER BY u.usage_date
        """
        mv_query_template = """
        SELECT usage_date AS date, SUM(total_dbus) AS value
        FROM `{catalog}`.`{schema}`.`daily_apps_summary`
        WHERE usage_date BETWEEN :start_date AND :end_date
          {ws_filter}
        GROUP BY usage_date
        ORDER BY usage_date
        """
    elif kpi == "apps_count":
        raw_query = f"""
        SELECT u.usage_date as date,
               COUNT(DISTINCT u.usage_metadata.app_id) as value
        FROM system.billing.usage u
        WHERE u.usage_date BETWEEN :start_date AND :end_date
          AND u.usage_quantity > 0
          AND u.billing_origin_product = 'APPS'
          AND u.usage_metadata.app_id IS NOT NULL
          {raw_app_filter}
          {raw_ws_filter}
        GROUP BY u.usage_date
        ORDER BY u.usage_date
        """
        mv_query_template = """
        SELECT usage_date AS date, COUNT(DISTINCT app_id) AS value
        FROM `{catalog}`.`{schema}`.`daily_apps_summary`
        WHERE usage_date BETWEEN :start_date AND :end_date
          AND app_id <> 'Unknown'
          {app_filter}
          {ws_filter}
        GROUP BY usage_date
        ORDER BY usage_date
        """
    elif kpi == "apps_avg_cost_per_app":
        raw_query = f"""
        SELECT
          usage_date as date,
          SUM(usage_quantity * COALESCE(p.pricing.default, 0))
            / NULLIF(COUNT(DISTINCT COALESCE(u.usage_metadata.app_id, 'unknown')), 0) as value
        FROM system.billing.usage u
        {current_list_price_join()}
        WHERE u.usage_date BETWEEN :start_date AND :end_date
          AND u.usage_quantity > 0
          AND u.billing_origin_product = 'APPS'
          {raw_app_filter}
          {raw_ws_filter}
        GROUP BY usage_date
        ORDER BY usage_date
        """
        mv_query_template = """
        SELECT usage_date AS date,
          SUM(total_spend) / NULLIF(COUNT(DISTINCT app_id), 0) AS value
        FROM `{catalog}`.`{schema}`.`daily_apps_summary`
        WHERE usage_date BETWEEN :start_date AND :end_date
          {app_filter}
          {ws_filter}
        GROUP BY usage_date
        ORDER BY usage_date
        """
    else:
        return {"error": f"Unknown KPI: {kpi}"}

    try:
        if _check_mv_available():
            cat, sch = get_catalog_schema()
            query = mv_query_template.format(
                catalog=cat,
                schema=sch,
                app_filter=mv_app_filter,
                ws_filter=mv_ws_filter + source_label_filter_clause(mv_query_template),
            )
            query = apply_mv_overrides(query, cat, sch)
            results = await asyncio.to_thread(execute_query, query, params)
        else:
            if selected_source_labels():
                raise RuntimeError(
                    "Selected shared sources require the Apps managed table and verified unified view."
                )
            results = await asyncio.to_thread(execute_query, raw_query, params)
    except Exception as e:
        logger.error("Apps KPI trend query failed for %s: %s", kpi, e)
        return {
            "available": False,
            "error_code": str(getattr(e, "code", "APPS_TREND_FAILED")),
            "unavailable_reason": "This trend is unavailable for the selected source.",
            "kpi": kpi, "granularity": granularity, "data_points": [],
            "summary": {"period_start_value": 0, "period_end_value": 0,
                         "change_amount": 0, "change_percent": 0,
                         "min_value": 0, "max_value": 0, "avg_value": 0,
                         "trend": "flat"},
        }

    daily_points = [{"date": str(r["date"]), "value": float(r["value"] or 0)} for r in results]

    # Aggregate into weekly/monthly buckets
    if granularity == "weekly" and daily_points:
        buckets: dict[str, list[float]] = {}
        for dp in daily_points:
            d = datetime.strptime(dp["date"], "%Y-%m-%d")
            week_start = d - timedelta(days=d.weekday())
            key = week_start.strftime("%Y-%m-%d")
            buckets.setdefault(key, []).append(dp["value"])
        data_points = [{"date": k, "value": sum(v)} for k, v in sorted(buckets.items())]
    elif granularity == "monthly" and daily_points:
        buckets_m: dict[str, list[float]] = {}
        for dp in daily_points:
            key = dp["date"][:7] + "-01"
            buckets_m.setdefault(key, []).append(dp["value"])
        data_points = [{"date": k, "value": sum(v)} for k, v in sorted(buckets_m.items())]
    else:
        data_points = daily_points

    if not data_points:
        return {
            "kpi": kpi, "granularity": granularity, "data_points": [],
            "summary": {"period_start_value": 0, "period_end_value": 0,
                         "change_amount": 0, "change_percent": 0,
                         "min_value": 0, "max_value": 0, "avg_value": 0,
                         "trend": "flat"},
        }

    all_values = [dp["value"] for dp in data_points]
    start_val = all_values[0]
    end_val = all_values[-1]
    change = end_val - start_val
    change_pct = (change / start_val * 100) if start_val > 0 else 0
    trend = "flat" if abs(change_pct) < 5 else ("increasing" if change_pct > 0 else "decreasing")

    _resp = {
        "kpi": kpi,
        "granularity": granularity,
        "data_points": data_points,
        "summary": {
            "period_start_value": round(start_val, 2),
            "period_end_value": round(end_val, 2),
            "change_amount": round(change, 2),
            "change_percent": round(change_pct, 2),
            "min_value": round(min(all_values), 2),
            "max_value": round(max(all_values), 2),
            "avg_value": round(sum(all_values) / len(all_values), 2),
            "trend": trend,
        },
    }
    delta_cache_put(
        _dkey,
        cache_endpoint,
        _resp,
        ttl_seconds=cache_ttls.TREND,
        generation=_cache_generation,
    )
    return _resp


# ── Thumbnail proxy ──────────────────────────────────────────────────

# Cache only validated registry IDs. TTLCache is both bounded and LRU-evicting.
_thumbnail_cache: TTLCache[str, tuple[bytes | None, str | None]] = TTLCache(
    maxsize=128,
    ttl=cache_ttls.THUMBNAIL,
)
_THUMBNAIL_APP_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
MAX_THUMBNAIL_BYTES = 2 * 1024 * 1024
_ALLOWED_IMAGE_TYPES = {
    "image/png",
    "image/jpeg",
    "image/webp",
    "image/gif",
    "image/x-icon",
    "image/vnd.microsoft.icon",
}

# Paths to try for app thumbnails (order matters)
_THUMBNAIL_PATHS = [
    "/static/thumbnail.png",
    "/static/dbfavicon.png",
    "/favicon.ico",
]


def _thumbnail_error(status_code: int) -> Response:
    return Response(status_code=status_code, headers={"Cache-Control": "no-store"})


def _thumbnail_response(
    content: bytes,
    content_type: str | None,
    if_none_match: str | None,
) -> Response:
    etag = f'"{hashlib.sha256(content).hexdigest()}"'
    headers = {
        "Cache-Control": f"private, max-age={cache_ttls.THUMBNAIL}",
        "ETag": etag,
    }
    requested_etags = if_none_match.split(",") if isinstance(if_none_match, str) else []
    if any(candidate.strip().removeprefix("W/") in {"*", etag} for candidate in requested_etags):
        return Response(status_code=304, headers=headers)
    return Response(
        content=content,
        media_type=content_type or "image/png",
        headers=headers,
    )


def _normalized_https_origin(url: str) -> tuple[str, str, int] | None:
    """Return an exact normalized HTTPS origin, including effective port."""
    parsed = urlsplit(url)
    if (
        parsed.scheme.lower() != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    try:
        port = parsed.port or 443
    except ValueError:
        return None
    return ("https", parsed.hostname.rstrip(".").lower(), port)


@router.get("/thumbnail")
async def get_app_thumbnail(
    app_id: str = Query(..., description="App UUID"),
    if_none_match: str | None = Header(default=None, alias="If-None-Match"),
) -> Response:
    """Proxy a trusted app thumbnail without exposing workspace credentials."""
    if not _THUMBNAIL_APP_ID_RE.fullmatch(app_id):
        return _thumbnail_error(400)

    registry = _get_app_registry()
    entry = registry.get(app_id)
    if not entry:
        return _thumbnail_error(404)

    cached = _thumbnail_cache.get(app_id)
    if cached is not None:
        data, content_type = cached
        if data:
            return _thumbnail_response(data, content_type, if_none_match)
        return _thumbnail_error(404)

    app_url = str(entry["url"]).strip()
    if app_url and "://" not in app_url:
        app_url = f"https://{app_url}"

    workspace_host = ""
    workspace_client = None
    try:
        workspace_client = get_workspace_client()
        workspace_host = str(
            getattr(workspace_client.config, "host", None) or ""
        ).rstrip("/")
        if workspace_host and "://" not in workspace_host:
            workspace_host = f"https://{workspace_host}"
    except Exception:
        pass

    workspace_origin = _normalized_https_origin(workspace_host)
    app_origin = _normalized_https_origin(app_url)
    trusted_origins = {
        origin for origin in (workspace_origin, app_origin) if origin is not None
    }
    targets: list[tuple[str, bool]] = []
    metadata = (
        _app_details_cache.get(app_id, {}).get("metadata")
        or entry.get("metadata")
        or {}
    )
    thumbnail_source = str(metadata.get("_thumbnail_source_url") or "")
    if thumbnail_source:
        target = (
            urljoin(f"{workspace_host}/", thumbnail_source.lstrip("/"))
            if thumbnail_source.startswith("/") and workspace_host
            else thumbnail_source
        )
        target_origin = _normalized_https_origin(target)
        if target_origin in trusted_origins:
            targets.append((target, target_origin == workspace_origin))

    # Older apps may not expose thumbnail_url. Probe conventional static assets,
    # but only on the exact app hostname returned by the trusted Apps API.
    if app_origin in trusted_origins:
        targets.extend((f"{app_url.rstrip('/')}{path}", False) for path in _THUMBNAIL_PATHS)

    def _workspace_auth_headers() -> dict[str, str]:
        # Called only after the target's exact scheme/hostname/port has matched
        # the configured workspace origin.
        if workspace_client is None:
            return {}
        authenticate = getattr(workspace_client.config, "authenticate", None)
        if callable(authenticate):
            authenticated = authenticate() or {}
            authorization = authenticated.get("Authorization") or authenticated.get("authorization")
            if authorization:
                return {"Authorization": str(authorization)}
        token = str(getattr(workspace_client.config, "token", None) or "")
        return {"Authorization": f"Bearer {token}"} if token else {}

    async with httpx.AsyncClient(timeout=5.0, follow_redirects=False) as client:
        for target, workspace_auth_allowed in targets:
            auth_attempts = [{}]
            if workspace_auth_allowed:
                auth_attempts.append(None)
            for headers in auth_attempts:
                try:
                    request_headers = (
                        _workspace_auth_headers() if headers is None else headers
                    )
                    if headers is None and not request_headers:
                        continue
                    async with client.stream(
                        "GET", target, headers=request_headers
                    ) as resp:
                        content_type = (
                            resp.headers.get("content-type", "")
                            .split(";", 1)[0]
                            .lower()
                        )
                        content_length = resp.headers.get("content-length")
                        if (
                            resp.status_code != 200
                            or content_type not in _ALLOWED_IMAGE_TYPES
                            or (
                                content_length
                                and int(content_length) > MAX_THUMBNAIL_BYTES
                            )
                        ):
                            continue
                        chunks: list[bytes] = []
                        size = 0
                        exceeded = False
                        async for chunk in resp.aiter_bytes():
                            size += len(chunk)
                            if size > MAX_THUMBNAIL_BYTES:
                                exceeded = True
                                break
                            chunks.append(chunk)
                        if exceeded or size < 16:
                            continue
                        content = b"".join(chunks)
                        _thumbnail_cache[app_id] = (content, content_type)
                        logger.info("Thumbnail found for app %s", entry.get("name"))
                        return _thumbnail_response(content, content_type, if_none_match)
                except (TypeError, ValueError):
                    continue
                except Exception as e:
                    logger.debug("Thumbnail fetch failed for app %s: %s", entry.get("name"), e)
                    continue

    logger.info("No custom thumbnail found for app %s (%s)", entry.get("name"), app_url)
    # A miss must stay a miss so the browser can render its deterministic,
    # identity-colored initials. Returning one generated blue image here made
    # every app without a platform thumbnail look identical.
    _thumbnail_cache[app_id] = (None, None)
    return _thumbnail_error(404)


# ── Connected artifacts ──────────────────────────────────────────────

@router.get("/connected-artifacts")
async def get_connected_artifacts() -> dict[str, Any]:
    """Get connected artifacts (serving endpoints, warehouses, etc.) for all apps."""
    try:
        registry, resources_by_app = await asyncio.wait_for(
            asyncio.to_thread(
                lambda: (_get_app_registry(), _get_app_resources())
            ),
            timeout=30.0,
        )
        stale = False
    except asyncio.TimeoutError:
        registry = dict(_app_name_cache)
        resources_by_app = {
            str(entry.get("name") or app_id): list(
                _app_details_cache.get(app_id, {}).get("resources") or []
            )
            for app_id, entry in registry.items()
        }
        stale = True

    artifacts: list[dict[str, Any]] = []
    for uid, entry in registry.items():
        app_name = entry["name"]
        app_resources = resources_by_app.get(app_name, [])
        for res in app_resources:
            artifacts.append({
                "app_id": uid,
                "app_name": app_name,
                "artifact_name": res["name"],
                "artifact_type": res["type"],
                "artifact_description": res["description"],
                "artifact_id": str(res.get("id", "")) or None,
            })

    return {
        "artifacts": artifacts,
        "count": len(artifacts),
        "stale": stale,
    }
