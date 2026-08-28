"""Databricks SQL connection factory."""

import hashlib
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import as_completed
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Callable, Generator

# Per-request user token set by UserAuthMiddleware when x-forwarded-access-token
# is present (Databricks Apps user authorization preview). Empty string = use SP.
_user_token: ContextVar[str] = ContextVar("_user_token", default="")
# Dashboard tab that owns SQL issued during the current HTTP request. The ASGI
# middleware sets this so selective cache clears can evict unhashed query results.
_request_cache_tag: ContextVar[str | None] = ContextVar("_request_cache_tag", default=None)

# Persisted auth-mode override file (written by POST /api/settings/auth-mode)
_AUTH_MODE_OVERRIDE_FILE = os.path.join(
    os.path.dirname(__file__), "..", ".settings", "auth_mode_override.json"
)


def _load_auth_mode_override() -> str:
    """Read the persisted auth mode preference. Returns 'sp' or 'unknown'."""
    try:
        if os.path.exists(_AUTH_MODE_OVERRIDE_FILE):
            with open(_AUTH_MODE_OVERRIDE_FILE) as f:
                data = json.load(f)
            if data.get("mode") == "sp":
                return "sp"
    except Exception:
        pass
    return "unknown"


# Auth mode is permanently locked to "sp". OAuth/user-token auth is preserved in
# the codebase for future use but is not active. The UserAuthMiddleware already
# gates on this value and will not forward user tokens when mode is "sp".
_auth_mode: str = "sp"  # always "sp" — OAuth disabled until further notice


def _lock_auth_mode(mode: str) -> None:
    """No-op — auth mode is permanently 'sp'. Kept for call-site compatibility."""
    if mode != "sp":
        logger.debug("_lock_auth_mode('%s') ignored — OAuth disabled, always SP.", mode)


def set_auth_mode_override(mode: str) -> None:
    """No-op stub — OAuth is disabled; auth mode is permanently 'sp'.

    Kept for call-site compatibility. The override file and Delta table writes
    are skipped. When OAuth is re-enabled in the future, restore the original
    implementation from git history.
    """
    if mode != "sp":
        logger.warning(
            "set_auth_mode_override('%s') ignored — OAuth is disabled. "
            "Auth mode is permanently locked to 'sp'.", mode
        )
    # Always keep _auth_mode as "sp" — no mutation needed

from cachetools import TTLCache
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    CreateWarehouseRequestWarehouseType,
    EndpointInfoWarehouseType,
    SpotInstancePolicy,
    State,
)

logger = logging.getLogger(__name__)


def get_host_url() -> str:
    """Return the Databricks workspace URL with https:// prefix.

    Handles the common case where DATABRICKS_HOST is set to just the hostname
    (e.g. '<workspace>.cloud.databricks.com') without a protocol prefix,
    as well as when the full URL is provided. Falls back to SDK config.
    """
    host = os.getenv("DATABRICKS_HOST", "")
    if not host:
        # Try SDK workspace client (works in Databricks Apps with OAuth)
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            host = w.config.host or ""
        except Exception:
            pass
    if not host:
        return ""
    host = host.rstrip("/")
    if not host.startswith("https://") and not host.startswith("http://"):
        host = f"https://{host}"
    return host


_CATALOG_OVERRIDE_FILE = os.path.join(
    os.path.dirname(__file__), "..", ".settings", "catalog_override.json"
)

# DBFS path for catalog/schema — survives git redeploys unlike .settings/.
# Written alongside the local file; read as fallback when local file is absent.
# Keyed per service-principal so that multiple independent deployments in the same
# workspace cannot read each other's stale catalog/schema from a shared path.
_SP_ID = os.getenv("DATABRICKS_CLIENT_ID", "").replace("-", "_")
_DBFS_OVERRIDE_PATH = (
    f"/databricks/cost-obs-app/{_SP_ID}/catalog_override.json"
    if _SP_ID else
    "/databricks/cost-obs-app/catalog_override.json"
)

# Locations that are forbidden as app storage targets. main.cost_obs is the old
# hardcoded default that shipped in app.yaml — it must never be auto-created.
_FORBIDDEN_STORAGE_LOCATIONS: frozenset[tuple[str, str]] = frozenset({
    ("main", "cost_obs"),
})

# Catalogs that are platform-owned or too broadly shared to be selected
# automatically. An administrator can still explicitly configure a dedicated
# schema in a non-system catalog; discovery is intentionally more conservative.
_RESERVED_DISCOVERY_CATALOGS: frozenset[str] = frozenset({
    "__databricks_internal",
    "hive_metastore",
    "main",
    "samples",
    "system",
})

# A schema is a cost-observability candidate only when it contains the core MV
# plus at least one app-owned configuration marker. Requiring both classes keeps
# an unrelated table with a common name from becoming a write target.
_DISCOVERY_CORE_MARKERS: frozenset[str] = frozenset({"daily_usage_summary"})
_DISCOVERY_APP_MARKERS: frozenset[str] = frozenset({
    "app_cloud_connections",
    "app_mv_refresh_state",
    "app_refresh_log",
    "app_schedule_settings",
    "app_settings",
    "app_user_permissions",
    "app_workspace_filter",
})

_catalog_discovery_lock = threading.Lock()
_catalog_discovery_cache: dict[str, Any] = {
    "catalog": "",
    "schema": "",
    "reason": None,
    "checked_at": 0.0,
}
_CATALOG_DISCOVERY_SUCCESS_TTL = 60 * 60
_CATALOG_DISCOVERY_FAILURE_TTL = 60
_catalog_write_safety_cache: dict[str, tuple[bool, str | None, float]] = {}
_CATALOG_WRITE_SAFETY_TTL = 5 * 60


class StorageConfigurationError(ValueError):
    """Catalog/schema config is invalid or resolves to a forbidden location."""
    pass


def validate_app_storage_target(catalog: str, schema: str) -> None:
    """Raise StorageConfigurationError if catalog/schema is unfit for app writes.

    Called by every code path that executes DDL or app-table writes so that a
    misconfigured deploy surfaces a clean error rather than silently touching
    forbidden locations.
    """
    if not catalog or not schema:
        raise StorageConfigurationError(
            "App storage location is not configured. "
            "Set COST_OBS_CATALOG and COST_OBS_SCHEMA in the Databricks Apps environment, "
            "then complete the setup wizard."
        )
    if (catalog.lower(), schema.lower()) in _FORBIDDEN_STORAGE_LOCATIONS:
        raise StorageConfigurationError(
            f"'{catalog}.{schema}' is a reserved default location and cannot be used as app storage. "
            "Set COST_OBS_CATALOG and COST_OBS_SCHEMA to a dedicated catalog and schema."
        )
    # Explicit/local/DBFS overrides predate safe auto-discovery and may point at
    # a Delta Sharing or foreign catalog. Verify the catalog type at write-path
    # boundaries so such stale configuration can never become a write target.
    cache_key = catalog.lower()
    cached = _catalog_write_safety_cache.get(cache_key)
    if cached and (time.monotonic() - cached[2]) < _CATALOG_WRITE_SAFETY_TTL:
        writable, reason, _ = cached
    else:
        writable, reason = True, None
        try:
            catalog_info = get_workspace_client().catalogs.get(catalog)
            catalog_type = _enum_value(getattr(catalog_info, "catalog_type", None))
            if (
                catalog_type in {
                    "DELTASHARING_CATALOG",
                    "FOREIGN_CATALOG",
                    "SYSTEM_CATALOG",
                }
                or getattr(catalog_info, "share_name", None)
                or getattr(catalog_info, "provider_name", None)
            ):
                writable = False
                reason = f"catalog type {catalog_type or 'shared/foreign'} is read-only"
            _catalog_write_safety_cache[cache_key] = (
                writable,
                reason,
                time.monotonic(),
            )
        except Exception as e:
            # Explicitly configured managed catalogs may not grant the Apps
            # identity catalog-metadata GET even though SQL writes are permitted.
            # Let the actual write enforce permissions, but never cache this
            # inconclusive lookup as safe.
            logger.debug("Could not verify catalog type for %s: %s", catalog, e)
    if not writable:
        raise StorageConfigurationError(
            f"'{catalog}.{schema}' cannot be used as app storage because {reason}. "
            "Choose an app-owned schema in a managed Unity Catalog catalog."
        )


def _read_dbfs_catalog_override() -> tuple[str, str]:
    """Read catalog/schema from DBFS. Best-effort — returns ("", "") on any error.

    On a successful read, also writes the value back to the local .settings/ file
    so subsequent calls within the same process use the fast local path instead of
    hitting the DBFS API on every request.
    """
    try:
        import base64
        w = get_workspace_client()
        resp = w.api_client.do(
            "GET", "/api/2.0/dbfs/read",
            query={"path": _DBFS_OVERRIDE_PATH, "length": 4096},
        )
        raw = resp.get("data", "")
        if not raw:
            return "", ""
        data = json.loads(base64.b64decode(raw).decode("utf-8"))
        cat = data.get("catalog", "").strip()
        sch = data.get("schema", "").strip()
        if cat and sch and (cat.lower(), sch.lower()) not in _FORBIDDEN_STORAGE_LOCATIONS:
            # Restore local file so this process doesn't hit DBFS on every call
            try:
                os.makedirs(os.path.dirname(_CATALOG_OVERRIDE_FILE), exist_ok=True)
                with open(_CATALOG_OVERRIDE_FILE, "w") as f:
                    json.dump({"catalog": cat, "schema": sch}, f)
                logger.info("Restored local catalog override from DBFS: %s.%s", cat, sch)
            except Exception:
                pass
            return cat, sch
    except Exception:
        pass
    return "", ""


def _enum_value(value: Any) -> str:
    """Return a stable upper-case value for SDK enum or string fields."""
    raw = getattr(value, "value", value)
    return str(raw or "").split(".")[-1].upper()


def _discover_app_storage_target() -> tuple[str, str, str | None]:
    """Discover one existing app-owned managed schema via Unity Catalog.

    Discovery is deliberately read-only and conservative:
    - only managed catalogs are considered;
    - platform/reserved/shared/foreign catalogs are rejected;
    - the schema owner must match the running app service principal;
    - the core cost MV and at least one app config marker must both exist; and
    - the core marker must itself be a managed table.

    Returns ``(catalog, schema, None)`` only for one unambiguous candidate.
    Otherwise the empty pair and a user-facing blocked reason are returned.
    """
    try:
        w = get_workspace_client()
        me = w.current_user.me()
    except Exception as e:
        return "", "", f"Could not inspect Unity Catalog as the app identity: {e}"

    identity_values = {
        str(value).strip().lower()
        for value in (
            os.getenv("DATABRICKS_CLIENT_ID", ""),
            getattr(me, "id", None),
            getattr(me, "user_name", None),
        )
        if value and str(value).strip()
    }
    if not identity_values:
        return "", "", "Could not determine the app service-principal identity."

    candidates: list[tuple[str, str]] = []
    try:
        catalogs = list(w.catalogs.list())
    except Exception as e:
        return "", "", f"Could not list Unity Catalog catalogs: {e}"

    for catalog_info in catalogs:
        catalog = str(getattr(catalog_info, "name", "") or "").strip()
        if not catalog or catalog.lower() in _RESERVED_DISCOVERY_CATALOGS:
            continue
        catalog_type = _enum_value(getattr(catalog_info, "catalog_type", None))
        # No type is treated as unknown and therefore unsafe. In particular,
        # DELTASHARING_CATALOG and FOREIGN_CATALOG must never become write targets.
        if catalog_type not in {"MANAGED_CATALOG"}:
            continue
        if getattr(catalog_info, "share_name", None) or getattr(catalog_info, "provider_name", None):
            continue

        try:
            schemas = list(w.schemas.list(catalog_name=catalog))
        except Exception as e:
            logger.debug("Storage discovery could not list schemas in %s: %s", catalog, e)
            continue

        for schema_info in schemas:
            schema = str(getattr(schema_info, "name", "") or "").strip()
            owner = str(getattr(schema_info, "owner", "") or "").strip().lower()
            if not schema or schema.lower() in {"default", "information_schema"}:
                continue
            if owner not in identity_values:
                continue
            schema_catalog_type = _enum_value(getattr(schema_info, "catalog_type", None))
            if schema_catalog_type and schema_catalog_type != "MANAGED_CATALOG":
                continue
            try:
                tables = list(w.tables.list(catalog_name=catalog, schema_name=schema))
            except Exception as e:
                logger.debug("Storage discovery could not list tables in %s.%s: %s", catalog, schema, e)
                continue

            by_name = {
                str(getattr(table, "name", "") or "").strip().lower(): table
                for table in tables
                if getattr(table, "name", None)
            }
            if not _DISCOVERY_CORE_MARKERS.issubset(by_name):
                continue
            if not (_DISCOVERY_APP_MARKERS & set(by_name)):
                continue
            core_type = _enum_value(getattr(by_name["daily_usage_summary"], "table_type", None))
            if core_type != "MANAGED":
                continue
            candidates.append((catalog, schema))

    if len(candidates) == 1:
        catalog, schema = candidates[0]
        return catalog, schema, None
    if not candidates:
        return (
            "",
            "",
            "No unambiguous app-owned managed schema with cost-observability marker tables was found.",
        )
    locations = ", ".join(f"{catalog}.{schema}" for catalog, schema in candidates)
    return (
        "",
        "",
        "Multiple app-owned managed schemas contain cost-observability marker tables; "
        f"configure the intended location explicitly. Candidates: {locations}",
    )


def _discover_app_storage_target_cached() -> tuple[str, str]:
    """Return a cached safe discovery result, persisting a unique match."""
    now = time.monotonic()
    cached_at = float(_catalog_discovery_cache.get("checked_at") or 0)
    has_result = bool(_catalog_discovery_cache.get("catalog"))
    ttl = _CATALOG_DISCOVERY_SUCCESS_TTL if has_result else _CATALOG_DISCOVERY_FAILURE_TTL
    if cached_at and (now - cached_at) < ttl:
        return (
            str(_catalog_discovery_cache.get("catalog") or ""),
            str(_catalog_discovery_cache.get("schema") or ""),
        )

    with _catalog_discovery_lock:
        now = time.monotonic()
        cached_at = float(_catalog_discovery_cache.get("checked_at") or 0)
        has_result = bool(_catalog_discovery_cache.get("catalog"))
        ttl = _CATALOG_DISCOVERY_SUCCESS_TTL if has_result else _CATALOG_DISCOVERY_FAILURE_TTL
        if cached_at and (now - cached_at) < ttl:
            return (
                str(_catalog_discovery_cache.get("catalog") or ""),
                str(_catalog_discovery_cache.get("schema") or ""),
            )

        catalog, schema, reason = _discover_app_storage_target()
        _catalog_discovery_cache.update(
            {"catalog": catalog, "schema": schema, "reason": reason, "checked_at": now}
        )
        if not catalog or not schema:
            logger.warning("App storage auto-discovery blocked: %s", reason)
            return "", ""

        logger.info("Discovered existing app-owned storage schema: %s.%s", catalog, schema)
        # The local file accelerates this process; DBFS is attempted for runtimes
        # where it is available. Both are best-effort because the discovered UC
        # schema remains the durable source from which a future deploy can recover.
        try:
            save_catalog_schema(catalog, schema)
        except Exception as e:
            logger.warning("Discovered storage target but could not persist override: %s", e)
        return catalog, schema


def get_catalog_schema_status() -> dict[str, Any]:
    """Expose the current storage resolution and any auto-discovery block reason."""
    catalog, schema = get_catalog_schema()
    return {
        "catalog": catalog,
        "schema": schema,
        "configured": bool(catalog and schema),
        "block_reason": None if catalog and schema else _catalog_discovery_cache.get("reason"),
    }


def _write_dbfs_catalog_override(catalog: str, schema: str) -> None:
    """Write catalog/schema to DBFS. Best-effort, non-fatal.

    Merges into the shared DBFS settings JSON so existing flags (setup_complete,
    build_state) are preserved.  A plain overwrite here would destroy setup_complete
    if called after the wizard completes (e.g. from POST /api/settings/catalog).
    """
    try:
        import base64
        existing = _read_dbfs_settings()
        existing["catalog"] = catalog
        existing["schema"] = schema
        w = get_workspace_client()
        content = base64.b64encode(json.dumps(existing).encode()).decode("ascii")
        w.api_client.do(
            "POST", "/api/2.0/dbfs/put",
            body={"path": _DBFS_OVERRIDE_PATH, "contents": content, "overwrite": True},
        )
        logger.info("DBFS catalog override saved: %s.%s", catalog, schema)
    except Exception as e:
        logger.warning("Could not write DBFS catalog override (non-fatal): %s", e)


def _read_dbfs_settings() -> dict:
    """Read the raw DBFS settings JSON. Returns {} on any error."""
    try:
        import base64
        w = get_workspace_client()
        resp = w.api_client.do("GET", "/api/2.0/dbfs/read",
                               query={"path": _DBFS_OVERRIDE_PATH, "length": 8192})
        raw = resp.get("data", "")
        if raw:
            return json.loads(base64.b64decode(raw).decode("utf-8"))
    except Exception:
        pass
    return {}


def write_dbfs_setup_complete() -> None:
    """Set setup_complete=True in the DBFS settings JSON. Best-effort, non-fatal.

    Called when the setup wizard finishes.  The flag survives container restarts
    (Databricks Apps recreates the container on every stop/start, wiping .settings/).
    """
    try:
        import base64
        existing = _read_dbfs_settings()
        existing["setup_complete"] = True
        w = get_workspace_client()
        content = base64.b64encode(json.dumps(existing).encode()).decode("ascii")
        w.api_client.do("POST", "/api/2.0/dbfs/put",
                        body={"path": _DBFS_OVERRIDE_PATH, "contents": content, "overwrite": True})
        logger.info("DBFS setup_complete flag written")
    except Exception as e:
        logger.warning("Could not write DBFS setup_complete flag (non-fatal): %s", e)


def read_dbfs_setup_complete() -> bool:
    """Return True if a previous wizard run set setup_complete in DBFS."""
    return bool(_read_dbfs_settings().get("setup_complete"))


def write_dbfs_build_state(state: dict) -> None:
    """Persist build task state to DBFS so it survives pod restarts. Best-effort, non-fatal.

    Merges into the shared DBFS settings JSON so a single file holds all
    durable app state (setup_complete, catalog override, build_state).
    """
    try:
        import base64
        existing = _read_dbfs_settings()
        existing["build_state"] = state
        w = get_workspace_client()
        content = base64.b64encode(json.dumps(existing).encode()).decode("ascii")
        w.api_client.do("POST", "/api/2.0/dbfs/put",
                        body={"path": _DBFS_OVERRIDE_PATH, "contents": content, "overwrite": True})
        logger.debug("DBFS build_state written (status=%s)", state.get("status"))
    except Exception as e:
        logger.debug("Could not write DBFS build_state (non-fatal): %s", e)


def read_dbfs_build_state() -> dict | None:
    """Return the last-persisted build task state from DBFS, or None if absent."""
    return _read_dbfs_settings().get("build_state") or None


def get_catalog_schema() -> tuple[str, str]:
    """Return the catalog and schema for cost observability tables.

    Priority:
      1. COST_OBS_CATALOG/COST_OBS_SCHEMA env vars (set in Databricks Apps UI — always win)
      2. .settings/catalog_override.json (local file written by wizard, wiped on git redeploy)
      3. DBFS /databricks/cost-obs-app/catalog_override.json (survives git redeploys)
      4. One unambiguous app-owned managed UC schema containing cost-obs markers

    Returns ("", "") when no location is configured (setup wizard not yet run).
    Never returns a forbidden location — logs CRITICAL and returns ("", "") instead.
    Use validate_app_storage_target() at write-path boundaries to raise explicitly.
    """
    # 1. Env vars are authoritative — set in the Databricks Apps UI
    catalog = os.getenv("COST_OBS_CATALOG", "").strip()
    schema = os.getenv("COST_OBS_SCHEMA", "").strip()
    if catalog and schema:
        if (catalog.lower(), schema.lower()) in _FORBIDDEN_STORAGE_LOCATIONS:
            logger.critical(
                "COST_OBS_CATALOG=%s and COST_OBS_SCHEMA=%s resolve to a forbidden default location. "
                "Update the app environment variables to a dedicated catalog and schema. "
                "Returning empty to prevent writes to a reserved location.",
                catalog, schema,
            )
            return "", ""
        return catalog, schema

    # 2. Local override file (written by wizard during setup)
    try:
        if os.path.exists(_CATALOG_OVERRIDE_FILE):
            with open(_CATALOG_OVERRIDE_FILE) as f:
                data = json.load(f)
            cat = data.get("catalog", "").strip()
            sch = data.get("schema", "").strip()
            if cat and sch:
                if (cat.lower(), sch.lower()) in _FORBIDDEN_STORAGE_LOCATIONS:
                    logger.warning(
                        "catalog_override.json contains forbidden location %s.%s — ignoring.",
                        cat, sch,
                    )
                else:
                    return cat, sch
    except Exception:
        pass

    # 3. DBFS fallback — survives git redeploys that wipe .settings/
    catalog, schema = _read_dbfs_catalog_override()
    if catalog and schema:
        return catalog, schema

    # 4. Last-resort safe discovery. This is intentionally conservative and
    # remains blocked when no candidate or more than one candidate is found.
    return _discover_app_storage_target_cached()


def save_catalog_schema(catalog: str, schema: str) -> None:
    """Persist a catalog/schema override (written from the Setup Wizard).

    Writes to both the local .settings/ file (fast) and DBFS (survives redeploys).
    """
    catalog = catalog.strip()
    schema = schema.strip()
    validate_app_storage_target(catalog, schema)
    os.makedirs(os.path.dirname(_CATALOG_OVERRIDE_FILE), exist_ok=True)
    with open(_CATALOG_OVERRIDE_FILE, "w") as f:
        json.dump({"catalog": catalog, "schema": schema}, f)
    logger.info("Catalog override saved locally: %s.%s", catalog, schema)
    _write_dbfs_catalog_override(catalog, schema)
    _catalog_discovery_cache.update(
        {
            "catalog": catalog,
            "schema": schema,
            "reason": None,
            "checked_at": time.monotonic(),
        }
    )


# ── MV table name overrides ─────────────────────────────────────────────────

_MV_OVERRIDES_FILE = os.path.join(
    os.path.dirname(__file__), "..", ".settings", "mv_table_overrides.json"
)


def get_mv_table_overrides() -> dict[str, str]:
    """Load per-table overrides mapping logical name → fully-qualified table path.

    Returns empty dict when no overrides are configured.
    """
    try:
        with open(_MV_OVERRIDES_FILE) as f:
            data = json.load(f)
        return {k: v for k, v in data.items() if isinstance(k, str) and isinstance(v, str) and v.strip()}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def save_mv_table_overrides(overrides: dict[str, str]) -> None:
    """Persist MV table overrides."""
    os.makedirs(os.path.dirname(_MV_OVERRIDES_FILE), exist_ok=True)
    with open(_MV_OVERRIDES_FILE, "w") as f:
        json.dump(overrides, f, indent=2)
    logger.info("MV table overrides saved: %s", list(overrides.keys()))


# ── Additional MV sources (union of Delta-shared / cross-workspace MVs) ───────
#
# Extra locations (typically Delta-shared in from another workspace) whose tables
# share the EXACT structure of this app's own MVs. When one or more sources are
# configured, every MV read is routed through a per-table `<name>__unified` view
# (built by materialized_views.rebuild_unified_views) that UNION ALLs the local
# table with each source's same-named table, tagging every row with a
# `source_label` column. This is additive — local data is always included.

_MV_SOURCES_FILE = os.path.join(
    os.path.dirname(__file__), "..", ".settings", "mv_sources.json"
)

# Suffix of the per-table union view. Public so the router/view-builder agree.
MV_UNIFIED_SUFFIX = "__unified"
MV_UNIFIED_TABLE_NAMES = (
    "daily_usage_summary",
    "daily_product_breakdown",
    "daily_workspace_breakdown",
    "sql_tool_attribution",
    "daily_query_stats",
    "dbsql_cost_per_query",
    "daily_tag_summary",
    "daily_apps_summary",
)

# Which MV tables currently have a built `<name>__unified` view. Written by
# rebuild_unified_views; read by apply_mv_overrides so it only ever remaps a table
# to a view that actually exists (never to a missing __unified view).
_MV_UNIFIED_TABLES_FILE = os.path.join(
    os.path.dirname(__file__), "..", ".settings", "mv_unified_views.json"
)


# Short-lived cache of the unified views that physically exist in the app schema.
# Routing (apply_mv_overrides) hits get_unified_view_tables per query, so we can't
# query information_schema every time — cache it briefly.
_unified_views_live: dict[str, Any] = {"tables": None, "checked_at": 0.0}
_UNIFIED_VIEWS_LIVE_TTL = 300  # 5 minutes


def _list_existing_unified_views(*, force_refresh: bool = False) -> list[str] | None:
    """The MV tables that ACTUALLY have a `<name>__unified` view in the app schema.

    This is the authoritative routing source: a view that exists is routable. It
    sidesteps the written registry, which drifts when `rebuild_unified_views` probes
    time out on a cold warehouse at startup (a table is wrongly deemed "missing",
    never registered, and routing silently falls back to the base table — which
    kills the MV source-label filter for that table). Cached ~5min. None on error.
    """
    now = time.time()
    if (
        not force_refresh
        and _unified_views_live["tables"] is not None
        and (now - _unified_views_live["checked_at"]) < _UNIFIED_VIEWS_LIVE_TTL
    ):
        return _unified_views_live["tables"]
    try:
        catalog, schema = get_catalog_schema()
        if not catalog or not schema:
            return None
        rows = execute_query(
            "SELECT table_name FROM system.information_schema.views "
            "WHERE table_catalog = :c AND table_schema = :s AND table_name LIKE :p",
            {"c": catalog, "s": schema, "p": f"%{MV_UNIFIED_SUFFIX}"},
            no_cache=True,
        )
        suffix_len = len(MV_UNIFIED_SUFFIX)
        names = [
            r["table_name"][:-suffix_len]
            for r in (rows or [])
            if str(r.get("table_name", "")).endswith(MV_UNIFIED_SUFFIX)
        ]
        _unified_views_live["tables"] = names
        _unified_views_live["checked_at"] = now
        return names
    except Exception as e:
        logger.debug("Could not list existing unified views (non-fatal): %s", e)
        return None


def get_unified_view_tables() -> list[str]:
    """MV table names that currently have a `<name>__unified` view to route through.

    Authoritative source is the views that PHYSICALLY EXIST in the app schema
    (`_list_existing_unified_views`, cached ~5min) — a view that exists is routable,
    so routing self-heals regardless of registry drift. Falls back to the local
    .settings file, then the durable Delta table, only if that lookup fails.
    """
    live = _list_existing_unified_views()
    if live is not None:
        return live
    try:
        with open(_MV_UNIFIED_TABLES_FILE) as f:
            data = json.load(f)
        if isinstance(data, list):
            return [t for t in data if isinstance(t, str)]
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    return read_delta_unified_view_tables()


def save_unified_view_tables(tables: list[str]) -> None:
    """Record which MV tables have a built unified view — local file (fast) + durable
    Delta table (cross-worker + survives redeploys)."""
    clean = [t for t in tables if isinstance(t, str)]
    # Views just changed — drop the live-existence cache so routing re-reads the
    # actual __unified views on the next query instead of a stale snapshot.
    _unified_views_live["tables"] = None
    _unified_views_live["checked_at"] = 0.0
    try:
        os.makedirs(os.path.dirname(_MV_UNIFIED_TABLES_FILE), exist_ok=True)
        tmp_path = f"{_MV_UNIFIED_TABLES_FILE}.{os.getpid()}.tmp"
        with open(tmp_path, "w") as f:
            json.dump(clean, f)
        os.replace(tmp_path, _MV_UNIFIED_TABLES_FILE)
    except OSError as e:
        logger.debug("Could not persist unified-view table list to file: %s", e)
    write_delta_unified_view_tables(clean)


def _unified_views_table() -> str:
    catalog, schema = get_catalog_schema()
    if not catalog or not schema:
        return ""
    return f"`{catalog}`.`{schema}`.`app_unified_views`"


def _ensure_unified_views_table() -> None:
    table = _unified_views_table()
    if not table:
        return
    execute_write(
        f"CREATE TABLE IF NOT EXISTS {table} "
        f"(table_name STRING NOT NULL, updated_at TIMESTAMP) USING DELTA",
        None,
    )


def read_delta_unified_view_tables() -> list[str]:
    """Built-unified-view table names from the durable Delta table. [] on any error."""
    try:
        table = _unified_views_table()
        if not table:
            return []
        _ensure_unified_views_table()
        rows = execute_query(f"SELECT table_name FROM {table}", None, no_cache=True)
        return [r["table_name"] for r in (rows or []) if r.get("table_name")]
    except Exception as e:
        logger.debug("Could not read unified-view list from Delta (non-fatal): %s", e)
        return []


def write_delta_unified_view_tables(tables: list[str]) -> None:
    """Persist the built-unified-view list to the durable Delta table, ATOMICALLY.

    Uses a single `INSERT OVERWRITE` (one statement) rather than a DELETE followed by
    per-row INSERTs. The old row-by-row loop could partially fail (a transient error /
    timeout mid-loop) and silently persist a TRUNCATED list — which is how
    `daily_usage_summary` dropped out of routing and the MV source-label filter went
    dead after a redeploy wiped the local cache. One statement can't half-apply.
    """
    try:
        table = _unified_views_table()
        if not table:
            return
        _ensure_unified_views_table()
        clean = [t for t in tables if isinstance(t, str) and t.strip()]
        if not clean:
            # No unified views → empty the table (single statement).
            execute_write(f"DELETE FROM {table}", None)
            return
        # Build one multi-row VALUES clause. Names are our own MV table identifiers,
        # but single-quote-escape defensively. INSERT OVERWRITE atomically replaces
        # all rows, so an interrupted write leaves the prior list intact rather than
        # a partial one.
        values = ", ".join(
            "('" + t.replace("'", "''") + "', current_timestamp())" for t in clean
        )
        execute_write(f"INSERT OVERWRITE {table} VALUES {values}", None)
    except Exception as e:
        logger.warning("Could not persist unified-view list to Delta (non-fatal): %s", e)


def _valid_mv_source(s: object) -> bool:
    return (
        isinstance(s, dict)
        and isinstance(s.get("label"), str) and bool(s["label"].strip())
        and isinstance(s.get("catalog"), str) and bool(s["catalog"].strip())
        and isinstance(s.get("schema"), str) and bool(s["schema"].strip())
    )


def get_mv_sources() -> list[dict]:
    """Additional MV source locations to union into MV reads.

    Each entry: {"label": str, "catalog": str, "schema": str, "tables"?: list[str]}.
    Read order: local .settings file (fast, present during a worker's lifetime), then
    a durable Delta table (survives git redeploys and works where DBFS is disabled —
    e.g. GCP), then DBFS (legacy). Never raises — returns [] on any error.
    """
    try:
        with open(_MV_SOURCES_FILE) as f:
            data = json.load(f)
        # A present file is authoritative for this worker — even when it's an empty
        # list (the user removed every source), so we don't resurrect stale entries.
        if isinstance(data, list):
            return [s for s in data if _valid_mv_source(s)]
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        pass
    # File missing (fresh redeploy wiped .settings) — restore from the durable stores.
    delta = read_delta_mv_sources()
    if delta:
        return [s for s in delta if _valid_mv_source(s)]
    dbfs = read_dbfs_mv_sources()
    return [s for s in dbfs if _valid_mv_source(s)] if dbfs else []


def save_mv_sources(sources: list[dict]) -> None:
    """Persist additional MV sources: local .settings file (fast) + durable Delta
    table (survives redeploys) + DBFS (legacy best-effort). Preserves the optional
    per-source `tables` selection from the Browse multiselect."""
    clean: list[dict] = []
    for s in sources:
        if not _valid_mv_source(s):
            continue
        entry = {"label": s["label"].strip(), "catalog": s["catalog"].strip(), "schema": s["schema"].strip()}
        if isinstance(s.get("tables"), list):
            picked = [str(t).strip() for t in s["tables"] if str(t).strip()]
            if picked:
                entry["tables"] = picked
        if s.get("cloud"):
            entry["cloud"] = str(s["cloud"]).strip().lower()
        if s.get("added_at"):
            entry["added_at"] = str(s["added_at"])
        clean.append(entry)
    os.makedirs(os.path.dirname(_MV_SOURCES_FILE), exist_ok=True)
    temp_path = f"{_MV_SOURCES_FILE}.{os.getpid()}.{threading.get_ident()}.tmp"
    with open(temp_path, "w") as f:
        json.dump(clean, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    # Prepare the local copy first, then land Delta atomically, and only publish
    # the prepared file after Delta succeeds. A failed durable write therefore
    # cannot become visible to another app worker as a successful source change.
    try:
        write_delta_mv_sources(clean)
    except Exception:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise
    os.replace(temp_path, _MV_SOURCES_FILE)
    write_dbfs_mv_sources(clean)
    logger.info("MV sources saved: %s", [s["label"] for s in clean])


def _mv_sources_table() -> str:
    """Fully-qualified Delta table for durable MV-source persistence, or '' when the
    storage location isn't configured yet."""
    catalog, schema = get_catalog_schema()
    if not catalog or not schema:
        return ""
    return f"`{catalog}`.`{schema}`.`app_mv_sources`"


def _ensure_mv_sources_table() -> None:
    """Create the durable MV-sources table if it doesn't exist, and add the cloud /
    added_at columns to a pre-existing table (migration for deployments created before
    those columns were introduced)."""
    table = _mv_sources_table()
    if not table:
        return
    execute_write(
        f"CREATE TABLE IF NOT EXISTS {table} "
        f"(label STRING NOT NULL, catalog STRING NOT NULL, schema STRING NOT NULL, "
        f"tables STRING, cloud STRING, added_at STRING, updated_at TIMESTAMP) USING DELTA",
        None,
    )
    # Best-effort column adds for tables created before cloud/added_at existed.
    for col in ("cloud STRING", "added_at STRING"):
        try:
            execute_write(f"ALTER TABLE {table} ADD COLUMNS ({col})", None)
        except Exception:
            pass  # column already present


def read_delta_mv_sources() -> list[dict]:
    """MV sources from the durable Delta table (survives redeploys). [] on any error."""
    try:
        table = _mv_sources_table()
        if not table:
            return []
        _ensure_mv_sources_table()
        rows = execute_query(f"SELECT label, catalog, schema, tables, cloud, added_at FROM {table}", None, no_cache=True)
        out: list[dict] = []
        for r in (rows or []):
            entry = {"label": r.get("label"), "catalog": r.get("catalog"), "schema": r.get("schema")}
            raw = r.get("tables")
            if raw:
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list) and parsed:
                        entry["tables"] = [str(x) for x in parsed]
                except (json.JSONDecodeError, TypeError):
                    pass
            if r.get("cloud"):
                entry["cloud"] = r["cloud"]
            if r.get("added_at"):
                entry["added_at"] = r["added_at"]
            out.append(entry)
        return out
    except Exception as e:
        logger.debug("Could not read mv_sources from Delta (non-fatal): %s", e)
        return []


def write_delta_mv_sources(sources: list[dict]) -> None:
    """Atomically replace all MV sources in the durable Delta table.

    `added_at` is carried on each source so its original add-time survives the
    replace-all rewrite; only genuinely new sources get a fresh timestamp (set by
    the caller)."""
    table = _mv_sources_table()
    if not table:
        return
    _ensure_mv_sources_table()
    if not sources:
        execute_write(
            f"INSERT OVERWRITE {table} "
            "SELECT CAST(NULL AS STRING) AS label, CAST(NULL AS STRING) AS catalog, "
            "CAST(NULL AS STRING) AS schema, CAST(NULL AS STRING) AS tables, "
            "CAST(NULL AS STRING) AS cloud, CAST(NULL AS STRING) AS added_at, "
            "CAST(NULL AS TIMESTAMP) AS updated_at WHERE FALSE",
            None,
        )
    else:
        params: dict[str, Any] = {}
        value_rows: list[str] = []
        for index, source in enumerate(sources):
            value_rows.append(
                f"(:label_{index}, :catalog_{index}, :schema_{index}, "
                f":tables_{index}, :cloud_{index}, :added_at_{index})"
            )
            params.update({
                f"label_{index}": source["label"],
                f"catalog_{index}": source["catalog"],
                f"schema_{index}": source["schema"],
                f"tables_{index}": json.dumps(source["tables"])
                if isinstance(source.get("tables"), list)
                else None,
                f"cloud_{index}": source.get("cloud"),
                f"added_at_{index}": source.get("added_at"),
            })
        execute_write(
            f"INSERT OVERWRITE {table} "
            "SELECT source.label, source.catalog, source.schema, source.tables, "
            "source.cloud, source.added_at, current_timestamp() AS updated_at "
            f"FROM VALUES {', '.join(value_rows)} "
            "AS source(label, catalog, schema, tables, cloud, added_at)",
            params,
        )
    logger.info("MV sources persisted to Delta table (%d source(s))", len(sources))


def write_dbfs_mv_sources(sources: list[dict]) -> None:
    """Persist MV sources to DBFS so they survive git redeploys. Best-effort."""
    try:
        import base64
        existing = _read_dbfs_settings()
        existing["mv_sources"] = sources
        w = get_workspace_client()
        content = base64.b64encode(json.dumps(existing).encode()).decode("ascii")
        w.api_client.do("POST", "/api/2.0/dbfs/put",
                        body={"path": _DBFS_OVERRIDE_PATH, "contents": content, "overwrite": True})
        logger.debug("DBFS mv_sources written (%d source(s))", len(sources))
    except Exception as e:
        logger.debug("Could not write DBFS mv_sources (non-fatal): %s", e)


def read_dbfs_mv_sources() -> list[dict]:
    """Return MV sources persisted to DBFS, or [] if absent."""
    val = _read_dbfs_settings().get("mv_sources")
    return val if isinstance(val, list) else []


def get_local_source_label() -> str:
    """Label for THIS workspace's own rows in the unified `source_label` column.

    Uses the workspace name from the host (matches the app's other naming), with
    the workspace id and a generic fallback so the column is never blank.
    """
    try:
        w = get_workspace_client()
        host = (w.config.host or "").replace("https://", "").replace("http://", "")
        name = host.split(".")[0] if host else ""
        if name:
            return name
    except Exception:
        pass
    wsid = os.getenv("DATABRICKS_WORKSPACE_ID", "").strip()
    return f"workspace {wsid}" if wsid else "This workspace"


# Request-scoped selection of MV source labels to filter reads by. Set per request
# by UserAuthMiddleware from the `source_labels` query param. Empty = no filter
# (all sources shown) — the default. Only meaningful when additional sources are
# configured, since `source_label` only exists on the `<name>__unified` views.
_source_labels: ContextVar[list[str]] = ContextVar("source_labels", default=[])


def set_source_labels(labels: list[str]) -> object:
    """Set the current request's source-label selection; returns a reset token."""
    return _source_labels.set(list(labels or []))


def reset_source_labels(token: object) -> None:
    try:
        _source_labels.reset(token)  # type: ignore[arg-type]
    except Exception:
        pass


def selected_source_labels() -> list[str]:
    """Non-blank source labels selected for the current request."""
    return [str(x) for x in _source_labels.get() if str(x).strip()]


def _mv_tables_referenced_by_template(mv_query: str) -> set[str]:
    """Managed-table names referenced by an unformatted MV SQL template."""
    placeholder_pattern = r"`\{catalog\}`\.`\{schema\}`\.`([^`]+)`"
    names = set(re.findall(placeholder_pattern, mv_query))
    return names.intersection(MV_UNIFIED_TABLE_NAMES)


def source_label_filter_clause(mv_query: str | None = None) -> str:
    """`` AND source_label IN ('a','b') `` for the current selection, else ''.

    Single-quote-escaped. Appended into the MV templates' existing `{ws_filter}`
    slot by the MV query builders, so it filters unified-view rows by source.
    When a template is supplied, the clause is emitted only if every referenced
    managed table has a physically verified unified view. ``apply_mv_overrides``
    then raises instead of executing an unfiltered base-table query when one is
    unavailable.
    """
    labels = selected_source_labels()
    if not labels:
        return ""
    if mv_query is not None:
        referenced = _mv_tables_referenced_by_template(mv_query)
        live = _list_existing_unified_views()
        explicit = get_mv_table_overrides()
        if (
            not referenced
            or live is None
            or not referenced.issubset(set(live))
            or any(table in explicit for table in referenced)
        ):
            return ""
    quoted = ", ".join("'" + x.replace("'", "''") + "'" for x in labels)
    return f" AND source_label IN ({quoted})"


def apply_mv_overrides(sql: str, catalog: str, schema: str) -> str:
    """Rewrite default MV table references in a SQL string, in two layers:

      1. Explicit per-table overrides (get_mv_table_overrides) — user-set paths.
      2. Additional-MV-source unions: when extra sources are configured, each MV
         table is read through its `<name>__unified` view (built by
         rebuild_unified_views), which UNION ALLs local + shared rows and tags
         each with a source_label column. Local data is always included.

    Never routes a table through its own unified view when that view does not
    apply here (the view DDL itself references base tables directly and is built
    without going through this function, so there is no recursion).
    """
    overrides = dict(get_mv_table_overrides())
    selected = selected_source_labels()
    # Selected-source routing is strict: verify physical views now rather than
    # trusting the normal five-minute discovery cache.
    live = _list_existing_unified_views(force_refresh=True) if selected else None
    if selected and live is None:
        raise RuntimeError(
            "Selected shared sources cannot be queried because unified-view "
            "existence could not be verified."
        )
    routable = live if selected else get_unified_view_tables()
    # Route reads only through unified views that were actually built (never to a
    # missing __unified view). Empty when no sources are configured.
    for t in routable:
        # Don't override a table the user already remapped explicitly.
        overrides.setdefault(t, f"`{catalog}`.`{schema}`.`{t}{MV_UNIFIED_SUFFIX}`")
    if selected:
        live_set = set(live or [])
        for table in MV_UNIFIED_TABLE_NAMES:
            quoted_base = f"`{catalog}`.`{schema}`.`{table}`"
            plain_base = f"{catalog}.{schema}.{table}"
            if quoted_base not in sql and plain_base not in sql:
                continue
            if table in get_mv_table_overrides():
                raise RuntimeError(
                    f"Selected shared sources cannot be applied to explicitly "
                    f"overridden managed table {table}."
                )
            if table not in live_set:
                raise RuntimeError(
                    f"Selected shared sources cannot be queried because unified "
                    f"view {table}{MV_UNIFIED_SUFFIX} does not physically exist."
                )
            if "source_label" not in sql.lower():
                raise RuntimeError(
                    f"Selected shared sources cannot be queried because the "
                    f"{table} query has no source-label filter."
                )
    if not overrides:
        return sql
    for logical_name, full_path in overrides.items():
        sql = sql.replace(f"`{catalog}`.`{schema}`.`{logical_name}`", full_path)
        sql = sql.replace(f"{catalog}.{schema}.{logical_name}", full_path)
    return sql


def get_catalog_schema_info() -> dict:
    """Return catalog/schema info for the settings read-only display."""
    try:
        if os.path.exists(_CATALOG_OVERRIDE_FILE):
            with open(_CATALOG_OVERRIDE_FILE) as f:
                data = json.load(f)
            cat = data.get("catalog", "").strip()
            sch = data.get("schema", "").strip()
            if cat and sch:
                return {"catalog": cat, "schema": sch, "source": "override"}
    except Exception:
        pass
    catalog = os.getenv("COST_OBS_CATALOG", "")
    schema = os.getenv("COST_OBS_SCHEMA", "")
    return {"catalog": catalog, "schema": schema, "source": "env"}


# Dedicated warehouse configuration
DEDICATED_WAREHOUSE_NAME = "Cost Observability App"
DEDICATED_WAREHOUSE_SIZE = "Large"  # Large for 14+ parallel queries
DEDICATED_WAREHOUSE_MIN_CLUSTERS = 1
DEDICATED_WAREHOUSE_MAX_CLUSTERS = 2
DEDICATED_WAREHOUSE_AUTO_STOP_MINS = 10

# Bounded TTL cache for query results (2 hour TTL, max 200 entries)
# Using cachetools.TTLCache to prevent unbounded memory growth
_CACHE_MAX_SIZE = 200  # Max number of cached queries
_CACHE_TTL = 2 * 60 * 60  # 2 hours
_query_cache: TTLCache = TTLCache(maxsize=_CACHE_MAX_SIZE, ttl=_CACHE_TTL)

# SQL connection timeout in seconds
# Set high to accommodate slow system table scans (system.query.history 30-day range)
_CONNECTION_TIMEOUT = 300


def clear_query_cache(pattern: str | None = None) -> int:
    """Clear the query cache.

    Args:
        pattern: Optional string pattern to match cache keys.
                 If provided, only clears matching entries.
                 If None, clears entire cache.

    Returns:
        Number of entries cleared
    """
    global _query_cache
    if pattern is None:
        count = len(_query_cache)
        _query_cache.clear()
        logger.info(f"Cleared entire query cache ({count} entries)")
        return count
    else:
        # Clear entries matching pattern
        keys_to_clear = [k for k in _query_cache.keys() if pattern in k]
        for key in keys_to_clear:
            _query_cache.pop(key, None)
        logger.info(f"Cleared {len(keys_to_clear)} cache entries matching '{pattern}'")
        return len(keys_to_clear)


# ── Delta shared response cache ───────────────────────────────────────────────
# Cross-worker cache for expensive bundle payloads.  The per-process TTLCache
# above is not shared across FastAPI workers, so every worker re-computes the
# same expensive bundle on cache miss.  Storing results in a small Delta table
# lets any worker's computation be reused by all others.
#
# TTL policy (set by callers):
#   account-wide bundles (no ws filter): 1800s (30 min)
#   workspace-scoped bundles:             600s (10 min)

_CREATE_RESPONSE_CACHE_TABLE = """
CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`app_response_cache` (
  cache_key    STRING NOT NULL,
  endpoint     STRING,
  payload_json STRING,
  computed_at  TIMESTAMP,
  expires_at   TIMESTAMP
)
CLUSTER BY (cache_key)
"""

_response_cache_table_ready: bool = False


def _ensure_response_cache_table() -> bool:
    """Create app_response_cache if it doesn't exist. Returns False on any error.

    Before setup completes (no catalog configured): returns False silently (debug log).
    After setup (catalog configured but create fails): logs at WARNING — config drift.
    """
    global _response_cache_table_ready
    if _response_cache_table_ready:
        return True
    try:
        cat, sch = get_catalog_schema()
        if not cat or not sch:
            logger.debug("Delta cache table skipped — catalog not configured yet")
            return False
        tok = _user_token.set("")  # always as SP — app-owned table
        try:
            execute_query(
                _CREATE_RESPONSE_CACHE_TABLE.format(catalog=cat, schema=sch),
                no_cache=True,
            )
        finally:
            _user_token.reset(tok)
        _response_cache_table_ready = True
        return True
    except Exception as e:
        cat, sch = get_catalog_schema()
        if cat and sch:
            logger.warning("Could not ensure response cache table (config drift?): %s", e)
        else:
            logger.debug("Could not ensure response cache table (pre-setup): %s", e)
        return False


def bundle_cache_key(endpoint: str, start_date: str, end_date: str, workspace_ids: list[str] | None) -> str:
    """Stable MD5 cache key for a bundle request.

    Includes the active source-label selection so the data-source filter actually
    changes the cached result — without it, every filter selection hashed to the
    same key and the cached combined (all-sources) bundle was served regardless.
    """
    ws_part = ",".join(sorted(workspace_ids)) if workspace_ids else ""
    labels = sorted(str(x) for x in _source_labels.get() if str(x).strip())
    src_part = ",".join(labels)
    raw = f"{endpoint}:{start_date}:{end_date}:{ws_part}:{src_part}"
    return hashlib.md5(raw.encode()).hexdigest()


# L1 in-process cache in front of the Delta SQL cache — avoids a warehouse
# round-trip on every bundle request when the payload is already warm.
_delta_l1: TTLCache = TTLCache(maxsize=50, ttl=300)
_delta_l1_endpoints: dict[str, str] = {}
_delta_l1_generations: dict[str, int] = {}

# A manual tab clear must fence out work that started before the clear, including
# work running in another uvicorn process.  The generation registry lives in
# /tmp, which is shared by all workers in the app container, and is protected by
# flock plus a process-local lock.  Holding the operation lock through a remote
# cache read/write gives clear a strict before/after ordering:
#   old write -> clear deletes it, or clear -> old write is rejected.
_CACHE_GENERATION_STATE_PATH = "/tmp/cost-obs-cache-generations.json"
_CACHE_GENERATION_LOCK_PATH = "/tmp/cost-obs-cache-generations.lock"
_cache_generation_thread_lock = threading.RLock()


@dataclass(frozen=True)
class CacheGeneration:
    """Snapshot captured when a cache-producing operation starts."""

    endpoint: str
    value: int


def _read_cache_generation_state() -> dict[str, Any]:
    try:
        with open(_CACHE_GENERATION_STATE_PATH) as f:
            state = json.load(f)
        if isinstance(state, dict):
            return {
                "sequence": int(state.get("sequence", 0)),
                "global": int(state.get("global", 0)),
                "prefixes": {
                    str(k): int(v)
                    for k, v in (state.get("prefixes") or {}).items()
                },
            }
    except (FileNotFoundError, json.JSONDecodeError, OSError, TypeError, ValueError):
        pass
    return {"sequence": 0, "global": 0, "prefixes": {}}


def _write_cache_generation_state(state: dict[str, Any]) -> None:
    temp_path = f"{_CACHE_GENERATION_STATE_PATH}.{os.getpid()}.{threading.get_ident()}.tmp"
    with open(temp_path, "w") as f:
        json.dump(state, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp_path, _CACHE_GENERATION_STATE_PATH)


@contextmanager
def _cache_generation_operation_lock() -> Generator[dict[str, Any], None, None]:
    """Cross-process lock for generation checks and cache mutations."""
    import fcntl

    with _cache_generation_thread_lock:
        with open(_CACHE_GENERATION_LOCK_PATH, "a+") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            try:
                yield _read_cache_generation_state()
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)


def _cache_generation_value(endpoint: str, state: dict[str, Any]) -> int:
    values = [int(state.get("global", 0))]
    values.extend(
        int(generation)
        for prefix, generation in (state.get("prefixes") or {}).items()
        if endpoint.startswith(prefix)
    )
    return max(values, default=0)


def capture_cache_generation(endpoint: str) -> CacheGeneration:
    """Capture the invalidation generation for an endpoint before computing it."""
    # The state file is atomically replaced, so request threads can snapshot it
    # without waiting behind a potentially slow remote cache write.
    state = _read_cache_generation_state()
    return CacheGeneration(endpoint=endpoint, value=_cache_generation_value(endpoint, state))


def _cache_generation_is_current(
    generation: CacheGeneration, state: dict[str, Any]
) -> bool:
    return generation.value == _cache_generation_value(generation.endpoint, state)


def delta_cache_get(key: str) -> dict | None:
    """Read a bundle payload from the Delta response cache. Returns None on miss/error.

    On the asyncio event loop: returns None immediately on L1 miss — never blocks.
    On daemon/sync threads: runs the SQL check with a 5-second timeout.
    Async handlers wanting a Delta read should call asyncio.to_thread(delta_cache_get, key).
    """
    if key in _delta_l1:
        endpoint = _delta_l1_endpoints.get(key)
        cached_generation = _delta_l1_generations.get(key)
        if endpoint is None or cached_generation is None:
            return _delta_l1[key]
        with _cache_generation_thread_lock:
            state = _read_cache_generation_state()
            if cached_generation == _cache_generation_value(endpoint, state):
                return _delta_l1[key]
            _delta_l1.pop(key, None)
            _delta_l1_endpoints.pop(key, None)
            _delta_l1_generations.pop(key, None)
    # Skip the blocking SQL check when called from the event loop thread.
    # The blocking _cfwait below would freeze the event loop for up to 5 seconds;
    # return None (cache miss) instead and let the handler recompute.
    try:
        import asyncio as _asyncio
        _asyncio.get_running_loop()
        return None  # on event loop — don't block
    except RuntimeError:
        pass  # not on event loop — safe to proceed with blocking SQL
    try:
        cat, sch = get_catalog_schema()
        if not cat or not sch:
            return None
        import threading, contextvars
        from concurrent.futures import Future, wait as _cfwait
        ctx = contextvars.copy_context()

        def _run():
            with _cache_generation_operation_lock() as state:
                tok = _user_token.set("")
                try:
                    rows = execute_query(
                        f"SELECT payload_json, endpoint FROM `{cat}`.`{sch}`.`app_response_cache` "
                        f"WHERE cache_key = :key AND expires_at > CURRENT_TIMESTAMP() LIMIT 1",
                        {"key": key},
                        no_cache=True,
                    )
                finally:
                    _user_token.reset(tok)
                endpoint = str(rows[0].get("endpoint") or "") if rows else ""
                generation = _cache_generation_value(endpoint, state)
                return rows, generation

        future: Future = Future()

        def _daemon_run(f=future, fn=ctx.run, inner=_run):
            try:
                f.set_result(fn(inner))
            except Exception as e:
                f.set_exception(e)

        threading.Thread(target=_daemon_run, daemon=True, name="sql-delta-cache-get").start()
        done, _ = _cfwait([future], timeout=5.0)
        if not done:
            logger.debug("Delta cache SQL read timed out after 5s — treating as cache miss")
            return None
        rows, generation = future.result()
        if rows and rows[0].get("payload_json"):
            import gzip, base64 as _b64
            result = json.loads(gzip.decompress(_b64.b64decode(rows[0]["payload_json"])).decode())
            _delta_l1[key] = result
            if rows[0].get("endpoint"):
                _delta_l1_endpoints[key] = str(rows[0]["endpoint"])
                _delta_l1_generations[key] = generation
            return result
    except Exception as e:
        logger.debug("Delta cache read failed (non-fatal): %s", e)
    return None


def delta_cache_put(
    key: str,
    endpoint: str,
    payload: dict,
    ttl_seconds: int = 600,
    generation: CacheGeneration | None = None,
) -> bool:
    """Write a bundle payload to the Delta response cache. Fails silently; one retry on conflict.

    L1 is updated synchronously and immediately. The Delta SQL write runs in a fire-and-forget
    daemon thread so the caller (async handler or daemon thread) is never blocked.
    """
    operation_generation = generation or capture_cache_generation(endpoint)
    if operation_generation.endpoint != endpoint:
        raise ValueError("Cache generation endpoint must match the cache write endpoint.")

    # Keep the request path non-blocking with respect to remote Delta writes.
    # The local lock still orders this worker's L1 update against a clear; the
    # daemon re-check below performs the authoritative cross-process fence.
    with _cache_generation_thread_lock:
        state = _read_cache_generation_state()
        if not _cache_generation_is_current(operation_generation, state):
            logger.info("Rejected stale cache write for %s after invalidation", endpoint)
            return False
        _delta_l1[key] = payload
        _delta_l1_endpoints[key] = endpoint
        _delta_l1_generations[key] = operation_generation.value
        for stale_key in set(_delta_l1_endpoints) - set(_delta_l1):
            _delta_l1_endpoints.pop(stale_key, None)
            _delta_l1_generations.pop(stale_key, None)

    def _write(
        _key=key,
        _endpoint=endpoint,
        _payload=payload,
        _ttl=ttl_seconds,
        _generation=operation_generation,
    ):
        with _cache_generation_operation_lock() as state:
            if not _cache_generation_is_current(_generation, state):
                logger.info("Rejected stale Delta cache write for %s after invalidation", _endpoint)
                return
            if not _ensure_response_cache_table():
                return
            cat, sch = get_catalog_schema()
            if not cat or not sch:
                return
            import gzip, base64 as _b64
            compressed = _b64.b64encode(gzip.compress(json.dumps(_payload).encode())).decode("ascii")
            merge_sql = f"""MERGE INTO `{cat}`.`{sch}`.`app_response_cache` AS tgt
                USING (SELECT
                    :key      AS cache_key,
                    :endpoint AS endpoint,
                    :payload  AS payload_json,
                    CURRENT_TIMESTAMP() AS computed_at,
                    TIMESTAMPADD(SECOND, {_ttl}, CURRENT_TIMESTAMP()) AS expires_at
                ) AS src
                ON tgt.cache_key = src.cache_key
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED BY TARGET THEN INSERT *"""
            merge_params = {"key": _key, "endpoint": _endpoint, "payload": compressed}
            for attempt in range(2):
                try:
                    tok = _user_token.set("")
                    try:
                        execute_query(
                            f"DELETE FROM `{cat}`.`{sch}`.`app_response_cache` WHERE expires_at < CURRENT_TIMESTAMP()",
                            no_cache=True,
                        )
                        execute_query(merge_sql, merge_params, no_cache=True)
                    finally:
                        _user_token.reset(tok)
                    return
                except Exception as e:
                    if attempt == 0:
                        logger.debug("Delta cache write conflict, retrying: %s", e)
                    else:
                        logger.debug("Delta cache write failed after retry (non-fatal): %s", e)

    threading.Thread(target=_write, daemon=True, name="delta-cache-put").start()
    return True


def delta_cache_invalidate(pattern: str | None = None) -> None:
    """Delete Delta cache entries, optionally filtered by endpoint prefix."""
    with _cache_generation_operation_lock() as state:
        sequence = int(state.get("sequence", 0)) + 1
        state["sequence"] = sequence
        if pattern:
            state.setdefault("prefixes", {})[pattern] = sequence
        else:
            state["global"] = sequence
            state["prefixes"] = {}
        _write_cache_generation_state(state)

        # Keep endpoint metadata beside the hashed L1 keys so a per-tab refresh
        # does not evict unrelated tabs from this worker's response cache.
        if pattern:
            matching_keys = [
                key for key, endpoint in _delta_l1_endpoints.items()
                if endpoint.startswith(pattern)
            ]
            for key in matching_keys:
                _delta_l1.pop(key, None)
                _delta_l1_endpoints.pop(key, None)
                _delta_l1_generations.pop(key, None)
        else:
            _delta_l1.clear()
            _delta_l1_endpoints.clear()
            _delta_l1_generations.clear()

        # Clear Delta while retaining the same operation lock. An older writer
        # either completed before this delete or will observe the new generation.
        try:
            cat, sch = get_catalog_schema()
            if not cat or not sch:
                return
            tok = _user_token.set("")
            try:
                if pattern:
                    execute_query(
                        f"DELETE FROM `{cat}`.`{sch}`.`app_response_cache` WHERE endpoint LIKE :pat",
                        {"pat": f"{pattern}%"},
                        no_cache=True,
                    )
                else:
                    execute_query(
                        f"DELETE FROM `{cat}`.`{sch}`.`app_response_cache`",
                        no_cache=True,
                    )
            finally:
                _user_token.reset(tok)
        except Exception as e:
            logger.debug("Delta cache invalidation failed (non-fatal): %s", e)

# Singleton WorkspaceClient instance
_workspace_client: WorkspaceClient | None = None
_workspace_client_lock = threading.Lock()

_account_client: Any | None = None
_account_client_lock = threading.Lock()


def get_workspace_client() -> WorkspaceClient:
    """Get or create a singleton WorkspaceClient instance.

    Double-checked locking ensures only one thread runs WorkspaceClient() init
    (prevents concurrent auth fetches on restart). Failures are NOT cached —
    the next caller acquires the lock and retries from scratch, which is correct
    for transient errors (a permanent error cache bricked all SDK usage on any
    single transient init failure).
    """
    global _workspace_client

    if _workspace_client is not None:
        return _workspace_client

    with _workspace_client_lock:
        if _workspace_client is not None:
            return _workspace_client
        token = os.getenv("DATABRICKS_TOKEN")
        host = os.getenv("DATABRICKS_HOST")
        if token and host:
            _workspace_client = WorkspaceClient(host=host, token=token)
        else:
            _workspace_client = WorkspaceClient()
        logger.info("Created WorkspaceClient singleton")

    return _workspace_client


def get_user_workspace_client() -> WorkspaceClient:
    """Get a WorkspaceClient using the current request's OAuth token if available.

    Falls back to the SP singleton when no user token is in context.
    Used for SDK calls (warehouse listing, etc.) that should run as the user
    rather than the SP so they respect the user's permissions.
    """
    user_token = _user_token.get()
    if user_token and _auth_mode != "sp":
        host = os.getenv("DATABRICKS_HOST", "")
        if host:
            # Databricks Apps injects DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET for
            # the SP, so any WorkspaceClient() call also picks them up from env.  When we
            # ALSO pass token=user_token the SDK sees two auth methods ("pat" + "oauth") and
            # raises "more than one authorization method configured".
            #
            # Setting auth_type="pat" is the SDK-supported escape hatch: _validate() returns
            # early when auth_type is set (line 668 of config.py), and init_auth() then uses
            # the PAT credential provider, ignoring the M2M OAuth env vars.
            return WorkspaceClient(host=host, token=user_token, auth_type="pat")
    return get_workspace_client()


def _account_console_host() -> str:
    """Derive the account-console host URL from the workspace host, per cloud.

    Account-level APIs (e.g. SCIM listing of account service principals) are served
    from a different host than the workspace, and Databricks Apps does not inject an
    env var for it. We map the workspace host's cloud to the documented account
    console URL. DATABRICKS_ACCOUNT_HOST overrides the derivation when set.
    """
    override = os.getenv("DATABRICKS_ACCOUNT_HOST", "").strip()
    if override:
        override = override.rstrip("/")
        return override if override.startswith("http") else f"https://{override}"
    ws = get_host_url()  # https://<workspace-host>
    if "azuredatabricks.net" in ws:
        return "https://accounts.azuredatabricks.net"
    if "gcp.databricks.com" in ws:
        return "https://accounts.gcp.databricks.com"
    return "https://accounts.cloud.databricks.com"  # AWS default


def _detect_account_id() -> str:
    """Return the Databricks account ID.

    DATABRICKS_ACCOUNT_ID env var wins; otherwise auto-detect from system.billing.usage.
    Returns "" if neither is available.
    """
    account_id = os.getenv("DATABRICKS_ACCOUNT_ID", "").strip()
    if account_id:
        return account_id
    try:
        rows = execute_query(
            "SELECT DISTINCT account_id FROM system.billing.usage "
            "WHERE usage_date >= CURRENT_DATE - 7 AND account_id IS NOT NULL LIMIT 1"
        )
        return str(rows[0].get("account_id", "")) if rows else ""
    except Exception as e:
        logger.debug("Could not auto-detect account_id from billing: %s", e)
        return ""


def get_account_client():
    """Get or create a singleton AccountClient, or None if account access can't be built.

    Reuses the app SP's OAuth M2M credentials (DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET,
    injected by the Databricks Apps runtime) against the account-console host. Requires the SP
    to hold account-level permissions (account admin). Best-effort by design: any failure
    returns None so callers degrade gracefully to workspace-scoped behavior. Failures are
    NOT cached here — retry throttling is the caller's responsibility (e.g. the SP-name cache
    fail TTL) — matching get_workspace_client's philosophy.
    """
    global _account_client

    if _account_client is not None:
        return _account_client

    with _account_client_lock:
        if _account_client is not None:
            return _account_client

        account_id = _detect_account_id()
        if not account_id:
            logger.info(
                "AccountClient unavailable: no account_id resolved "
                "(set DATABRICKS_ACCOUNT_ID or ensure system.billing.usage is readable)."
            )
            return None

        host = _account_console_host()
        client_id = os.getenv("DATABRICKS_CLIENT_ID", "")
        client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "")
        try:
            from databricks.sdk import AccountClient
            if client_id and client_secret:
                # Force oauth-m2m so the SDK ignores any ambient DATABRICKS_TOKEN and does
                # not raise "more than one authorization method configured".
                client = AccountClient(
                    host=host,
                    account_id=account_id,
                    client_id=client_id,
                    client_secret=client_secret,
                    auth_type="oauth-m2m",
                )
            else:
                # Local dev / non-Apps: let the SDK resolve ambient auth (config profile).
                client = AccountClient(host=host, account_id=account_id)
            _account_client = client  # only cache on successful construction
            logger.info("Created AccountClient singleton (account %s, host %s)", account_id, host)
        except Exception as e:
            logger.warning("AccountClient init failed (account %s, host %s): %s", account_id, host, e)
            return None

    return _account_client


def ensure_dedicated_warehouse() -> tuple[str, str]:
    """Ensure a dedicated serverless SQL warehouse exists for the app.

    Creates a Large serverless warehouse if one doesn't exist with the expected name.
    Returns the warehouse ID and HTTP path.

    Returns:
        Tuple of (warehouse_id, http_path)
    """
    w = get_workspace_client()

    # Check if dedicated warehouse already exists
    logger.info(f"Checking for dedicated warehouse: {DEDICATED_WAREHOUSE_NAME}")
    existing_warehouses = list(w.warehouses.list())

    for warehouse in existing_warehouses:
        if warehouse.name == DEDICATED_WAREHOUSE_NAME:
            warehouse_id = warehouse.id
            http_path = f"/sql/1.0/warehouses/{warehouse_id}"
            logger.info(f"Found existing dedicated warehouse: {warehouse_id} ({warehouse.cluster_size})")

            # Check if warehouse needs to be started
            if warehouse.state in [State.STOPPED, State.STOPPING]:
                logger.info(f"Starting warehouse {warehouse_id}...")
                w.warehouses.start(warehouse_id)

            # Check if it's undersized and warn
            size_order = ["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"]
            current_idx = size_order.index(warehouse.cluster_size) if warehouse.cluster_size in size_order else -1
            target_idx = size_order.index(DEDICATED_WAREHOUSE_SIZE) if DEDICATED_WAREHOUSE_SIZE in size_order else 4

            if current_idx < target_idx:
                logger.warning(
                    f"Dedicated warehouse is sized {warehouse.cluster_size}, "
                    f"but {DEDICATED_WAREHOUSE_SIZE} is recommended. Consider resizing for better performance."
                )

            return warehouse_id, http_path

    # Create new dedicated warehouse
    logger.info(f"Creating dedicated serverless warehouse: {DEDICATED_WAREHOUSE_NAME} ({DEDICATED_WAREHOUSE_SIZE})")

    try:
        warehouse = w.warehouses.create(
            name=DEDICATED_WAREHOUSE_NAME,
            cluster_size=DEDICATED_WAREHOUSE_SIZE,
            warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
            enable_serverless_compute=True,
            min_num_clusters=DEDICATED_WAREHOUSE_MIN_CLUSTERS,
            max_num_clusters=DEDICATED_WAREHOUSE_MAX_CLUSTERS,
            auto_stop_mins=DEDICATED_WAREHOUSE_AUTO_STOP_MINS,
            spot_instance_policy=SpotInstancePolicy.COST_OPTIMIZED,
        )

        warehouse_id = warehouse.id
        http_path = f"/sql/1.0/warehouses/{warehouse_id}"

        logger.info("=" * 60)
        logger.info("Created Dedicated SQL Warehouse")
        logger.info("=" * 60)
        logger.info(f"  Name: {DEDICATED_WAREHOUSE_NAME}")
        logger.info(f"  ID: {warehouse_id}")
        logger.info(f"  Size: {DEDICATED_WAREHOUSE_SIZE}")
        logger.info(f"  Type: Serverless")
        logger.info(f"  Min Clusters: {DEDICATED_WAREHOUSE_MIN_CLUSTERS}")
        logger.info(f"  Max Clusters: {DEDICATED_WAREHOUSE_MAX_CLUSTERS}")
        logger.info(f"  Auto-Stop: {DEDICATED_WAREHOUSE_AUTO_STOP_MINS} minutes")
        logger.info("=" * 60)

        return warehouse_id, http_path

    except Exception as e:
        logger.error(f"Failed to create dedicated warehouse: {e}")
        raise


def setup_warehouse_connection() -> str:
    """Set up the warehouse connection for the app.

    Priority:
    1. DATABRICKS_HTTP_PATH env var (explicit path set in app.yaml)
    2. DATABRICKS_WAREHOUSE_ID env var — injected by Databricks Apps when a
       sql_warehouse resource is declared with valueFrom in app.yaml

    The warehouse is fixed at deploy time and cannot be changed via the settings
    UI. To switch warehouses, update the app resource binding and redeploy.

    Returns:
        The HTTP path being used
    """
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")

    if http_path and http_path.lower() != "auto":
        logger.info(f"Using configured warehouse: {http_path}")
        return http_path

    # Databricks Apps sql_warehouse resource binding
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
    if warehouse_id:
        http_path = f"/sql/1.0/warehouses/{warehouse_id}"
        os.environ["DATABRICKS_HTTP_PATH"] = http_path
        logger.info(f"Using warehouse from DATABRICKS_WAREHOUSE_ID resource: {http_path}")
        return http_path

    raise ValueError(
        "No SQL warehouse configured. Set DATABRICKS_HTTP_PATH to an explicit warehouse path "
        "(e.g. /sql/1.0/warehouses/<id>) in app.yaml, or add a sql_warehouse resource binding "
        "so DATABRICKS_WAREHOUSE_ID is injected automatically."
    )


def _get_cache_key(query: str, params: dict[str, Any] | None, *, tag: str | None = None) -> str:
    """Generate a cache key from query and params.

    When running under user authorization, the token hash is included so each
    user's results are cached independently (respects row/column-level security).

    Args:
        tag: Optional prefix for pattern-based cache invalidation (e.g. "use_case").
    """
    key_data = query + json.dumps(params or {}, sort_keys=True)
    token = _user_token.get()
    if token:
        # Use first 16 chars of token hash — enough to distinguish users without
        # exposing the token itself in log output or cache inspection.
        token_prefix = hashlib.md5(token.encode()).hexdigest()[:16]
        key_data = token_prefix + ":" + key_data
    hash_key = hashlib.md5(key_data.encode()).hexdigest()
    return f"{tag}:{hash_key}" if tag else hash_key


def _strip_host_scheme(host: str) -> str:
    """Strip https:// or http:// from a hostname."""
    if host.startswith("https://"):
        return host[8:]
    elif host.startswith("http://"):
        return host[7:]
    return host


def _is_scope_error(exc: Exception) -> bool:
    """Return True if exception indicates the token lacks the 'sql' OAuth scope."""
    msg = str(exc).lower()
    return (
        "required scopes" in msg
        or "does not have required scopes" in msg
        # Databricks SQL HTTP connector returns this when the bearer token
        # lacks the 'sql' OAuth scope — not a network error, a scope rejection.
        or "error during request to server" in msg
    )


def _is_permission_error(exc: Exception) -> bool:
    """Return True if exception indicates the user token lacks table/schema privileges."""
    msg = str(exc).lower()
    return any(s in msg for s in (
        "permission_denied", "insufficient_privileges", "not authorized",
        "user does not have", "does not have privilege",
    ))


@contextmanager
def get_connection() -> Generator[Any, None, None]:
    """Get a Databricks SQL connection as a context manager.

    Auth priority:
    1. Per-request user token from x-forwarded-access-token (user authorization
       preview — set by UserAuthMiddleware when the feature is enabled).
    2. DATABRICKS_TOKEN env var (local dev with explicit PAT/token).
    3. SP OAuth via WorkspaceClient (standard Databricks Apps SP identity).
    """
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")

    if not http_path:
        # Startup warehouse resolution may have failed — try again lazily.
        # This handles cases where DATABRICKS_WAREHOUSE_ID is injected by the
        # Apps resource binding but setup_warehouse_connection() failed at boot.
        try:
            http_path = setup_warehouse_connection()
        except Exception as e:
            raise ValueError(
                "SQL warehouse not configured. "
                "Add a SQL warehouse resource (key: sql-warehouse) in the Databricks Apps UI, "
                "or set DATABRICKS_HTTP_PATH explicitly in app.yaml."
            ) from e

    # 1. User authorization (Databricks Apps preview feature)
    user_token = _user_token.get()
    if user_token:
        host = os.getenv("DATABRICKS_HOST", "")
        if not host:
            w = get_workspace_client()
            host = w.config.host or ""
        conn = sql.connect(
            server_hostname=_strip_host_scheme(host),
            http_path=http_path,
            access_token=user_token,
            _socket_timeout=_CONNECTION_TIMEOUT,
        )
        try:
            yield conn
        finally:
            conn.close()
        return

    dev_token = os.getenv("DATABRICKS_TOKEN")
    dev_host = os.getenv("DATABRICKS_HOST")

    if dev_token and dev_host:
        # 2. Local development with explicit credentials
        conn = sql.connect(
            server_hostname=_strip_host_scheme(dev_host),
            http_path=http_path,
            access_token=dev_token,
            _socket_timeout=_CONNECTION_TIMEOUT,
        )
    else:
        # 3. Databricks App environment — use SP OAuth token from SDK
        w = get_workspace_client()
        config = w.config
        server_hostname = _strip_host_scheme(config.host)

        # config.authenticate() returns {"Authorization": "Bearer <token>"}
        headers = config.authenticate()
        access_token = headers.get("Authorization", "").replace("Bearer ", "")
        if not access_token:
            raise ValueError("Failed to get OAuth token from WorkspaceClient")

        conn = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            _socket_timeout=_CONNECTION_TIMEOUT,
        )

    try:
        yield conn
    finally:
        conn.close()


def execute_write(query: str, params: dict[str, Any] | None = None) -> int:
    """Execute a write operation (INSERT/UPDATE/DELETE) and return affected rows.

    Does not cache results as these are write operations.
    Delta tables auto-commit every DML statement; explicit commit() is not needed
    and may raise NotSupportedError on some connector versions.
    """
    start_time = time.time()

    def _run(force_sp: bool = False) -> int:
        ctx_tok = _user_token.set("") if force_sp else None
        try:
            with get_connection() as conn:
                with conn.cursor() as cursor:
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    return cursor.rowcount if cursor.rowcount is not None else 0
        finally:
            if ctx_tok is not None:
                _user_token.reset(ctx_tok)

    try:
        affected_rows = _run()
        if _user_token.get() and _auth_mode == "unknown":
            _lock_auth_mode("user")
    except Exception as exc:
        if _is_scope_error(exc) and _user_token.get():
            _lock_auth_mode("sp")
            affected_rows = _run(force_sp=True)
        elif _is_permission_error(exc) and _user_token.get():
            logger.warning(f"User token permission denied on write, retrying as SP: {exc}")
            affected_rows = _run(force_sp=True)
        else:
            raise

    elapsed = time.time() - start_time
    _sql_tag = " ".join(query.split())[:60]
    logger.info(f"Write query executed in {elapsed:.2f}s ({affected_rows} rows affected) [{_sql_tag}]")
    return affected_rows


def execute_query(query: str, params: dict[str, Any] | None = None, *, cache_tag: str | None = None, no_cache: bool = False) -> list[dict[str, Any]]:
    """Execute a SQL query and return results as a list of dicts.

    Results are cached for 10 minutes to reduce load on Databricks.

    Args:
        cache_tag: Optional tag for pattern-based cache invalidation (e.g. "use_case").
        no_cache: If True, skip cache read/write entirely (use for security-sensitive queries).
    """
    start_time = time.time()

    # Check cache first (TTLCache handles expiration automatically)
    if not no_cache:
        effective_cache_tag = cache_tag or _request_cache_tag.get()
        cache_key = _get_cache_key(query, params, tag=effective_cache_tag)
        if cache_key in _query_cache:
            logger.info(f"Cache hit - returned in {(time.time() - start_time)*1000:.0f}ms")
            return _query_cache[cache_key]

    def _run(force_sp: bool = False) -> list[dict[str, Any]]:
        """Execute the query. force_sp=True forces SP identity for this call."""
        ctx_tok = _user_token.set("") if force_sp else None
        try:
            with get_connection() as conn:
                with conn.cursor() as cursor:
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    if cursor.description is not None:
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()
                        return [dict(zip(columns, row)) for row in rows]
                    return []
        finally:
            if ctx_tok is not None:
                _user_token.reset(ctx_tok)

    # Execute query — detect and lock auth mode on first use.
    try:
        result = _run()
        # Lock to user mode on first successful user-token query
        if _user_token.get() and _auth_mode == "unknown":
            _lock_auth_mode("user")
    except Exception as exc:
        if _is_scope_error(exc) and _user_token.get():
            # Token present but lacks sql scope — lock to SP for all future requests
            _lock_auth_mode("sp")
            result = _run(force_sp=True)
        elif _is_permission_error(exc) and _user_token.get():
            # Token has sql scope but user lacks table privileges — retry as SP.
            # Don't lock permanently: this may be table-specific and the admin
            # can resolve it by setting Force SP in Settings → Permissions.
            logger.warning(f"User token permission denied, retrying as SP: {exc}")
            result = _run(force_sp=True)
        else:
            raise

    # Cache the result (TTLCache handles expiration automatically)
    if not no_cache:
        effective_cache_tag = cache_tag or _request_cache_tag.get()
        cache_key = _get_cache_key(query, params, tag=effective_cache_tag)
        _query_cache[cache_key] = result
    elapsed = time.time() - start_time
    _sql_tag = " ".join(query.split())[:60]
    logger.info(f"Query [{_sql_tag}] executed in {elapsed:.2f}s ({len(result)} rows)")
    return result


def get_auth_status() -> dict:
    """Return current auth mode for the settings UI auth indicator.

    Reads the in-process auth state without touching the database.
    """
    token = _user_token.get()
    locked_to_sp = _auth_mode == "sp"
    token_present = bool(token)
    user_token_active = token_present and not locked_to_sp

    if user_token_active:
        identity = "user_oauth"
    else:
        identity = "service_principal"

    # Attempt to decode JWT claims (no verification — informational only)
    has_sql_scope: bool | None = None
    user_email: str | None = None
    token_scopes: list[str] = []
    if token:
        try:
            import base64
            payload_b64 = token.split(".")[1]
            padded = payload_b64 + "=" * (-len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(padded))
            scp = payload.get("scp", payload.get("scope", ""))
            token_scopes = scp.split() if isinstance(scp, str) else list(scp)
            has_sql_scope = "sql" in token_scopes
            user_email = payload.get("upn") or payload.get("email") or payload.get("preferred_username") or None
        except Exception:
            pass

    # Check whether a manual override is saved on disk
    override_mode: str | None = None
    try:
        if os.path.exists(_AUTH_MODE_OVERRIDE_FILE):
            with open(_AUTH_MODE_OVERRIDE_FILE) as f:
                override_mode = json.load(f).get("mode")
    except Exception:
        pass

    return {
        # Simplified fields used by the header badge
        "user_token_active": user_token_active,
        "identity": identity,
        "locked_to_sp": locked_to_sp,
        "has_sql_scope": has_sql_scope,
        # Richer fields for the Permissions settings panel
        "auth_mode": _auth_mode,          # "unknown" | "user" | "sp"
        "token_present": token_present,   # OAuth header received from Databricks Apps
        "token_scopes": token_scopes,     # scopes decoded from the JWT
        "user_email": user_email,         # email from JWT claims
        "override_mode": override_mode,   # "sp" | "auto" | None (manual override on disk)
    }


def execute_queries_parallel(
    query_funcs: list[tuple[str, Callable[[], list[dict[str, Any]]]]],
    timeout: float | None = 30.0,
) -> dict[str, list[dict[str, Any]] | None]:
    """Execute multiple queries in parallel using daemon threads.

    Daemon threads are automatically killed when the process exits, so the app
    can shut down cleanly on SIGTERM without waiting for slow SQL connections
    (which have a 300-second socket timeout) to finish.

    Args:
        query_funcs: List of (name, lambda) tuples where lambda executes the query
        timeout: Optional wall-clock timeout in seconds. Queries not finished by
                 deadline return None; their daemon threads are killed on process exit.

    Returns:
        Dictionary mapping query names to results
    """
    import contextvars
    import threading
    from concurrent.futures import Future, wait as _cfwait, ALL_COMPLETED

    start_time = time.time()
    results: dict[str, list[dict[str, Any]] | None] = {}

    def _timed(name: str, fn: Callable[[], list[dict[str, Any]]]) -> Callable[[], list[dict[str, Any]]]:
        def wrapped() -> list[dict[str, Any]]:
            t0 = time.time()
            result = fn()
            rows = len(result) if isinstance(result, list) else -1
            logger.info("✓ %s: %.2fs rows=%d", name, time.time() - t0, rows)
            return result
        return wrapped

    _EXPECTED_CODES = (
        "[INSUFFICIENT_PERMISSIONS]",
        "[UNRESOLVED_COLUMN",
        "[TABLE_OR_VIEW_NOT_FOUND]",
        "[SCHEMA_NOT_FOUND]",
    )

    def _collect(future: Future, name: str) -> None:
        try:
            results[name] = future.result()
        except Exception as e:
            if any(code in str(e) for code in _EXPECTED_CODES):
                logger.warning("✗ %s failed (non-fatal): %s", name, e)
            else:
                logger.error("✗ %s failed: %s", name, e)
            results[name] = None

    futures_map: dict[Future, str] = {}
    for name, func in query_funcs:
        future: Future = Future()
        futures_map[future] = name

        # Snapshot the CURRENT (request) context per query and run the worker inside
        # it. Raw threads don't inherit ContextVars, so without this the workers see
        # the DEFAULTS for _source_labels (→ the MV source-label filter silently does
        # nothing on every bundle endpoint) and _user_token (→ user-auth reads fall
        # back to the SP). copy_context() is called here in the request thread so each
        # worker gets an independent, correctly-populated snapshot.
        _ctx = contextvars.copy_context()

        def _run(f=future, fn=_timed(name, func), ctx=_ctx):
            try:
                f.set_result(ctx.run(fn))
            except Exception as e:
                f.set_exception(e)

        threading.Thread(target=_run, daemon=True, name=f"sql-{name}").start()

    if timeout is not None:
        done, not_done = _cfwait(list(futures_map.keys()), timeout=timeout, return_when=ALL_COMPLETED)
        for future in not_done:
            elapsed = time.time() - start_time
            logger.error("✗ %s timed out after %.1fs (daemon thread will be killed on exit)", futures_map[future], elapsed)
            results[futures_map[future]] = None
        for future in done:
            _collect(future, futures_map[future])
    else:
        for future in as_completed(futures_map):
            _collect(future, futures_map[future])

    total_elapsed = time.time() - start_time
    logger.info("Parallel execution: %.2fs total (%d/%d queries completed)", total_elapsed, len(results), len(query_funcs))

    return results
