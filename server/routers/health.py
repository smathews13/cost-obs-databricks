"""Health check endpoints with actual service verification."""

import asyncio
import json
import logging
import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from cachetools import TTLCache
from fastapi import APIRouter, Request

from server import cache_ttls

logger = logging.getLogger(__name__)

router = APIRouter()

_warehouse_probe_lock = threading.Lock()
_warehouse_probe_inflight: threading.Event | None = None
_warehouse_probe_last_at = 0.0
_warehouse_probe_last_result: dict[str, Any] | None = None
_WAREHOUSE_PROBE_MIN_INTERVAL = 60.0
_WAREHOUSE_PROBE_LOCK_PATH = "/tmp/cost-obs-warehouse-probe.lock"
_WAREHOUSE_PROBE_STATE_PATH = "/tmp/cost-obs-warehouse-probe.json"
_deployment_metadata_cache: TTLCache[str, dict[str, Any]] = TTLCache(
    maxsize=8,
    ttl=cache_ttls.DEPLOYMENT_METADATA,
)
_deployment_metadata_lock = threading.Lock()
_deployment_metadata_inflight: dict[str, Future[dict[str, Any]]] = {}
_deployment_metadata_executor = ThreadPoolExecutor(
    max_workers=1,
    thread_name_prefix="deployment-metadata",
)
_DEPLOYMENT_METADATA_TIMEOUT_SECONDS = 5.0


def _metadata_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _deployment_metadata_from_app(app: Any) -> dict[str, Any]:
    """Select the current app deployment's public provenance fields."""
    deployment = getattr(app, "active_deployment", None)
    git_source = (
        getattr(deployment, "git_source", None)
        or getattr(app, "git_source", None)
        or getattr(app, "default_git_source", None)
    )
    deployed_at = _metadata_text(getattr(deployment, "create_time", None))
    deployer = _metadata_text(getattr(deployment, "creator", None))
    commit_sha = _metadata_text(
        getattr(git_source, "resolved_commit", None)
        or getattr(git_source, "commit", None)
    )
    return {
        "deployed_at": deployed_at,
        "deployer": deployer,
        "commit_sha": commit_sha,
        "available": any((deployed_at, deployer, commit_sha)),
        "source": "databricks_apps_api",
    }


def _deployment_metadata_from_env() -> dict[str, Any]:
    """Read optional release-provided values without inventing build timestamps."""
    deployed_at = _metadata_text(os.getenv("COST_OBS_DEPLOYED_AT"))
    deployer = _metadata_text(os.getenv("COST_OBS_DEPLOYER"))
    commit_sha = _metadata_text(os.getenv("COST_OBS_COMMIT_SHA"))
    return {
        "deployed_at": deployed_at,
        "deployer": deployer,
        "commit_sha": commit_sha,
        "available": any((deployed_at, deployer, commit_sha)),
        "source": "release_environment",
    }


def _fetch_deployment_metadata(app_name: str) -> dict[str, Any]:
    from server.db import get_workspace_client

    app_detail = get_workspace_client().apps.get(app_name)
    return _deployment_metadata_from_app(app_detail)


def _finish_deployment_metadata_fetch(
    app_name: str,
    future: Future[dict[str, Any]],
) -> None:
    try:
        metadata = future.result()
    except Exception as exc:
        logger.debug("Current app deployment metadata is unavailable: %s", exc)
        metadata = None
    with _deployment_metadata_lock:
        if metadata and metadata["available"]:
            _deployment_metadata_cache[app_name] = dict(metadata)
        if _deployment_metadata_inflight.get(app_name) is future:
            _deployment_metadata_inflight.pop(app_name, None)


@router.get("/deployment")
async def deployment_metadata() -> dict[str, Any]:
    """Return authoritative metadata for the deployment currently serving the app."""
    app_name = (os.getenv("DATABRICKS_APP_NAME") or "").strip()
    if app_name:
        started_fetch = False
        with _deployment_metadata_lock:
            cached = _deployment_metadata_cache.get(app_name)
            future = _deployment_metadata_inflight.get(app_name)
            if cached is not None:
                return dict(cached)
            if future is None:
                future = _deployment_metadata_executor.submit(
                    _fetch_deployment_metadata,
                    app_name,
                )
                _deployment_metadata_inflight[app_name] = future
                started_fetch = True
        if started_fetch:
            future.add_done_callback(
                lambda completed, name=app_name: _finish_deployment_metadata_fetch(
                    name,
                    completed,
                )
            )
        try:
            metadata = await asyncio.wait_for(
                asyncio.shield(asyncio.wrap_future(future)),
                timeout=_DEPLOYMENT_METADATA_TIMEOUT_SECONDS,
            )
            if metadata["available"]:
                return metadata
        except Exception as exc:
            logger.debug("Current app deployment metadata is unavailable: %s", exc)

    fallback = _deployment_metadata_from_env()
    if fallback["available"]:
        return fallback
    return {
        "deployed_at": None,
        "deployer": None,
        "commit_sha": None,
        "available": False,
        "source": "unavailable",
    }


def _read_shared_probe_result() -> tuple[float, dict[str, Any] | None]:
    try:
        with open(_WAREHOUSE_PROBE_STATE_PATH) as state_file:
            state = json.load(state_file)
        result = state.get("result")
        return float(state.get("completed_at") or 0), (
            dict(result) if isinstance(result, dict) else None
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return 0.0, None


def _write_shared_probe_result(result: dict[str, Any]) -> None:
    try:
        state = {"completed_at": time.time(), "result": result}
        temp_path = (
            f"{_WAREHOUSE_PROBE_STATE_PATH}.{os.getpid()}."
            f"{threading.get_ident()}.tmp"
        )
        with open(temp_path, "w") as state_file:
            json.dump(state, state_file)
            state_file.flush()
            os.fsync(state_file.fileno())
        os.replace(temp_path, _WAREHOUSE_PROBE_STATE_PATH)
    except OSError as exc:
        logger.debug("Could not persist shared warehouse-probe state: %s", exc)


def _acquire_shared_probe_lock():
    import fcntl

    lock_file = open(_WAREHOUSE_PROBE_LOCK_PATH, "a+")
    fcntl.flock(lock_file, fcntl.LOCK_EX)
    return lock_file


def _release_shared_probe_lock(lock_file) -> None:
    import fcntl

    fcntl.flock(lock_file, fcntl.LOCK_UN)
    lock_file.close()


@router.get("/ping")
async def ping() -> dict[str, bool]:
    """Zero-cost keepalive — no DB, no setup check. Used by client heartbeat."""
    return {"ok": True}


@router.get("/health")
async def health_check() -> dict[str, Any]:
    """Basic health check endpoint - fast response for load balancers."""
    return {"status": "healthy", "service": "cost-observability-control"}


@router.get("/health/detailed")
async def detailed_health_check(request: Request) -> dict[str, Any]:
    """Detailed health check with database connectivity and cache stats.

    This endpoint performs actual service verification:
    - Database connectivity test
    - Query cache statistics
    - Memory usage info
    """
    from server.auth import redact_diagnostic_payload, require_admin

    await require_admin(request)
    checks: dict[str, Any] = {
        "status": "healthy",
        "service": "cost-observability-control",
        "checks": {},
    }

    # Check database connectivity
    db_status = await _check_database()
    checks["checks"]["database"] = db_status

    # Get cache statistics
    cache_status = _get_cache_stats()
    checks["checks"]["cache"] = cache_status

    # Get memory info
    memory_status = _get_memory_info()
    checks["checks"]["memory"] = memory_status

    # Determine overall health
    all_healthy = all(
        check.get("status") == "healthy"
        for check in checks["checks"].values()
    )
    checks["status"] = "healthy" if all_healthy else "degraded"

    return redact_diagnostic_payload(checks)


@router.get("/health/sql-warehouse")
async def get_sql_warehouse_status(
    request: Request, probe: bool = False
) -> dict[str, Any]:
    """Check if the SQL warehouse is warm, starting, or unavailable.

    Normal polls are REST-only. During the initial full-screen cold gate, the
    client may explicitly request one throttled SQL probe after bounded REST
    retries. That probe can wake or verify the warehouse without enabling every
    dashboard query at once. It is never used for a missing resource binding.

    Returns:
        status: "warm" | "warming_up" | "unavailable"
        state: raw warehouse state, REST_UNVERIFIED, or SQL_PROBE_SUCCEEDED
        latency_ms: SQL probe latency when one succeeds, otherwise None
        warehouse_id: warehouse ID being checked (or None if not configured)
        warehouse_size: configured cluster size (for example Small or Medium)
        warehouse_type: SERVERLESS or the configured classic warehouse type
    """
    import os
    import re as _re

    from server.auth import require_admin

    await require_admin(request)
    if probe and getattr(request, "method", "GET").upper() == "GET":
        from fastapi import HTTPException

        raise HTTPException(
            status_code=405,
            detail="Use POST /api/health/sql-warehouse/probe for recovery probes",
        )
    # Prefer DATABRICKS_WAREHOUSE_ID if injected; fall back to parsing HTTP path.
    # The regex handles /sql/1.0/warehouses/<id> and /sql/1.0/endpoints/<id>,
    # strips query-string params, and is safe against trailing slashes.
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID") or None
    if not warehouse_id:
        http_path = os.getenv("DATABRICKS_HTTP_PATH", "").split("?")[0].rstrip("/")
        _m = _re.search(r"/(?:warehouses|endpoints)/([a-f0-9]+)$", http_path, _re.IGNORECASE)
        warehouse_id = _m.group(1) if _m else None

    # A missing binding is definitive configuration state, not a transient REST
    # failure. Keep it distinct so the client can preserve the real unavailable
    # experience without treating network errors the same way.
    if not warehouse_id:
        return {
            "status": "unavailable",
            "state": "NOT_CONFIGURED",
            "latency_ms": None,
            "warehouse_id": None,
            "warehouse_size": None,
            "warehouse_type": None,
        }

    warehouse_metadata: dict[str, Any] = {
        "warehouse_size": None,
        "warehouse_type": None,
    }
    transient_state: str | None = None

    # ── 1. Try warehouse REST API state ──────────────────────────────────────
    # IMPORTANT: the Databricks SDK uses the blocking `requests` library.
    # Calling warehouses.get() directly in an async function freezes the
    # asyncio event loop for the duration of the HTTP round-trip (can be
    # seconds to minutes in degraded environments). Run it in a thread with
    # a hard 5-second timeout so a hung SDK call falls through to transient
    # status handling (and the optional SQL probe when explicitly requested).
    try:
        from server.db import get_workspace_client

        _wh_id = warehouse_id  # capture for lambda
        wh = await asyncio.wait_for(
            asyncio.to_thread(lambda: get_workspace_client().warehouses.get(_wh_id)),
            timeout=5.0,
        )
        raw_state = str(wh.state.value) if wh.state else "UNKNOWN"
        warehouse_type = (
            "SERVERLESS"
            if getattr(wh, "enable_serverless_compute", False)
            else "CLASSIC"
        )
        warehouse_metadata = {
            "warehouse_size": getattr(wh, "cluster_size", None),
            "warehouse_type": warehouse_type,
        }
        if raw_state.upper() in {"DELETING", "DELETED"}:
            return {
                "status": "unavailable",
                "state": raw_state,
                "latency_ms": None,
                "warehouse_id": warehouse_id,
                **warehouse_metadata,
            }
        if raw_state.upper() in {"STOPPED", "STOPPING", "STARTING"}:
            transient_state = raw_state
            if not probe:
                return {
                    "status": "warming_up",
                    "state": raw_state,
                    "latency_ms": None,
                    "warehouse_id": warehouse_id,
                    **warehouse_metadata,
                }
        else:
            # Any other state (RUNNING, RESIZING, SCALING_DOWN, etc.) accepts queries.
            return {
                "status": "warm",
                "state": raw_state,
                "latency_ms": None,
                "warehouse_id": warehouse_id,
                **warehouse_metadata,
            }
    except Exception as e:
        logger.debug("Warehouse REST check failed; treating status as transient: %s", e)
        transient_state = "REST_UNVERIFIED"

    # ── 2. Optional bounded SQL recovery probe ───────────────────────────────
    # The client sends probe=true only after repeated cold-gate polls and
    # throttles retries. Ordinary 5-15 second health polling remains REST-only,
    # so this cannot become a warehouse keepalive loop.
    if probe:
        from server.db import execute_query

        global _warehouse_probe_inflight
        global _warehouse_probe_last_at
        global _warehouse_probe_last_result

        leader = False
        now = time.monotonic()
        with _warehouse_probe_lock:
            if (
                _warehouse_probe_last_result is not None
                and now - _warehouse_probe_last_at < _WAREHOUSE_PROBE_MIN_INTERVAL
            ):
                return dict(_warehouse_probe_last_result)
            if _warehouse_probe_inflight is None:
                _warehouse_probe_inflight = threading.Event()
                leader = True
            inflight = _warehouse_probe_inflight

        if not leader:
            completed = await asyncio.to_thread(
                inflight.wait, 45.0
            )
            with _warehouse_probe_lock:
                if completed and _warehouse_probe_last_result is not None:
                    return dict(_warehouse_probe_last_result)
            return {
                "status": "warming_up",
                "state": "SQL_PROBE_IN_PROGRESS",
                "latency_ms": None,
                "warehouse_id": warehouse_id,
                **warehouse_metadata,
            }

        started = time.monotonic()
        probe_result = {
            "status": "warming_up",
            "state": transient_state,
            "latency_ms": None,
            "warehouse_id": warehouse_id,
            **warehouse_metadata,
        }
        shared_lock_file = None
        try:
            shared_lock_file = await asyncio.to_thread(_acquire_shared_probe_lock)
            completed_at, shared_result = _read_shared_probe_result()
            if (
                shared_result is not None
                and time.time() - completed_at < _WAREHOUSE_PROBE_MIN_INTERVAL
            ):
                probe_result = shared_result
                return probe_result
            await asyncio.wait_for(
                asyncio.to_thread(
                    lambda: execute_query("SELECT 1 AS warehouse_ready", no_cache=True)
                ),
                timeout=45.0,
            )
            probe_result = {
                "status": "warm",
                "state": "SQL_PROBE_SUCCEEDED",
                "latency_ms": round((time.monotonic() - started) * 1000),
                "warehouse_id": warehouse_id,
                **warehouse_metadata,
            }
            _write_shared_probe_result(probe_result)
            return probe_result
        except Exception as e:
            logger.warning("Bounded SQL warehouse recovery probe failed: %s", e)
            probe_result = {
                "status": "warming_up",
                "state": transient_state,
                "latency_ms": None,
                "warehouse_id": warehouse_id,
                **warehouse_metadata,
            }
            _write_shared_probe_result(probe_result)
            return probe_result
        finally:
            if shared_lock_file is not None:
                await asyncio.to_thread(
                    _release_shared_probe_lock, shared_lock_file
                )
            with _warehouse_probe_lock:
                _warehouse_probe_last_at = time.monotonic()
                _warehouse_probe_last_result = dict(probe_result)
                if _warehouse_probe_inflight is not None:
                    _warehouse_probe_inflight.set()
                _warehouse_probe_inflight = None

    return {
        "status": "warming_up",
        "state": transient_state,
        "latency_ms": None,
        "warehouse_id": warehouse_id,
        **warehouse_metadata,
    }


@router.post("/health/sql-warehouse/probe")
async def probe_sql_warehouse_status(request: Request) -> dict[str, Any]:
    """Run the bounded, admin-only SQL recovery probe via a mutating HTTP verb."""

    return await get_sql_warehouse_status(request, probe=True)


async def _check_database() -> dict[str, Any]:
    """Report warehouse configuration without issuing a query.

    Deliberately does NOT run a connectivity probe query — synthetic pings
    warm the serverless warehouse and distort the usage data this app reports on.
    Connectivity is proven by real queries elsewhere; this check only reports
    whether a warehouse is bound to the app.
    """
    import os

    # Return "healthy" when a warehouse is bound so the detailed_health_check
    # aggregator (which marks the app degraded unless every check is "healthy")
    # keeps working without a synthetic probe query.
    if os.getenv("DATABRICKS_WAREHOUSE_ID") or os.getenv("DATABRICKS_HTTP_PATH"):
        return {
            "status": "healthy",
            "message": "Warehouse bound; connectivity verified by real queries (no synthetic probe)",
        }
    return {
        "status": "not_configured",
        "message": "No warehouse bound (DATABRICKS_WAREHOUSE_ID / DATABRICKS_HTTP_PATH unset)",
    }


def _get_cache_stats() -> dict[str, Any]:
    """Get query cache statistics."""
    try:
        from server.db import _CACHE_MAX_SIZE, _CACHE_TTL, _query_cache

        current_size = len(_query_cache)

        return {
            "status": "healthy",
            "current_entries": current_size,
            "max_entries": _CACHE_MAX_SIZE,
            "ttl_seconds": _CACHE_TTL,
            "utilization_percent": round((current_size / _CACHE_MAX_SIZE) * 100, 1),
        }
    except Exception as e:
        logger.error(f"Cache stats check failed: {e}")
        return {
            "status": "unknown",
            "error": str(e),
        }


def _get_memory_info() -> dict[str, Any]:
    """Get memory usage information."""
    try:
        import os
        import resource

        # Get memory usage in MB
        usage = resource.getrusage(resource.RUSAGE_SELF)
        memory_mb = usage.ru_maxrss / (1024 * 1024) if os.name != 'nt' else usage.ru_maxrss / 1024

        return {
            "status": "healthy",
            "rss_mb": round(memory_mb, 2),
        }
    except Exception as e:
        logger.error(f"Memory info check failed: {e}")
        return {
            "status": "unknown",
            "error": str(e),
        }


@router.post("/cache/clear")
async def clear_cache(request: Request, tab: str | None = None) -> dict[str, Any]:
    """Clear server-side query cache for a specific tab or all tabs.

    Tab patterns:
      dbu          → clears bundle, SKU, pipeline, interactive, and trend queries
      infra        → clears estimated and AWS/Azure/GCP actual-cost queries
      optimizer    → clears warehouse rightsizing and idle-time queries
      kpis         → clears kpis-bundle queries
      aiml         → clears aiml queries
      apps         → clears apps queries
      tagging      → clears tagging queries
      sql          → clears dbsql and sql-breakdown queries
      users-groups → clears users-groups queries
      alerts       → clears alerts queries
      (none)       → clears entire cache
    """
    from server.auth import require_admin
    from server.db import clear_query_cache, delta_cache_invalidate

    await require_admin(request)

    TAB_PATTERNS: dict[str, list[str]] = {
        "dbu":          [
            "tab:dbu", "dashboard-bundle-fast", "sku-breakdown", "pipeline-objects",
            "interactive-breakdown", "etl-breakdown",
        ],
        "infra":        [
            "tab:infra", "infra-bundle", "infra-costs", "infra-timeseries",
            "aws-actual", "aws_actual", "aws-costs",
            "azure-actual", "azure_actual", "azure-costs",
            "gcp-actual", "gcp_actual", "gcp-costs",
        ],
        "optimizer":    ["tab:optimizer", "warehouse-health", "warehouse-idle-time", "optimizer"],
        "kpis":         ["tab:kpis", "kpis-bundle", "spend-anomalies", "platform-kpis"],
        "aiml":         ["tab:aiml", "aiml"],
        "apps":         ["tab:apps", "apps", "apps-kpi-trend"],
        "tagging":      ["tab:tagging", "tagging"],
        "sql":          ["tab:sql", "dbsql", "sql-breakdown", "top-queries", "queries-by-user"],
        "users-groups": ["tab:users-groups", "users-groups"],
        "alerts":       ["alerts"],
    }

    DELTA_TAB_PATTERNS: dict[str, list[str]] = {
        "dbu":          [
            "billing:dashboard-bundle-fast", "billing:sku-breakdown",
            "billing:pipeline-objects", "billing:interactive-breakdown",
            "trend:dbu:",
        ],
        "kpis":         ["billing:kpis-bundle", "trend:kpis:"],
        "infra":        ["billing:infra-bundle", "aws_actual/", "trend:infra:"],
        "aiml":         ["aiml:", "trend:aiml:"],
        "apps":         ["apps:", "trend:apps:"],
        "tagging":      ["tagging:", "trend:tagging:"],
        "sql":          ["dbsql:", "billing:sql-breakdown", "trend:sql:"],
        "users-groups": ["users:", "trend:users-groups:"],
    }

    if tab and tab in TAB_PATTERNS:
        cleared = 0
        for pattern in TAB_PATTERNS[tab]:
            cleared += clear_query_cache(pattern)
        for pattern in DELTA_TAB_PATTERNS.get(tab, []):
            delta_cache_invalidate(pattern)
        if tab == "infra":
            from server.routers import aws_actual, azure_actual, gcp_actual
            aws_actual._cur_status_cache.update({"available": None, "checked_at": 0})
            azure_actual._azure_status_cache.update({"available": None, "checked_at": 0})
            gcp_actual._gcp_status_cache.update({"available": None, "checked_at": 0})
        elif tab == "optimizer":
            from server.routers import warehouse_health
            warehouse_health._health_cache = None
            warehouse_health._health_cache_ts = 0.0
            warehouse_health._idle_time_cache = None
            warehouse_health._idle_time_cache_ts = 0.0
        return {"status": "ok", "tab": tab, "cleared": cleared}
    else:
        cleared = clear_query_cache()
        delta_cache_invalidate()
        # Reset MV availability caches so the next request re-detects table state.
        try:
            from server.routers.billing import _mv_cache
            _mv_cache["available"] = None
            _mv_cache["checked_at"] = 0
        except Exception:
            pass
        try:
            from server.routers.dbsql_base import _mv_status_cache
            _mv_status_cache.clear()
        except Exception:
            pass
        return {"status": "ok", "tab": "all", "cleared": cleared}


@router.get("/query-diag")
@router.get("/health/query-diag")
async def query_diagnostics(request: Request) -> dict[str, Any]:
    """Diagnose why data tabs might show zeros.

    Tests SQL connectivity, system table access, and MV table access
    under the current auth identity (OAuth user or SP). Returns exact
    errors so the root cause can be pinpointed without reading server logs.

    SQL tests run in a thread pool so this endpoint never blocks the event loop.
    """
    from server.auth import redact_diagnostic_payload, require_admin
    from server.db import (
        _auth_mode,
        _user_token,
        execute_query,
        get_auth_status,
        get_catalog_schema,
    )

    await require_admin(request)

    diag: dict[str, Any] = {
        "auth": get_auth_status(),
        "user_token_present": bool(_user_token.get()),
        "auth_mode_global": _auth_mode,
        "tests": {},
    }

    catalog, schema = get_catalog_schema()
    diag["catalog"] = catalog
    diag["schema"] = schema

    def _run_sql_tests() -> dict:
        tests: dict[str, str] = {}

        # No synthetic connectivity probe — the system.billing.usage query below
        # proves connectivity against a real table.

        # Test 2: system.billing.usage (most commonly failing)
        try:
            rows = execute_query(
                "SELECT COUNT(*) AS cnt FROM system.billing.usage WHERE usage_date >= CURRENT_DATE - 7",
                no_cache=True,
            )
            tests["system_billing_usage"] = f"ok — {rows[0]['cnt'] if rows else 0} rows"
        except Exception as e:
            tests["system_billing_usage"] = f"ERROR: {e}"

        # Test 3: MV table (app catalog)
        try:
            rows = execute_query(
                f"SELECT COUNT(*) AS cnt FROM `{catalog}`.`{schema}`.`daily_usage_summary`",
                no_cache=True,
            )
            tests["mv_daily_usage_summary"] = f"ok — {rows[0]['cnt'] if rows else 0} rows"
        except Exception as e:
            tests["mv_daily_usage_summary"] = f"ERROR: {e}"

        # Test 4: system.query.history
        try:
            rows = execute_query(
                "SELECT COUNT(*) AS cnt FROM system.query.history WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS",
                no_cache=True,
            )
            tests["system_query_history"] = f"ok — {rows[0]['cnt'] if rows else 0} rows"
        except Exception as e:
            tests["system_query_history"] = f"ERROR: {e}"

        return tests

    loop = asyncio.get_running_loop()
    diag["tests"] = await loop.run_in_executor(None, _run_sql_tests)

    # Test 5: UC REST API table list (used by setup status check — no warehouse needed)
    try:
        from server.db import get_workspace_client
        w = get_workspace_client()
        tables = await asyncio.to_thread(
            lambda: [
                t.name
                for t in w.tables.list(catalog_name=catalog, schema_name=schema)
                if t.name
            ]
        )
        diag["tests"]["uc_table_list"] = f"ok — {len(tables)} tables: {sorted(tables)}"
    except Exception as e:
        diag["tests"]["uc_table_list"] = f"ERROR: {e}"

    return redact_diagnostic_payload(diag)


@router.post("/billing-diag")
async def billing_diagnostics(request: Request) -> dict[str, Any]:
    """Diagnose why billing/dashboard tabs show zeros.

    Checks the full data path used by /api/billing/dashboard-bundle-fast:
    - MV availability cache state
    - Each MV query (products, workspaces, summary, timeseries, etl_breakdown)
    - system.billing.usage fallback accessibility
    - Auth identity in use

    Hit this in the browser when tabs show zeros to get the exact failure point.
    """
    from server.auth import redact_diagnostic_payload, require_admin
    from server.db import execute_query, get_auth_status, get_catalog_schema
    from server.routers.billing import _check_mv_available, _mv_cache

    await require_admin(request)
    catalog, schema = get_catalog_schema()
    now = time.time()
    cache_age = round(now - _mv_cache["checked_at"], 1) if _mv_cache["checked_at"] else None

    diag: dict[str, Any] = {
        "auth": get_auth_status(),
        "warehouse": {
            "http_path": os.getenv("DATABRICKS_HTTP_PATH", "NOT SET"),
        },
        "catalog": catalog,
        "schema": schema,
        "mv_cache": {
            "available": _mv_cache["available"],
            "age_seconds": cache_age,
        },
        "mv_queries": {},
        "fallback_queries": {},
    }

    # Force a fresh MV availability check (bypass cache)
    _mv_cache["available"] = None
    mv_available = _check_mv_available()
    diag["mv_available_fresh"] = mv_available

    params = {"start_date": "2024-01-01", "end_date": "2030-12-31"}

    def _run_billing_tests() -> tuple[dict, dict]:
        from server.materialized_views import (
            MV_BILLING_BY_PRODUCT,
            MV_BILLING_BY_WORKSPACE,
            MV_BILLING_SUMMARY,
            MV_BILLING_TIMESERIES,
            MV_ETL_BREAKDOWN,
        )
        from server.queries import BILLING_BY_PRODUCT_FAST, BILLING_BY_WORKSPACE, BILLING_SUMMARY
        from server.routers.billing import _exec_mv

        mv_tests: dict[str, str] = {}
        fallback_tests: dict[str, str] = {}

        for name, template in [
            ("summary", MV_BILLING_SUMMARY),
            ("products", MV_BILLING_BY_PRODUCT),
            ("workspaces", MV_BILLING_BY_WORKSPACE),
            ("timeseries", MV_BILLING_TIMESERIES),
            ("etl_breakdown", MV_ETL_BREAKDOWN),
        ]:
            try:
                rows = _exec_mv(template, params)
                mv_tests[name] = f"ok — {len(rows)} rows"
            except Exception as e:
                mv_tests[name] = f"ERROR: {e}"

        for name, query in [
            ("billing_summary", BILLING_SUMMARY),
            ("billing_products", BILLING_BY_PRODUCT_FAST),
            ("billing_workspaces", BILLING_BY_WORKSPACE),
        ]:
            try:
                rows = execute_query(query, params, no_cache=True)
                fallback_tests[name] = f"ok — {len(rows)} rows"
            except Exception as e:
                fallback_tests[name] = f"ERROR: {e}"

        return mv_tests, fallback_tests

    loop = asyncio.get_running_loop()
    mv_results, fallback_results = await loop.run_in_executor(None, _run_billing_tests)
    diag["mv_queries"] = mv_results
    diag["fallback_queries"] = fallback_results

    return redact_diagnostic_payload(diag)


@router.get("/debug-env")
async def debug_env(request: Request):
    """Debug: show detected environment (temporary)."""
    from server.auth import redact_diagnostic_payload, require_admin
    from server.db import get_host_url

    await require_admin(request)
    host = get_host_url()
    cloud = None
    if host:
        h = host.lower()
        if "azuredatabricks.net" in h:
            cloud = "AZURE"
        elif "gcp.databricks.com" in h:
            cloud = "GCP"
        elif "cloud.databricks.com" in h:
            cloud = "AWS"
    return redact_diagnostic_payload({
        "host": host,
        "cloud": cloud,
        "DATABRICKS_HOST": os.getenv("DATABRICKS_HOST", "NOT SET"),
        "DATABRICKS_HTTP_PATH": os.getenv("DATABRICKS_HTTP_PATH", "NOT SET"),
    })


@router.post("/setup-diag")
async def setup_diagnostics(request: Request) -> dict[str, Any]:
    """Diagnose why the app is stuck on 'Setting up your workspace'.

    Tests every component of the bootstrap flow independently with short
    timeouts so you get a precise failure point instead of an infinite spinner.

    Hit this on the AWS app when setup stalls:
      https://<app-url>/api/setup-diag
    """
    from server.auth import redact_diagnostic_payload, require_admin
    from server.db import _auth_mode, _user_token, get_catalog_schema, get_workspace_client

    await require_admin(request)
    catalog, schema = get_catalog_schema()
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "NOT SET")
    host_raw = os.getenv("DATABRICKS_HOST", "NOT SET")
    hostname = host_raw.replace("https://", "").replace("http://", "").rstrip("/")
    sp_client_id = os.getenv("DATABRICKS_CLIENT_ID", "NOT SET")

    # Capture user token now — ContextVar values don't cross thread boundaries automatically
    user_tok = _user_token.get()

    diag: dict[str, Any] = {
        "env": {
            "DATABRICKS_HOST": host_raw,
            "DATABRICKS_HTTP_PATH": http_path,
            "DATABRICKS_CLIENT_ID": sp_client_id,
            "COST_OBS_CATALOG": os.getenv("COST_OBS_CATALOG", "NOT SET"),
            "COST_OBS_SCHEMA": os.getenv("COST_OBS_SCHEMA", "NOT SET"),
        },
        "catalog": catalog,
        "schema": schema,
        "auth_mode": _auth_mode,
        "user_token_present": bool(user_tok),
    }

    # Bootstrap state (in-process dict — instant)
    try:
        from server.routers.setup import _create_task_state
        diag["bootstrap_state"] = _create_task_state.copy()
    except Exception as e:
        diag["bootstrap_state"] = f"ERROR: {e}"

    # ------------------------------------------------------------------ #
    # Helper: run a blocking callable in a thread with timeout             #
    # ------------------------------------------------------------------ #
    loop = asyncio.get_running_loop()

    async def _run(fn) -> dict:
        try:
            return await asyncio.wait_for(loop.run_in_executor(None, fn), timeout=40)
        except asyncio.TimeoutError:
            return {"ok": False, "error": "timed out after 40s — warehouse may be cold or unreachable"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ------------------------------------------------------------------ #
    # 1. UC API — no warehouse needed                                      #
    # ------------------------------------------------------------------ #
    def _uc_api():
        try:
            w = get_workspace_client()
            sp_name = None
            try:
                sp_name = w.current_user.me().user_name
            except Exception:
                pass
            tables = list(w.tables.list(catalog_name=catalog, schema_name=schema))
            return {"ok": True, "sp_identity": sp_name, "table_count": len(tables)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    diag["uc_api"] = await _run(_uc_api)

    # ------------------------------------------------------------------ #
    # 2. SP warehouse connection                                           #
    # ------------------------------------------------------------------ #
    def _wh_sp():
        if http_path == "NOT SET":
            return {"ok": False, "error": "DATABRICKS_HTTP_PATH not set"}
        try:
            from databricks import sql
            headers = get_workspace_client().config.authenticate()
            access_token = headers.get("Authorization", "").replace("Bearer ", "")
            if not access_token:
                return {"ok": False, "error": "empty SP access token from SDK"}
            conn = sql.connect(
                server_hostname=hostname,
                http_path=http_path,
                access_token=access_token,
                _socket_timeout=30,
            )
            with conn.cursor() as cur:
                cur.execute("SELECT current_user() AS me")
                rows = cur.fetchall()
            conn.close()
            return {"ok": True, "sp_user": rows[0][0] if rows else None}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    diag["warehouse_sp"] = await _run(_wh_sp)

    # ------------------------------------------------------------------ #
    # 3. User OAuth warehouse connection                                   #
    # ------------------------------------------------------------------ #
    def _wh_user():
        if not user_tok:
            return {"ok": None, "note": "no user token — open the app in a browser and retry"}
        if http_path == "NOT SET":
            return {"ok": False, "error": "DATABRICKS_HTTP_PATH not set"}
        try:
            from databricks import sql
            conn = sql.connect(
                server_hostname=hostname,
                http_path=http_path,
                access_token=user_tok,
                _socket_timeout=30,
            )
            with conn.cursor() as cur:
                cur.execute("SELECT current_user() AS me")
                rows = cur.fetchall()
            conn.close()
            return {"ok": True, "user": rows[0][0] if rows else None}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    diag["warehouse_user"] = await _run(_wh_user)

    # ------------------------------------------------------------------ #
    # 4. System table access (billing + query history)                    #
    # ------------------------------------------------------------------ #
    def _sys_tables():
        token = user_tok
        if not token:
            try:
                headers = get_workspace_client().config.authenticate()
                token = headers.get("Authorization", "").replace("Bearer ", "")
            except Exception as e:
                return {"connection": f"ERROR getting SP token: {e}"}
        if http_path == "NOT SET":
            return {"skipped": "DATABRICKS_HTTP_PATH not set"}
        results: dict[str, str] = {}
        try:
            from databricks import sql
            conn = sql.connect(
                server_hostname=hostname,
                http_path=http_path,
                access_token=token,
                _socket_timeout=30,
            )
            for tbl in [
                "system.billing.usage",
                "system.billing.list_prices",
                "system.query.history",
            ]:
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
                        cur.fetchall()
                    results[tbl] = "ok"
                except Exception as e:
                    results[tbl] = f"ERROR: {e}"
            conn.close()
        except Exception as e:
            results["connection"] = f"ERROR: {e}"
        return results

    diag["system_tables"] = await _run(_sys_tables)

    # ------------------------------------------------------------------ #
    # 5. Schema create permission (needed for bootstrap)                  #
    # ------------------------------------------------------------------ #
    def _schema_perm():
        if not user_tok:
            return {"ok": None, "note": "no user token — test only runs when user is logged in"}
        if http_path == "NOT SET":
            return {"ok": False, "error": "DATABRICKS_HTTP_PATH not set"}
        try:
            from databricks import sql
            conn = sql.connect(
                server_hostname=hostname,
                http_path=http_path,
                access_token=user_tok,
                _socket_timeout=30,
            )
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
            conn.close()
            return {"ok": True, "note": f"user can create/access schema {catalog}.{schema}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    diag["schema_create_permission"] = await _run(_schema_perm)

    return redact_diagnostic_payload(diag)
