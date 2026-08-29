"""Health check endpoints with actual service verification."""

import asyncio
import logging
import time
from typing import Any

from fastapi import APIRouter, BackgroundTasks

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/ping")
async def ping() -> dict[str, bool]:
    """Zero-cost keepalive — no DB, no setup check. Used by client heartbeat."""
    return {"ok": True}


@router.get("/health")
async def health_check() -> dict[str, Any]:
    """Basic health check endpoint - fast response for load balancers."""
    return {"status": "healthy", "service": "cost-observability-control"}


@router.get("/health/detailed")
async def detailed_health_check() -> dict[str, Any]:
    """Detailed health check with database connectivity and cache stats.

    This endpoint performs actual service verification:
    - Database connectivity test
    - Query cache statistics
    - Memory usage info
    """
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

    return checks


@router.get("/health/sql-warehouse")
async def get_sql_warehouse_status(probe: bool = False) -> dict[str, Any]:
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

        started = time.monotonic()
        try:
            await asyncio.wait_for(
                asyncio.to_thread(
                    lambda: execute_query("SELECT 1 AS warehouse_ready", no_cache=True)
                ),
                timeout=45.0,
            )
            return {
                "status": "warm",
                "state": "SQL_PROBE_SUCCEEDED",
                "latency_ms": round((time.monotonic() - started) * 1000),
                "warehouse_id": warehouse_id,
                **warehouse_metadata,
            }
        except Exception as e:
            logger.warning("Bounded SQL warehouse recovery probe failed: %s", e)

    return {
        "status": "warming_up",
        "state": transient_state,
        "latency_ms": None,
        "warehouse_id": warehouse_id,
        **warehouse_metadata,
    }


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
        from server.db import _query_cache, _CACHE_MAX_SIZE, _CACHE_TTL

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


def _run_prewarm():
    """Run cache prewarm in background."""
    # prewarm_cache_sync() disabled — 7 parallel billing queries compete with first user request
    logger.info("Cache prewarm skipped (disabled for stability)")
    # prewarm_all_tabs() disabled — 9 tagging + 5 AI/ML queries saturate the warehouse at startup
    logger.info("All-tabs prewarm skipped (disabled for stability)")


@router.post("/cache/clear")
async def clear_cache(tab: str | None = None) -> dict[str, Any]:
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
    from server.db import clear_query_cache, delta_cache_invalidate

    TAB_PATTERNS: dict[str, list[str]] = {
        "dbu":          [
            "tab:dbu", "dashboard-bundle-fast", "sku-breakdown", "pipeline-objects",
            "interactive-breakdown", "etl-breakdown", "kpi-trend",
        ],
        "infra":        [
            "tab:infra", "infra-bundle", "infra-costs", "infra-timeseries",
            "aws-actual", "aws_actual", "aws-costs",
            "azure-actual", "azure_actual", "azure-costs",
            "gcp-actual", "gcp_actual", "gcp-costs",
        ],
        "optimizer":    ["tab:optimizer", "warehouse-health", "warehouse-idle-time", "optimizer"],
        "kpis":         ["tab:kpis", "kpis-bundle", "spend-anomalies", "platform-kpis", "platform-kpi-trend"],
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
        "users-groups": ["users:"],
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


@router.post("/prewarm")
async def trigger_cache_prewarm(background_tasks: BackgroundTasks) -> dict[str, Any]:
    """Trigger cache pre-warming for all dashboard queries.

    This runs in the background and returns immediately.
    Use /api/health/detailed to check cache status.
    """
    background_tasks.add_task(_run_prewarm)
    return {
        "status": "started",
        "message": "Cache pre-warming started in background. Check /api/health/detailed for cache stats."
    }


@router.get("/query-diag")
async def query_diagnostics() -> dict[str, Any]:
    """Diagnose why data tabs might show zeros.

    Tests SQL connectivity, system table access, and MV table access
    under the current auth identity (OAuth user or SP). Returns exact
    errors so the root cause can be pinpointed without reading server logs.

    SQL tests run in a thread pool so this endpoint never blocks the event loop.
    """
    import asyncio
    from server.db import execute_query, get_auth_status, _user_token, _auth_mode, get_catalog_schema

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
        tables = [t.name for t in w.tables.list(catalog_name=catalog, schema_name=schema) if t.name]
        diag["tests"]["uc_table_list"] = f"ok — {len(tables)} tables: {sorted(tables)}"
    except Exception as e:
        diag["tests"]["uc_table_list"] = f"ERROR: {e}"

    return diag


@router.get("/billing-diag")
async def billing_diagnostics() -> dict[str, Any]:
    """Diagnose why billing/dashboard tabs show zeros.

    Checks the full data path used by /api/billing/dashboard-bundle-fast:
    - MV availability cache state
    - Each MV query (products, workspaces, summary, timeseries, etl_breakdown)
    - system.billing.usage fallback accessibility
    - Auth identity in use

    Hit this in the browser when tabs show zeros to get the exact failure point.
    """
    import asyncio
    import os
    import time
    from server.db import execute_query, get_auth_status, get_catalog_schema, _auth_mode, _user_token
    from server.routers.billing import _mv_cache, _check_mv_available

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
            MV_BILLING_SUMMARY, MV_BILLING_BY_PRODUCT, MV_BILLING_BY_WORKSPACE,
            MV_BILLING_TIMESERIES, MV_ETL_BREAKDOWN,
        )
        from server.routers.billing import _exec_mv
        from server.queries import BILLING_SUMMARY, BILLING_BY_PRODUCT_FAST, BILLING_BY_WORKSPACE

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

    return diag


@router.get("/debug-env")
async def debug_env():
    """Debug: show detected environment (temporary)."""
    import os
    from server.db import get_host_url
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
    return {
        "host": host,
        "cloud": cloud,
        "DATABRICKS_HOST": os.getenv("DATABRICKS_HOST", "NOT SET"),
        "DATABRICKS_HTTP_PATH": os.getenv("DATABRICKS_HTTP_PATH", "NOT SET"),
    }


@router.get("/setup-diag")
async def setup_diagnostics() -> dict[str, Any]:
    """Diagnose why the app is stuck on 'Setting up your workspace'.

    Tests every component of the bootstrap flow independently with short
    timeouts so you get a precise failure point instead of an infinite spinner.

    Hit this on the AWS app when setup stalls:
      https://<app-url>/api/setup-diag
    """
    import os
    import asyncio
    from server.db import get_catalog_schema, get_workspace_client, _user_token, _auth_mode

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

    return diag
