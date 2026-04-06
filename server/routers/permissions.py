"""Permissions check endpoints for system table access verification."""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fastapi import APIRouter, Request

from server.db import get_workspace_client

router = APIRouter()
logger = logging.getLogger(__name__)

# Dedicated executor so permissions checks don't contend with startup tasks
_permissions_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="permissions")

# Simple in-process cache so repeated wizard loads are instant (5-min TTL)
_permissions_cache: dict[str, Any] | None = None
_permissions_cache_ts: float = 0.0
_PERMISSIONS_CACHE_TTL = 300  # 5 minutes

# Required system tables and their descriptions
REQUIRED_PERMISSIONS = [
    {
        "table": "system.billing.usage",
        "name": "Billing Usage",
        "description": "Core billing and DBU consumption data",
        "required": True,
    },
    {
        "table": "system.billing.list_prices",
        "name": "List Prices",
        "description": "SKU pricing for cost calculations",
        "required": True,
    },
    {
        "table": "system.query.history",
        "name": "Query History",
        "description": "DBSQL query analytics and cost attribution",
        "required": False,
    },
    {
        "table": "system.compute.clusters",
        "name": "Clusters",
        "description": "Cluster metadata for interactive workloads",
        "required": False,
    },
    {
        "table": "system.lakeflow.pipelines",
        "name": "SDP Pipelines",
        "description": "SDP pipeline names and metadata",
        "required": False,
    },
    {
        "table": "system.serving.served_entities",
        "name": "Model Serving",
        "description": "Model serving endpoint information",
        "required": False,
    },
    {
        "table": "system.access.audit",
        "name": "Audit Logs",
        "description": "Workspace audit events (optional)",
        "required": False,
    },
]


def check_table_access(table: str) -> tuple[bool, str]:
    """Check if the app can query a system table.

    Returns (granted, error_message). error_message is empty string on success.

    Runs SELECT 1 FROM <table> LIMIT 1 via the configured SQL warehouse —
    the same path the app uses at runtime. This is more reliable than the
    UC metadata API (tables.get), which requires USE CATALOG + USE SCHEMA
    grants in addition to SELECT and can return false negatives even when
    the table is fully queryable.
    """
    from server.db import execute_query
    try:
        execute_query(f"SELECT 1 FROM {table} LIMIT 1", no_cache=True)
        return True, ""
    except Exception as e:
        logger.warning(f"Access check failed for {table}: {type(e).__name__}: {e}")
        return False, str(e)


def _get_current_user() -> tuple[str, str]:
    """Return (email, display_name) for the current identity."""
    try:
        w = get_workspace_client()
        current_user = w.current_user.me()
        email = current_user.user_name or "unknown"
        name = current_user.display_name or email
        return email, name
    except Exception as e:
        logger.warning(f"Could not get current user: {e}")
        return "unknown", "Unknown User"


def _check_permissions_sync(bypass_cache: bool = False) -> dict[str, Any]:
    """Run all permission checks and user lookup in parallel.

    Results are cached for _PERMISSIONS_CACHE_TTL seconds to avoid hitting
    the UC REST API on every wizard page load. Pass bypass_cache=True to force
    a fresh check (e.g. after the user grants new permissions).
    """
    global _permissions_cache, _permissions_cache_ts

    if not bypass_cache and _permissions_cache is not None:
        age = time.monotonic() - _permissions_cache_ts
        if age < _PERMISSIONS_CACHE_TTL:
            logger.debug(f"Returning cached permissions result (age: {age:.0f}s)")
            return _permissions_cache

    from concurrent.futures import as_completed

    # Fire table checks + user lookup all in parallel
    with ThreadPoolExecutor(max_workers=len(REQUIRED_PERMISSIONS) + 1) as pool:
        future_to_table = {
            pool.submit(check_table_access, perm["table"]): perm["table"]
            for perm in REQUIRED_PERMISSIONS
        }
        user_future = pool.submit(_get_current_user)

        access_results: dict[str, tuple[bool, str]] = {}
        for future in as_completed(future_to_table):
            table = future_to_table[future]
            access_results[table] = future.result()

        user_email, user_name = user_future.result()

    # Assemble results
    results = []
    granted_count = 0
    required_granted = 0
    required_count = 0

    for perm in REQUIRED_PERMISSIONS:
        has_access, error_msg = access_results[perm["table"]]

        if has_access:
            granted_count += 1
            if perm["required"]:
                required_granted += 1

        if perm["required"]:
            required_count += 1

        row = {
            "table": perm["table"],
            "name": perm["name"],
            "description": perm["description"],
            "required": perm["required"],
            "granted": has_access,
        }
        if error_msg:
            row["error"] = error_msg
        results.append(row)

    # Determine overall status
    all_required_granted = required_granted == required_count

    result = {
        "permissions": results,
        "summary": {
            "total": len(results),
            "granted": granted_count,
            "required_count": required_count,
            "required_granted": required_granted,
            "all_required_granted": all_required_granted,
            "ready_to_use": all_required_granted,
        },
        "user": {
            "email": user_email,
            "name": user_name,
        },
        "help_url": "https://docs.databricks.com/en/admin/system-tables/index.html",
    }

    _permissions_cache = result
    _permissions_cache_ts = time.monotonic()
    return result


@router.get("/check")
async def check_permissions(request: Request, refresh: bool = False) -> dict[str, Any]:
    """
    Check user's access to required system tables.

    When Databricks Apps user authorization is active (x-forwarded-access-token
    present), checks run as the end user and results are not cached (each user
    may have different grants). Otherwise cached for 5 minutes per process.
    Pass ?refresh=true to force a live re-check (e.g. after granting permissions).
    """
    from server.db import _user_token

    # Read the token directly from the request rather than relying on middleware
    # ContextVar propagation, which is unreliable through BaseHTTPMiddleware.
    user_token = request.headers.get("x-forwarded-access-token", "")
    using_user_auth = bool(user_token)

    # Set ContextVar here in the async handler so it's guaranteed to propagate
    # into run_in_executor (which copies the current context to its thread).
    ctx_tok = _user_token.set(user_token)
    try:
        bypass = refresh or using_user_auth
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            _permissions_executor,
            lambda: _check_permissions_sync(bypass_cache=bypass),
        )
    finally:
        _user_token.reset(ctx_tok)

    result["auth_mode"] = "user" if using_user_auth else "service_principal"
    return result
