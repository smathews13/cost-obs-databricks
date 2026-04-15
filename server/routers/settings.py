"""App settings endpoints - Cloud infrastructure connections management."""

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

router = APIRouter()
logger = logging.getLogger(__name__)


def _require_admin(request: Request) -> str:
    """Raise 403 if the requesting user is not an admin. Returns email on success."""
    email = request.headers.get("X-Forwarded-Email", os.getenv("USER", "dev@local"))
    perms = _load_user_permissions()
    if email not in perms.get("admins", []):
        raise HTTPException(status_code=403, detail="Admin role required")
    return email

# File-based storage for cloud connections (simple, no DB needed)
SETTINGS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", ".settings")
CLOUD_CONNECTIONS_FILE = os.path.join(SETTINGS_DIR, "cloud_connections.json")
WEBHOOK_SETTINGS_FILE = os.path.join(SETTINGS_DIR, "webhook_settings.json")
WAREHOUSE_SETTINGS_FILE = os.path.join(SETTINGS_DIR, "warehouse_settings.json")
TELEMETRY_SETTINGS_FILE = os.path.join(SETTINGS_DIR, "telemetry_settings.json")
PRICING_SETTINGS_FILE = os.path.join(SETTINGS_DIR, "pricing_settings.json")
USER_PERMISSIONS_FILE = os.path.join(SETTINGS_DIR, "user_permissions.json")
# Legacy file path for backward compatibility
AZURE_CONNECTIONS_FILE = os.path.join(SETTINGS_DIR, "azure_connections.json")


class CloudConnectionCreate(BaseModel):
    name: str
    provider: str  # "azure", "aws", "gcp"
    # Azure fields
    tenant_id: Optional[str] = None
    subscription_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    # AWS fields
    aws_account_id: Optional[str] = None
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    region: Optional[str] = None
    # GCP fields
    project_id: Optional[str] = None
    service_account_key: Optional[str] = None


def _load_connections() -> list[dict]:
    """Load cloud connections from disk, migrating legacy Azure-only file if needed."""
    # Try new file first
    if os.path.exists(CLOUD_CONNECTIONS_FILE):
        try:
            with open(CLOUD_CONNECTIONS_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []

    # Fall back to legacy Azure file and migrate
    if os.path.exists(AZURE_CONNECTIONS_FILE):
        try:
            with open(AZURE_CONNECTIONS_FILE) as f:
                connections = json.load(f)
            # Add provider field to legacy connections
            for conn in connections:
                if "provider" not in conn:
                    conn["provider"] = "azure"
            # Save to new file
            _save_connections(connections)
            return connections
        except (json.JSONDecodeError, IOError):
            return []

    return []


def _save_connections(connections: list[dict]) -> None:
    """Save cloud connections to disk."""
    os.makedirs(SETTINGS_DIR, exist_ok=True)
    with open(CLOUD_CONNECTIONS_FILE, "w") as f:
        json.dump(connections, f, indent=2)


def _mask_connection(conn: dict) -> dict:
    """Mask sensitive fields in a connection for API response."""
    masked = dict(conn)
    for secret_field in ("client_secret", "secret_access_key", "service_account_key"):
        val = masked.get(secret_field)
        if val and len(val) > 4:
            masked[secret_field] = "***" + val[-4:]
        elif val:
            masked[secret_field] = "****"
    return masked


@router.get("/config")
async def get_app_config():
    """Return current app configuration: warehouse, identity, and storage location."""
    from server.db import get_catalog_schema, get_workspace_client

    result: dict[str, Any] = {
        "warehouse": None,
        "identity": None,
        "storage_location": None,
    }

    # SQL Warehouse info
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
    warehouse_id = http_path.split("/")[-1] if http_path else None
    if warehouse_id:
        try:
            w = get_workspace_client()
            wh = w.warehouses.get(warehouse_id)
            result["warehouse"] = {
                "id": wh.id,
                "name": wh.name,
                "size": wh.cluster_size,
                "state": str(wh.state.value) if wh.state else "UNKNOWN",
            }
        except Exception as e:
            logger.warning(f"Could not fetch warehouse details: {e}")
            result["warehouse"] = {"id": warehouse_id, "name": None, "size": None, "state": "UNKNOWN"}

    # Service principal / current identity
    try:
        w = get_workspace_client()
        me = w.current_user.me()
        result["identity"] = {
            "display_name": me.display_name,
            "user_name": me.user_name,
        }
    except Exception as e:
        logger.warning(f"Could not fetch current identity: {e}")

    # Storage location (catalog.schema)
    try:
        catalog, schema = get_catalog_schema()
        result["storage_location"] = {"catalog": catalog, "schema": schema}
    except Exception as e:
        logger.warning(f"Could not fetch catalog/schema: {e}")

    return result


@router.get("/tables")
async def get_tables_status():
    """Return status of each MV table: exists, row count, max date, days behind."""
    from server.db import get_catalog_schema, execute_query

    MV_TABLES = [
        "daily_usage_summary",
        "daily_product_breakdown",
        "daily_workspace_breakdown",
        "sql_tool_attribution",
        "daily_query_stats",
        "dbsql_cost_per_query",
        "app_user_permissions",
    ]
    # Which tables are conceptually "materialized views" (rebuilt on schedule)
    # vs persistent managed tables
    MV_SET = {
        "daily_usage_summary", "daily_product_breakdown", "daily_workspace_breakdown",
        "sql_tool_attribution", "daily_query_stats", "dbsql_cost_per_query",
    }

    try:
        catalog, schema = get_catalog_schema()
    except Exception as e:
        return {"catalog": None, "schema": None, "tables": [], "error": str(e)}

    from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
    from datetime import date

    # Tables that don't have a usage_date column — use an alternate date expression or skip date
    date_expr_overrides = {
        "dbsql_cost_per_query": "CAST(MAX(start_time) AS DATE)",
    }
    no_date_tables = {"app_user_permissions"}

    def check_table(table_name: str, fqn: str, table_type: str) -> dict:
        skip_date = table_name in no_date_tables
        try:
            if skip_date:
                rows = execute_query(f"SELECT COUNT(*) as cnt FROM {fqn}")
                cnt = rows[0]["cnt"] if rows else 0
                return {"name": table_name, "table_type": table_type, "exists": True, "row_count": cnt, "max_date": None, "days_behind": None}
            else:
                date_expr = date_expr_overrides.get(table_name, "MAX(usage_date)")
                rows = execute_query(
                    f"SELECT COUNT(*) as cnt, {date_expr} as max_date FROM {fqn}"
                )
                if not rows:
                    return {"name": table_name, "table_type": table_type, "exists": True, "row_count": 0, "max_date": None, "days_behind": None}
                cnt = rows[0].get("cnt", 0)
                max_date = rows[0].get("max_date")
                max_date_str = str(max_date) if max_date else None
                days_behind = None
                if max_date_str:
                    from datetime import date as _date
                    try:
                        delta = _date.today() - _date.fromisoformat(max_date_str[:10])
                        days_behind = delta.days
                    except Exception:
                        pass
                return {"name": table_name, "table_type": table_type, "exists": True, "row_count": int(cnt), "max_date": max_date_str, "days_behind": days_behind}
        except Exception as e:
            err = str(e)
            if "TABLE_OR_VIEW_NOT_FOUND" in err or "does not exist" in err.lower() or "not found" in err.lower():
                return {"name": table_name, "table_type": table_type, "exists": False, "row_count": None, "max_date": None, "days_behind": None}
            return {"name": table_name, "table_type": table_type, "exists": None, "row_count": None, "max_date": None, "days_behind": None, "error": err[:200]}

    # Build task list: (table_name, fqn, table_type)
    tasks = [
        (t, f"`{catalog}`.`{schema}`.`{t}`", "Materialized View" if t in MV_SET else "Table")
        for t in MV_TABLES
    ]

    # Add app telemetry OTel tables if configured
    tel = _load_telemetry_settings()
    tel_catalog = tel.get("catalog", "").strip()
    tel_schema = tel.get("schema_name", "").strip()
    tel_prefix = tel.get("table_prefix", "").strip()
    if tel_catalog and tel_schema:
        otel_tables = ["otel_spans", "otel_metrics", "otel_logs"]
        for ot in otel_tables:
            full_name = f"{tel_prefix}{ot}" if tel_prefix else ot
            fqn = f"`{tel_catalog}`.`{tel_schema}`.`{full_name}`"
            tasks.append((full_name, fqn, "Telemetry"))
            no_date_tables.add(full_name)  # OTel tables don't have usage_date

    results = []
    _TABLE_CHECK_TIMEOUT = 25  # seconds — keeps total request under proxy timeout limits
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(check_table, name, fqn, ttype): (name, fqn, ttype) for name, fqn, ttype in tasks}
        try:
            for fut in as_completed(futures, timeout=_TABLE_CHECK_TIMEOUT):
                results.append(fut.result())
        except FuturesTimeoutError:
            # Some queries didn't finish (cold warehouse). Return partial results:
            # completed futures + placeholder rows for anything still pending.
            completed_names = {r["name"] for r in results}
            for fut, (name, _fqn, ttype) in futures.items():
                if name not in completed_names:
                    results.append({
                        "name": name, "table_type": ttype, "exists": None,
                        "row_count": None, "max_date": None, "days_behind": None,
                        "error": "timed out — warehouse may be starting up",
                    })
            logger.warning("Table status check timed out — warehouse likely cold")

    # Preserve original order
    order = {name: i for i, (name, _, _) in enumerate(tasks)}
    results.sort(key=lambda r: order.get(r["name"], 99))

    # Detect auth/permission failures — surface a top-level auth_error so the UI
    # can show an actionable message instead of per-row ⚠ icons.
    _PERM_SIGNALS = ("PERMISSION_DENIED", "INSUFFICIENT_PRIVILEGES", "not authorized",
                     "Not authorized", "Unauthorized", "User does not have", "403")
    perm_errors = [
        r for r in results
        if r.get("error") and any(s in r["error"] for s in _PERM_SIGNALS)
    ]
    auth_error = None
    if perm_errors and len(perm_errors) >= len(tasks) // 2:
        auth_error = (
            "The app service principal lacks permission to read these tables. "
            "Open the app as a workspace admin (with SQL scope) so queries run under your credentials, "
            "or run dba_deploy.sh to grant the required Unity Catalog permissions."
        )

    # Read MV refresh log (atomic write guarantees no partial read)
    refresh_status = None
    _log_path = os.path.join(os.path.dirname(__file__), "..", "..", ".settings", "mv_refresh_log.json")
    try:
        with open(_log_path) as _f:
            _log = json.load(_f)
        from datetime import datetime as _dt, timezone as _tz
        _last = _dt.strptime(_log["last_refresh_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=_tz.utc)
        _hours = (_dt.now(_tz.utc) - _last).total_seconds() / 3600
        refresh_status = {
            "last_refresh_utc": _log["last_refresh_utc"],
            "duration_seconds": _log.get("duration_seconds"),
            "hours_since_refresh": round(_hours, 1),
            "stale": _hours > 26,
            "status": _log.get("status", "unknown"),
        }
        if _log.get("error"):
            refresh_status["error"] = _log["error"]
    except (FileNotFoundError, KeyError, ValueError, OSError):
        pass

    return {"catalog": catalog, "schema": schema, "tables": results, "auth_error": auth_error, "refresh_status": refresh_status}


_CONTRACT_SETTINGS_FILE = os.path.join(
    os.path.dirname(__file__), "..", "..", ".settings", "contract_settings.json"
)

_CONTRACT_EMPTY = {"start_date": None, "end_date": None, "total_commit_usd": None, "notes": ""}


def _load_contract_settings() -> dict:
    try:
        with open(_CONTRACT_SETTINGS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return _CONTRACT_EMPTY.copy()


@router.get("/contract")
async def get_contract_settings():
    """Return saved contract terms (or empty defaults)."""
    return _load_contract_settings()


@router.post("/contract")
async def save_contract_settings(body: dict):
    """Persist contract terms after basic validation."""
    from datetime import date as _date
    errors = []
    start = body.get("start_date") or ""
    end = body.get("end_date") or ""
    commit = body.get("total_commit_usd")
    try:
        _date.fromisoformat(start)
    except (ValueError, TypeError):
        errors.append("start_date must be a valid ISO date (YYYY-MM-DD)")
    try:
        _date.fromisoformat(end)
    except (ValueError, TypeError):
        errors.append("end_date must be a valid ISO date (YYYY-MM-DD)")
    if commit is None or not isinstance(commit, (int, float)) or commit <= 0:
        errors.append("total_commit_usd must be a positive number")
    if errors:
        from fastapi import HTTPException
        raise HTTPException(status_code=422, detail="; ".join(errors))
    data = {
        "start_date": start,
        "end_date": end,
        "total_commit_usd": float(commit),
        "notes": (body.get("notes") or "").strip(),
    }
    os.makedirs(os.path.dirname(_CONTRACT_SETTINGS_FILE), exist_ok=True)
    with open(_CONTRACT_SETTINGS_FILE, "w") as f:
        json.dump(data, f)
    return data


@router.get("/catalog")
async def get_catalog_settings():
    """Return current catalog/schema and whether it's from an override or env vars."""
    from server.db import get_catalog_schema_info
    return get_catalog_schema_info()


@router.post("/catalog")
async def set_catalog_settings(body: dict):
    """Save a catalog/schema override. Clears the query cache so new values take effect immediately."""
    from server.db import save_catalog_override, clear_query_cache
    catalog = (body.get("catalog") or "").strip()
    schema = (body.get("schema") or "").strip()
    if not catalog or not schema:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="catalog and schema are required")
    save_catalog_override(catalog, schema)
    clear_query_cache()
    return {"status": "ok", "catalog": catalog, "schema": schema, "source": "override"}


@router.delete("/catalog")
async def reset_catalog_settings():
    """Remove catalog/schema override and revert to env var values."""
    from server.db import clear_catalog_override, get_catalog_schema_info, clear_query_cache
    clear_catalog_override()
    clear_query_cache()
    return {"status": "ok", **get_catalog_schema_info()}


@router.post("/refresh-mvs")
async def trigger_mv_refresh(request: Request):
    """Trigger an immediate MV rebuild (CREATE OR REPLACE TABLE for all MV tables)."""
    import asyncio
    from server.app import _run_mv_refresh

    user_token = request.headers.get("x-forwarded-access-token") or None
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(None, lambda: _run_mv_refresh(user_token=user_token))
        return {"status": "ok", "result": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@router.get("/auth-status")
async def get_auth_status_endpoint():
    """Return current auth mode for the settings UI indicator."""
    from server.db import get_auth_status
    return get_auth_status()


class AuthModeRequest(BaseModel):
    mode: str  # "sp" | "auto"


@router.post("/auth-mode")
async def set_auth_mode(body: AuthModeRequest):
    """Override the SQL query auth mode.

    mode='sp'   — force all queries through the service principal.
    mode='auto' — clear the override and re-enable OAuth auto-detection.

    The change takes effect immediately for new requests. A page refresh
    is required for the header badge to update.
    """
    if body.mode not in ("sp", "auto"):
        raise HTTPException(status_code=422, detail="mode must be 'sp' or 'auto'")
    from server.db import set_auth_mode_override
    set_auth_mode_override(body.mode)
    return {"status": "ok", "mode": body.mode}


@router.get("/warehouses")
async def list_warehouses():
    """List all SQL warehouses the user has access to."""
    from server.db import get_user_workspace_client

    current_http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
    current_id = current_http_path.split("/")[-1] if current_http_path else None

    try:
        w = get_user_workspace_client()
        warehouses = list(w.warehouses.list())

        result = []
        for wh in warehouses:
            state = str(wh.state.value) if wh.state else "UNKNOWN"
            result.append({
                "id": wh.id,
                "name": wh.name,
                "size": wh.cluster_size,
                "state": state,
                "is_current": wh.id == current_id,
            })

        # If the currently configured warehouse isn't in the list (SP visibility gap),
        # fetch it directly and prepend so it's always selectable.
        if current_id and not any(r["id"] == current_id for r in result):
            try:
                wh = w.warehouses.get(current_id)
                state = str(wh.state.value) if wh.state else "UNKNOWN"
                result.insert(0, {
                    "id": wh.id,
                    "name": wh.name,
                    "size": wh.cluster_size,
                    "state": state,
                    "is_current": True,
                })
            except Exception as e2:
                logger.warning(f"Could not fetch current warehouse {current_id}: {e2}")
                # Still surface it with minimal info so the UI doesn't show "No warehouses found"
                result.insert(0, {"id": current_id, "name": None, "size": None, "state": "UNKNOWN", "is_current": True})

        # Sort: current first, then running, then by name
        result.sort(key=lambda x: (not x["is_current"], x["state"] != "RUNNING", x["name"] or ""))
        return result
    except Exception as e:
        logger.error(f"Failed to list warehouses: {e}")
        # Last resort: return just the configured warehouse so UI isn't empty
        if current_id:
            return [{"id": current_id, "name": None, "size": None, "state": "UNKNOWN", "is_current": True}]
        raise HTTPException(status_code=500, detail=str(e))


def _load_warehouse_settings() -> dict:
    """Load saved warehouse preference from disk."""
    if os.path.exists(WAREHOUSE_SETTINGS_FILE):
        try:
            with open(WAREHOUSE_SETTINGS_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def _save_warehouse_settings(settings: dict) -> None:
    """Save warehouse preference to disk."""
    os.makedirs(SETTINGS_DIR, exist_ok=True)
    with open(WAREHOUSE_SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=2)


class WarehouseSwitch(BaseModel):
    warehouse_id: str


@router.post("/warehouse")
async def switch_warehouse(body: WarehouseSwitch):
    """Switch the active SQL warehouse powering the app."""
    from server.db import get_workspace_client

    warehouse_id = body.warehouse_id
    try:
        w = get_workspace_client()
        # Verify the warehouse exists and is accessible
        wh = w.warehouses.get(warehouse_id)

        # Update the environment variable so all future queries use this warehouse
        new_http_path = f"/sql/1.0/warehouses/{warehouse_id}"
        os.environ["DATABRICKS_HTTP_PATH"] = new_http_path

        state = str(wh.state.value) if wh.state else "UNKNOWN"

        # If the warehouse is stopped, attempt to start it
        if state == "STOPPED":
            try:
                w.warehouses.start(warehouse_id)
                logger.info(f"Started warehouse {warehouse_id} ({wh.name})")
                state = "STARTING"
            except Exception as e:
                logger.warning(f"Could not start warehouse {warehouse_id}: {e}")

        # Persist the warehouse preference to disk so it survives restarts
        _save_warehouse_settings({
            "warehouse_id": warehouse_id,
            "http_path": new_http_path,
            "warehouse_name": wh.name,
            "switched_at": datetime.utcnow().isoformat(),
        })

        logger.info(f"Switched active warehouse to {warehouse_id} ({wh.name})")

        return {
            "success": True,
            "warehouse": {
                "id": wh.id,
                "name": wh.name,
                "size": wh.cluster_size,
                "state": state,
            },
            "http_path": new_http_path,
        }
    except Exception as e:
        logger.error(f"Failed to switch warehouse: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cloud-provider")
async def get_cloud_provider():
    """Detect the base cloud provider from the Databricks workspace host URL."""
    from server.db import get_workspace_client

    host = os.getenv("DATABRICKS_HOST", "")
    # Try getting host from workspace client if env var is empty
    if not host:
        try:
            w = get_workspace_client()
            host = w.config.host or ""
        except Exception:
            pass

    host = host.lower()
    if ".azuredatabricks.net" in host or "adb-" in host:
        provider = "azure"
    elif ".gcp.databricks.com" in host:
        provider = "gcp"
    else:
        # Default to AWS (.cloud.databricks.com and others)
        provider = "aws"

    return {"provider": provider, "host": host}


@router.get("/cloud-connections")
async def list_cloud_connections():
    """List all cloud connections (secrets are masked)."""
    connections = _load_connections()
    return [_mask_connection(c) for c in connections]


# Keep legacy endpoint for backward compatibility
@router.get("/azure-connections")
async def list_azure_connections():
    """List Azure connections (legacy endpoint, returns all connections)."""
    connections = _load_connections()
    return [_mask_connection(c) for c in connections]


@router.post("/cloud-connections")
async def create_cloud_connection(request: Request, conn: CloudConnectionCreate):
    """Create a new cloud connection."""
    _require_admin(request)
    if conn.provider not in ("azure", "aws", "gcp"):
        raise HTTPException(status_code=400, detail="Invalid provider. Must be azure, aws, or gcp.")

    connections = _load_connections()

    new_conn = {
        "id": str(uuid.uuid4())[:8],
        "name": conn.name,
        "provider": conn.provider,
        "created_at": datetime.utcnow().isoformat(),
    }

    if conn.provider == "azure":
        new_conn.update({
            "tenant_id": conn.tenant_id,
            "subscription_id": conn.subscription_id,
            "client_id": conn.client_id,
            "client_secret": conn.client_secret,
        })
    elif conn.provider == "aws":
        new_conn.update({
            "aws_account_id": conn.aws_account_id,
            "access_key_id": conn.access_key_id,
            "secret_access_key": conn.secret_access_key,
            "region": conn.region,
        })
    elif conn.provider == "gcp":
        new_conn.update({
            "project_id": conn.project_id,
            "service_account_key": conn.service_account_key,
        })

    connections.append(new_conn)
    _save_connections(connections)

    logger.info(f"Created {conn.provider.upper()} connection: {conn.name}")

    return _mask_connection(new_conn)


# Keep legacy endpoint for backward compatibility
@router.post("/azure-connections")
async def create_azure_connection(conn: CloudConnectionCreate):
    """Create an Azure connection (legacy endpoint)."""
    conn.provider = "azure"
    return await create_cloud_connection(conn)


@router.delete("/cloud-connections/{connection_id}")
async def delete_cloud_connection(request: Request, connection_id: str):
    """Delete a cloud connection."""
    _require_admin(request)
    connections = _load_connections()
    original_count = len(connections)
    connections = [c for c in connections if c.get("id") != connection_id]

    if len(connections) == original_count:
        raise HTTPException(status_code=404, detail="Connection not found")

    _save_connections(connections)
    logger.info(f"Deleted cloud connection: {connection_id}")
    return {"status": "deleted", "id": connection_id}


# Keep legacy endpoint for backward compatibility
@router.delete("/azure-connections/{connection_id}")
async def delete_azure_connection(connection_id: str):
    """Delete an Azure connection (legacy endpoint)."""
    return await delete_cloud_connection(connection_id)


# ── Webhook Settings ─────────────────────────────────────────────────────

class WebhookSettings(BaseModel):
    slack_webhook_url: str = ""


def _load_webhook_settings() -> dict:
    """Load webhook settings from disk."""
    if os.path.exists(WEBHOOK_SETTINGS_FILE):
        try:
            with open(WEBHOOK_SETTINGS_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def _save_webhook_settings(settings: dict) -> None:
    """Save webhook settings to disk."""
    os.makedirs(SETTINGS_DIR, exist_ok=True)
    with open(WEBHOOK_SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=2)


@router.get("/webhook")
async def get_webhook_settings() -> dict[str, Any]:
    """Get current webhook settings."""
    settings = _load_webhook_settings()
    # Mask the URL for security
    url = settings.get("slack_webhook_url", "")
    masked = ""
    if url:
        # Only show scheme+host to confirm it's configured without exposing path tokens
        masked = "https://hooks.slack.com/services/****" if "hooks.slack.com" in url else "****"
    return {"slack_webhook_url": masked, "configured": bool(url)}


@router.post("/webhook")
async def save_webhook_settings(request: Request, settings: WebhookSettings) -> dict[str, Any]:
    """Save webhook settings."""
    _require_admin(request)
    _save_webhook_settings({"slack_webhook_url": settings.slack_webhook_url})
    logger.info("Webhook settings updated")
    return {"status": "saved"}


@router.post("/webhook/test")
async def test_webhook(request: Request) -> dict[str, Any]:
    """Send a test message to the configured Slack webhook."""
    _require_admin(request)
    settings = _load_webhook_settings()
    url = settings.get("slack_webhook_url", "")
    if not url:
        return {"success": False, "error": "No webhook URL configured"}

    payload = {
        "text": "Cost Observability & Control - Test notification. Your webhook is working!"
    }
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                return {"success": True, "message": "Test message sent"}
            return {"success": False, "error": f"Slack returned status {resp.status_code}"}
    except Exception as e:
        logger.error(f"Webhook test failed: {e}")
        return {"success": False, "error": str(e)}


@router.post("/webhook/send-alert")
async def send_webhook_alert(alert_data: dict[str, Any]) -> dict[str, Any]:
    """Send an alert notification to the configured Slack webhook."""
    settings = _load_webhook_settings()
    url = settings.get("slack_webhook_url", "")
    if not url:
        return {"success": False, "error": "No webhook URL configured"}

    # Format alert message
    alert_type = alert_data.get("alert_type", "alert")
    usage_date = alert_data.get("usage_date", "unknown")
    daily_spend = alert_data.get("daily_spend", 0)
    change_pct = alert_data.get("change_percent", 0)

    text = (
        f":rotating_light: *Cost Alert: {alert_type.title()}*\n"
        f"Date: {usage_date}\n"
        f"Daily Spend: ${daily_spend:,.2f}\n"
    )
    if change_pct:
        text += f"Change: {change_pct:+.1f}%\n"

    payload = {"text": text}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                return {"success": True}
            return {"success": False, "error": f"Slack returned status {resp.status_code}"}
    except Exception as e:
        logger.error(f"Webhook alert failed: {e}")
        return {"success": False, "error": str(e)}


# ── Telemetry Settings ────────────────────────────────────────────────────

class TelemetrySettings(BaseModel):
    catalog: str = ""
    schema_name: str = ""
    table_prefix: str = ""


def _load_telemetry_settings() -> dict:
    if os.path.exists(TELEMETRY_SETTINGS_FILE):
        try:
            with open(TELEMETRY_SETTINGS_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def _save_telemetry_settings(settings: dict) -> None:
    os.makedirs(SETTINGS_DIR, exist_ok=True)
    with open(TELEMETRY_SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=2)


@router.get("/telemetry")
async def get_telemetry_settings() -> dict[str, Any]:
    """Return current app telemetry destination settings.

    Falls back to the app's own catalog/schema (from env) when nothing is saved,
    so OTel table monitoring works out of the box without requiring manual config.
    """
    from server.db import get_catalog_schema
    stored = _load_telemetry_settings()
    # Use stored values if present, otherwise fall back to the app's catalog/schema
    if not stored.get("catalog"):
        try:
            default_catalog, default_schema = get_catalog_schema()
        except Exception:
            default_catalog, default_schema = "", ""
    else:
        default_catalog = stored["catalog"]
        default_schema = stored.get("schema_name", "")
    return {
        "catalog": stored.get("catalog") or default_catalog,
        "schema_name": stored.get("schema_name") or default_schema,
        "table_prefix": stored.get("table_prefix", ""),
        "is_default": not bool(stored.get("catalog")),  # True = using app default, not custom
    }


@router.post("/telemetry")
async def save_telemetry_settings_endpoint(settings: TelemetrySettings) -> dict[str, Any]:
    """Save app telemetry destination settings."""
    _save_telemetry_settings({
        "catalog": settings.catalog,
        "schema_name": settings.schema_name,
        "table_prefix": settings.table_prefix,
    })
    logger.info("Telemetry settings updated")
    return {"status": "ok"}


# ── Lakebase Status ──────────────────────────────────────────────────────────

@router.get("/lakebase-status")
async def get_lakebase_status() -> dict[str, Any]:
    """Return Lakebase (PostgreSQL) connection status and config."""
    endpoint_name = os.getenv("ENDPOINT_NAME")
    pg_host = os.getenv("PGHOST")
    pg_database = os.getenv("PGDATABASE")
    pg_user = os.getenv("PGUSER") or os.getenv("DATABRICKS_CLIENT_ID")

    configured = all([endpoint_name, pg_host, pg_database])
    if not configured:
        missing = [k for k, v in {
            "ENDPOINT_NAME": endpoint_name,
            "PGHOST": pg_host,
            "PGDATABASE": pg_database,
        }.items() if not v]
        return {"configured": False, "missing_vars": missing}

    # Check if we can actually connect
    connected = False
    try:
        from server.postgres import _get_pool
        pool = _get_pool()
        if pool is not None:
            with pool.connection() as conn:
                conn.execute("SELECT 1")
            connected = True
    except Exception as e:
        logger.warning(f"Lakebase connectivity check failed: {e}")

    return {
        "configured": True,
        "connected": connected,
        "endpoint_name": endpoint_name,
        "host": pg_host,
        "database": pg_database,
        "user": pg_user,
    }


# ── User Permissions ──────────────────────────────────────────────────────────

class UserPermissionsModel(BaseModel):
    admins: list[str] = []
    consumers: list[str] = []


def _permissions_table() -> str:
    """Return the fully-qualified Delta table name for user permissions."""
    from server.db import get_catalog_schema
    catalog, schema = get_catalog_schema()
    return f"`{catalog}`.`{schema}`.`app_user_permissions`"


def _ensure_permissions_table() -> None:
    """Create the permissions table if it doesn't exist."""
    from server.db import execute_write
    table = _permissions_table()
    execute_write(
        f"CREATE TABLE IF NOT EXISTS {table} "
        f"(role STRING NOT NULL, email STRING NOT NULL, "
        f"updated_at TIMESTAMP) "
        f"USING DELTA",
        None,
    )


def _load_user_permissions() -> dict:
    """Load permissions — Lakebase first, then Delta table, then local file."""
    # Primary: Lakebase (persistent PostgreSQL, no Delta permissions needed)
    try:
        from server.postgres import load_permissions as lakebase_load
        result = lakebase_load()
        if result is not None:
            return result
    except Exception as e:
        logger.warning(f"Could not load permissions from Lakebase: {e}")

    # Secondary: Delta table
    try:
        from server.db import execute_query
        _ensure_permissions_table()
        table = _permissions_table()
        rows = execute_query(f"SELECT role, email FROM {table}", None, no_cache=True)
        admins = [r["email"] for r in rows if r.get("role") == "admin"]
        consumers = [r["email"] for r in rows if r.get("role") == "consumer"]
        if admins or consumers:
            logger.info(f"Loaded permissions from Delta table ({len(admins)} admins, {len(consumers)} consumers)")
            return {"admins": admins, "consumers": consumers}
    except Exception as e:
        logger.warning(f"Could not load permissions from Delta table: {e}")

    # Fallback: local file (ephemeral — only useful in dev)
    try:
        if os.path.exists(USER_PERMISSIONS_FILE):
            with open(USER_PERMISSIONS_FILE) as f:
                data = json.load(f)
            return {"admins": data.get("admins", []), "consumers": data.get("consumers", [])}
    except (json.JSONDecodeError, IOError):
        pass
    return {"admins": [], "consumers": []}


def _save_user_permissions_to_table(admins: list[str], consumers: list[str]) -> None:
    """Write permissions to Delta table (replaces all rows)."""
    from server.db import execute_write, clear_query_cache
    # Ensure the table exists before writing. If this raises, the SP lacks
    # CREATE TABLE permission — propagate so the caller gets a clear error.
    _ensure_permissions_table()
    table = _permissions_table()
    execute_write(f"DELETE FROM {table}", None)
    rows = [("admin", e) for e in admins] + [("consumer", e) for e in consumers]
    if rows:
        for role, email in rows:
            execute_write(
                f"INSERT INTO {table} (role, email) VALUES (:role, :email)",
                {"role": role, "email": email},
            )
    # Invalidate cached permission reads so the change is visible immediately
    clear_query_cache("perms")
    logger.info(f"Saved user permissions to Delta table ({len(admins)} admins, {len(consumers)} consumers)")


@router.get("/user-permissions")
async def get_user_permissions() -> dict:
    """Return the admin and consumer user lists."""
    perms = _load_user_permissions()
    try:
        from server.db import get_catalog_schema
        catalog, schema = get_catalog_schema()
        perms["table_location"] = f"{catalog}.{schema}.app_user_permissions"
    except Exception:
        perms["table_location"] = None
    return perms


@router.post("/user-permissions")
async def save_user_permissions(request: Request, data: UserPermissionsModel) -> dict:
    """Save permissions — Lakebase first, Delta table as fallback."""
    _require_admin(request)
    # Primary: Lakebase
    try:
        from server.postgres import save_permissions as lakebase_save
        lakebase_save(data.admins, data.consumers)
        logger.info(f"Permissions saved to Lakebase ({len(data.admins)} admins, {len(data.consumers)} consumers)")
        return {"status": "ok"}
    except Exception as e:
        logger.warning(f"Lakebase permissions save failed, trying Delta table: {e}")

    # Fallback: Delta table
    try:
        _save_user_permissions_to_table(data.admins, data.consumers)
        logger.info(f"Permissions saved to Delta table ({len(data.admins)} admins, {len(data.consumers)} consumers)")
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"All permissions storage backends failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to save permissions — check server logs")


# ── Customer Discounts ────────────────────────────────────────────────────────

_ACCOUNT_PRICES_SQL = """
SELECT
  sku_name,
  cloud,
  currency_code,
  usage_unit,
  pricing.default        AS list_price,
  TRY(pricing.effective_list.default) AS effective_list_price,
  price_start_time       AS start_time,
  price_end_time         AS end_time
FROM system.billing.account_prices
WHERE price_end_time IS NULL
   OR price_end_time > CURRENT_TIMESTAMP
ORDER BY sku_name, cloud
"""

_LIST_PRICES_SQL = """
SELECT
  sku_name,
  cloud,
  currency_code,
  usage_unit,
  pricing.default        AS list_price,
  TRY(pricing.effective_list.default) AS effective_list_price,
  price_start_time       AS start_time,
  price_end_time         AS end_time
FROM system.billing.list_prices
WHERE price_end_time IS NULL
   OR price_end_time > CURRENT_TIMESTAMP
ORDER BY sku_name, cloud
"""


@router.get("/account-prices")
async def get_account_prices() -> dict[str, Any]:
    """Return customer-specific account prices from system.billing.account_prices.

    Falls back to system.billing.list_prices if account_prices is not available
    (the table is currently in private preview).
    """
    from server.db import execute_query as _exec

    _TRANSIENT_ERRORS = ("table", "not found", "does not exist", "cannot resolve", "http_path", "warehouse")

    # Try account_prices first (negotiated rates, private preview)
    try:
        rows = _exec(_ACCOUNT_PRICES_SQL)
        source = "account_prices"
    except Exception as e:
        err = str(e).lower()
        if any(kw in err for kw in _TRANSIENT_ERRORS):
            logger.info(f"system.billing.account_prices not available ({e}), falling back to list_prices")
            try:
                rows = _exec(_LIST_PRICES_SQL)
                source = "list_prices"
            except Exception as e2:
                logger.debug(f"system.billing.list_prices also unavailable: {e2}")
                return {"available": False, "prices": [], "source": None,
                        "message": "Billing price tables not accessible"}
        else:
            logger.warning(f"account_prices query failed: {e}")
            return {"available": False, "prices": [], "source": None, "message": str(e)}

    prices = [
        {
            "sku_name": r.get("sku_name") or "",
            "cloud": r.get("cloud") or "",
            "currency_code": r.get("currency_code") or "USD",
            "usage_unit": r.get("usage_unit") or "DBU",
            "list_price": float(r.get("list_price") or 0),
            "effective_list_price": float(r.get("effective_list_price") or r.get("list_price") or 0),
            "start_time": str(r.get("start_time")) if r.get("start_time") else None,
            "end_time": str(r.get("end_time")) if r.get("end_time") else None,
        }
        for r in rows
    ]
    return {"available": True, "prices": prices, "source": source, "count": len(prices)}


# ── Pricing Mode ──────────────────────────────────────────────────────────────

def _load_pricing_settings() -> dict:
    try:
        with open(PRICING_SETTINGS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"use_account_prices": False}


def _save_pricing_settings(settings: dict) -> None:
    os.makedirs(SETTINGS_DIR, exist_ok=True)
    with open(PRICING_SETTINGS_FILE, "w") as f:
        json.dump(settings, f)


@router.get("/pricing-mode")
async def get_pricing_mode() -> dict[str, Any]:
    """Return the current pricing mode setting."""
    settings = _load_pricing_settings()
    return {
        "use_account_prices": settings.get("use_account_prices", False),
    }


@router.put("/pricing-mode")
async def set_pricing_mode(data: dict) -> dict[str, Any]:
    """Save the pricing mode setting."""
    use_account_prices = bool(data.get("use_account_prices", False))
    _save_pricing_settings({"use_account_prices": use_account_prices})
    return {"use_account_prices": use_account_prices, "status": "ok"}


# Usage-weighted blended account price multiplier query
_ACCOUNT_PRICE_MULTIPLIER_SQL = """
WITH recent_usage AS (
  SELECT
    u.sku_name,
    u.cloud,
    SUM(u.usage_quantity) AS total_quantity
  FROM system.billing.usage u
  WHERE u.usage_date >= CURRENT_DATE - INTERVAL 30 DAY
    AND u.usage_quantity > 0
  GROUP BY u.sku_name, u.cloud
),
price_comparison AS (
  SELECT
    cu.sku_name,
    cu.total_quantity,
    COALESCE(lp.pricing.default, 0)   AS list_price,
    COALESCE(ap.pricing.default, 0)   AS account_price
  FROM recent_usage cu
  LEFT JOIN system.billing.list_prices lp
    ON cu.sku_name = lp.sku_name AND cu.cloud = lp.cloud AND lp.price_end_time IS NULL
  LEFT JOIN system.billing.account_prices ap
    ON cu.sku_name = ap.sku_name AND cu.cloud = ap.cloud AND ap.price_end_time IS NULL
  WHERE lp.pricing.default > 0
    AND ap.pricing.default > 0
)
SELECT
  SUM(total_quantity * account_price) / NULLIF(SUM(total_quantity * list_price), 0) AS multiplier,
  COUNT(DISTINCT sku_name) AS sku_count,
  SUM(total_quantity * list_price)   AS weighted_list_spend,
  SUM(total_quantity * account_price) AS weighted_account_spend
FROM price_comparison
"""


@router.get("/account-price-multiplier")
async def get_account_price_multiplier() -> dict[str, Any]:
    """Compute a usage-weighted blended account price multiplier.

    Returns the ratio of account-negotiated prices to list prices,
    weighted by recent usage quantity. Used by the frontend to scale
    all spend figures when 'use_account_prices' is enabled.

    Returns multiplier=1.0 if account_prices table is unavailable.
    """
    from server.db import execute_query as _exec

    pricing_settings = _load_pricing_settings()
    use_account_prices = pricing_settings.get("use_account_prices", False)

    if not use_account_prices:
        return {"multiplier": 1.0, "available": False, "sku_count": 0, "discount_percent": 0}

    try:
        rows = _exec(_ACCOUNT_PRICE_MULTIPLIER_SQL)
        if not rows or rows[0].get("multiplier") is None:
            return {"multiplier": 1.0, "available": False, "sku_count": 0, "discount_percent": 0}
        row = rows[0]
        multiplier = float(row["multiplier"])
        sku_count = int(row.get("sku_count") or 0)
        discount_percent = round((1.0 - multiplier) * 100, 2)
        return {
            "multiplier": multiplier,
            "available": True,
            "sku_count": sku_count,
            "discount_percent": discount_percent,
            "weighted_list_spend": float(row.get("weighted_list_spend") or 0),
            "weighted_account_spend": float(row.get("weighted_account_spend") or 0),
        }
    except Exception as e:
        err = str(e).lower()
        if any(kw in err for kw in ("table", "not found", "does not exist", "cannot resolve")):
            logger.info("system.billing.account_prices not available for multiplier computation")
            return {"multiplier": 1.0, "available": False, "sku_count": 0, "discount_percent": 0,
                    "message": "system.billing.account_prices not available (private preview)"}
        logger.warning(f"Account price multiplier computation failed: {e}")
        return {"multiplier": 1.0, "available": False, "sku_count": 0, "discount_percent": 0}
