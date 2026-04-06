"""Databricks SQL connection factory."""

import hashlib
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Callable, Generator

# Per-request user token set by UserAuthMiddleware when x-forwarded-access-token
# is present (Databricks Apps user authorization preview). Empty string = use SP.
_user_token: ContextVar[str] = ContextVar("_user_token", default="")

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
    (e.g. 'fevm-cmegdemos.cloud.databricks.com') without a protocol prefix,
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


def get_catalog_schema() -> tuple[str, str]:
    """Get the catalog and schema for cost observability tables from environment."""
    catalog = os.getenv("COST_OBS_CATALOG", "main")
    schema = os.getenv("COST_OBS_SCHEMA", "cost_obs")
    return catalog, schema


# Dedicated warehouse configuration
DEDICATED_WAREHOUSE_NAME = "Cost Observability App"
DEDICATED_WAREHOUSE_SIZE = "Large"  # Large for 14+ parallel queries
DEDICATED_WAREHOUSE_MIN_CLUSTERS = 1
DEDICATED_WAREHOUSE_MAX_CLUSTERS = 2
DEDICATED_WAREHOUSE_AUTO_STOP_MINS = 10

# Bounded TTL cache for query results (2 hour TTL, max 500 entries, ~1GB limit)
# Using cachetools.TTLCache to prevent unbounded memory growth
_CACHE_MAX_SIZE = 500  # Max number of cached queries
_CACHE_TTL = 4 * 60 * 60  # 4 hours - cost data doesn't change intra-day
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
            del _query_cache[key]
        logger.info(f"Cleared {len(keys_to_clear)} cache entries matching '{pattern}'")
        return len(keys_to_clear)

# Singleton WorkspaceClient instance
_workspace_client: WorkspaceClient | None = None


def get_workspace_client() -> WorkspaceClient:
    """Get or create a singleton WorkspaceClient instance.

    This prevents creating a new client on every request, which is expensive.
    The client is thread-safe and can be shared across requests.
    """
    global _workspace_client

    if _workspace_client is None:
        token = os.getenv("DATABRICKS_TOKEN")
        host = os.getenv("DATABRICKS_HOST")

        if token and host:
            # Local development with explicit credentials
            _workspace_client = WorkspaceClient(host=host, token=token)
        else:
            # Databricks App environment - use default auth
            _workspace_client = WorkspaceClient()

        logger.info("Created WorkspaceClient singleton")

    return _workspace_client


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


def _load_saved_warehouse_http_path() -> str:
    """Read the warehouse HTTP path persisted by the settings UI, if any."""
    import json
    settings_file = os.path.join(
        os.path.dirname(__file__), "..", ".settings", "warehouse_settings.json"
    )
    try:
        with open(settings_file) as f:
            data = json.load(f)
        return data.get("http_path", "")
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return ""


def setup_warehouse_connection() -> str:
    """Set up the warehouse connection for the app.

    Priority:
    1. DATABRICKS_HTTP_PATH env var (explicit config in app.yaml)
    2. Warehouse saved via the in-app settings UI (warehouse_settings.json)
    3. Auto-create/find a dedicated warehouse (last resort)

    Returns:
        The HTTP path being used
    """
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")

    # Fall back to warehouse saved via the in-app settings UI
    if not http_path or http_path.lower() == "auto":
        saved = _load_saved_warehouse_http_path()
        if saved:
            os.environ["DATABRICKS_HTTP_PATH"] = saved
            logger.info(f"Restored warehouse from saved settings: {saved}")
            return saved

    # If no HTTP path or set to 'auto', try to create/use a dedicated warehouse
    if not http_path or http_path.lower() == "auto":
        logger.info("DATABRICKS_HTTP_PATH not set or set to 'auto' - attempting dedicated warehouse")
        try:
            warehouse_id, http_path = ensure_dedicated_warehouse()
            os.environ["DATABRICKS_HTTP_PATH"] = http_path
            logger.info(f"Set DATABRICKS_HTTP_PATH to: {http_path}")
            return http_path
        except Exception as e:
            logger.error(
                f"Failed to create/find dedicated warehouse: {e}. "
                "This typically happens when running as a Databricks App service principal "
                "without warehouse creation permissions. "
                "Set DATABRICKS_HTTP_PATH to an explicit warehouse path "
                "(e.g. /sql/1.0/warehouses/<id>) in app.yaml env vars."
            )
            raise ValueError(
                "DATABRICKS_HTTP_PATH is set to 'auto' but warehouse auto-creation failed. "
                "Set DATABRICKS_HTTP_PATH to an explicit warehouse path in app.yaml."
            ) from e
    else:
        logger.info(f"Using configured warehouse: {http_path}")
        return http_path


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
        raise ValueError("Missing DATABRICKS_HTTP_PATH environment variable.")

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

    with get_connection() as conn:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # rowcount gives the number of affected rows for DML statements
            affected_rows = cursor.rowcount if cursor.rowcount is not None else 0

    elapsed = time.time() - start_time
    logger.info(f"Write query executed in {elapsed:.2f}s ({affected_rows} rows affected)")
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
        cache_key = _get_cache_key(query, params, tag=cache_tag)
        if cache_key in _query_cache:
            logger.info(f"Cache hit - returned in {(time.time() - start_time)*1000:.0f}ms")
            return _query_cache[cache_key]

    # Execute query with proper parameterized queries to prevent SQL injection
    with get_connection() as conn:
        with conn.cursor() as cursor:
            if params:
                # Use Databricks SQL connector's native parameter binding
                # Pass parameters as second positional argument (DB-API 2.0 standard)
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if cursor.description is not None:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                result = [dict(zip(columns, row)) for row in rows]
            else:
                result = []  # DDL statement (CREATE/DROP/etc.) — no result set

    # Cache the result (TTLCache handles expiration automatically)
    if not no_cache:
        cache_key = _get_cache_key(query, params, tag=cache_tag)
        _query_cache[cache_key] = result
    elapsed = time.time() - start_time
    logger.info(f"Query executed in {elapsed:.2f}s ({len(result)} rows)")
    return result


def execute_queries_parallel(
    query_funcs: list[tuple[str, Callable[[], list[dict[str, Any]]]]]
) -> dict[str, list[dict[str, Any]] | None]:
    """Execute multiple queries in parallel using ThreadPoolExecutor.

    Args:
        query_funcs: List of (name, lambda) tuples where lambda executes the query

    Returns:
        Dictionary mapping query names to results

    Example:
        queries = [
            ("summary", lambda: execute_query(SUMMARY_QUERY, params)),
            ("products", lambda: execute_query(PRODUCTS_QUERY, params)),
        ]
        results = execute_queries_parallel(queries)
        summary_data = results["summary"]
    """
    start_time = time.time()
    results: dict[str, list[dict[str, Any]] | None] = {}

    # Use ThreadPoolExecutor for parallel execution
    # Max 6 workers to avoid overwhelming the warehouse
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all queries
        future_to_name = {
            executor.submit(func): name
            for name, func in query_funcs
        }

        # Collect results as they complete
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            query_start = time.time()
            try:
                results[name] = future.result()
                query_elapsed = time.time() - query_start
                logger.info(f"✓ {name}: {query_elapsed:.2f}s")
            except Exception as e:
                logger.error(f"✗ {name} failed: {e}")
                results[name] = None

    total_elapsed = time.time() - start_time
    logger.info(f"Parallel execution completed: {total_elapsed:.2f}s total ({len(results)} queries)")

    return results
