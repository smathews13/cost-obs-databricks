"""Cost Observability & Control (COC) - FastAPI Application"""

import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import date, timedelta

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from server.routers import aiml, alerts, apps, aws_actual, azure_actual, billing, dbsql, dbsql_prpr, genie, health, permissions, query_origin, reconciliation, settings, setup, tagging, use_cases, user, users_groups, warehouse_health

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware for request/response logging with correlation IDs."""

    async def dispatch(self, request: Request, call_next):
        # Generate request ID for correlation
        request_id = str(uuid.uuid4())[:8]
        start_time = time.time()

        # Log incoming request
        logger.info(
            f"[{request_id}] → {request.method} {request.url.path} "
            f"(client: {request.client.host if request.client else 'unknown'})"
        )

        # Process request
        response = await call_next(request)

        # Calculate duration
        duration_ms = (time.time() - start_time) * 1000

        # Log response
        logger.info(
            f"[{request_id}] ← {response.status_code} in {duration_ms:.0f}ms"
        )

        # Add request ID to response headers for debugging
        response.headers["X-Request-ID"] = request_id

        return response


def setup_and_check_warehouse():
    """Set up dedicated warehouse and log configuration.

    This function:
    1. Creates a dedicated Large serverless warehouse if needed (when DATABRICKS_HTTP_PATH is 'auto' or not set)
    2. Uses an existing warehouse if DATABRICKS_HTTP_PATH is configured
    3. Logs the warehouse configuration for verification
    """
    try:
        from server.db import setup_warehouse_connection, get_workspace_client

        # Set up the warehouse connection (creates dedicated warehouse if needed)
        http_path = setup_warehouse_connection()

        # Extract warehouse ID from HTTP path
        warehouse_id = http_path.split("/")[-1] if http_path else None

        if warehouse_id:
            try:
                w = get_workspace_client()
                warehouse = w.warehouses.get(warehouse_id)

                # Log warehouse configuration
                logger.info("=" * 60)
                logger.info("SQL Warehouse Configuration")
                logger.info("=" * 60)
                logger.info(f"  Name: {warehouse.name}")
                logger.info(f"  ID: {warehouse.id}")
                logger.info(f"  Size: {warehouse.cluster_size}")
                logger.info(f"  Type: {'Serverless' if warehouse.enable_serverless_compute else 'Pro'}")
                logger.info(f"  Min Clusters: {warehouse.min_num_clusters}")
                logger.info(f"  Max Clusters: {warehouse.max_num_clusters}")
                logger.info(f"  State: {warehouse.state}")
                logger.info(f"  Auto-Stop: {warehouse.auto_stop_mins} minutes")
                logger.info("=" * 60)

                # Check if warehouse is undersized for 14+ parallel queries
                recommended_size = "Large"
                size_order = ["2X-Small", "X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"]
                current_idx = size_order.index(warehouse.cluster_size) if warehouse.cluster_size in size_order else -1
                recommended_idx = size_order.index(recommended_size) if recommended_size in size_order else 4

                if current_idx < recommended_idx:
                    logger.warning(
                        f"⚠️  Warehouse '{warehouse.name}' is sized {warehouse.cluster_size}. "
                        f"Recommended: {recommended_size} or larger for optimal performance with 14+ parallel queries."
                    )
                else:
                    logger.info(f"✓ Warehouse size {warehouse.cluster_size} meets recommended size ({recommended_size})")

            except Exception as e:
                logger.warning(f"Could not fetch warehouse details: {e}")
        else:
            logger.warning("No warehouse ID found in DATABRICKS_HTTP_PATH")

    except Exception as e:
        logger.error(f"Warehouse setup failed: {e}")
        raise  # This is critical - we can't proceed without a warehouse


def setup_materialized_views():
    """Create materialized views if they don't exist."""
    try:
        from server.materialized_views import (
            check_materialized_views_exist,
            create_materialized_views,
            get_catalog_schema,
        )

        catalog, schema = get_catalog_schema()
        logger.info(f"Checking materialized views in {catalog}.{schema}...")

        # Check if tables exist
        tables = check_materialized_views_exist(catalog, schema)
        missing = [name for name, exists in tables.items() if not exists]

        if missing:
            logger.info(f"Creating missing materialized views: {missing}")
            results = create_materialized_views(catalog, schema)
            success = sum(1 for v in results.values() if v == "created")
            logger.info(f"Materialized views setup complete: {success}/{len(results)} tables created")
        else:
            logger.info("All materialized views already exist")

    except Exception as e:
        logger.warning(f"Materialized views setup failed (non-fatal): {e}")


def prewarm_cache_sync():
    """Pre-warm the query cache with common queries on startup (synchronous)."""
    try:
        from server.db import execute_query, execute_queries_parallel
        from server.queries import (
            BILLING_SUMMARY,
            BILLING_BY_PRODUCT_FAST,
            BILLING_BY_WORKSPACE,
            BILLING_TIMESERIES_FAST,
            ETL_BREAKDOWN,
        )

        # Default 30-day range
        params = {
            "start_date": (date.today() - timedelta(days=30)).isoformat(),
            "end_date": date.today().isoformat(),
        }

        logger.info("Pre-warming cache with default 30-day queries...")

        # Run fast queries in parallel to warm cache
        queries = [
            ("summary", lambda: execute_query(BILLING_SUMMARY, params)),
            ("products", lambda: execute_query(BILLING_BY_PRODUCT_FAST, params)),
            ("workspaces", lambda: execute_query(BILLING_BY_WORKSPACE, params)),
            ("timeseries", lambda: execute_query(BILLING_TIMESERIES_FAST, params)),
            ("etl", lambda: execute_query(ETL_BREAKDOWN, params)),
        ]

        results = execute_queries_parallel(queries)
        success_count = sum(1 for v in results.values() if v is not None)
        logger.info(f"Cache pre-warming complete: {success_count}/{len(queries)} queries cached")

    except Exception as e:
        logger.warning(f"Cache pre-warming failed (non-fatal): {e}")


def prewarm_all_tabs():
    """Pre-warm cache for ALL tabs (runs in background after initial prewarm)."""
    try:
        from server.db import execute_query, execute_queries_parallel
        from server.routers.tagging import (
            TAGGING_SUMMARY, UNTAGGED_CLUSTERS, UNTAGGED_JOBS,
            UNTAGGED_PIPELINES, UNTAGGED_WAREHOUSES, UNTAGGED_ENDPOINTS,
            COST_BY_TAG, COST_BY_TAG_KEY, TAG_COVERAGE_TIMESERIES,
        )
        from server.routers.aiml import (
            AIML_SUMMARY, FMAPI_PROVIDER_COSTS, SERVERLESS_INFERENCE_BY_ENDPOINT,
            AIML_BY_CATEGORY, AIML_TIMESERIES,
        )
        from server.routers.use_cases import router as use_cases_router
        from server.routers.query_origin import (
            _SUMMARY_SQL, _SUMMARY_SQL_NO_COST,
            _TIMESERIES_SQL, _TIMESERIES_SQL_NO_COST,
            _BY_WAREHOUSE_SQL, _BY_WAREHOUSE_SQL_NO_COST,
        )
        from server.db import get_catalog_schema

        params = {
            "start_date": (date.today() - timedelta(days=30)).isoformat(),
            "end_date": date.today().isoformat(),
        }

        logger.info("Pre-warming ALL tabs cache in background...")

        # Query origin — pre-warm all endpoints (system.query.history × dbsql_cost_per_query can be slow)
        catalog, schema = get_catalog_schema()
        origin_queries = [
            ("summary", _SUMMARY_SQL.format(catalog=catalog, schema=schema), _SUMMARY_SQL_NO_COST),
            ("timeseries", _TIMESERIES_SQL.format(catalog=catalog, schema=schema), _TIMESERIES_SQL_NO_COST),
            ("by_warehouse", _BY_WAREHOUSE_SQL.format(catalog=catalog, schema=schema), _BY_WAREHOUSE_SQL_NO_COST),
        ]
        for name, sql_cost, sql_no_cost in origin_queries:
            try:
                execute_query(sql_cost, params)
                logger.info(f"Pre-warmed query origin {name} (with cost)")
            except Exception:
                try:
                    execute_query(sql_no_cost, params)
                    logger.info(f"Pre-warmed query origin {name} (no cost fallback)")
                except Exception as e:
                    logger.warning(f"Query origin {name} pre-warm failed (non-fatal): {e}")

        # Tagging queries
        tagging_queries = [
            ("tag_summary", lambda: execute_query(TAGGING_SUMMARY, params)),
            ("tag_clusters", lambda: execute_query(UNTAGGED_CLUSTERS, params)),
            ("tag_jobs", lambda: execute_query(UNTAGGED_JOBS, params)),
            ("tag_pipelines", lambda: execute_query(UNTAGGED_PIPELINES, params)),
            ("tag_warehouses", lambda: execute_query(UNTAGGED_WAREHOUSES, params)),
            ("tag_endpoints", lambda: execute_query(UNTAGGED_ENDPOINTS, params)),
            ("tag_cost_by_tag", lambda: execute_query(COST_BY_TAG, params)),
            ("tag_keys", lambda: execute_query(COST_BY_TAG_KEY, params)),
            ("tag_timeseries", lambda: execute_query(TAG_COVERAGE_TIMESERIES, params)),
        ]

        # AI/ML queries
        aiml_queries = [
            ("aiml_summary", lambda: execute_query(AIML_SUMMARY, params)),
            ("aiml_providers", lambda: execute_query(FMAPI_PROVIDER_COSTS, params)),
            ("aiml_endpoints", lambda: execute_query(SERVERLESS_INFERENCE_BY_ENDPOINT, params)),
            ("aiml_categories", lambda: execute_query(AIML_BY_CATEGORY, params)),
            ("aiml_timeseries", lambda: execute_query(AIML_TIMESERIES, params)),
        ]

        # Run all queries in parallel
        all_queries = tagging_queries + aiml_queries
        results = execute_queries_parallel(all_queries)
        success_count = sum(1 for v in results.values() if v is not None)
        logger.info(f"Background cache pre-warming complete: {success_count}/{len(all_queries)} queries cached")

    except Exception as e:
        logger.warning(f"Background cache pre-warming failed (non-fatal): {e}")


def startup_tasks():
    """Run all startup tasks: setup warehouse, setup MVs, create job, warm cache, setup alerts."""
    # Restore saved warehouse preference (if user previously switched warehouses)
    current_http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
    if current_http_path and current_http_path != "auto":
        try:
            from server.routers.settings import _load_warehouse_settings
            saved = _load_warehouse_settings()
            saved_http_path = saved.get("http_path")
            if saved_http_path and saved_http_path != current_http_path:
                os.environ["DATABRICKS_HTTP_PATH"] = saved_http_path
                logger.info(f"Restored saved warehouse preference: {saved.get('warehouse_name', saved_http_path)}")
        except Exception as e:
            logger.warning(f"Could not restore warehouse preference (non-fatal): {e}")

    # Step 0: Set up dedicated warehouse (creates Large serverless warehouse if needed)
    setup_and_check_warehouse()

    # Step 1: Create materialized views if needed
    setup_materialized_views()

    # Step 2: Create/update refresh job
    # File lock prevents 4 uvicorn workers from all creating duplicate jobs simultaneously.
    try:
        import fcntl
        from server.jobs import create_or_update_refresh_job
        lock_path = "/tmp/cost-obs-job-setup.lock"
        with open(lock_path, "w") as lock_file:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                create_or_update_refresh_job()
            except BlockingIOError:
                logger.info("Job setup already running in another worker — skipping")
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
    except Exception as e:
        logger.warning(f"Job creation failed (non-fatal): {e}")

    # Step 3: Pre-warm cache (billing - fast queries first)
    prewarm_cache_sync()

    # Step 4: Create default cost monitoring alerts
    # Use a file lock so only one uvicorn worker runs this — all workers share
    # the same filesystem, so fcntl.flock prevents the race that creates duplicates.
    try:
        import fcntl
        from server.alert_manager import create_default_cost_alerts
        lock_path = "/tmp/cost-obs-alert-setup.lock"
        with open(lock_path, "w") as lock_file:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                logger.info("Setting up default cost monitoring alerts...")
                results = create_default_cost_alerts(
                    spike_threshold_percent=20.0,
                    daily_threshold_amount=50000.0,
                    workspace_threshold_amount=10000.0
                )
                logger.info(
                    f"Alert setup complete: {len(results['created'])} created, "
                    f"{len(results['skipped'])} skipped, {len(results['errors'])} errors"
                )
            except BlockingIOError:
                logger.info("Alert setup already running in another worker — skipping")
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
    except Exception as e:
        logger.warning(f"Alert setup failed (non-fatal): {e}")

    # Step 5: Create default example use case
    try:
        from server.routers.use_cases import create_default_use_case
        logger.info("Setting up default example use case...")
        uc_result = create_default_use_case()
        if uc_result["created"]:
            logger.info("Default use case created successfully")
        elif uc_result["skipped"]:
            logger.info("Default use case already exists, skipped")
        elif uc_result["error"]:
            logger.warning(f"Default use case creation failed: {uc_result['error']}")
    except Exception as e:
        logger.warning(f"Use case setup failed (non-fatal): {e}")

    # Step 6: Pre-warm permissions check (warms SDK auth + caches result for wizard)
    try:
        from server.routers.permissions import _check_permissions_sync
        logger.info("Pre-warming permissions check...")
        _check_permissions_sync()
        logger.info("Permissions pre-warm complete")
    except Exception as e:
        logger.warning(f"Permissions pre-warm failed (non-fatal): {e}")

    # Step 7: Pre-warm ALL tabs (slower queries, runs after alerts)
    prewarm_all_tabs()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Run all startup tasks in background thread (don't block startup)
    asyncio.get_event_loop().run_in_executor(None, startup_tasks)
    yield


app = FastAPI(
    title="Cost Observability & Control (COC)",
    description="Cost observability and analytics control dashboard",
    version="0.1.0",
    lifespan=lifespan,
)

# Request logging middleware
app.add_middleware(RequestLoggingMiddleware)

# CORS configuration - externalized for production
# Set CORS_ORIGINS env var for production (comma-separated list of origins)
cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173")
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins.split(","),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With", "X-Forwarded-Email"],
)

# Include routers
app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(user.router, prefix="/api/user", tags=["user"])
app.include_router(billing.router, prefix="/api/billing", tags=["billing"])
app.include_router(genie.router, prefix="/api/genie", tags=["genie"])
app.include_router(setup.router, prefix="/api/setup", tags=["setup"])
app.include_router(aiml.router, prefix="/api/aiml", tags=["aiml"])
app.include_router(apps.router, prefix="/api/apps", tags=["apps"])
app.include_router(tagging.router, prefix="/api/tagging", tags=["tagging"])
app.include_router(aws_actual.router, prefix="/api/aws-actual", tags=["aws-actual"])
app.include_router(azure_actual.router, prefix="/api/azure-actual", tags=["azure-actual"])
app.include_router(dbsql.router, prefix="/api/dbsql", tags=["dbsql"])
app.include_router(dbsql_prpr.router, prefix="/api/dbsql-prpr", tags=["dbsql-prpr"])
app.include_router(alerts.router, prefix="/api/alerts", tags=["alerts"])
app.include_router(use_cases.router, prefix="/api/use-cases", tags=["use-cases"])
app.include_router(permissions.router, prefix="/api/permissions", tags=["permissions"])
app.include_router(settings.router, prefix="/api/settings", tags=["settings"])
app.include_router(reconciliation.router, prefix="/api/reconciliation", tags=["reconciliation"])
app.include_router(users_groups.router, prefix="/api/users-groups", tags=["users-groups"])
app.include_router(query_origin.router, prefix="/api/sql/query-origin", tags=["query-origin"])
app.include_router(warehouse_health.router, prefix="/api/sql/warehouse-health", tags=["warehouse-health"])

# Serve static files in production
static_dir = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.exists(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
