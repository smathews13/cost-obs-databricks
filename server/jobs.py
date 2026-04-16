"""Databricks job management for Cost Observability dashboard.

This module handles creating and managing the scheduled job that refreshes
the materialized views daily.
"""

import logging
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    CronSchedule,
    JobSettings,
    PauseStatus,
    Task,
    SqlTask,
    SqlTaskFile,
    Source,
)
from databricks.sdk.service.workspace import ImportFormat, Language

from server.db import get_workspace_client

logger = logging.getLogger(__name__)

JOB_NAME = "cost-obs-refresh-materialized-views"
SQL_FILE_PATH = "/Workspace/Shared/cost-obs/refresh_materialized_views.sql"


def get_refresh_sql(lookback_days: int = 730) -> str:
    """Get the SQL to refresh all materialized views."""
    from server.materialized_views import (
        CREATE_DAILY_USAGE_SUMMARY,
        CREATE_DAILY_PRODUCT_BREAKDOWN,
        CREATE_DAILY_WORKSPACE_BREAKDOWN,
        CREATE_SQL_TOOL_ATTRIBUTION,
        CREATE_QUERY_STATS,
        CREATE_DBSQL_COST_PER_QUERY,
        CREATE_DBSQL_COST_PER_QUERY_PRPR,
        CREATE_SCHEMA_SQL,
        get_catalog_schema,
    )

    catalog, schema = get_catalog_schema()
    fmt = dict(catalog=catalog, schema=schema, billing_lookback_days=lookback_days)

    sql_statements = [
        f"-- Cost Observability Materialized Views Refresh",
        f"-- Catalog: {catalog}, Schema: {schema}",
        f"-- This job runs daily to refresh pre-aggregated tables",
        "",
        CREATE_SCHEMA_SQL.format(**fmt),
        "",
        CREATE_DAILY_USAGE_SUMMARY.format(**fmt),
        "",
        CREATE_DAILY_PRODUCT_BREAKDOWN.format(**fmt),
        "",
        CREATE_DAILY_WORKSPACE_BREAKDOWN.format(**fmt),
        "",
        CREATE_SQL_TOOL_ATTRIBUTION.format(**fmt),
        "",
        CREATE_QUERY_STATS.format(**fmt),
        "",
        CREATE_DBSQL_COST_PER_QUERY.format(**fmt),
        "",
        CREATE_DBSQL_COST_PER_QUERY_PRPR.format(**fmt),
    ]

    return ";\n".join(sql_statements)


def upload_sql_file(w: WorkspaceClient, sql_content: str) -> str:
    """Upload the SQL file to the workspace."""
    import base64

    # Ensure directory exists
    dir_path = "/".join(SQL_FILE_PATH.split("/")[:-1])
    try:
        w.workspace.mkdirs(dir_path)
    except Exception:
        pass  # Directory may already exist

    # Upload the SQL file
    w.workspace.import_(
        path=SQL_FILE_PATH,
        content=base64.b64encode(sql_content.encode()).decode(),
        format=ImportFormat.AUTO,
        overwrite=True,
    )
    logger.info(f"Uploaded SQL file to {SQL_FILE_PATH}")
    return SQL_FILE_PATH


def find_existing_job(w: WorkspaceClient) -> int | None:
    """Find existing refresh job by name, return job_id or None."""
    try:
        jobs = w.jobs.list(name=JOB_NAME)
        for job in jobs:
            if job.settings and job.settings.name == JOB_NAME:
                return job.job_id
    except Exception as e:
        logger.debug(f"Error searching for existing job: {e}")
    return None


def get_warehouse_id() -> str | None:
    """Get the SQL warehouse ID from environment."""
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
    # Extract warehouse ID from path like /sql/1.0/warehouses/abc123
    if "/warehouses/" in http_path:
        return http_path.split("/warehouses/")[-1]
    return None


def create_or_update_refresh_job() -> dict:
    """Create or update the daily refresh job.

    Returns:
        Dict with job_id and status
    """
    try:
        w = get_workspace_client()
        warehouse_id = get_warehouse_id()

        if not warehouse_id:
            logger.warning("No SQL warehouse configured, skipping job creation")
            return {"status": "skipped", "reason": "No SQL warehouse configured"}

        # Get the refresh SQL and upload to workspace
        refresh_sql = get_refresh_sql()
        sql_file_path = upload_sql_file(w, refresh_sql)

        # Check for existing job
        existing_job_id = find_existing_job(w)

        # Job settings - use SqlTaskFile to reference the uploaded SQL
        job_settings = JobSettings(
            name=JOB_NAME,
            description="Daily refresh of Cost Observability materialized views. "
                       "This job pre-aggregates billing and query data for fast dashboard queries.",
            schedule=CronSchedule(
                quartz_cron_expression="0 0 6 * * ?",  # 6 AM UTC daily
                timezone_id="UTC",
                pause_status=PauseStatus.UNPAUSED,
            ),
            tasks=[
                Task(
                    task_key="refresh_materialized_views",
                    description="Refresh all cost observability materialized view tables",
                    sql_task=SqlTask(
                        warehouse_id=warehouse_id,
                        file=SqlTaskFile(
                            path=sql_file_path,
                            source=Source.WORKSPACE,
                        ),
                    ),
                ),
            ],
            max_concurrent_runs=1,
            timeout_seconds=3600,  # 1 hour timeout
        )

        if existing_job_id:
            # Update existing job
            w.jobs.update(job_id=existing_job_id, new_settings=job_settings)
            logger.info(f"Updated existing refresh job: {JOB_NAME} (ID: {existing_job_id})")
            return {"status": "updated", "job_id": existing_job_id, "job_name": JOB_NAME}
        else:
            # Create new job
            job = w.jobs.create(
                name=JOB_NAME,
                description=job_settings.description,
                schedule=job_settings.schedule,
                tasks=job_settings.tasks,
                max_concurrent_runs=job_settings.max_concurrent_runs,
                timeout_seconds=job_settings.timeout_seconds,
            )
            logger.info(f"Created refresh job: {JOB_NAME} (ID: {job.job_id})")
            return {"status": "created", "job_id": job.job_id, "job_name": JOB_NAME}

    except Exception as e:
        logger.error(f"Failed to create/update refresh job: {e}")
        return {"status": "error", "error": str(e)}


def run_refresh_job_now() -> dict:
    """Trigger the refresh job to run immediately.

    Returns:
        Dict with run_id and status
    """
    try:
        w = get_workspace_client()
        job_id = find_existing_job(w)

        if not job_id:
            # Create the job first
            result = create_or_update_refresh_job()
            if result.get("status") == "error":
                return result
            job_id = result.get("job_id")

        if job_id:
            run = w.jobs.run_now(job_id=job_id)
            logger.info(f"Started refresh job run: {run.run_id}")
            return {"status": "started", "run_id": run.run_id, "job_id": job_id}
        else:
            return {"status": "error", "error": "Could not find or create job"}

    except Exception as e:
        logger.error(f"Failed to run refresh job: {e}")
        return {"status": "error", "error": str(e)}


def get_job_status() -> dict:
    """Get the status of the refresh job.

    Returns:
        Dict with job status and recent runs
    """
    try:
        w = get_workspace_client()
        job_id = find_existing_job(w)

        if not job_id:
            return {"status": "not_found", "job_name": JOB_NAME}

        job = w.jobs.get(job_id=job_id)

        # Get recent runs
        runs = list(w.jobs.list_runs(job_id=job_id, limit=5))
        recent_runs = []
        for run in runs:
            recent_runs.append({
                "run_id": run.run_id,
                "state": run.state.life_cycle_state.value if run.state else None,
                "result": run.state.result_state.value if run.state and run.state.result_state else None,
                "start_time": run.start_time,
                "end_time": run.end_time,
            })

        return {
            "status": "found",
            "job_id": job_id,
            "job_name": JOB_NAME,
            "schedule": job.settings.schedule.quartz_cron_expression if job.settings and job.settings.schedule else None,
            "paused": job.settings.schedule.pause_status.value if job.settings and job.settings.schedule else None,
            "recent_runs": recent_runs,
        }

    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        return {"status": "error", "error": str(e)}
