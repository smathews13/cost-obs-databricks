"""
GCP Actual Costs Router — queries GCP billing export data via Lakehouse Federation.

Mirrors aws_actual.py / azure_actual.py but for GCP Cloud Billing export data
federated from BigQuery into Unity Catalog via a BigQuery connection.

Setup:
  1. In Unity Catalog, create a BigQuery connection:
       CREATE CONNECTION gcp_billing TYPE BIGQUERY
       OPTIONS (credentials '<service_account_key_json>');
  2. Create a foreign catalog pointing at the billing dataset:
       CREATE FOREIGN CATALOG gcp_billing_catalog
       USING CONNECTION gcp_billing
       OPTIONS (dataProjectId '<project_id>');
  3. Set env vars on the Databricks App and redeploy:
       GCP_COST_CATALOG=gcp_billing_catalog
       GCP_COST_SCHEMA=<bigquery_billing_dataset>   (e.g. all_billing_data)
       GCP_COST_TABLE=gcp_billing_export_v1_<billing_account_id>

If GCP_COST_TABLE is omitted, the router discovers a single standard export
table whose name starts with `gcp_billing_export_v1_`. Curated flat gold tables
are not accepted by this raw-export adapter.
"""

import asyncio
import logging
import os
import re
import time
from typing import Any

from fastapi import APIRouter, Query

from server.db import execute_query
from server.request_limits import default_date_range, validate_date_range

logger = logging.getLogger(__name__)
router = APIRouter()

_gcp_status_cache: dict[str, Any] = {
    "available": None,
    "checked_at": 0,
    "table": None,
    "reason": None,
    "message": None,
}
_GCP_STATUS_TTL = 300  # 5 minutes


def get_catalog_schema_table() -> tuple[str, str, str]:
    """Get catalog, schema, and table from environment or defaults.

    Defaults assume a Lakehouse Federation foreign catalog over the BigQuery
    billing export dataset. Override via env vars for a curated Delta table.
    """
    catalog = os.environ.get("GCP_COST_CATALOG", "billing")
    schema = os.environ.get("GCP_COST_SCHEMA", "gcp")
    table = os.environ.get("GCP_COST_TABLE", "")
    return catalog, schema, table


# ── SQL templates ─────────────────────────────────────────────────────────────
# These match the standard GCP Cloud Billing export schema (v1):
#   https://cloud.google.com/billing/docs/how-to/export-data-bigquery-tables/standard-usage
#
# Key columns:
#   usage_start_time / usage_end_time  TIMESTAMP
#   service.description                STRING  (e.g. "Compute Engine")
#   sku.description                    STRING  (e.g. "N1 Predefined Instance Core")
#   project.id                         STRING
#   cost                               FLOAT64 (in billing currency)
#   currency                           STRING
#   labels                             ARRAY<STRUCT<key STRING, value STRING>>
#   resource.name                      STRING  (instance/cluster identifier)
#   resource.global_name               STRING
# ─────────────────────────────────────────────────────────────────────────────

CHECK_GCP_TABLE = """
SELECT table_name
FROM {catalog}.information_schema.tables
WHERE table_schema = :schema
  AND table_name = :table
LIMIT 1
"""

DISCOVER_GCP_TABLE = """
SELECT table_name
FROM {catalog}.information_schema.tables
WHERE table_schema = :schema
  AND table_name LIKE 'gcp_billing_export_v1_%'
ORDER BY table_name
LIMIT 2
"""

GCP_NET_COST_SQL = """(
  COALESCE(cost, 0)
  + COALESCE(
      AGGREGATE(
        credits,
        CAST(0.0 AS DOUBLE),
        (credit_total, credit) ->
          credit_total + COALESCE(CAST(credit.amount AS DOUBLE), 0.0)
      ),
      0.0
    )
)"""

GCP_ACTUAL_SUMMARY = """
SELECT
  SUM(/* GCP_NET_COST */)                AS total_cost,
  currency                               AS currency,
  COUNT(DISTINCT project.id)             AS project_count,
  COUNT(DISTINCT service.description)    AS service_count,
  COUNT(DISTINCT DATE(usage_start_time)) AS days_in_range
FROM `{catalog}`.`{schema}`.`{table}`
WHERE DATE(usage_start_time) >= :start_date
  AND DATE(usage_start_time) <= :end_date
GROUP BY currency
ORDER BY total_cost DESC
LIMIT 1
""".replace("/* GCP_NET_COST */", GCP_NET_COST_SQL)

GCP_COSTS_BY_SERVICE = """
SELECT
  service.description                     AS service,
  SUM(/* GCP_NET_COST */)                 AS total_cost,
  COUNT(DISTINCT DATE(usage_start_time))  AS days_active
FROM `{catalog}`.`{schema}`.`{table}`
WHERE DATE(usage_start_time) >= :start_date
  AND DATE(usage_start_time) <= :end_date
GROUP BY service.description
ORDER BY total_cost DESC
LIMIT 50
""".replace("/* GCP_NET_COST */", GCP_NET_COST_SQL)

GCP_COSTS_BY_PROJECT = """
SELECT
  project.id                              AS project_id,
  project.name                            AS project_name,
  SUM(/* GCP_NET_COST */)                 AS total_cost,
  COUNT(DISTINCT service.description)     AS service_count
FROM `{catalog}`.`{schema}`.`{table}`
WHERE DATE(usage_start_time) >= :start_date
  AND DATE(usage_start_time) <= :end_date
GROUP BY project.id, project.name
ORDER BY total_cost DESC
LIMIT 50
""".replace("/* GCP_NET_COST */", GCP_NET_COST_SQL)

GCP_COSTS_BY_SKU = """
SELECT
  service.description  AS service,
  sku.description      AS sku,
  SUM(/* GCP_NET_COST */) AS total_cost
FROM `{catalog}`.`{schema}`.`{table}`
WHERE DATE(usage_start_time) >= :start_date
  AND DATE(usage_start_time) <= :end_date
GROUP BY service.description, sku.description
ORDER BY total_cost DESC
LIMIT 100
""".replace("/* GCP_NET_COST */", GCP_NET_COST_SQL)

GCP_COSTS_TIMESERIES = """
SELECT
  DATE(usage_start_time) AS date,
  service.description    AS service,
  SUM(/* GCP_NET_COST */) AS daily_cost
FROM `{catalog}`.`{schema}`.`{table}`
WHERE DATE(usage_start_time) >= :start_date
  AND DATE(usage_start_time) <= :end_date
GROUP BY DATE(usage_start_time), service.description
ORDER BY date
""".replace("/* GCP_NET_COST */", GCP_NET_COST_SQL)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _defaults(start_date, end_date):
    default_start, default_end = default_date_range()
    return validate_date_range(
        start_date,
        end_date,
        default_start=default_start,
        default_end=default_end,
    )


def _quoted_identifier(value: str, field: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", value or ""):
        raise ValueError(f"{field} must be a single Unity Catalog identifier")
    return f"`{value}`"


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/status")
async def get_gcp_status() -> dict[str, Any]:
    """Check if GCP billing tables are available (cached 5 min)."""
    catalog, schema, configured_table = get_catalog_schema_table()
    if (
        _gcp_status_cache["available"] is not None
        and (time.time() - _gcp_status_cache["checked_at"]) < _GCP_STATUS_TTL
    ):
        available = _gcp_status_cache["available"]
        table = _gcp_status_cache["table"]
        reason = _gcp_status_cache["reason"]
        message = _gcp_status_cache["message"]
    else:
        table = configured_table
        reason = None
        message = None
        try:
            quoted_catalog = _quoted_identifier(catalog, "GCP_COST_CATALOG")
            _quoted_identifier(schema, "GCP_COST_SCHEMA")
            if table:
                _quoted_identifier(table, "GCP_COST_TABLE")
            if table == "actuals_gold":
                available = False
                reason = "unsupported_table_contract"
                message = (
                    "GCP actual costs require the raw standard BigQuery billing "
                    "export. Flat actuals_gold tables are not supported."
                )
                table = None
            elif table:
                results = await asyncio.to_thread(
                    execute_query,
                    CHECK_GCP_TABLE.format(catalog=quoted_catalog),
                    {"schema": schema, "table": table},
                    cache_tag="gcp-actual",
                )
                available = len(results) > 0
            else:
                results = await asyncio.to_thread(
                    execute_query,
                    DISCOVER_GCP_TABLE.format(catalog=quoted_catalog),
                    {"schema": schema},
                    cache_tag="gcp-actual",
                )
                candidates = [
                    str(row.get("table_name") or "")
                    for row in results
                    if row.get("table_name")
                ]
                if len(candidates) == 1:
                    table = candidates[0]
                    available = True
                elif len(candidates) > 1:
                    table = None
                    available = False
                    reason = "multiple_export_tables"
                    message = (
                        "Multiple standard GCP billing export tables were found. "
                        "Set GCP_COST_TABLE to the intended suffixed table name."
                    )
                else:
                    table = None
                    available = False
        except Exception as exc:
            # Permission, network, timeout, and capacity failures are transient;
            # only a successful empty existence query establishes absence.
            logger.warning(
                "GCP billing availability query failed transiently: %s",
                exc,
            )
            raise
        _gcp_status_cache["available"] = available
        _gcp_status_cache["checked_at"] = time.time()
        _gcp_status_cache["table"] = table
        _gcp_status_cache["reason"] = reason
        _gcp_status_cache["message"] = message

    return {
        "gcp_available": available,
        "catalog": catalog,
        "schema": schema,
        "table": table if available else None,
        "reason": reason,
        "message": message,
    }


@router.get("/summary")
async def get_gcp_actual_summary(
    start_date: str = Query(default=None),
    end_date: str = Query(default=None),
) -> dict[str, Any]:
    """Get summary of actual GCP costs."""
    start_date, end_date = _defaults(start_date, end_date)

    status = await get_gcp_status()
    if not status["gcp_available"]:
        return {
            "available": False,
            "scoped_out": bool(status.get("scoped_out")),
            "reason": status.get("reason"),
            "message": status.get("message") or "GCP billing data not configured. Connect a BigQuery billing export via Lakehouse Federation.",
            "start_date": start_date,
            "end_date": end_date,
        }
    catalog, schema, table = status["catalog"], status["schema"], status["table"]

    results = await asyncio.to_thread(execute_query,
        GCP_ACTUAL_SUMMARY.format(catalog=catalog, schema=schema, table=table),
        {"start_date": start_date, "end_date": end_date},
        cache_tag="gcp-actual",
    )

    if not results:
        return {
            "available": True,
            "total_cost": 0,
            "currency": "USD",
            "project_count": 0,
            "service_count": 0,
            "days_in_range": 0,
            "start_date": start_date,
            "end_date": end_date,
        }

    row = results[0]
    return {
        "available": True,
        "total_cost": float(row.get("total_cost") or 0),
        "currency": row.get("currency") or "USD",
        "project_count": row.get("project_count") or 0,
        "service_count": row.get("service_count") or 0,
        "days_in_range": row.get("days_in_range") or 0,
        "start_date": start_date,
        "end_date": end_date,
    }


@router.get("/by-service")
async def get_gcp_costs_by_service(
    start_date: str = Query(default=None),
    end_date: str = Query(default=None),
) -> dict[str, Any]:
    """Get actual GCP costs broken down by GCP service (Compute Engine, GCS, BigQuery, etc.)."""
    start_date, end_date = _defaults(start_date, end_date)

    status = await get_gcp_status()
    if not status["gcp_available"]:
        return {"available": False, "services": [], "start_date": start_date, "end_date": end_date}
    catalog, schema, table = status["catalog"], status["schema"], status["table"]

    results = await asyncio.to_thread(execute_query,
        GCP_COSTS_BY_SERVICE.format(catalog=catalog, schema=schema, table=table),
        {"start_date": start_date, "end_date": end_date},
        cache_tag="gcp-actual",
    )

    total_cost = sum(float(r.get("total_cost") or 0) for r in results)
    services = [
        {
            "service": r.get("service") or "Other",
            "total_cost": float(r.get("total_cost") or 0),
            "days_active": r.get("days_active") or 0,
            "percentage": float(r.get("total_cost") or 0) / total_cost * 100 if total_cost > 0 else 0,
        }
        for r in results
    ]

    return {
        "available": True,
        "services": services,
        "total_cost": total_cost,
        "start_date": start_date,
        "end_date": end_date,
    }


@router.get("/by-project")
async def get_gcp_costs_by_project(
    start_date: str = Query(default=None),
    end_date: str = Query(default=None),
) -> dict[str, Any]:
    """Get actual GCP costs broken down by GCP project."""
    start_date, end_date = _defaults(start_date, end_date)

    status = await get_gcp_status()
    if not status["gcp_available"]:
        return {"available": False, "projects": [], "start_date": start_date, "end_date": end_date}
    catalog, schema, table = status["catalog"], status["schema"], status["table"]

    results = await asyncio.to_thread(execute_query,
        GCP_COSTS_BY_PROJECT.format(catalog=catalog, schema=schema, table=table),
        {"start_date": start_date, "end_date": end_date},
        cache_tag="gcp-actual",
    )

    total_cost = sum(float(r.get("total_cost") or 0) for r in results)
    projects = [
        {
            "project_id": r.get("project_id") or "unknown",
            "project_name": r.get("project_name") or r.get("project_id") or "unknown",
            "total_cost": float(r.get("total_cost") or 0),
            "service_count": r.get("service_count") or 0,
            "percentage": float(r.get("total_cost") or 0) / total_cost * 100 if total_cost > 0 else 0,
        }
        for r in results
    ]

    return {
        "available": True,
        "projects": projects,
        "total_cost": total_cost,
        "start_date": start_date,
        "end_date": end_date,
    }


@router.get("/by-sku")
async def get_gcp_costs_by_sku(
    start_date: str = Query(default=None),
    end_date: str = Query(default=None),
) -> dict[str, Any]:
    """Get actual GCP costs broken down by SKU."""
    start_date, end_date = _defaults(start_date, end_date)

    status = await get_gcp_status()
    if not status["gcp_available"]:
        return {"available": False, "skus": [], "start_date": start_date, "end_date": end_date}
    catalog, schema, table = status["catalog"], status["schema"], status["table"]

    results = await asyncio.to_thread(execute_query,
        GCP_COSTS_BY_SKU.format(catalog=catalog, schema=schema, table=table),
        {"start_date": start_date, "end_date": end_date},
        cache_tag="gcp-actual",
    )

    total_cost = sum(float(r.get("total_cost") or 0) for r in results)
    skus = [
        {
            "service": r.get("service") or "Other",
            "sku": r.get("sku") or "Other",
            "total_cost": float(r.get("total_cost") or 0),
            "percentage": float(r.get("total_cost") or 0) / total_cost * 100 if total_cost > 0 else 0,
        }
        for r in results
    ]

    return {
        "available": True,
        "skus": skus,
        "total_cost": total_cost,
        "start_date": start_date,
        "end_date": end_date,
    }


@router.get("/timeseries")
async def get_gcp_costs_timeseries(
    start_date: str = Query(default=None),
    end_date: str = Query(default=None),
) -> dict[str, Any]:
    """Get daily GCP costs timeseries by service."""
    start_date, end_date = _defaults(start_date, end_date)

    status = await get_gcp_status()
    if not status["gcp_available"]:
        return {"available": False, "timeseries": [], "services": [], "start_date": start_date, "end_date": end_date}
    catalog, schema, table = status["catalog"], status["schema"], status["table"]

    results = await asyncio.to_thread(execute_query,
        GCP_COSTS_TIMESERIES.format(catalog=catalog, schema=schema, table=table),
        {"start_date": start_date, "end_date": end_date},
        cache_tag="gcp-actual",
    )

    data_by_date: dict[str, dict] = {}
    services_set: set[str] = set()

    for row in results:
        date_str = str(row.get("date"))
        svc = row.get("service") or "Other"
        cost = float(row.get("daily_cost") or 0)
        services_set.add(svc)
        if date_str not in data_by_date:
            data_by_date[date_str] = {"date": date_str}
        data_by_date[date_str][svc] = cost

    services = sorted(list(services_set))
    timeseries = []
    for date_str in sorted(data_by_date.keys()):
        row = data_by_date[date_str]
        for svc in services:
            row.setdefault(svc, 0)
        timeseries.append(row)

    return {
        "available": True,
        "timeseries": timeseries,
        "services": services,
        "start_date": start_date,
        "end_date": end_date,
    }


@router.get("/dashboard-bundle")
async def get_gcp_actual_dashboard_bundle(
    start_date: str = Query(default=None),
    end_date: str = Query(default=None),
) -> dict[str, Any]:
    """Get all GCP actual cost data in a single parallel request."""
    start_date, end_date = _defaults(start_date, end_date)

    status = await get_gcp_status()
    if not status["gcp_available"]:
        return {
            "available": False,
            "scoped_out": bool(status.get("scoped_out")),
            "reason": status.get("reason"),
            "message": status.get("message") or "GCP billing data not configured. Set up Lakehouse Federation to BigQuery billing export.",
            "start_date": start_date,
            "end_date": end_date,
        }

    # Establish the authoritative total first, then admit at most two optional
    # detail queries at a time. A large GCP export must not consume the whole
    # process executor or turn one detail failure into a full Cloud-tab failure.
    summary = await get_gcp_actual_summary(start_date, end_date)
    semaphore = asyncio.Semaphore(2)

    async def optional(name: str, call):
        async with semaphore:
            try:
                return name, await call(), None
            except Exception as exc:
                logger.warning("Optional GCP actual-cost section %s failed: %s", name, exc)
                return name, None, getattr(exc, "code", "QUERY_FAILED")

    outcomes = await asyncio.gather(
        optional("by_service", lambda: get_gcp_costs_by_service(start_date, end_date)),
        optional("by_project", lambda: get_gcp_costs_by_project(start_date, end_date)),
        optional("by_sku", lambda: get_gcp_costs_by_sku(start_date, end_date)),
        optional("timeseries", lambda: get_gcp_costs_timeseries(start_date, end_date)),
    )
    sections = {name: value for name, value, _error in outcomes}
    failures = {name: error for name, _value, error in outcomes if error}

    return {
        "available": True,
        "availability": "partial" if failures else "available",
        "partial_reasons": failures,
        "summary": summary,
        "by_service": sections["by_service"],
        "by_project": sections["by_project"],
        "by_sku": sections["by_sku"],
        "timeseries": sections["timeseries"],
        "start_date": start_date,
        "end_date": end_date,
    }
