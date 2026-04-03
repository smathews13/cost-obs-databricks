"""Reconciliation API endpoints for validating cost accuracy.

Runs cross-checks across different aggregation dimensions to detect
double-counting, missing attribution, or data quality issues.
"""

import logging
from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Query

from server.db import execute_query, execute_queries_parallel, get_catalog_schema
from server.queries.reconciliation import (
    RECON_BY_PRODUCT,
    RECON_BY_WORKSPACE,
    RECON_GROUND_TRUTH,
    RECON_MV_DAILY_SUMMARY,
    RECON_MV_VS_LIVE,
    RECON_NULL_ATTRIBUTION,
    RECON_PRICE_COVERAGE,
    RECON_PRICE_UNIQUENESS,
    RECON_QUERY_HISTORY_DUPES,
    RECON_SQL_ATTRIBUTION,
)
from server.materialized_views import check_materialized_views_exist

router = APIRouter()
logger = logging.getLogger(__name__)

TOLERANCE_PCT = 0.01  # 0.01% tolerance for floating point rounding


def _default_start() -> str:
    return (date.today() - timedelta(days=30)).isoformat()


def _default_end() -> str:
    return date.today().isoformat()


def _check_result(name: str, passed: bool, details: dict, *, status_override: str | None = None) -> dict:
    return {
        "name": name,
        "status": status_override if status_override else ("pass" if passed else "fail"),
        "details": details,
    }


@router.get("/run")
async def run_reconciliation(
    start_date: str = Query(default=None, description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(default=None, description="End date (YYYY-MM-DD)"),
) -> dict[str, Any]:
    """Run all reconciliation checks and return results."""
    params = {
        "start_date": start_date or _default_start(),
        "end_date": end_date or _default_end(),
    }

    checks: list[dict] = []

    # ── Run ground truth + product + workspace + price coverage in parallel ──
    parallel_queries = [
        ("ground_truth", lambda: execute_query(RECON_GROUND_TRUTH, params)),
        ("by_product", lambda: execute_query(RECON_BY_PRODUCT, params)),
        ("by_workspace", lambda: execute_query(RECON_BY_WORKSPACE, params)),
        ("price_coverage", lambda: execute_query(RECON_PRICE_COVERAGE, params)),
        ("null_attribution", lambda: execute_query(RECON_NULL_ATTRIBUTION, params)),
        ("price_uniqueness", lambda: execute_query(RECON_PRICE_UNIQUENESS, params)),
    ]

    results = execute_queries_parallel(parallel_queries)

    # ── CHECK 1: Ground truth baseline ──
    gt = results.get("ground_truth", [])
    gt_row = gt[0] if gt else {}
    gt_spend = float(gt_row.get("total_spend") or 0)
    gt_dbus = float(gt_row.get("total_dbus") or 0)
    gt_rows = int(gt_row.get("total_rows") or 0)

    checks.append(_check_result(
        "ground_truth",
        gt_spend > 0,
        {
            "total_spend": gt_spend,
            "total_dbus": gt_dbus,
            "total_rows": gt_rows,
            "workspace_count": int(gt_row.get("workspace_count") or 0),
            "days_in_range": int(gt_row.get("days_in_range") or 0),
        },
    ))

    # ── CHECK 2: Product category completeness ──
    product_rows = results.get("by_product", [])
    product_spend_sum = sum(float(r.get("total_spend") or 0) for r in product_rows)
    product_dbu_sum = sum(float(r.get("total_dbus") or 0) for r in product_rows)
    spend_diff = abs(product_spend_sum - gt_spend)
    spend_diff_pct = (spend_diff / gt_spend * 100) if gt_spend > 0 else 0

    checks.append(_check_result(
        "product_completeness",
        spend_diff_pct <= TOLERANCE_PCT,
        {
            "ground_truth_spend": gt_spend,
            "product_sum_spend": product_spend_sum,
            "difference": spend_diff,
            "difference_pct": round(spend_diff_pct, 6),
            "categories": [
                {
                    "category": r.get("product_category"),
                    "spend": float(r.get("total_spend") or 0),
                    "dbus": float(r.get("total_dbus") or 0),
                    "rows": int(r.get("row_count") or 0),
                }
                for r in product_rows
            ],
        },
    ))

    # ── CHECK 3: Workspace completeness ──
    ws_rows = results.get("by_workspace", [])
    ws_spend_sum = sum(float(r.get("total_spend") or 0) for r in ws_rows)
    ws_diff = abs(ws_spend_sum - gt_spend)
    ws_diff_pct = (ws_diff / gt_spend * 100) if gt_spend > 0 else 0

    checks.append(_check_result(
        "workspace_completeness",
        ws_diff_pct <= TOLERANCE_PCT,
        {
            "ground_truth_spend": gt_spend,
            "workspace_sum_spend": ws_spend_sum,
            "difference": ws_diff,
            "difference_pct": round(ws_diff_pct, 6),
            "workspace_count": len(ws_rows),
        },
    ))

    # ── CHECK 4: Price coverage ──
    pc = results.get("price_coverage", [])
    pc_row = pc[0] if pc else {}
    unpriced_pct = float(pc_row.get("unpriced_pct") or 0)
    unpriced_dbu_pct = float(pc_row.get("unpriced_dbu_pct") or 0)

    checks.append(_check_result(
        "price_coverage",
        unpriced_dbu_pct < 5.0,  # <5% unpriced DBUs is acceptable
        {
            "total_rows": int(pc_row.get("total_rows") or 0),
            "priced_rows": int(pc_row.get("priced_rows") or 0),
            "unpriced_rows": int(pc_row.get("unpriced_rows") or 0),
            "unpriced_row_pct": unpriced_pct,
            "total_dbus": float(pc_row.get("total_dbus") or 0),
            "unpriced_dbus": float(pc_row.get("unpriced_dbus") or 0),
            "unpriced_dbu_pct": unpriced_dbu_pct,
        },
    ))

    # ── CHECK 5: NULL attribution ──
    na = results.get("null_attribution", [])
    na_row = na[0] if na else {}
    unattr_pct = float(na_row.get("unattributed_pct") or 0)

    checks.append(_check_result(
        "null_attribution",
        True,  # informational — always pass, just surface the data
        {
            "total_rows": int(na_row.get("total_rows") or 0),
            "total_spend": float(na_row.get("total_spend") or 0),
            "fully_unattributed_rows": int(na_row.get("fully_unattributed_rows") or 0),
            "unattributed_spend": float(na_row.get("unattributed_spend") or 0),
            "unattributed_pct": unattr_pct,
        },
    ))

    # ── CHECK 6: Price uniqueness ──
    pu = results.get("price_uniqueness", [])
    checks.append(_check_result(
        "price_uniqueness",
        len(pu) == 0,
        {
            "duplicate_price_skus": len(pu),
            "skus": [
                {
                    "sku_name": r.get("sku_name"),
                    "cloud": r.get("cloud"),
                    "active_prices": int(r.get("active_price_count") or 0),
                    "min_price": float(r.get("min_price") or 0),
                    "max_price": float(r.get("max_price") or 0),
                }
                for r in pu
            ],
        },
    ))

    # ── CHECK 7: SQL attribution (run separately — uses query.history, slow) ──
    try:
        sql_results = execute_query(RECON_SQL_ATTRIBUTION, params)
        sql_row = sql_results[0] if sql_results else {}
        billing_spend = float(sql_row.get("billing_spend") or 0)
        attributed_spend = float(sql_row.get("attributed_spend") or 0)
        sql_diff_pct = abs(float(sql_row.get("spend_difference_pct") or 0))

        # SQL attribution is informational — query history never covers 100% of
        # warehouse activity (idle time, system queries, metadata ops), so a gap
        # between billing and attributed spend is expected, not a failure.
        checks.append(_check_result(
            "sql_attribution",
            sql_diff_pct <= TOLERANCE_PCT,
            {
                "billing_spend": billing_spend,
                "billing_dbus": float(sql_row.get("billing_dbus") or 0),
                "attributed_spend": attributed_spend,
                "attributed_dbus": float(sql_row.get("attributed_dbus") or 0),
                "difference": float(sql_row.get("spend_difference") or 0),
                "difference_pct": float(sql_row.get("spend_difference_pct") or 0),
            },
            status_override=None if sql_diff_pct <= TOLERANCE_PCT else "info",
        ))
    except Exception as e:
        logger.warning(f"SQL attribution check failed: {e}")
        checks.append(_check_result(
            "sql_attribution",
            False,
            {"error": str(e)},
        ))

    # ── CHECK 8: Query history duplicates ──
    try:
        dupe_results = execute_query(RECON_QUERY_HISTORY_DUPES, params)
        dupe_row = dupe_results[0] if dupe_results else {}
        dupe_pct = float(dupe_row.get("duplicate_pct") or 0)

        checks.append(_check_result(
            "query_history_duplicates",
            dupe_pct < 1.0,  # <1% duplicates is acceptable
            {
                "total_rows": int(dupe_row.get("total_rows") or 0),
                "unique_statements": int(dupe_row.get("unique_statements") or 0),
                "duplicate_rows": int(dupe_row.get("duplicate_rows") or 0),
                "duplicate_pct": dupe_pct,
            },
        ))
    except Exception as e:
        logger.warning(f"Query history duplicate check failed: {e}")
        checks.append(_check_result(
            "query_history_duplicates",
            False,
            {"error": str(e)},
        ))

    # ── CHECK 9: MV vs live comparison ──
    try:
        catalog, schema = get_catalog_schema()
        tables = check_materialized_views_exist(catalog, schema)
        if tables.get("daily_usage_summary"):
            mv_query = RECON_MV_DAILY_SUMMARY.format(catalog=catalog, schema=schema)

            def _run_mv_query():
                try:
                    from server.postgres import execute_pg_mv
                    result = execute_pg_mv(RECON_MV_DAILY_SUMMARY, params, catalog, schema)
                    if result is not None:
                        return result
                except Exception:
                    pass
                return execute_query(mv_query, params)

            mv_live_results = execute_queries_parallel([
                ("live", lambda: execute_query(RECON_MV_VS_LIVE, params)),
                ("mv", _run_mv_query),
            ])

            live_by_date = {
                str(r["usage_date"]): float(r.get("live_spend") or 0)
                for r in mv_live_results.get("live", [])
            }
            mv_by_date = {
                str(r["usage_date"]): float(r.get("mv_spend") or 0)
                for r in mv_live_results.get("mv", [])
            }

            all_dates = sorted(set(live_by_date) | set(mv_by_date))
            mismatches = []
            for d in all_dates:
                live_val = live_by_date.get(d, 0)
                mv_val = mv_by_date.get(d, 0)
                diff = abs(live_val - mv_val)
                if live_val > 0 and (diff / live_val * 100) > TOLERANCE_PCT:
                    mismatches.append({
                        "date": d,
                        "live_spend": live_val,
                        "mv_spend": mv_val,
                        "difference": diff,
                    })

            # If the only mismatches are today's date, treat as informational —
            # the MV hasn't refreshed yet with today's still-accumulating data.
            today_str = date.today().isoformat()
            non_today_mismatches = [m for m in mismatches if m["date"] != today_str]
            if len(mismatches) > 0 and len(non_today_mismatches) == 0:
                mv_status_override = "info"
            elif len(non_today_mismatches) > 0:
                mv_status_override = None  # real failure
            else:
                mv_status_override = None  # all pass

            checks.append(_check_result(
                "mv_vs_live",
                len(mismatches) == 0,
                {
                    "total_dates_compared": len(all_dates),
                    "mismatched_dates": len(mismatches),
                    "mismatches": mismatches[:10],  # limit to first 10
                },
                status_override=mv_status_override,
            ))
        else:
            checks.append(_check_result(
                "mv_vs_live",
                True,
                {"skipped": "daily_usage_summary table does not exist"},
            ))
    except Exception as e:
        logger.warning(f"MV vs live check failed: {e}")
        checks.append(_check_result(
            "mv_vs_live",
            False,
            {"error": str(e)},
        ))

    # ── Summary ──
    total_checks = len(checks)
    passed = sum(1 for c in checks if c["status"] == "pass")
    info = sum(1 for c in checks if c["status"] == "info")
    failed = sum(1 for c in checks if c["status"] == "fail")

    return {
        "summary": {
            "total_checks": total_checks,
            "passed": passed,
            "info": info,
            "failed": failed,
            "status": "healthy" if failed == 0 else "issues_detected",
        },
        "date_range": {
            "start_date": params["start_date"],
            "end_date": params["end_date"],
        },
        "checks": checks,
    }
