#!/usr/bin/env python3
"""
generate_synthetic_aws_costs.py
--------------------------------
Generates synthetic AWS CUR 2.0 data that mirrors the aws_cost_gold table
used by the Cost Observability app.

Creates the schema + aws_cost_gold table if they don't exist, then inserts
realistic synthetic records to test the Cloud Costs (AWS) integration.

Usage:
    # From repo root — uses env vars DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
    python scripts/generate_synthetic_aws_costs.py

    # With overrides
    python scripts/generate_synthetic_aws_costs.py \\
        --days 90 \\
        --catalog main \\
        --schema aws_costs \\
        --seed 42

Environment variables (same as the app):
    DATABRICKS_HOST        Workspace URL  e.g. https://adb-1234.azuredatabricks.net
    DATABRICKS_TOKEN       Personal access token
    DATABRICKS_HTTP_PATH   SQL warehouse path  /sql/1.0/warehouses/<id>
    COST_OBS_CATALOG       Default: main
    AWS_COST_SCHEMA        Default: aws_costs
"""

import argparse
import math
import os
import random
import time
import uuid
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def _env(key: str, default: str) -> str:
    return os.getenv(key, default)


# ---------------------------------------------------------------------------
# Realistic cluster / warehouse profiles
# ---------------------------------------------------------------------------

WORKSPACES = [
    {"workspace_id": "1111111111111111", "workspace_name": "prod-us-east-1", "account_id": "123456789012", "region": "us-east-1"},
    {"workspace_id": "2222222222222222", "workspace_name": "dev-us-west-2",  "account_id": "123456789012", "region": "us-west-2"},
    {"workspace_id": "3333333333333333", "workspace_name": "analytics-eu",   "account_id": "987654321098", "region": "eu-west-1"},
]

# Instance types match cloud_pricing.py pricing table
CLUSTER_PROFILES = [
    # name, cluster_source, driver_type, worker_type, team, env, project, avg_daily_cost_base, charge_type_mix
    {"name": "etl-main-prod",       "source": "JOB",         "driver": "m5.2xlarge",  "workers": "m5.2xlarge",  "team": "data-eng",    "env": "prod",    "project": "etl",       "base_cost": 320, "gpu": False},
    {"name": "ml-training-gpu",     "source": "JOB",         "driver": "m5.xlarge",   "workers": "g4dn.12xlarge","team": "ml-platform", "env": "prod",    "project": "ml-train",  "base_cost": 850, "gpu": True},
    {"name": "analytics-shared",    "source": "UI",          "driver": "r5.2xlarge",  "workers": "r5.xlarge",   "team": "analytics",   "env": "prod",    "project": "reporting", "base_cost": 210, "gpu": False},
    {"name": "dev-exploration",     "source": "UI",          "driver": "m5.large",    "workers": "m5.large",    "team": "analytics",   "env": "dev",     "project": "research",  "base_cost": 65,  "gpu": False},
    {"name": "dlt-streaming-prod",  "source": "DLT",         "driver": "m5.xlarge",   "workers": "m5.xlarge",   "team": "data-eng",    "env": "prod",    "project": "streaming", "base_cost": 190, "gpu": False},
    {"name": "dbt-jobs-nightly",    "source": "JOB",         "driver": "m5.large",    "workers": "m5.xlarge",   "team": "data-eng",    "env": "prod",    "project": "dbt",       "base_cost": 145, "gpu": False},
    {"name": "feature-store-build", "source": "JOB",         "driver": "r5.4xlarge",  "workers": "r5.2xlarge",  "team": "ml-platform", "env": "prod",    "project": "features",  "base_cost": 425, "gpu": False},
    {"name": "ad-hoc-research",     "source": "UI",          "driver": "m5.xlarge",   "workers": "m5.large",    "team": "data-sci",    "env": "dev",     "project": "research",  "base_cost": 40,  "gpu": False},
    {"name": "compliance-reports",  "source": "JOB",         "driver": "c5.2xlarge",  "workers": "c5.xlarge",   "team": "finance",     "env": "prod",    "project": "finance",   "base_cost": 90,  "gpu": False},
    {"name": "inference-serving",   "source": "JOB",         "driver": "m5.2xlarge",  "workers": "g4dn.xlarge", "team": "ml-platform", "env": "prod",    "project": "serving",   "base_cost": 380, "gpu": True},
    {"name": "data-quality-checks", "source": "JOB",         "driver": "m5.large",    "workers": "m5.large",    "team": "data-eng",    "env": "prod",    "project": "dq",        "base_cost": 55,  "gpu": False},
    {"name": "backfill-historical", "source": "JOB",         "driver": "r5.4xlarge",  "workers": "r5.2xlarge",  "team": "data-eng",    "env": "prod",    "project": "etl",       "base_cost": 0,   "gpu": False, "spike_days": True},
]

WAREHOUSE_PROFILES = [
    {"name": "prod-shared-warehouse",  "team": "analytics",   "env": "prod", "project": "reporting", "base_cost": 280},
    {"name": "dev-warehouse",          "team": "data-sci",    "env": "dev",  "project": "research",  "base_cost": 70},
    {"name": "bi-dashboard-warehouse", "team": "finance",     "env": "prod", "project": "finance",   "base_cost": 155},
    {"name": "ml-feature-queries",     "team": "ml-platform", "env": "prod", "project": "features",  "base_cost": 95},
]

REGIONS = ["us-east-1", "us-west-2", "eu-west-1"]


def _make_id(prefix: str, seed_str: str) -> str:
    """Deterministic pseudo-ID from a seed string."""
    rng = random.Random(seed_str)
    hex_part = "".join(f"{rng.randint(0, 255):02x}" for _ in range(8))
    return f"{prefix}{hex_part[:4]}-{hex_part[4:]}"


def _cluster_id(name: str) -> str:
    return _make_id("", f"cluster-{name}").replace("-", "")[:16]


def _warehouse_id(name: str) -> str:
    return _make_id("", f"wh-{name}").replace("-", "")[:16]


def _job_id(name: str) -> str:
    rng = random.Random(f"job-{name}")
    return str(rng.randint(100000, 999999))


# ---------------------------------------------------------------------------
# Cost generation helpers
# ---------------------------------------------------------------------------

def _weekday_factor(d: date) -> float:
    """Weekdays cost more; weekend is typically ~40% of weekday."""
    return 0.42 if d.weekday() >= 5 else 1.0


def _trend_factor(d: date, end: date, days: int) -> float:
    """Slight upward cost trend over the period (+20% from start to end)."""
    age_frac = (end - d).days / max(days, 1)
    return 1.0 + 0.20 * (1 - age_frac)


def _noise(rng: random.Random, sigma: float = 0.12) -> float:
    """Gaussian-ish noise via Box-Muller."""
    u = max(rng.random(), 1e-9)
    v = rng.random()
    n = math.sqrt(-2 * math.log(u)) * math.cos(2 * math.pi * v)
    return 1.0 + n * sigma


def _instance_family(instance_type: str) -> str:
    dot = instance_type.find(".")
    return instance_type[:dot] if dot > 0 else instance_type


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

def generate_gold_rows(
    days: int,
    seed: int,
    catalog: str,
    schema: str,
) -> list[dict]:
    """Return a list of dicts matching the aws_cost_gold schema."""
    rng = random.Random(seed)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days - 1)

    rows: list[dict] = []

    # Assign each cluster to a workspace
    clusters_with_meta = []
    for i, cp in enumerate(CLUSTER_PROFILES):
        ws = WORKSPACES[i % len(WORKSPACES)]
        cluster_id = _cluster_id(cp["name"])
        job_id = _job_id(cp["name"]) if cp["source"] == "JOB" else None
        clusters_with_meta.append({**cp, "cluster_id": cluster_id, "job_id": job_id, "workspace": ws})

    warehouses_with_meta = []
    for i, wp in enumerate(WAREHOUSE_PROFILES):
        ws = WORKSPACES[i % len(WORKSPACES)]
        wh_id = _warehouse_id(wp["name"])
        warehouses_with_meta.append({**wp, "warehouse_id": wh_id, "workspace": ws})

    current = start_date
    while current <= end_date:
        wf = _weekday_factor(current)
        tf = _trend_factor(current, end_date, days)
        date_str = current.isoformat()

        # --- Cluster rows (Compute + Storage + Networking per cluster per day) ---
        for cp in clusters_with_meta:
            ws = cp["workspace"]

            # Skip inactive dev clusters on weekends
            if current.weekday() >= 5 and cp["env"] == "dev" and rng.random() < 0.65:
                current_for_loop = current
                current = current_for_loop + timedelta(days=1)
                # Don't skip the entire date, just this cluster
                pass

            # Spike clusters only active in first 2 weeks as a backfill burst
            if cp.get("spike_days") and (end_date - current).days > 14:
                continue

            base = cp["base_cost"]
            if base == 0 and cp.get("spike_days"):
                base = rng.uniform(1200, 2800)  # big backfill burst

            daily_cost = base * wf * tf * _noise(rng)

            # Compute cost (dominant ~80%)
            compute_cost = daily_cost * rng.uniform(0.76, 0.84)
            storage_cost = daily_cost * rng.uniform(0.08, 0.14)
            network_cost = daily_cost - compute_cost - storage_cost

            pricing_term = rng.choices(
                ["OnDemand", "ReservedInstance", "SavingsPlan"],
                weights=[0.45, 0.30, 0.25],
            )[0]

            discount = 0.0 if pricing_term == "OnDemand" else rng.uniform(0.15, 0.35)

            for charge_type, cost in [
                ("Compute", compute_cost),
                ("Storage", storage_cost),
                ("Networking", network_cost),
            ]:
                # Unblended ≈ on-demand rate; net_unblended = after negotiated discount
                unblended = cost
                net_unblended = cost * (1 - discount)
                blended = cost * rng.uniform(0.95, 1.02)
                amortized = unblended + (unblended * 0.05 if pricing_term != "OnDemand" else 0)
                net_amortized = net_unblended + (net_unblended * 0.04 if pricing_term != "OnDemand" else 0)
                usage_amount = cost / rng.uniform(0.10, 0.50)  # hours

                rows.append({
                    "usage_date": date_str,
                    "usage_account_id": ws["account_id"],
                    "region": ws["region"],
                    "charge_type": charge_type,
                    "pricing_term": pricing_term,
                    "instance_type": cp["workers"] if charge_type == "Compute" else None,
                    "instance_family": _instance_family(cp["workers"]) if charge_type == "Compute" else None,
                    "cluster_id": cp["cluster_id"],
                    "cluster_name": cp["name"],
                    "warehouse_id": None,
                    "job_id": cp["job_id"],
                    "instance_pool_id": None,
                    "cluster_creator": f"svc-{cp['team']}@company.com",
                    "cost_center": cp["team"].upper().replace("-", "_"),
                    "environment": cp["env"],
                    "project": cp["project"],
                    "team": cp["team"],
                    "unblended_cost": round(unblended, 6),
                    "net_unblended_cost": round(net_unblended, 6),
                    "blended_cost": round(blended, 6),
                    "amortized_cost": round(amortized, 6),
                    "net_amortized_cost": round(net_amortized, 6),
                    "total_usage_amount": round(usage_amount, 4),
                    "line_item_count": rng.randint(8, 96),
                    "currency_code": "USD",
                })

        # --- Warehouse rows (SQL compute) ---
        for wp in warehouses_with_meta:
            ws = wp["workspace"]
            if current.weekday() >= 5 and wp["env"] == "dev" and rng.random() < 0.75:
                continue

            daily_cost = wp["base_cost"] * wf * tf * _noise(rng)
            pricing_term = rng.choices(
                ["OnDemand", "SavingsPlan"], weights=[0.60, 0.40]
            )[0]
            discount = 0.0 if pricing_term == "OnDemand" else rng.uniform(0.10, 0.25)

            unblended = daily_cost
            net_unblended = daily_cost * (1 - discount)
            blended = daily_cost * rng.uniform(0.96, 1.01)
            amortized = unblended
            net_amortized = net_unblended

            rows.append({
                "usage_date": date_str,
                "usage_account_id": ws["account_id"],
                "region": ws["region"],
                "charge_type": "Compute",
                "pricing_term": pricing_term,
                "instance_type": "m5.2xlarge",
                "instance_family": "m5",
                "cluster_id": None,
                "cluster_name": None,
                "warehouse_id": wp["warehouse_id"],
                "job_id": None,
                "instance_pool_id": None,
                "cluster_creator": None,
                "cost_center": wp["team"].upper().replace("-", "_"),
                "environment": wp["env"],
                "project": wp["project"],
                "team": wp["team"],
                "unblended_cost": round(unblended, 6),
                "net_unblended_cost": round(net_unblended, 6),
                "blended_cost": round(blended, 6),
                "amortized_cost": round(amortized, 6),
                "net_amortized_cost": round(net_amortized, 6),
                "total_usage_amount": round(daily_cost / 0.30, 4),
                "line_item_count": rng.randint(4, 48),
                "currency_code": "USD",
            })

        current += timedelta(days=1)

    return rows


# ---------------------------------------------------------------------------
# Databricks SQL execution
# ---------------------------------------------------------------------------

def _run_sql(client, warehouse_id: str, statement: str, wait_timeout: str = "50s") -> None:
    from databricks.sdk.service.sql import StatementState
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout=wait_timeout,
    )
    if resp.status.state not in (StatementState.SUCCEEDED,):
        raise RuntimeError(f"SQL failed [{resp.status.state}]: {resp.status.error}")


def _warehouse_id_from_path(http_path: str) -> str:
    return http_path.rstrip("/").split("/")[-1]


DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'Synthetic AWS cost data for UI testing'"

DDL_GOLD_TABLE = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.aws_cost_gold (
  usage_date          DATE,
  usage_account_id    STRING,
  region              STRING,
  charge_type         STRING,
  pricing_term        STRING,
  instance_type       STRING,
  instance_family     STRING,
  usage_metadata      STRUCT<
                        cluster_id:       STRING,
                        cluster_name:     STRING,
                        warehouse_id:     STRING,
                        job_id:           STRING,
                        instance_pool_id: STRING,
                        cluster_creator:  STRING
                      >,
  cost_center         STRING,
  environment         STRING,
  project             STRING,
  team                STRING,
  unblended_cost      DOUBLE,
  net_unblended_cost  DOUBLE,
  blended_cost        DOUBLE,
  amortized_cost      DOUBLE,
  net_amortized_cost  DOUBLE,
  total_usage_amount  DOUBLE,
  line_item_count     BIGINT,
  currency_code       STRING
)
USING DELTA
COMMENT 'Synthetic AWS CUR 2.0 gold table — generated for UI testing'
PARTITIONED BY (usage_date)
"""

DDL_TRUNCATE = "DELETE FROM {catalog}.{schema}.aws_cost_gold WHERE 1=1"


def _rows_to_insert_sql(catalog: str, schema: str, rows: list[dict]) -> list[str]:
    """Convert rows into batched INSERT SQL statements (200 rows per batch)."""

    def _val(v) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return str(v).upper()
        if isinstance(v, (int, float)):
            return str(v)
        return "'" + str(v).replace("'", "''") + "'"

    statements = []
    batch_size = 200
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        value_clauses = []
        for r in batch:
            um = (
                f"STRUCT("
                f"{_val(r['cluster_id'])}, "
                f"{_val(r['cluster_name'])}, "
                f"{_val(r['warehouse_id'])}, "
                f"{_val(r['job_id'])}, "
                f"{_val(r['instance_pool_id'])}, "
                f"{_val(r['cluster_creator'])}"
                f")"
            )
            value_clauses.append(
                f"(CAST({_val(r['usage_date'])} AS DATE), "
                f"{_val(r['usage_account_id'])}, "
                f"{_val(r['region'])}, "
                f"{_val(r['charge_type'])}, "
                f"{_val(r['pricing_term'])}, "
                f"{_val(r['instance_type'])}, "
                f"{_val(r['instance_family'])}, "
                f"{um}, "
                f"{_val(r['cost_center'])}, "
                f"{_val(r['environment'])}, "
                f"{_val(r['project'])}, "
                f"{_val(r['team'])}, "
                f"{_val(r['unblended_cost'])}, "
                f"{_val(r['net_unblended_cost'])}, "
                f"{_val(r['blended_cost'])}, "
                f"{_val(r['amortized_cost'])}, "
                f"{_val(r['net_amortized_cost'])}, "
                f"{_val(r['total_usage_amount'])}, "
                f"{_val(r['line_item_count'])}, "
                f"{_val(r['currency_code'])})"
            )
        stmts_str = ",\n  ".join(value_clauses)
        statements.append(
            f"INSERT INTO {catalog}.{schema}.aws_cost_gold VALUES\n  {stmts_str}"
        )
    return statements


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic AWS cost data")
    parser.add_argument("--days",    type=int, default=int(_env("SYNTHETIC_DAYS", "90")),  help="Days of history to generate (default 90)")
    parser.add_argument("--catalog", type=str, default=_env("COST_OBS_CATALOG", "main"),   help="Unity Catalog catalog name")
    parser.add_argument("--schema",  type=str, default=_env("AWS_COST_SCHEMA", "aws_costs"),help="Schema name for AWS cost tables")
    parser.add_argument("--seed",    type=int, default=42,                                  help="Random seed for reproducibility")
    parser.add_argument("--truncate",action="store_true",                                   help="Truncate existing data before inserting")
    parser.add_argument("--dry-run", action="store_true",                                   help="Print row count but do not write to Databricks")
    args = parser.parse_args()

    print(f"[synthetic-aws] Generating {args.days} days of synthetic AWS cost data...")
    print(f"[synthetic-aws] Target: {args.catalog}.{args.schema}.aws_cost_gold")

    rows = generate_gold_rows(days=args.days, seed=args.seed, catalog=args.catalog, schema=args.schema)
    total_cost = sum(r["net_unblended_cost"] for r in rows)
    print(f"[synthetic-aws] Generated {len(rows):,} rows  |  Total synthetic cost: ${total_cost:,.2f}")

    if args.dry_run:
        print("[synthetic-aws] Dry run — no data written.")
        return

    # Connect to Databricks
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("[synthetic-aws] ERROR: databricks-sdk not installed. Run: pip install databricks-sdk")
        raise SystemExit(1)

    http_path = _env("DATABRICKS_HTTP_PATH", "")
    if not http_path:
        print("[synthetic-aws] ERROR: DATABRICKS_HTTP_PATH not set")
        raise SystemExit(1)

    wh_id = _warehouse_id_from_path(http_path)
    client = WorkspaceClient()
    print(f"[synthetic-aws] Connected to {client.config.host}")

    # Create schema + table
    print("[synthetic-aws] Creating schema and table...")
    _run_sql(client, wh_id, DDL_SCHEMA.format(catalog=args.catalog, schema=args.schema))
    _run_sql(client, wh_id, DDL_GOLD_TABLE.format(catalog=args.catalog, schema=args.schema))

    if args.truncate:
        print("[synthetic-aws] Truncating existing data...")
        _run_sql(client, wh_id, DDL_TRUNCATE.format(catalog=args.catalog, schema=args.schema))

    # Insert in batches
    insert_statements = _rows_to_insert_sql(args.catalog, args.schema, rows)
    print(f"[synthetic-aws] Inserting {len(rows):,} rows in {len(insert_statements)} batches...")
    for i, stmt in enumerate(insert_statements, 1):
        _run_sql(client, wh_id, stmt)
        if i % 10 == 0 or i == len(insert_statements):
            print(f"[synthetic-aws]   {i}/{len(insert_statements)} batches complete")

    print(f"[synthetic-aws] Done. {len(rows):,} rows written to {args.catalog}.{args.schema}.aws_cost_gold")
    print(f"[synthetic-aws] Refresh the app and switch to 'Actual AWS Costs' in the Cloud Costs tab.")


if __name__ == "__main__":
    main()
