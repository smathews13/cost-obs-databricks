#!/usr/bin/env python3
"""
generate_synthetic_azure_costs.py
-----------------------------------
Generates synthetic Azure Cost Management Export data that mirrors the
azure_cost_gold table used by the Cost Observability app.

Creates the schema + azure_cost_gold table if they don't exist, then inserts
realistic synthetic records to test the Cloud Costs (Azure) integration.

Usage:
    # From repo root — uses env vars DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
    python scripts/generate_synthetic_azure_costs.py

    # With overrides
    python scripts/generate_synthetic_azure_costs.py \\
        --days 90 \\
        --catalog main \\
        --schema azure_costs \\
        --seed 42

Environment variables (same as the app):
    DATABRICKS_HOST        Workspace URL
    DATABRICKS_TOKEN       Personal access token
    DATABRICKS_HTTP_PATH   SQL warehouse path
    COST_OBS_CATALOG       Default: main
    AZURE_COST_SCHEMA      Default: azure_costs
"""

import argparse
import math
import os
import random
from datetime import date, timedelta


# ---------------------------------------------------------------------------
# Realistic Azure workload profiles
# ---------------------------------------------------------------------------

SUBSCRIPTIONS = [
    {"subscription_id": "aaaa1111-bbbb-cccc-dddd-eeee00000001", "subscription_name": "prod-databricks",     "resource_group_prefix": "rg-dbx-prod"},
    {"subscription_id": "aaaa1111-bbbb-cccc-dddd-eeee00000002", "subscription_name": "dev-databricks",      "resource_group_prefix": "rg-dbx-dev"},
    {"subscription_id": "aaaa1111-bbbb-cccc-dddd-eeee00000003", "subscription_name": "analytics-platform",  "resource_group_prefix": "rg-dbx-analytics"},
]

LOCATIONS = ["eastus", "westeurope", "southeastasia"]

# VM sizes match cloud_pricing.py Azure pricing table
CLUSTER_PROFILES = [
    {"name": "etl-main-prod",       "source": "JOB",  "driver": "Standard_D8s_v3",  "workers": "Standard_D8s_v3",  "team": "data-eng",    "env": "prod", "project": "etl",       "base_cost": 290},
    {"name": "ml-training-gpu",     "source": "JOB",  "driver": "Standard_D4s_v3",  "workers": "Standard_NC12",    "team": "ml-platform", "env": "prod", "project": "ml-train",  "base_cost": 780},
    {"name": "analytics-shared",    "source": "UI",   "driver": "Standard_E8s_v3",  "workers": "Standard_E4s_v3",  "team": "analytics",   "env": "prod", "project": "reporting", "base_cost": 195},
    {"name": "dev-exploration",     "source": "UI",   "driver": "Standard_D4s_v3",  "workers": "Standard_D4s_v3",  "team": "analytics",   "env": "dev",  "project": "research",  "base_cost": 58},
    {"name": "dlt-streaming-prod",  "source": "DLT",  "driver": "Standard_D8s_v3",  "workers": "Standard_D8s_v3",  "team": "data-eng",    "env": "prod", "project": "streaming", "base_cost": 175},
    {"name": "dbt-jobs-nightly",    "source": "JOB",  "driver": "Standard_D4s_v3",  "workers": "Standard_D8s_v3",  "team": "data-eng",    "env": "prod", "project": "dbt",       "base_cost": 130},
    {"name": "feature-store-build", "source": "JOB",  "driver": "Standard_E16s_v3", "workers": "Standard_E8s_v3",  "team": "ml-platform", "env": "prod", "project": "features",  "base_cost": 395},
    {"name": "ad-hoc-research",     "source": "UI",   "driver": "Standard_D4s_v3",  "workers": "Standard_D4s_v3",  "team": "data-sci",    "env": "dev",  "project": "research",  "base_cost": 36},
    {"name": "compliance-reports",  "source": "JOB",  "driver": "Standard_F8s_v2",  "workers": "Standard_F8s_v2",  "team": "finance",     "env": "prod", "project": "finance",   "base_cost": 85},
    {"name": "inference-serving",   "source": "JOB",  "driver": "Standard_D8s_v3",  "workers": "Standard_NC6",     "team": "ml-platform", "env": "prod", "project": "serving",   "base_cost": 355},
    {"name": "data-quality-checks", "source": "JOB",  "driver": "Standard_D4s_v3",  "workers": "Standard_D4s_v3",  "team": "data-eng",    "env": "prod", "project": "dq",        "base_cost": 50},
    {"name": "backfill-historical", "source": "JOB",  "driver": "Standard_E16s_v3", "workers": "Standard_E8s_v3",  "team": "data-eng",    "env": "prod", "project": "etl",       "base_cost": 0,  "spike_days": True},
]

WAREHOUSE_PROFILES = [
    {"name": "prod-shared-warehouse",  "team": "analytics",   "env": "prod", "project": "reporting", "base_cost": 260},
    {"name": "dev-warehouse",          "team": "data-sci",    "env": "dev",  "project": "research",  "base_cost": 62},
    {"name": "bi-dashboard-warehouse", "team": "finance",     "env": "prod", "project": "finance",   "base_cost": 140},
    {"name": "ml-feature-queries",     "team": "ml-platform", "env": "prod", "project": "features",  "base_cost": 88},
]

# Azure meter categories for Databricks VM workloads
METER_CATEGORY_MAP = {
    "Compute":    ("Virtual Machines",  "D Series"),
    "Storage":    ("Storage",           "Premium SSD Managed Disks"),
    "Networking": ("Bandwidth",         "Inter-Region Outbound"),
    "Other":      ("Azure App Service", ""),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env(key: str, default: str) -> str:
    return os.getenv(key, default)


def _make_id(seed_str: str) -> str:
    rng = random.Random(seed_str)
    return "".join(f"{rng.randint(0, 255):02x}" for _ in range(8))


def _cluster_id(name: str) -> str:
    return _make_id(f"cluster-{name}")[:16]


def _warehouse_id(name: str) -> str:
    return _make_id(f"wh-{name}")[:16]


def _job_id(name: str) -> str:
    return str(random.Random(f"job-{name}").randint(100000, 999999))


def _instance_family(vm_size: str) -> str:
    import re
    m = re.match(r"^(Standard_[A-Z]+)", vm_size)
    return m.group(1) if m else vm_size


def _resource_id(subscription_id: str, rg: str, vm_name: str) -> str:
    return (
        f"/subscriptions/{subscription_id}/resourceGroups/{rg}"
        f"/providers/Microsoft.Compute/virtualMachines/{vm_name}"
    )


def _weekday_factor(d: date) -> float:
    return 0.42 if d.weekday() >= 5 else 1.0


def _trend_factor(d: date, end: date, days: int) -> float:
    age_frac = (end - d).days / max(days, 1)
    return 1.0 + 0.20 * (1 - age_frac)


def _noise(rng: random.Random, sigma: float = 0.12) -> float:
    u = max(rng.random(), 1e-9)
    v = rng.random()
    n = math.sqrt(-2 * math.log(u)) * math.cos(2 * math.pi * v)
    return 1.0 + n * sigma


# ---------------------------------------------------------------------------
# Data generation → azure_cost_gold rows
# ---------------------------------------------------------------------------

def generate_gold_rows(days: int, seed: int) -> list[dict]:
    rng = random.Random(seed)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days - 1)

    rows: list[dict] = []

    clusters_meta = []
    for i, cp in enumerate(CLUSTER_PROFILES):
        sub = SUBSCRIPTIONS[i % len(SUBSCRIPTIONS)]
        rg = f"{sub['resource_group_prefix']}-{cp['name'][:12]}"
        cluster_id = _cluster_id(cp["name"])
        job_id = _job_id(cp["name"]) if cp["source"] == "JOB" else None
        clusters_meta.append({
            **cp,
            "subscription": sub,
            "resource_group": rg,
            "location": LOCATIONS[i % len(LOCATIONS)],
            "cluster_id": cluster_id,
            "job_id": job_id,
        })

    warehouses_meta = []
    for i, wp in enumerate(WAREHOUSE_PROFILES):
        sub = SUBSCRIPTIONS[i % len(SUBSCRIPTIONS)]
        rg = f"{sub['resource_group_prefix']}-warehouses"
        wh_id = _warehouse_id(wp["name"])
        warehouses_meta.append({
            **wp,
            "subscription": sub,
            "resource_group": rg,
            "location": LOCATIONS[i % len(LOCATIONS)],
            "warehouse_id": wh_id,
        })

    current = start_date
    while current <= end_date:
        wf = _weekday_factor(current)
        tf = _trend_factor(current, end_date, days)
        date_str = current.isoformat()

        # --- Cluster rows ---
        for cp in clusters_meta:
            sub = cp["subscription"]

            if current.weekday() >= 5 and cp["env"] == "dev" and rng.random() < 0.65:
                pass  # will still be processed, just lower cost

            if cp.get("spike_days") and (end_date - current).days > 14:
                current = current + timedelta(days=1)
                break

            base = cp["base_cost"]
            if base == 0 and cp.get("spike_days"):
                base = rng.uniform(1100, 2600)

            daily_cost = base * wf * tf * _noise(rng)

            compute_cost = daily_cost * rng.uniform(0.76, 0.84)
            storage_cost = daily_cost * rng.uniform(0.09, 0.14)
            network_cost = max(0, daily_cost - compute_cost - storage_cost)

            pricing_model = rng.choices(
                ["OnDemand", "Reservation", "SavingsPlan"],
                weights=[0.45, 0.32, 0.23],
            )[0]

            for charge_type, cost in [
                ("Compute", compute_cost),
                ("Storage", storage_cost),
                ("Networking", network_cost),
            ]:
                meter_cat, meter_sub = METER_CATEGORY_MAP.get(charge_type, ("Other", ""))
                vm_size = cp["workers"] if charge_type == "Compute" else None

                rows.append({
                    "usage_date": date_str,
                    "subscription_id": sub["subscription_id"],
                    "subscription_name": sub["subscription_name"],
                    "resource_group": cp["resource_group"],
                    "location": cp["location"],
                    "charge_type": charge_type,
                    "pricing_model": pricing_model,
                    "meter_category": meter_cat,
                    "meter_subcategory": meter_sub,
                    "consumed_service": "Microsoft.Compute" if charge_type in ("Compute", "Storage") else "Microsoft.Network",
                    "vm_size": vm_size,
                    "instance_family": _instance_family(vm_size) if vm_size else None,
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
                    "cost_in_billing_currency": round(cost, 6),
                    "cost_in_usd": round(cost * rng.uniform(0.98, 1.02), 6),
                    "effective_price": round(cost / max(rng.uniform(0.5, 8.0), 0.01), 6),
                    "unit_price": round(cost / max(rng.uniform(0.5, 8.0), 0.01) * rng.uniform(1.0, 1.3), 6),
                    "total_quantity": round(cost / rng.uniform(0.08, 0.45), 4),
                    "line_item_count": rng.randint(6, 96),
                    "currency_code": "USD",
                })

        # --- Warehouse rows ---
        for wp in warehouses_meta:
            sub = wp["subscription"]
            if current.weekday() >= 5 and wp["env"] == "dev" and rng.random() < 0.75:
                continue

            daily_cost = wp["base_cost"] * wf * tf * _noise(rng)
            pricing_model = rng.choices(["OnDemand", "SavingsPlan"], weights=[0.60, 0.40])[0]

            rows.append({
                "usage_date": date_str,
                "subscription_id": sub["subscription_id"],
                "subscription_name": sub["subscription_name"],
                "resource_group": wp["resource_group"],
                "location": wp["location"],
                "charge_type": "Compute",
                "pricing_model": pricing_model,
                "meter_category": "Virtual Machines",
                "meter_subcategory": "D Series",
                "consumed_service": "Microsoft.Compute",
                "vm_size": "Standard_D8s_v3",
                "instance_family": "Standard_D",
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
                "cost_in_billing_currency": round(daily_cost, 6),
                "cost_in_usd": round(daily_cost * rng.uniform(0.98, 1.02), 6),
                "effective_price": round(daily_cost / max(rng.uniform(1.0, 6.0), 0.01), 6),
                "unit_price": round(daily_cost / max(rng.uniform(1.0, 6.0), 0.01) * rng.uniform(1.0, 1.25), 6),
                "total_quantity": round(daily_cost / rng.uniform(0.20, 0.50), 4),
                "line_item_count": rng.randint(4, 48),
                "currency_code": "USD",
            })

        current += timedelta(days=1)

    return rows


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'Synthetic Azure cost data for UI testing'"

DDL_GOLD_TABLE = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.azure_cost_gold (
  usage_date              DATE,
  subscription_id         STRING,
  subscription_name       STRING,
  resource_group          STRING,
  location                STRING,
  charge_type             STRING,
  pricing_model           STRING,
  meter_category          STRING,
  meter_subcategory       STRING,
  consumed_service        STRING,
  vm_size                 STRING,
  instance_family         STRING,
  usage_metadata          STRUCT<
                            cluster_id:       STRING,
                            cluster_name:     STRING,
                            warehouse_id:     STRING,
                            job_id:           STRING,
                            instance_pool_id: STRING,
                            cluster_creator:  STRING
                          >,
  cost_center             STRING,
  environment             STRING,
  project                 STRING,
  team                    STRING,
  cost_in_billing_currency   DOUBLE,
  cost_in_usd                DOUBLE,
  effective_price            DOUBLE,
  unit_price                 DOUBLE,
  total_quantity             DOUBLE,
  line_item_count            BIGINT,
  currency_code              STRING
)
USING DELTA
COMMENT 'Synthetic Azure Cost Export gold table — generated for UI testing'
PARTITIONED BY (usage_date)
"""

DDL_TRUNCATE = "DELETE FROM {catalog}.{schema}.azure_cost_gold WHERE 1=1"


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

def _run_sql(client, warehouse_id: str, statement: str) -> None:
    from databricks.sdk.service.sql import StatementState
    resp = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="50s",
    )
    if resp.status.state not in (StatementState.SUCCEEDED,):
        raise RuntimeError(f"SQL failed [{resp.status.state}]: {resp.status.error}")


def _warehouse_id_from_path(http_path: str) -> str:
    return http_path.rstrip("/").split("/")[-1]


def _rows_to_insert_sql(catalog: str, schema: str, rows: list[dict]) -> list[str]:
    def _val(v) -> str:
        if v is None:
            return "NULL"
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
                f"{_val(r['subscription_id'])}, "
                f"{_val(r['subscription_name'])}, "
                f"{_val(r['resource_group'])}, "
                f"{_val(r['location'])}, "
                f"{_val(r['charge_type'])}, "
                f"{_val(r['pricing_model'])}, "
                f"{_val(r['meter_category'])}, "
                f"{_val(r['meter_subcategory'])}, "
                f"{_val(r['consumed_service'])}, "
                f"{_val(r['vm_size'])}, "
                f"{_val(r['instance_family'])}, "
                f"{um}, "
                f"{_val(r['cost_center'])}, "
                f"{_val(r['environment'])}, "
                f"{_val(r['project'])}, "
                f"{_val(r['team'])}, "
                f"{_val(r['cost_in_billing_currency'])}, "
                f"{_val(r['cost_in_usd'])}, "
                f"{_val(r['effective_price'])}, "
                f"{_val(r['unit_price'])}, "
                f"{_val(r['total_quantity'])}, "
                f"{_val(r['line_item_count'])}, "
                f"{_val(r['currency_code'])})"
            )
        stmts_str = ",\n  ".join(value_clauses)
        statements.append(
            f"INSERT INTO {catalog}.{schema}.azure_cost_gold VALUES\n  {stmts_str}"
        )
    return statements


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic Azure cost data")
    parser.add_argument("--days",    type=int, default=int(_env("SYNTHETIC_DAYS", "90")),     help="Days of history to generate (default 90)")
    parser.add_argument("--catalog", type=str, default=_env("COST_OBS_CATALOG", "main"),      help="Unity Catalog catalog name")
    parser.add_argument("--schema",  type=str, default=_env("AZURE_COST_SCHEMA", "azure_costs"), help="Schema name for Azure cost tables")
    parser.add_argument("--seed",    type=int, default=42,                                     help="Random seed for reproducibility")
    parser.add_argument("--truncate",action="store_true",                                      help="Truncate existing data before inserting")
    parser.add_argument("--dry-run", action="store_true",                                      help="Print row count but do not write to Databricks")
    args = parser.parse_args()

    print(f"[synthetic-azure] Generating {args.days} days of synthetic Azure cost data...")
    print(f"[synthetic-azure] Target: {args.catalog}.{args.schema}.azure_cost_gold")

    rows = generate_gold_rows(days=args.days, seed=args.seed)
    total_cost = sum(r["cost_in_billing_currency"] for r in rows)
    print(f"[synthetic-azure] Generated {len(rows):,} rows  |  Total synthetic cost: ${total_cost:,.2f}")

    if args.dry_run:
        print("[synthetic-azure] Dry run — no data written.")
        return

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("[synthetic-azure] ERROR: databricks-sdk not installed. Run: pip install databricks-sdk")
        raise SystemExit(1)

    http_path = _env("DATABRICKS_HTTP_PATH", "")
    if not http_path:
        print("[synthetic-azure] ERROR: DATABRICKS_HTTP_PATH not set")
        raise SystemExit(1)

    wh_id = _warehouse_id_from_path(http_path)
    client = WorkspaceClient()
    print(f"[synthetic-azure] Connected to {client.config.host}")

    print("[synthetic-azure] Creating schema and table...")
    _run_sql(client, wh_id, DDL_SCHEMA.format(catalog=args.catalog, schema=args.schema))
    _run_sql(client, wh_id, DDL_GOLD_TABLE.format(catalog=args.catalog, schema=args.schema))

    if args.truncate:
        print("[synthetic-azure] Truncating existing data...")
        _run_sql(client, wh_id, DDL_TRUNCATE.format(catalog=args.catalog, schema=args.schema))

    insert_statements = _rows_to_insert_sql(args.catalog, args.schema, rows)
    print(f"[synthetic-azure] Inserting {len(rows):,} rows in {len(insert_statements)} batches...")
    for i, stmt in enumerate(insert_statements, 1):
        _run_sql(client, wh_id, stmt)
        if i % 10 == 0 or i == len(insert_statements):
            print(f"[synthetic-azure]   {i}/{len(insert_statements)} batches complete")

    print(f"[synthetic-azure] Done. {len(rows):,} rows written to {args.catalog}.{args.schema}.azure_cost_gold")
    print(f"[synthetic-azure] Refresh the app and switch to 'Actual Azure Costs' in the Cloud Costs tab.")


if __name__ == "__main__":
    main()
