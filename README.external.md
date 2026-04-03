# Cost Observability & Control for Databricks

[![Deploy to Databricks](https://img.shields.io/badge/Deploy%20to-Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://accounts.cloud.databricks.com/select-workspace?destination_url=/apps/install?repo_url=https://github.com/smathews13/cost-obs-databricks)

> **⚠️ Not Official Databricks Software**
> This application is built and maintained by the Databricks field engineering team and is **not an official Databricks product**. It is not covered by Databricks Support SLAs. Your Databricks account team can help you deploy, configure, and troubleshoot this app as part of your engagement.

> **🔧 Customization Notice**
> You are welcome to modify and customize this application's source code to fit your organization's requirements. However, be aware that local customizations may conflict with future upstream updates. We recommend tracking your changes in a fork and reviewing diffs carefully before pulling upstream updates.

---

A full-stack Databricks App for account-level compute cost visibility, chargeback, and anomaly detection across your entire Databricks platform.

Built on FastAPI + React, deployed as a [Databricks App](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html) with OAuth authentication and serverless compute built in. Supports **multi-cloud deployment** across AWS and Azure with automatic cloud detection.

---

## What It Does

### DBU Overview
| Feature | Description |
|---|---|
| **Spend Over Time** | Daily spend timeseries by product category |
| **Spend by Product** | Horizontal bar chart with workspace filter — SQL, ETL, Interactive, Model Serving, Vector Search, Fine-Tuning, AI Functions, Serverless |
| **Spend by SKU** | Top 10 SKUs with workspace filter |
| **Spend by User** | Top spenders by DBU cost |
| **Workspace Table** | Per-workspace cost breakdown with top products/users |
| **Interactive Compute** | All-purpose cluster usage by user, cluster, or notebook with historical toggle |
| **ETL Breakdown** | Jobs and SDP pipeline spend with type filters, pagination, and historical toggle |
| **Account Prices Toggle** | Switch between list prices and negotiated account prices (from `system.billing.account_prices`, private preview) |

### KPIs & Trends
| Feature | Description |
|---|---|
| **Platform KPIs** | Total spend, DBUs, successful runs, active clusters, workspaces, models served |
| **KPI Drill-Downs** | Click any KPI to see daily/monthly trend lines in a modal |
| **Spend Anomalies** | Largest day-over-day spend changes with date search and AI analysis |

### SQL
| Feature | Description |
|---|---|
| **Query Spend by Source** | Daily cost timeseries by query source type (DBSQL, Genie, Dashboard, etc.) |
| **Warehouse Spend by Type** | Daily spend area chart segmented by Serverless/Pro/Classic |
| **Warehouses by Size** | Distribution of warehouses by size with workspace filter |
| **Top Users** | Highest-cost SQL users |
| **Query Source Breakdown** | Drill-down table by source type |
| **Most Expensive Queries** | Top queries with historical toggle, pagination, and query profile links |
| **Warehouse Rightsizing** | Automated recommendations to right-size overprovisioned warehouses based on `system.query.history` utilization heuristics |

### Cloud Costs
| Feature | Description |
|---|---|
| **Multi-Cloud Support** | Auto-detects AWS or Azure from workspace URL; displays cloud-specific logos, instance types, pricing links, and setup guides |
| **Infrastructure KPIs** | Total cloud cost, DBU hours, avg active clusters/day, avg cluster cost — all derived from billing data |
| **Cost Over Time** | Area chart of estimated infrastructure costs with instance family filter bubbles |
| **Instance Family Usage** | DBU hours by EC2 (AWS) or VM series (Azure) instance family |
| **Cluster Table** | Per-cluster cost attribution with instance types, pricing links, pagination, and historical toggle |
| **Actual Costs Integration** | Toggle between estimated and actual costs when AWS CUR 2.0 or Azure Cost Management Export is configured |
| **Cloud Integration Wizard** | In-app 5-step setup guide for both AWS and Azure actual cost integration |
| **2025 Pricing** | Updated EC2 and Azure VM pricing covering: AWS m7i, r7i, c7i, i4i, g6; Azure Dv6, Ev5/v6, NC A100 v4, ND A100 v4, NVadsA10 v5 |

### AI/ML
| Feature | Description |
|---|---|
| **AI/ML Spend Over Time** | Stacked area chart by AI/ML category |
| **Cost by Category** | Donut chart of spend distribution |
| **Top Serverless Endpoints** | Highest-cost inference endpoints |
| **ML Runtime Clusters** | Clusters running ML/GPU runtimes with hyperlinks, pagination, and historical toggle |
| **Agent Bricks** | Knowledge Assistants and other agent types with type filters, pagination, and historical toggle |

### Apps
| Feature | Description |
|---|---|
| **App Cost Dashboard** | Per-app spend with SKU breakdown drill-down |
| **Connected Artifacts** | Serving endpoints, SQL warehouses, and other resources used by apps |

### Tagging Hub
| Feature | Description |
|---|---|
| **Tag Coverage** | Tagged vs untagged spend ratio |
| **Spend by Tag** | Cost attribution by tag key/value pairs |
| **Spend by Key** | Horizontal bar chart of top tag keys |
| **Untagged Resources** | Clusters, jobs, pipelines, warehouses, and endpoints missing tags — with dynamic suggested tags per resource type, historical toggle, and pagination |

### Users
| Feature | Description |
|---|---|
| **Users by Spend** | Ranked list of users by total DBU cost across all products |
| **Spend Over Time per User** | Daily timeseries for any selected user |
| **Product Breakdown** | Cost split by product category per user |
| **User Growth Trend** | Active user count over time |

### Lakebase
| Feature | Description |
|---|---|
| **Instance Overview** | Status and configuration of Databricks-managed PostgreSQL (Lakebase) instances |
| **Migration Status** | Progress tracker for app backing store configuration |
| **Usage Timeseries** | Lakebase billing data over time |
| **Cost Comparison** | Materialized view refresh cost vs. Lakebase hosting cost |

> **Note:** Lakebase is optional. When not configured, the app falls back to Delta tables for all persistent state (alerts, settings, permissions). See [Lakebase Integration](#lakebase-integration) below.

### Use Cases
| Feature | Description |
|---|---|
| **Business Use Case Tracking** | Define and track cost attribution to business use cases |
| **Lifecycle Stages** | POC → pilot → production progression tracking |
| **Cost per Use Case** | DBU cost allocated to each tracked initiative |

### Alerts
| Feature | Description |
|---|---|
| **Threshold Alerts** | Daily spend spike detection with configurable thresholds |
| **Email Digest** | Scheduled alert digest via SMTP or webhook |
| **Slack Webhooks** | Post alerts to Slack channels |
| **Alert History** | Historical log of triggered alerts |

### Settings
| Feature | Description |
|---|---|
| **General** | Date range selection and display preferences |
| **Configuration** | Warehouse, catalog, schema, and Genie Space configuration |
| **Connections** | Shows the default Databricks workspace environment (cloud provider + host) |
| **User Permissions** | Admin-only management of who has admin vs. read-only access to the app |
| **Accuracy Checks** | 9-check billing reconciliation with pass/fail status |
| **Account Pricing** | Toggle between standard list prices and negotiated account prices |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Databricks App                        │
│                                                         │
│  ┌──────────────┐          ┌──────────────────────────┐ │
│  │  React + TS  │◄────────►│  FastAPI (4 workers)     │ │
│  │  Vite + TW   │  REST    │  18 routers              │ │
│  └──────────────┘          └──────────┬───────────────┘ │
│                                       │                 │
└───────────────────────────────────────┼─────────────────┘
                                        │ Databricks SDK
                      ┌─────────────────┼──────────────────┐
                      │                 ▼                  │
                      │   ┌──────────────────────────────┐ │
                      │   │  SQL Warehouse (Serverless)  │ │
                      │   └─────────────┬────────────────┘ │
                      │                 │                  │
                      │   ┌─────────────▼────────────────┐ │
                      │   │  system.billing.usage        │ │
                      │   │  system.billing.list_prices  │ │
                      │   │  system.billing.account_prices│ │
                      │   │  system.query.history        │ │
                      │   │  system.compute.*            │ │
                      │   │  system.lakeflow.*           │ │
                      │   │  system.serving.*            │ │
                      │   │  system.access.*             │ │
                      │   │  Materialized Views          │ │
                      │   └──────────────────────────────┘ │
                      │         Databricks                 │
                      └────────────────────────────────────┘
                                        │
                      ┌─────────────────▼──────────────────┐
                      │  Lakebase (optional, PostgreSQL 16) │
                      │  App state: alerts, permissions,    │
                      │  settings, user preferences         │
                      │  Falls back to Delta tables if not  │
                      │  configured                         │
                      └────────────────────────────────────┘
```

### Data Sources

All billing and compute data is **account-level** — queries run against Unity Catalog system tables which span all workspaces in the account.

| System Table | Usage |
|---|---|
| `system.billing.usage` | Core spend/DBU data for all products |
| `system.billing.list_prices` | Standard SKU pricing for cost calculation |
| `system.billing.account_prices` | Negotiated/discounted account-specific prices (private preview) |
| `system.query.history` | SQL query attribution, source tracking, and rightsizing signals |
| `system.compute.clusters` | Cluster metadata, names, owners, ML runtime detection |
| `system.compute.warehouses` | Warehouse names, types, sizes |
| `system.lakeflow.pipelines` | SDP pipeline name resolution |
| `system.lakeflow.jobs` | Job name resolution |
| `system.lakeflow.job_run_timeline` | Job success/failure tracking for KPIs |
| `system.serving.served_entities` | ML endpoint metadata |
| `system.access.workspaces_latest` | Workspace name resolution |

### Performance Optimizations

| Optimization | Detail |
|---|---|
| **Materialized Views** | Pre-aggregated Delta tables for sub-second dashboard loads |
| **Parallel Query Execution** | `ThreadPoolExecutor` (10 workers) runs 6–8 queries concurrently per bundle endpoint |
| **4-Hour Query Cache** | `TTLCache` with 500 entries — cost data changes at most once per day |
| **SDK Call Caching** | Pipeline names, group membership, and app registry cached for 1 hour |
| **Bundle Endpoints** | Single API call returns all data for a tab (reduces HTTP round-trips) |
| **React Query** | 30-minute stale time, 1-hour GC — prevents redundant refetches |
| **Lazy-Loaded Chunks** | Each heavy tab (Cloud Costs, AI/ML, Tagging, etc.) is a separate JS chunk loaded on first visit |

---

## Deployment

### Deploy from Git

The recommended deployment method is to deploy directly from this Git repository using Databricks Apps' built-in Git integration. No local clone or file sync required — Databricks pulls the source code and pre-built assets directly from GitHub.

> See [docs/PRE_DEPLOYMENT_CHECKLIST.md](docs/PRE_DEPLOYMENT_CHECKLIST.md) for required permissions and environment prerequisites before deploying.

**Steps:**

1. In your Databricks workspace, go to **Apps → Create App → Deploy from Git**
2. Enter the repo URL: `https://github.com/smathews13/cost-obs-databricks`
3. **Git reference:** `main` (or a release tag e.g. `v1.0.0`)
4. **Reference type:** `Branch` (or `Tag` if pinning to a release)
5. **Source code path:** leave empty (the entire project is at the repo root)
6. Fill in the required environment variables (see `app.yaml` for the full list)

Or click the **Deploy to Databricks** button at the top of this README to launch directly into your workspace.

Databricks Apps supports GitHub, GitLab, Bitbucket, and other providers. You can pin to a branch, tag, or specific commit SHA. Private repositories require Git credentials configured on the app's service principal.

### Required Environment Variables

| Variable | Required | Description |
|---|---|---|
| `DATABRICKS_HOST` | Yes | Workspace URL, e.g. `https://abc.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Yes | Personal access token or service principal secret |
| `DATABRICKS_HTTP_PATH` | Yes | SQL warehouse path, or `auto` to create one automatically |
| `COST_OBS_CATALOG` | No | Unity Catalog for materialized views (default: `main`) |
| `COST_OBS_SCHEMA` | No | Schema name (default: `cost_obs`) |
| `GENIE_SPACE_ID` | No | Genie Space ID for AI cost chat |
| `AZURE_SUBSCRIPTION_ID` | No | Azure subscription ID (shown in account banner on Azure workspaces) |
| `SMTP_HOST` / `SMTP_*` | No | Email alert configuration |
| `ENDPOINT_NAME` | No | Lakebase endpoint name (optional — app falls back to Delta tables if not set) |
| `PGHOST` | No | Lakebase PostgreSQL hostname (optional — app falls back to Delta tables if not set) |
| `AWS_COST_CATALOG` | No | Catalog for AWS CUR tables (default: `billing`) |
| `AWS_COST_SCHEMA` | No | Schema for AWS CUR tables (default: `aws`) |
| `AZURE_COST_CATALOG` | No | Catalog for Azure cost tables (default: `billing`) |
| `AZURE_COST_SCHEMA` | No | Schema for Azure cost tables (default: `azure`) |

### First-Run Setup Wizard

On first deploy, the app automatically launches a 4-step setup wizard to configure your environment:

**Step 1 — Environment**
Detects cloud provider (AWS/Azure), warehouse status, authenticated identity, and catalog/schema configuration.

**Step 2 — Permissions**
Checks system table access. Displays exact `GRANT` statements for any missing permissions:
```sql
GRANT SELECT ON system.billing.usage TO `app-service-principal@...`;
GRANT SELECT ON system.billing.list_prices TO `app-service-principal@...`;
-- etc.
```

**Step 3 — Create Tables**
Creates materialized views pre-aggregating 365 days of billing history (typically 2–5 minutes).

**Step 4 — Complete**
Click **Go to Dashboard** to start exploring.

The wizard can be re-launched at any time from **Settings → General → Rerun Setup Wizard**.

---

## Local Development

### Prerequisites
- Python 3.11+
- [Bun](https://bun.sh) (frontend)
- Databricks workspace with system tables enabled
- A SQL warehouse HTTP path

### Setup

```bash
# Clone
git clone https://github.com/smathews13/cost-obs-databricks
cd cost-obs-databricks

# Backend
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Frontend
cd client && bun install && cd ..

# Configure
cp app.yaml.example app.yaml
# Edit app.yaml with your Databricks credentials
```

### Start Dev Servers

```bash
# Backend (port 8000)
source .venv/bin/activate
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com \
DATABRICKS_TOKEN=dapi... \
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-id \
COST_OBS_CATALOG=main \
uvicorn server.app:app --host 0.0.0.0 --port 8000 --reload

# Frontend (port 5173, separate terminal)
cd client && bun run dev
```

Open http://localhost:5173

---

## Cloud Cost Integration

The Cloud Costs tab can display **actual** AWS or Azure billing data alongside estimated costs. This requires deploying the [cloud-infra-costs](https://github.com/databricks-solutions/cloud-infra-costs) project separately.

### AWS (CUR 2.0)

The app reads from `billing.aws.actuals_gold` — the output of the `cloud-infra-costs/aws` Declarative Automation Bundle. Setup steps (also available in the in-app wizard):

1. Create an S3 bucket in your AWS payer account
2. Configure a CUR 2.0 Standard Data Export (Hourly, Parquet, include resource IDs)
3. Create a Unity Catalog Storage Credential + External Location pointing to the S3 bucket
4. Clone `cloud-infra-costs/aws`, set `catalog=billing`, `schema=aws`, `storage_location=<s3-path>`, and deploy the bundle
5. Verify `billing.aws.actuals_gold` populates and toggle "Actual Costs" in the Cloud Costs tab

Override table location via env vars: `AWS_COST_CATALOG`, `AWS_COST_SCHEMA`.

### Azure (Cost Management Export)

The app reads from `billing.azure.actuals_gold` — the output of the `cloud-infra-costs/azure` project. Setup steps:

1. Run Terraform from `cloud-infra-costs/azure/terraform` to provision the Storage Account, External Location, catalog, schema, and volume
2. Create Actuals (required), Amortized, and FOCUS cost exports in the Azure Portal → Cost Exports, pointing to the Terraform-provisioned container
3. Configure `databricks.yml` with `catalog=billing`, `schema=azure`, `warehouse_id=<id>`
4. Authenticate and deploy the bundle: `databricks bundle deploy --target dev`
5. Verify `billing.azure.actuals_gold` populates

Override table location via env vars: `AZURE_COST_CATALOG`, `AZURE_COST_SCHEMA`.

---

## Lakebase Integration

The app optionally uses [Databricks Lakebase](https://docs.databricks.com/en/database-objects/lakebase.html) (managed PostgreSQL 16) as its persistent backing store for:

- Alert configurations and thresholds
- User permissions (admin / read-only roles)
- App settings and preferences
- Webhook configurations

**Lakebase is optional.** If `PGHOST` and `ENDPOINT_NAME` are not set, the app automatically falls back to Delta tables for all persistent state. No configuration or extra setup is needed to get started — deploy from Git and the app handles the rest.

To enable Lakebase, set `ENDPOINT_NAME` and `PGHOST` in your app's environment variables after provisioning a Lakebase instance in your workspace. The app's service principal needs `CAN_MANAGE` on the Lakebase project to generate short-lived database credentials at startup.

For a deeper walkthrough of this pattern, see: [How to Use Lakebase as a Transactional Data Layer for Databricks Apps](https://www.databricks.com/blog/how-use-lakebase-transactional-data-layer-databricks-apps).

---

## App Observability

Databricks Apps has built-in [OpenTelemetry-based observability](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/observability) that automatically captures traces, logs, and metrics into Unity Catalog tables:

| UC Table | Contents |
|---|---|
| `otel_metrics` | Request counts, latency histograms, error rates |
| `otel_spans` | Distributed traces for API requests end-to-end |
| `otel_logs` | App log output with trace correlation |

All telemetry is queryable via SQL in your workspace.

---

## Security

- All dashboard API endpoints are authenticated via Databricks OAuth (handled by the Databricks Apps platform)
- The `X-Forwarded-Email` header is used to identify the requesting user
- Settings mutation endpoints (cloud connections, webhook config, user permissions) require **admin role** — enforced server-side before any state change
- Webhook URLs are masked in API responses (never returned in plaintext after save)

---

## Project Structure

```
cost-obs-databricks/
├── server/                      # FastAPI backend
│   ├── app.py                   # Entry point, startup tasks, router registration
│   ├── db.py                    # SQL connector, 4h TTL query cache, connection pool
│   ├── postgres.py              # Lakebase PostgreSQL connection pool (optional)
│   ├── materialized_views.py    # MV creation, refresh, and query templates
│   ├── alerting.py              # Spike detection logic
│   ├── alert_manager.py         # Alert persistence and delivery
│   ├── cloud_pricing.py         # EC2 / Azure VM pricing for cost estimates
│   ├── queries/
│   │   ├── __init__.py          # Core billing SQL
│   │   └── reconciliation.py   # Billing accuracy cross-checks
│   └── routers/                 # 18 API route handlers
│       ├── billing.py           # Core spend, KPIs, user/product breakdowns
│       ├── dbsql.py             # SQL tab bundle
│       ├── warehouse_health.py  # Warehouse utilization and rightsizing
│       ├── aiml.py              # AI/ML cost center
│       ├── apps.py              # Databricks Apps cost tracking
│       ├── tagging.py           # Tag coverage and untagged resource surfacing
│       ├── aws_actual.py        # AWS CUR actual cost queries
│       ├── azure_actual.py      # Azure actual cost queries
│       ├── alerts.py            # Threshold alerts and notifications
│       ├── use_cases.py         # Business use case tracking
│       ├── users_groups.py      # User spend analytics
│       ├── genie.py             # Genie AI integration
│       ├── settings.py          # App config, cloud connections, user permissions
│       └── setup.py             # First-run setup wizard
│
├── client/                      # React frontend
│   └── src/
│       ├── App.tsx              # Main dashboard (12 tabs, lazy-loaded chunks)
│       └── components/          # 30+ components
│
├── static/                      # Pre-built frontend assets (committed for git deployments)
├── app.yaml                     # Databricks Apps config with environment variables
├── app.yaml.example             # Environment variable template
├── pyproject.toml               # Python dependencies
└── docs/                        # Setup guides and architecture docs
```

---

## API Overview

The backend exposes a REST API at `/api/`. Key endpoints:

| Endpoint | Description |
|---|---|
| `GET /api/billing/dashboard-bundle-fast` | All DBU overview data in one parallel call |
| `GET /api/billing/by-product` | Spend by product category with workspace filter |
| `GET /api/billing/sku-breakdown` | Top SKUs with workspace filter |
| `GET /api/billing/spend-by-user-group` | Top users by spend |
| `GET /api/billing/infra-bundle` | Cloud cost estimates with billing-derived KPIs |
| `GET /api/dbsql/dashboard-bundle` | SQL tab data (sources, users, warehouses, queries) |
| `GET /api/warehouse-health/recommendations` | Rightsizing recommendations |
| `GET /api/aws-actual/dashboard-bundle` | AWS CUR actual cost data bundle |
| `GET /api/azure-actual/dashboard-bundle` | Azure actual cost data bundle |
| `GET /api/aiml/dashboard-bundle` | AI/ML cost center data |
| `GET /api/apps/dashboard-bundle` | Apps cost data |
| `GET /api/tagging/dashboard-bundle` | Tagging hub data |
| `GET /api/billing/platform-kpis-bundle` | Platform KPIs and anomalies |
| `GET /api/users-groups/bundle` | User spend analytics |
| `GET /api/reconciliation/run` | Run all 9 billing accuracy checks |
| `POST /api/genie/message` | Natural language cost query via Genie |
| `GET /api/health` | Health check |

Full interactive API docs at `http://localhost:8000/docs` (FastAPI Swagger UI).

---

## Cost Reconciliation

The app includes 9 automated billing accuracy checks:

1. **Ground truth baseline** — raw billing totals
2. **Product completeness** — product sum must equal ground truth (≤0.01% tolerance)
3. **Workspace completeness** — workspace sum must equal ground truth
4. **Price coverage** — % of billing rows with no matching list price
5. **Null attribution** — % of spend with no cluster/warehouse/job/pipeline ID
6. **Price uniqueness** — detect duplicate active prices per SKU
7. **SQL attribution** — `billing.usage` SQL spend vs `query.history` proportional allocation
8. **Query history duplicates** — detect duplicate rows in `system.query.history`
9. **MV vs live** — materialized view totals vs live query (staleness detection)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | React 19, TypeScript, Vite, Tailwind CSS 4, Recharts, TanStack Query v5 |
| Backend | Python 3.11+, FastAPI, Databricks SQL Connector, Databricks SDK 0.81+ |
| Data | Databricks system tables (account-level), Unity Catalog, Delta materialized views |
| Persistence | Lakebase (optional, Databricks-managed PostgreSQL 16); Delta table fallback |
| Deployment | Databricks Apps (managed OAuth, serverless compute), multi-cloud (AWS + Azure) |
| Caching | TTLCache (4h query cache, 1h SDK cache), React Query (30min stale time) |

---

## Docs

| Doc | Description |
|---|---|
| [Pre-Deployment Checklist](docs/PRE_DEPLOYMENT_CHECKLIST.md) | Required permissions and environment prerequisites |
| [Genie Setup](docs/GENIE_SETUP.md) | Configure Databricks Genie for AI cost queries |
| [Alerting System](docs/alerting_system.md) | Alert types, thresholds, and email/webhook setup |
| [DBSQL Cost Architecture](docs/dbsql_cost_architecture.md) | How SQL warehouse costs are attributed |
| [Performance](docs/PERFORMANCE_AUDIT.md) | Query optimization and materialized view strategy |
