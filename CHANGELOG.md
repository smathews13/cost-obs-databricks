# Changelog

## v1.0.0 — 2026-04-03

Initial customer release. All features below were built and validated across AWS and Azure deployments.

### Dashboard Tabs

- **DBU Overview** — daily spend timeseries, spend by product/SKU/user, workspace table, interactive compute, ETL breakdown
- **KPIs & Trends** — platform KPIs with clickable drill-down modals, spend anomaly detection
- **SQL** — query spend by source, warehouse spend by type/size, top users, most expensive queries, query origin attribution, warehouse rightsizing recommendations
- **Cloud Costs** — estimated AWS/Azure infrastructure costs, instance family breakdown, cluster table, actual cost toggle (AWS CUR 2.0 / Azure Cost Management Export), 5-step integration wizard
- **AI/ML** — stacked spend by category, top serverless endpoints, ML runtime clusters, Agent Bricks
- **Apps** — per-app cost attribution, connected artifacts
- **Tagging Hub** — tag coverage, spend by tag/key, untagged resources with suggested tags
- **Users** — ranked user spend, per-user timeseries, product breakdown, active user growth
- **Use Cases** — business use case tracking with lifecycle stages
- **Alerts** — threshold-based spend alerts, email digest, Slack webhook
- **Lakebase** — Lakebase instance status, migration tracker, cost comparison (gated by feature flag)

### Architecture

- FastAPI backend with 18 routers, 4 uvicorn workers
- React 19 + TypeScript + Vite frontend, 12 lazy-loaded tab chunks
- Databricks system tables as data source (account-level, all workspaces)
- Delta materialized views for sub-second load times
- Lakebase (PostgreSQL 16) as persistent backing store for alerts, permissions, settings
- Multi-cloud deploy automation via `dba_deploy.sh` (AWS + Azure)

### Features

- **Account Prices** — toggle between standard list prices and negotiated account prices (`system.billing.account_prices`, private preview)
- **Query Origin Attribution** — human vs Genie vs MCP/tool-use vs service principal, daily timeseries + per-warehouse breakdown
- **Warehouse Rightsizing** — automated recommendations based on `system.query.history` utilization heuristics
- **Cloud Integration Wizard** — in-app step-by-step guide for connecting AWS CUR 2.0 or Azure Cost Management Export
- **PDF Export** — multi-section cost reports for stakeholder sharing
- **Genie Assistant** — natural language cost queries via Databricks Genie
- **Admin Role Gating** — settings mutations (cloud connections, webhooks, user permissions) require admin role, enforced server-side
- **Setup Wizard** — guided first-run setup for permissions, materialized view creation, and configuration

### Performance

- Main bundle: 408 kB (down from 861 kB) via lazy-loaded tab chunks
- 30x+ query speed via materialized views pre-aggregating 365 days of history
- Parallel query execution (ThreadPoolExecutor, 10 workers) for bundle endpoints
- 4-hour query cache (TTLCache, 500 entries)
- React Query 30-minute stale time prevents redundant fetches

### Cost Accuracy Fixes

- ETL breakdown: removed incorrect `OR usage_metadata.dlt_pipeline_id IS NOT NULL` clause that was pulling in DATABASE/VECTOR_SEARCH/SQL spend
- DBSQL attribution MV (`dbsql_cost_per_query`): added to daily refresh job SQL
- Date boundary alignment between materialized views and source billing table

### Security

- Admin endpoint enforcement via `_require_admin()` on all settings mutation routes
- Parameterized SQL throughout (no f-string injection vectors)
- Webhook URLs masked in API responses
- No hardcoded secrets in committed code
