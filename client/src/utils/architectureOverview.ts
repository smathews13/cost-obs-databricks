export interface ArchitectureComponent {
  name: string;
  role: string;
}

export interface ArchitectureSourceGroup {
  label: string;
  tables: string[];
  note?: string;
}

export interface ArchitectureFlowColumn {
  title: string;
  lines: string[];
}

export interface ArchitectureRefreshPath {
  label: string;
  trigger: string;
  steps: string[];
}

export interface ArchitectureTabLineage {
  tab: string;
  uiComponents: string[];
  apiRoutes: string[];
  managedData: string[];
  sourceTables: string[];
  fallbacks?: string[];
  optionalSources?: string[];
}

export interface ArchitectureOverview {
  title: string;
  summary: string;
  components: ArchitectureComponent[];
  flowColumns: ArchitectureFlowColumn[];
  dataFlow: string[];
  securityGovernance: string[];
  refreshPaths: ArchitectureRefreshPath[];
  tabLineage: ArchitectureTabLineage[];
  sourceTables: ArchitectureSourceGroup[];
}

/**
 * Customer-safe architecture copy kept separate from the PDF renderer so the
 * wording and source inventory can be reviewed without changing layout code.
 */
export const ARCHITECTURE_OVERVIEW: ArchitectureOverview = {
  title: "Cost Observability Architecture",
  summary:
    "A Databricks App that turns governed platform usage and operational metadata into interactive cost analysis. The browser calls a FastAPI service, which queries a Databricks SQL Warehouse and serves results from system tables or app-managed Delta aggregates.",
  components: [
    {
      name: "React browser interface",
      role: "Interactive cost views, filters, settings, and customer-facing report downloads.",
    },
    {
      name: "FastAPI application",
      role: "Authenticated API layer that validates requests, coordinates queries, and shapes dashboard responses.",
    },
    {
      name: "Databricks SQL Warehouse",
      role: "Governed SQL execution plane for system-table reads, aggregates, and app-managed tables.",
    },
    {
      name: "Databricks system tables",
      role: "Account-level billing, query, compute, Lakeflow, serving, audit, and workspace metadata.",
    },
    {
      name: "App-managed Delta layer",
      role: "Pre-aggregated cost tables, durable settings and refresh state, plus a shared response cache.",
    },
    {
      name: "Optional cloud billing exports",
      role: "Configured AWS, Azure, or GCP billing exports can supplement Databricks charges with cloud actuals.",
    },
  ],
  flowColumns: [
    {
      title: "Browser / React",
      lines: ["Nine cost views", "Filters and reports", "React Query cache"],
    },
    {
      title: "FastAPI routes",
      lines: ["Authenticated REST", "Validation and shaping", "Tab cache control"],
    },
    {
      title: "SQL Warehouse",
      lines: ["Bound app resource", "Governed SQL execution", "Parallel bundle queries"],
    },
    {
      title: "App-managed Delta",
      lines: ["Eight aggregates", "Response cache", "Settings + refresh state"],
    },
    {
      title: "Governed sources",
      lines: ["Databricks system tables", "Optional cloud exports", "Optional shared aggregates"],
    },
  ],
  dataFlow: [
    "A signed-in user opens the React interface in Databricks Apps and selects dates, workspaces, or report options.",
    "The browser sends same-origin requests to tab-specific FastAPI routes; architecture exports contain static design content only.",
    "FastAPI serves a valid cached bundle when possible, otherwise submits governed SQL through the bound Databricks SQL Warehouse.",
    "Queries use app-managed Delta aggregates for supported summaries and use live system-table queries for detailed, specialized, or fallback views.",
    "Configured AWS, Azure, or GCP billing exports are queried only for the optional actual-cloud-cost views.",
    "FastAPI returns shaped JSON to React, which renders the dashboard or generates the architecture PDF locally.",
  ],
  securityGovernance: [
    "Databricks Apps authenticates the browser session and supplies user identity to the application.",
    "FastAPI uses the forwarded identity for app roles; settings mutations and rebuild actions require the app administrator role.",
    "User-scoped credentials are used only for setup operations that require the caller's privileges when available.",
    "The app service principal performs dashboard SQL and owns managed-table creation, scheduled refreshes, and cache writes.",
    "Unity Catalog privileges govern system-table and managed-table access; the setup flow verifies required grants.",
    "The bound SQL Warehouse separately enforces CAN USE and executes all table reads and writes.",
    "The architecture PDF contains static design information only and does not embed customer, account, workspace, or user identifiers.",
  ],
  refreshPaths: [
    {
      label: "Scheduled aggregate refresh",
      trigger: "Nightly by default; weekly or monthly can be selected.",
      steps: [
        "The FastAPI scheduler acquires the shared refresh lock.",
        "The app service principal incrementally MERGEs recent source partitions into the eight Delta aggregates.",
        "Refresh watermarks and history are persisted, unified source views are rebuilt, and response caches are invalidated.",
      ],
    },
    {
      label: "Administrator full rebuild",
      trigger: "Settings → Config → Rebuild.",
      steps: [
        "FastAPI queues a locked background rebuild after the administrator check.",
        "The app service principal recreates every aggregate from the configured history window.",
        "Progress is written to refresh state/history and all query and response caches are cleared on completion.",
      ],
    },
    {
      label: "On-demand tab refresh",
      trigger: "Refresh on a visible dashboard tab.",
      steps: [
        "React posts the tab name to /api/cache/clear.",
        "FastAPI invalidates only that tab's in-process and shared Delta response-cache entries.",
        "React Query refetches the active tab routes; this does not rebuild the aggregate tables.",
      ],
    },
  ],
  tabLineage: [
    {
      tab: "DBU Overview",
      uiComponents: [
        "Dashboard / SummaryCards / SpendChart",
        "ProductBreakdown / SKUBreakdown / WorkspaceTable",
        "InteractiveBreakdown / PipelineObjectsTable",
      ],
      apiRoutes: [
        "GET /api/billing/dashboard-bundle-fast",
        "GET /api/billing/sku-breakdown",
        "GET /api/billing/interactive-breakdown",
        "GET /api/billing/pipeline-objects",
        "GET /api/billing/kpi-trend",
      ],
      managedData: [
        "daily_usage_summary",
        "daily_product_breakdown",
        "app_response_cache",
      ],
      sourceTables: [
        "system.billing.usage",
        "system.billing.list_prices",
        "system.access.workspaces_latest",
        "system.compute.clusters",
      ],
      fallbacks: [
        "Summary, product, and timeseries queries scan billing usage and list prices when aggregates are absent or empty.",
        "Workspace, SKU, interactive-compute, and jobs/pipelines detail remain live queries.",
      ],
      optionalSources: [
        "system.lakeflow.pipelines for pipeline names; workspace API lookup is the non-table fallback.",
        "system.billing.account_prices for the optional account-pricing multiplier.",
      ],
    },
    {
      tab: "SQL",
      uiComponents: ["SQLWarehousing360"],
      apiRoutes: [
        "GET /api/billing/sql-breakdown",
        "GET /api/dbsql/dashboard-bundle",
        "GET /api/dbsql/top-queries",
        "GET /api/dbsql/top-queries-by-source",
        "GET /api/dbsql/queries-by-user",
        "GET /api/billing/platform-kpi-trend",
      ],
      managedData: [
        "sql_tool_attribution",
        "dbsql_cost_per_query",
        "app_response_cache",
      ],
      sourceTables: [
        "system.billing.usage",
        "system.billing.list_prices",
        "system.query.history",
        "system.compute.warehouses",
        "system.access.workspaces_latest",
      ],
      fallbacks: [
        "The DBSQL-versus-Genie summary falls back from sql_tool_attribution to live billing and query history.",
        "Per-query views report unavailable until dbsql_cost_per_query exists; they do not invent live cost attribution.",
      ],
    },
    {
      tab: "AI/ML",
      uiComponents: ["AIMLCostCenter"],
      apiRoutes: [
        "GET /api/aiml/dashboard-bundle",
        "GET /api/billing/kpi-trend",
      ],
      managedData: ["app_response_cache (bundle cache; analytic views are live)"],
      sourceTables: [
        "system.billing.usage",
        "system.billing.list_prices",
      ],
      fallbacks: [
        "Billing-only queries remain available when optional metadata tables cannot be read.",
        "ML runtime clusters are unavailable without compute metadata because billing alone cannot identify ML runtimes.",
      ],
      optionalSources: [
        "system.compute.clusters for model and ML-runtime names.",
        "system.serving.served_entities for Agent Bricks and endpoint names.",
        "system.access.workspaces_latest for workspace names.",
      ],
    },
    {
      tab: "Apps",
      uiComponents: ["AppsCostCenter"],
      apiRoutes: [
        "GET /api/apps/dashboard-bundle",
        "GET /api/apps/kpi-trend",
      ],
      managedData: [
        "daily_apps_summary",
        "app_response_cache",
      ],
      sourceTables: [
        "system.billing.usage",
        "system.billing.list_prices",
        "system.access.workspaces_latest",
      ],
      fallbacks: [
        "Every daily_apps_summary query has a live billing fallback.",
        "The Databricks Apps API enriches billing identifiers with current app names and connected resources; stale cached metadata is used if that API is temporarily unavailable.",
      ],
    },
    {
      tab: "Tagging",
      uiComponents: ["TaggingHub / UntaggedResourcesTable"],
      apiRoutes: [
        "GET /api/tagging/dashboard-bundle",
        "GET /api/tagging/top-objects-by-tag",
        "GET /api/billing/kpi-trend",
      ],
      managedData: [
        "daily_tag_summary",
        "daily_usage_summary",
        "app_response_cache",
      ],
      sourceTables: [
        "system.billing.usage",
        "system.billing.list_prices",
      ],
      fallbacks: [
        "Tag totals and coverage fall back to live billing scans when aggregate reads fail.",
        "Untagged-resource rows always originate in live billing data.",
      ],
      optionalSources: [
        "system.compute.clusters for cluster names and ownership.",
        "system.compute.warehouses for warehouse names.",
        "system.lakeflow.jobs and system.lakeflow.pipelines for job and pipeline names; workspace APIs are enrichment fallbacks.",
      ],
    },
    {
      tab: "Users",
      uiComponents: ["UsersGroups"],
      apiRoutes: ["GET /api/users-groups/bundle"],
      managedData: ["app_response_cache (bundle cache; analytic views are live)"],
      sourceTables: [
        "system.billing.usage",
        "system.billing.list_prices",
      ],
      optionalSources: [
        "Workspace SCIM Users and Groups APIs enrich billing identities with group membership.",
      ],
    },
    {
      tab: "KPIs & Trends",
      uiComponents: ["PlatformKPIsView / SpendAnomalies / KPITrendModal"],
      apiRoutes: [
        "GET /api/billing/kpis-bundle",
        "GET /api/billing/platform-kpi-trend",
      ],
      managedData: [
        "daily_usage_summary",
        "daily_query_stats",
        "daily_workspace_breakdown",
        "dbsql_cost_per_query",
        "app_response_cache",
      ],
      sourceTables: [
        "system.billing.usage",
        "system.billing.list_prices",
        "system.query.history",
        "system.lakeflow.job_run_timeline",
      ],
      fallbacks: [
        "Billing-backed KPIs remain available if Lakeflow access is missing.",
        "Query KPIs use daily_query_stats when available and otherwise query the same Delta aggregate directly; query-history trends stay live.",
      ],
    },
    {
      tab: "Cloud Costs",
      uiComponents: [
        "CloudCostsView",
        "AWSActualView / AzureActualView / GCPActualView",
      ],
      apiRoutes: [
        "GET /api/billing/infra-bundle",
        "GET /api/aws-actual/dashboard-bundle",
        "GET /api/azure-actual/dashboard-bundle",
        "GET /api/gcp-actual/dashboard-bundle",
        "GET /api/billing/kpi-trend",
      ],
      managedData: ["app_response_cache (bundle cache; estimates are live)"],
      sourceTables: [
        "system.billing.usage",
        "system.compute.clusters",
        "system.access.workspaces_latest",
      ],
      fallbacks: [
        "Estimated infrastructure cost remains available when no cloud billing export is configured.",
        "Rows with missing instance metadata or pricing are marked unavailable instead of being treated as valid estimates.",
      ],
      optionalSources: [
        "AWS Cost and Usage Report aggregate: configurable <catalog>.<schema>.actuals_gold.",
        "Azure Cost Management aggregate: configurable <catalog>.<schema>.actuals_gold.",
        "GCP Cloud Billing export: configurable <catalog>.<schema>.<table>, federated from BigQuery or curated as Delta.",
      ],
    },
    {
      tab: "Optimize",
      uiComponents: [
        "WarehouseIdleTimeView",
        "WarehouseRightsizingView",
        "OptimizeMethodologyPanel",
      ],
      apiRoutes: [
        "GET /api/sql/warehouse-health",
        "GET /api/sql/warehouse-health/idle-time",
      ],
      managedData: ["No Delta aggregate; results use a 30-minute in-process cache."],
      sourceTables: [
        "system.compute.warehouses",
        "system.compute.warehouse_events",
        "system.query.history",
        "system.billing.usage",
        "system.billing.list_prices",
      ],
      fallbacks: [
        "Idle-time uptime falls back from warehouse events to billing intervals and lowers attribution confidence.",
        "Rightsizing returns unavailable when required regional system tables cannot be read.",
      ],
    },
  ],
  sourceTables: [
    {
      label: "Core analytic system tables",
      tables: [
        "system.billing.usage",
        "system.billing.list_prices",
        "system.query.history",
        "system.compute.clusters",
        "system.compute.warehouses",
        "system.compute.warehouse_events",
        "system.lakeflow.jobs",
        "system.lakeflow.pipelines",
        "system.lakeflow.job_run_timeline",
        "system.serving.served_entities",
        "system.access.workspaces_latest",
      ],
    },
    {
      label: "Optional system table",
      tables: ["system.billing.account_prices"],
      note: "Used when account pricing is enabled and the table is available.",
    },
    {
      label: "Permission and readiness probe",
      tables: ["system.access.audit"],
      note: "Checked only to report optional audit-table readiness; it is not an analytic input to dashboard results.",
    },
    {
      label: "App-managed analytic tables",
      tables: [
        "daily_usage_summary",
        "daily_product_breakdown",
        "daily_workspace_breakdown",
        "sql_tool_attribution",
        "daily_query_stats",
        "dbsql_cost_per_query",
        "daily_tag_summary",
        "daily_apps_summary",
      ],
      note: "Created in the configured Unity Catalog location; catalog and schema names are intentionally omitted.",
    },
    {
      label: "Durable app state and cache",
      tables: [
        "app_settings",
        "app_schedule_settings",
        "app_refresh_log",
        "app_cloud_connections",
        "app_workspace_filter",
        "app_user_permissions",
        "app_mv_refresh_state",
        "app_alert_thresholds",
        "app_webhook_settings",
        "app_pricing_settings",
        "app_response_cache",
        "app_mv_sources",
        "app_unified_views",
      ],
      note: "Managed in the configured Unity Catalog location for settings, refresh coordination, access controls, connection configuration, and shared caching.",
    },
    {
      label: "Optional cloud billing sources",
      tables: [
        "AWS: <catalog>.<schema>.actuals_gold",
        "Azure: <catalog>.<schema>.actuals_gold",
        "GCP: <catalog>.<schema>.<table>",
      ],
      note: "Locations are administrator-configured. The GCP source can be a federated BigQuery billing export or a curated Delta table.",
    },
  ],
};
