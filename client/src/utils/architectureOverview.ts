export interface ArchitectureComponent {
  name: string;
  role: string;
}

export interface ArchitectureSourceGroup {
  label: string;
  tables: string[];
  note?: string;
}

export interface ArchitectureOverview {
  title: string;
  summary: string;
  components: ArchitectureComponent[];
  dataFlow: string[];
  securityGovernance: string[];
  refreshCacheBehavior: string[];
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
  dataFlow: [
    "A signed-in user opens the React interface in Databricks Apps and selects dates, workspaces, or report options.",
    "The browser sends same-origin API requests to FastAPI; no architecture export input includes account, workspace, or user identifiers.",
    "FastAPI submits governed SQL through the bound Databricks SQL Warehouse.",
    "Queries read app-managed Delta aggregates when available and fall back to the required system tables for live or specialized views.",
    "FastAPI caches selected response bundles, returns shaped JSON to React, and the browser renders the dashboard or generates the PDF locally.",
  ],
  securityGovernance: [
    "Databricks Apps authenticates the browser session and supplies user identity to the application.",
    "User-scoped on-behalf-of credentials are used only for setup operations that require the caller's privileges when available.",
    "The app service principal performs normal warehouse access and owns managed-table creation, refresh, and cache writes.",
    "Unity Catalog privileges govern system-table and managed-table access; the setup flow verifies required grants.",
    "The architecture PDF contains static design information only and does not embed customer, account, workspace, or user identifiers.",
  ],
  refreshCacheBehavior: [
    "Managed aggregates support scheduled refreshes (nightly, weekly, or monthly) and administrator-triggered on-demand rebuilds.",
    "Incremental refresh paths reprocess recent partitions; a full rebuild is available when source history or configuration changes.",
    "FastAPI keeps a short-lived in-process query cache and a shared Delta response cache for selected expensive dashboard bundles.",
    "Account-wide cached bundles use a longer lifetime than workspace-filtered bundles; explicit tab refreshes invalidate the relevant cache entries.",
    "If an aggregate or optional cloud source is unavailable, supported views use their documented live-query or unavailable-state behavior.",
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
      ],
      note: "Managed in the configured Unity Catalog location for settings, refresh coordination, access controls, connection configuration, and shared caching.",
    },
  ],
};
