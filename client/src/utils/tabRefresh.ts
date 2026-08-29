import type { Query, QueryClient, QueryKey } from "@tanstack/react-query";
import type { DashboardTab } from "./tabDemand";

type QueryKeyPredicate = (queryKey: QueryKey) => boolean;

const keyStartsWith = (queryKey: QueryKey, ...parts: string[]) =>
  parts.every((part, index) => queryKey[index] === part);

const keyHasKpi = (queryKey: QueryKey, prefix: string, kpis: ReadonlySet<string>) =>
  queryKey[0] === prefix && typeof queryKey[1] === "string" && kpis.has(queryKey[1]);

const DBU_TREND_KPIS = new Set([
  "total_spend", "total_dbus", "avg_daily_spend", "workspace_count",
]);
const billingKeys = (...names: string[]): QueryKeyPredicate =>
  (queryKey) => queryKey[0] === "billing" && names.includes(String(queryKey[1]));

/**
 * Dashboard query ownership is intentionally explicit. Manual refresh uses these
 * predicates so global account/settings queries and unopened tabs stay untouched.
 */
export const TAB_QUERY_KEY_PREDICATES: Record<DashboardTab, QueryKeyPredicate> = {
  dbu: (key) =>
    billingKeys(
      "dashboard-bundle-fast",
      "summary",
      "by-product",
      "by-workspace",
      "timeseries",
      "sku-breakdown",
      "pipeline-objects",
      "interactive-breakdown",
      "etl-breakdown",
    )(key) || keyHasKpi(key, "kpi-trend", DBU_TREND_KPIS),
  sql: (key) =>
    billingKeys("sql-breakdown")(key)
    || keyStartsWith(key, "dbsql")
    || keyStartsWith(key, "sql-kpi-trend")
    || keyStartsWith(key, "sql-platform-kpi-trend"),
  infra: (key) =>
    billingKeys("infra-bundle", "infra-costs", "infra-costs-timeseries", "aws-costs", "aws-costs-timeseries")(key)
    || keyStartsWith(key, "aws-actual")
    || keyStartsWith(key, "azure-actual")
    || keyStartsWith(key, "gcp-actual")
    || keyStartsWith(key, "infra-kpi-trend"),
  optimizer: (key) =>
    keyStartsWith(key, "warehouse-health") || keyStartsWith(key, "warehouse-idle-time"),
  kpis: (key) =>
    billingKeys("kpis-bundle", "spend-anomalies", "platform-kpis")(key)
    || keyStartsWith(key, "kpis-platform-kpi-trend"),
  aiml: (key) =>
    keyStartsWith(key, "aiml") || keyStartsWith(key, "aiml-kpi-trend"),
  apps: (key) =>
    keyStartsWith(key, "apps") || keyStartsWith(key, "apps-kpi-trend"),
  tagging: (key) =>
    keyStartsWith(key, "tagging") || keyStartsWith(key, "tagging-kpi-trend"),
  "users-groups": (key) => keyStartsWith(key, "users-groups"),
};

export const TAB_LOADING_SECTIONS: Record<DashboardTab, string[]> = {
  dbu: ["Spend Over Time", "Spend by Product", "Spend by SKU", "Workspace Breakdown", "Jobs and Pipelines", "Interactive Compute"],
  sql: ["Query Spend Summary", "Spend by Source", "Warehouse Spend", "SKU Breakdown", "Top Users by Query Spend", "Top Queries"],
  infra: ["Infrastructure Costs", "Cluster Costs", "Usage by Instance Family", "Cluster Cost Breakdown"],
  optimizer: ["Idle Time Analysis", "Rightsizing Recommendations"],
  kpis: ["Platform KPIs", "Data Activity", "Compute Activity", "Spend Changes and Trends"],
  aiml: ["AI/ML Spend Over Time", "Cost by AI Spend Category", "Top Model Serving Endpoints", "Top Models", "ML Runtime Clusters", "Agent Bricks"],
  apps: ["Apps Spend Over Time", "Apps by Spend", "Connected Resources"],
  tagging: ["Total Tag Coverage", "Tag Coverage Over Time", "Top Tags", "Spend by Key", "Untagged Resources"],
  "users-groups": ["Top Users by Spend", "User Spend by Product", "User Activity", "Service Principal Activity"],
};

export function isQueryOwnedByTab(tab: DashboardTab, queryKey: QueryKey): boolean {
  return TAB_QUERY_KEY_PREDICATES[tab](queryKey);
}

const DASHBOARD_TABS = Object.keys(TAB_QUERY_KEY_PREDICATES) as DashboardTab[];

export function isDashboardQuery(queryKey: QueryKey): boolean {
  return DASHBOARD_TABS.some((tab) => isQueryOwnedByTab(tab, queryKey));
}

/**
 * A source change is a client-side scope change, not a server-cache reset.
 * Mark every dashboard query stale so unopened tabs cannot reuse the prior
 * source, then refetch only the tab the user is currently looking at.
 */
export async function refreshSourceScopeData(
  queryClient: QueryClient,
  activeTab: DashboardTab,
): Promise<void> {
  const activePredicate = (query: Query) => isQueryOwnedByTab(activeTab, query.queryKey);
  await queryClient.cancelQueries({ predicate: activePredicate });
  await queryClient.invalidateQueries({
    predicate: (query) => isDashboardQuery(query.queryKey),
    refetchType: "none",
  });
  await queryClient.refetchQueries(
    { type: "active", predicate: activePredicate },
    { throwOnError: true },
  );
}

export async function refreshTabData(
  queryClient: QueryClient,
  tab: DashboardTab,
  fetcher: typeof fetch = fetch,
): Promise<void> {
  const predicate = (query: Query) => isQueryOwnedByTab(tab, query.queryKey);
  await queryClient.cancelQueries({ predicate });
  const response = await fetcher(`/api/cache/clear?tab=${encodeURIComponent(tab)}`, { method: "POST" });
  if (!response.ok) throw new Error(`Failed to clear ${tab} cache (${response.status})`);
  await queryClient.invalidateQueries({ predicate, refetchType: "none" });
  await queryClient.refetchQueries(
    { type: "active", predicate },
    { throwOnError: true },
  );
}
