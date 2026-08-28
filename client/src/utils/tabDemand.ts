import type { TabVisibility } from "@/components/SettingsDialog";
import type { Query, QueryClient } from "@tanstack/react-query";

export type DashboardTab = keyof TabVisibility;

export function buildExportScopeKey(
  startDate: string,
  endDate: string,
  workspaceIds: string[],
  sourceVersion: number,
  visibleTabs: DashboardTab[],
): string {
  return JSON.stringify([
    startDate,
    endDate,
    [...workspaceIds].sort(),
    sourceVersion,
    [...visibleTabs].sort(),
  ]);
}

export function isTabDataRequested(
  tab: DashboardTab,
  activeTab: DashboardTab,
  exportOpen: boolean,
  visibility: TabVisibility,
): boolean {
  return activeTab === tab || (exportOpen && visibility[tab]);
}

const SUBMIT_AND_POLL_TAB_KEYS: Partial<Record<DashboardTab, readonly [string, string]>> = {
  apps: ["apps", "dashboard-bundle"],
  aiml: ["aiml", "dashboard-bundle"],
  sql: ["dbsql", "dashboard-bundle"],
};

export function isRunningSubmitAndPollQuery(tab: DashboardTab, query: Query): boolean {
  const ownerKey = SUBMIT_AND_POLL_TAB_KEYS[tab];
  return Boolean(
    ownerKey
    && query.queryKey[0] === ownerKey[0]
    && query.queryKey[1] === ownerKey[1]
    && query.state.fetchStatus === "fetching",
  );
}

/**
 * Stop only an in-flight submit-and-poll request when its tab is left. React
 * Query retains any settled data already in the cache.
 */
export async function cancelRunningSubmitAndPollForTab(
  queryClient: QueryClient,
  tab: DashboardTab,
): Promise<void> {
  if (!SUBMIT_AND_POLL_TAB_KEYS[tab]) return;
  await queryClient.cancelQueries({
    predicate: (query) => isRunningSubmitAndPollQuery(tab, query),
  });
}
