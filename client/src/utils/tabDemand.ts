import type { TabVisibility } from "@/components/SettingsDialog";

export type DashboardTab = keyof TabVisibility;

export function buildExportScopeKey(
  startDate: string,
  endDate: string,
  workspaceIds: string[],
  sourceVersion: number,
): string {
  return JSON.stringify([startDate, endDate, [...workspaceIds].sort(), sourceVersion]);
}

export function isTabDataRequested(
  tab: DashboardTab,
  activeTab: DashboardTab,
  exportOpen: boolean,
  visibility: TabVisibility,
): boolean {
  return activeTab === tab || (exportOpen && visibility[tab]);
}
