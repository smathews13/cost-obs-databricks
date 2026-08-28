import type { TabVisibility } from "@/components/SettingsDialog";

export type DashboardTab = keyof TabVisibility;

export function isTabDataRequested(
  tab: DashboardTab,
  activeTab: DashboardTab,
  exportOpen: boolean,
  visibility: TabVisibility,
): boolean {
  return activeTab === tab || (exportOpen && visibility[tab]);
}
