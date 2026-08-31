import { useQuery } from "@tanstack/react-query";
import {
  buildFilteredUrl,
  getActiveSourceScopeKey,
  getWorkspaceScopeKey,
} from "./useBillingData";

export interface KPITrendDataPoint {
  date: string;
  value: number;
}

export interface KPITrendSummary {
  period_start_value: number;
  period_end_value: number;
  change_amount: number;
  change_percent: number;
  min_value: number;
  max_value: number;
  avg_value: number;
  trend: "increasing" | "decreasing" | "flat";
}

export interface KPITrendResponse {
  kpi: string;
  granularity: string;
  data_points: KPITrendDataPoint[];
  summary: KPITrendSummary;
}

export function trendOwnerTab(queryKeyPrefix: string): string {
  if (queryKeyPrefix.startsWith("users-groups-")) return "users-groups";
  if (queryKeyPrefix === "kpi-trend") return "dbu";
  return queryKeyPrefix.split("-")[0];
}

function useTrendQuery(
  queryKeyPrefix: string,
  endpoint: string,
  kpi: string,
  startDate: string,
  endDate: string,
  granularity: string = "daily",
  workspaceIds?: string[],
  enabled: boolean = true,
) {
  const wsKey = getWorkspaceScopeKey(workspaceIds);
  const sourceKey = getActiveSourceScopeKey();
  return useQuery<KPITrendResponse>({
    queryKey: [queryKeyPrefix, kpi, startDate, endDate, granularity, wsKey, sourceKey],
    queryFn: async () => {
      const params = new URLSearchParams({
        kpi,
        start_date: startDate,
        end_date: endDate,
        granularity,
      });
      params.set("tab", trendOwnerTab(queryKeyPrefix));

      const response = await fetch(buildFilteredUrl(`/api/billing/${endpoint}`, params, workspaceIds));

      if (!response.ok) {
        throw new Error(`Failed to fetch KPI trend: ${response.statusText}`);
      }

      return response.json();
    },
    enabled: enabled && !!kpi && !!startDate && !!endDate,
    staleTime: 5 * 60 * 1000,
  });
}

export function useKPITrend(
  kpi: string,
  startDate: string,
  endDate: string,
  granularity: string = "daily",
  workspaceIds?: string[],
  queryKeyPrefix: string = "kpi-trend",
  enabled: boolean = true,
) {
  return useTrendQuery(queryKeyPrefix, "kpi-trend", kpi, startDate, endDate, granularity, workspaceIds, enabled);
}

export function usePlatformKPITrend(
  kpi: string,
  startDate: string,
  endDate: string,
  granularity: string = "daily",
  workspaceIds?: string[],
  queryKeyPrefix: string = "platform-kpi-trend",
  enabled: boolean = true,
) {
  return useTrendQuery(queryKeyPrefix, "platform-kpi-trend", kpi, startDate, endDate, granularity, workspaceIds, enabled);
}

function useAppsTrendQuery(
  kpi: string,
  startDate: string,
  endDate: string,
  granularity: string = "daily",
  workspaceIds?: string[],
  enabled: boolean = true,
) {
  const wsKey = getWorkspaceScopeKey(workspaceIds);
  const sourceKey = getActiveSourceScopeKey();
  return useQuery<KPITrendResponse>({
    queryKey: ["apps-kpi-trend", kpi, startDate, endDate, granularity, wsKey, sourceKey],
    queryFn: async () => {
      const params = new URLSearchParams({ kpi, start_date: startDate, end_date: endDate, granularity });
      const response = await fetch(buildFilteredUrl("/api/apps/kpi-trend", params, workspaceIds));
      if (!response.ok) throw new Error(`Failed to fetch apps KPI trend: ${response.statusText}`);
      return response.json();
    },
    enabled: enabled && !!kpi && !!startDate && !!endDate,
    staleTime: 5 * 60 * 1000,
  });
}

export function useAppsKPITrend(
  kpi: string,
  startDate: string,
  endDate: string,
  granularity: string = "daily",
  workspaceIds?: string[],
  enabled: boolean = true,
) {
  return useAppsTrendQuery(kpi, startDate, endDate, granularity, workspaceIds, enabled);
}
