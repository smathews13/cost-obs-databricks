import { useState, useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import type { PlatformKPIsResponse, SpendAnomaliesResponse } from "@/types/billing";
import { SpendAnomalies } from "@/components/SpendAnomalies";
import { KPITrendModal } from "@/components/KPITrendModal";
import { formatNumber, formatBytesNoDecimal, formatRowCount, formatDurationSeconds } from "@/utils/formatters";
import { useFeatureAvailability } from "@/hooks/useFeatureAvailability";
import { C } from "@/theme";
import { PageHero, Chip, InfoPanel, SourceCapabilityNotice } from "@/components/brand";
import { LoadingPanels } from "@/components/Spinner";
import { KPICard as SharedKPICard } from "@/components/ui/KPICard";
import {
  buildFilteredUrl,
  getActiveSourceScopeKey,
  getWorkspaceScopeKey,
} from "@/hooks/useBillingData";

interface PlatformKPIsViewProps {
  data: PlatformKPIsResponse | undefined;
  isLoading: boolean;
  isFetching?: boolean;
  spendAnomalies: SpendAnomaliesResponse | undefined;
  anomaliesLoading: boolean;
  startDate?: string;
  endDate?: string;
  workspaceIds?: string[];
  workspaceNameMap?: Record<string, string>;
}

interface KPICardProps {
  title: string;
  value: string;
  subtitle?: string;
  infoTooltip?: string;
  icon: React.ReactNode;
  color: string;
  onClick?: () => void;
  isLoading?: boolean;
  titleNoWrap?: boolean;
  /** When set, renders an unavailable state with this reason instead of the value. */
  unavailableReason?: string;
}

function KPICard({ title, value, subtitle, infoTooltip, icon, onClick, isLoading, titleNoWrap, unavailableReason }: KPICardProps) {
  return (
    <SharedKPICard
      title={title}
      value={value}
      subtitle={subtitle}
      infoText={infoTooltip}
      icon={icon}
      onActivate={onClick}
      ariaLabel={`See ${title} trend`}
      isLoading={isLoading}
      titleNoWrap={titleNoWrap}
      unavailableReason={unavailableReason}
    />
  );
}

export const PLATFORM_KPI_TREND_KEYS = [
  "total_queries", "total_rows_read", "total_bytes_read", "total_compute_seconds",
  "total_jobs", "total_job_runs", "successful_runs", "active_notebooks",
  "active_workspaces", "models_served", "total_users", "stickiness",
] as const;
type PlatformKPITrendKey = typeof PLATFORM_KPI_TREND_KEYS[number];

export function PlatformKPIsView({ data, isLoading, isFetching, spendAnomalies, anomaliesLoading, startDate, endDate, workspaceIds, workspaceNameMap }: PlatformKPIsViewProps) {
  const queryClient = useQueryClient();

  // Per-feature availability from the shared hook (caches under READINESS_QUERY_KEY).
  const { tableGranted } = useFeatureAvailability();

  const servingGranted      = tableGranted("system.serving.served_entities");
  const queryHistoryGranted = tableGranted("system.query.history");

  // Only suppress a card when the dependency is **explicitly** denied, not when unknown.
  const localBillingUnavailable = data?.local_billing_available === false
    ? "local billing metrics are unavailable for the selected source"
    : undefined;
  const jobsUnavailable = localBillingUnavailable;
  const servingUnavailable = localBillingUnavailable
    || (servingGranted === false ? "serving.served_entities grant required" : undefined);
  const queryMetricsHaveData = Boolean(
    data && (
      data.total_queries > 0
      || data.total_rows_read > 0
      || data.total_bytes_read > 0
      || data.total_compute_seconds > 0
    )
  );
  const queryHistUnavailable = data?.query_metrics_available === false
    ? "query history is unavailable for the selected workspace/source"
    : queryHistoryGranted === false && !queryMetricsHaveData
      ? "query.history grant required"
      : undefined;
  const queryUsersUnavailable = data?.query_users_available === false
    ? "managed query-user data is unavailable for this scope"
    : undefined;
  const stickinessUnavailable = data?.stickiness_available === false
    ? "managed query-user data is unavailable for this scope"
    : undefined;

  const [selectedKPI, setSelectedKPI] = useState<{
    kpi: PlatformKPITrendKey;
    label: string;
  } | null>(null);

  const wsKey = getWorkspaceScopeKey(workspaceIds);
  const sourceKey = getActiveSourceScopeKey();

  // Pre-warm the first 3 KPI trends in the background; the rest load on card click
  useEffect(() => {
    if (!startDate || !endDate) return;
    for (const kpi of PLATFORM_KPI_TREND_KEYS.slice(0, 3)) {
      queryClient.prefetchQuery({
        queryKey: ["kpis-platform-kpi-trend", kpi, startDate, endDate, "daily", wsKey, sourceKey],
        queryFn: async () => {
          const params = new URLSearchParams({ kpi, start_date: startDate, end_date: endDate, granularity: "daily" });
          params.set("tab", "kpis");
          const res = await fetch(buildFilteredUrl("/api/billing/platform-kpi-trend", params, workspaceIds));
          if (!res.ok) throw new Error("prefetch failed");
          return res.json();
        },
        staleTime: 5 * 60 * 1000,
      });
    }
  }, [startDate, endDate, wsKey, sourceKey, workspaceIds, queryClient]);

  // Info box minimize state with localStorage persistence
  const MINIMIZE_KEY = "cost-obs-minimize-kpis-info";
  const [infoMinimized, setInfoMinimized] = useState(() => {
    if (typeof window !== "undefined") {
      return localStorage.getItem(MINIMIZE_KEY) === "true";
    }
    return false;
  });

  const handleMinimizeToggle = (checked: boolean) => {
    setInfoMinimized(checked);
    if (checked) {
      localStorage.setItem(MINIMIZE_KEY, "true");
    } else {
      localStorage.removeItem(MINIMIZE_KEY);
    }
  };

  if (isLoading) {
    return <LoadingPanels sections={[
      "Platform KPIs",
      "Data Activity",
      "Compute Activity",
      "Spend Changes and Trends",
    ]} />;
  }

  if (!data) {
    return (
      <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
        <div className="flex h-32 flex-col items-center justify-center gap-2 text-gray-500">
          <p className="text-base font-medium">No platform KPI data available</p>
          <p className="text-sm">Try adjusting the date range or verify system tables are accessible</p>
        </div>
      </div>
    );
  }
  if (data.error) {
    const sourceUnsupported = data.error_code === "SOURCE_SCOPE_UNSUPPORTED";
    if (sourceUnsupported) {
      return (
        <SourceCapabilityNotice
          title="Platform KPIs are not included in this source"
          description="The selected source does not publish the query, workspace, and query-user aggregates required by this tab."
          requiredAggregates={[
            "daily_query_stats",
            "daily_workspace_breakdown",
            "dbsql_cost_per_query",
          ]}
        />
      );
    }
    return (
      <SourceCapabilityNotice
        title="Platform KPI data is temporarily unavailable"
        description="The KPI query did not complete. Refresh this tab to retry."
      />
    );
  }

  const successRatePct = data.successful_runs_available && data.total_job_runs > 0
    ? ((data.successful_runs / data.total_job_runs) * 100).toFixed(1)
    : null;

  const stickinessPct = data.stickiness_pct != null
    ? Math.round(data.stickiness_pct)
    : null;

  const workspacePct = data.total_workspace_count && data.total_workspace_count > 0
    ? Math.round(((data.avg_daily_workspaces ?? data.active_workspaces) / data.total_workspace_count) * 100)
    : null;

  const handleKPIClick = (kpi: PlatformKPITrendKey, label: string) => {
    setSelectedKPI({ kpi, label });
  };

  return (
    <div className="animate-fade-in space-y-6">
      <InfoPanel
        title="KPIs & Trends tab methodology"
        minimized={infoMinimized}
        onToggle={handleMinimizeToggle}
      >
        <ul className="list-inside list-disc space-y-1">
          <li><strong>Query and Data Processing</strong>: SQL execution, data scanned, and compute time across all warehouses</li>
          <li><strong>Jobs and Workflows</strong>: Automated job executions, success rates, and notebook usage</li>
          <li><strong>Platform Utilization</strong>: Active workspaces, model serving endpoints, user adoption, and usage stickiness (DAU/MAU)</li>
          <li>Use each metric card's See trend button to view historical trends and patterns</li>
        </ul>
      </InfoPanel>

      <PageHero
        icon={
          <svg fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
          </svg>
        }
        title="Platform KPIs & Trends"
        subtitle={
          <>
            Platform health, usage metrics, and adoption tracking
            {workspaceIds && workspaceIds.length > 0 && (
              <Chip kind="workspace" label="Workspace(s)">
                {workspaceIds.length === 1 ? (workspaceNameMap?.[workspaceIds[0]] || workspaceIds[0]) : `${workspaceIds.length} workspaces`}
              </Chip>
            )}
          </>
        }
      />

      {data.data_stale && (
        <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-2.5 text-xs text-amber-700">
          Showing cached data because the live query returned no results. Values reflect the last successful load.
        </div>
      )}

      {/* Query & Data Processing Metrics */}
      <div>
        <h3 className="mb-4 text-lg font-semibold text-gray-900">Query & Data Processing</h3>
        <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-4">
          <KPICard
            title="Total Queries Executed"
            value={formatNumber(data.total_queries)}
            subtitle={queryUsersUnavailable
              ? "unique user count unavailable"
              : `from ${formatNumber(data.unique_query_users)} unique users`}
            isLoading={isLoading || isFetching}
            color="bg-orange-100"
            unavailableReason={queryHistUnavailable}
            onClick={!queryHistUnavailable && startDate && endDate ? () => handleKPIClick("total_queries", "Daily Queries Executed") : undefined}
            icon={
              <svg className="h-6 w-6 text-lava" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h3m-3 4h3m-6-4h.01M9 16h.01" />
              </svg>
            }
          />

          <KPICard
            title="Rows Processed"
            value={formatRowCount(data.total_rows_read)}
            subtitle="total data scanned"
            isLoading={isLoading || isFetching}
            color="bg-orange-100"
            unavailableReason={queryHistUnavailable}
            onClick={!queryHistUnavailable && startDate && endDate ? () => handleKPIClick("total_rows_read", "Rows Processed") : undefined}
            icon={
              <svg className="h-6 w-6 text-lava" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
              </svg>
            }
          />

          <KPICard
            title="Data Processed"
            value={formatBytesNoDecimal(data.total_bytes_read)}
            subtitle="total throughput"
            isLoading={isLoading || isFetching}
            color="bg-orange-100"
            unavailableReason={queryHistUnavailable}
            onClick={!queryHistUnavailable && startDate && endDate ? () => handleKPIClick("total_bytes_read", "Data Processed") : undefined}
            icon={
              <svg className="h-6 w-6 text-lava" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
              </svg>
            }
          />

          <KPICard
            title="Compute Time"
            value={formatDurationSeconds(data.total_compute_seconds)}
            subtitle="total processing time"
            isLoading={isLoading || isFetching}
            color="bg-orange-100"
            unavailableReason={queryHistUnavailable}
            onClick={!queryHistUnavailable && startDate && endDate ? () => handleKPIClick("total_compute_seconds", "Daily Active Processing Time") : undefined}
            icon={
              <svg className="h-6 w-6 text-lava" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            }
          />
        </div>
      </div>

      {/* Jobs & Workflows */}
      <div>
        <h3 className="mb-4 text-lg font-semibold text-gray-900">Jobs & Workflows</h3>
        <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-4">
          <KPICard
            title="Total Active Jobs"
            value={formatNumber(data.total_jobs)}
            subtitle={`${formatNumber(data.unique_job_owners)} unique owners`}
            infoTooltip="Distinct jobs with billing usage across the full selected period. The daily trend shows unique jobs active each day, which is typically lower: the same job counts once per day vs. once for the entire period."
            color="bg-orange-100"
            isLoading={isLoading || isFetching}
            unavailableReason={jobsUnavailable}
            onClick={!jobsUnavailable && data.total_jobs > 0 && startDate && endDate ? () => handleKPIClick("total_jobs", "Daily Active Jobs") : undefined}
            icon={
              <svg className="h-6 w-6 text-lava" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 13.255A23.931 23.931 0 0112 15c-3.183 0-6.22-.62-9-1.745M16 6V4a2 2 0 00-2-2h-4a2 2 0 00-2 2v2m4 6h.01M5 20h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
              </svg>
            }
          />

          <KPICard
            title="Job Runs"
            value={formatNumber(data.total_job_runs)}
            subtitle={(data.total_job_run_hours ?? 0) > 0 ? `${formatNumber(data.total_job_run_hours ?? 0)} hrs total run time` : "Total executions"}
            color="bg-orange-100"
            isLoading={isLoading || isFetching}
            unavailableReason={jobsUnavailable}
            onClick={!jobsUnavailable && data.total_job_runs > 0 && startDate && endDate ? () => handleKPIClick("total_job_runs", "Daily Job Runs") : undefined}
            icon={
              <svg className="h-6 w-6 text-lava" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
              </svg>
            }
          />

          {data.successful_runs_available && <KPICard
            title="Successful Runs"
            value={formatNumber(data.successful_runs)}
            subtitle={successRatePct !== null ? `${successRatePct}% success rate` : "No completed job runs"}
            color="bg-orange-100"
            isLoading={isLoading || isFetching}
            onClick={data.total_job_runs > 0 && startDate && endDate ? () => handleKPIClick("successful_runs", "Successful Runs") : undefined}
            icon={
              <svg className="h-6 w-6 text-lava" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            }
          />}

          <KPICard
            title="Total Compute Resources"
            titleNoWrap
            value={formatNumber(data.active_notebooks)}
            subtitle={`${formatNumber(data.total_clusters ?? data.active_notebooks)} clusters · ${formatNumber(data.sql_warehouses ?? 0)} SQL warehouses`}
            infoTooltip="Distinct clusters plus SQL warehouses with billing usage across the selected period. This includes serverless SQL warehouses, which do not appear as compute clusters."
            color="bg-orange-100"
            isLoading={isLoading || isFetching}
            unavailableReason={localBillingUnavailable}
            onClick={!localBillingUnavailable && startDate && endDate ? () => handleKPIClick("active_notebooks", "Daily Active Compute Resources") : undefined}
            icon={
              <svg className="h-6 w-6 text-lava" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01" />
              </svg>
            }
          />
        </div>
      </div>

      {/* Platform Utilization */}
      <div>
        <h3 className="mb-4 text-lg font-semibold text-gray-900">Platform Utilization</h3>
        <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-4">
          <KPICard
            title="Active Workspaces"
            value={formatNumber(data.avg_daily_workspaces != null && data.avg_daily_workspaces !== 0 ? data.avg_daily_workspaces : data.active_workspaces)}
            subtitle={workspacePct !== null ? `${workspacePct}% active of ${formatNumber(data.total_workspace_count ?? 0)} total` : "Avg workspaces active per day"}
            infoTooltip="Average number of distinct workspaces with billable usage per day in the selected period. Matches the daily trend average."
            color="bg-orange-100"
            isLoading={isLoading || isFetching}
            onClick={startDate && endDate ? () => handleKPIClick("active_workspaces", "Daily Active Workspaces") : undefined}
            icon={
              <svg className="h-6 w-6 text-lava" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
              </svg>
            }
          />

          {(data.models_served > 0 || servingUnavailable) && (
            <KPICard
              title="Unique Models Served"
              value={formatNumber(data.models_served)}
              subtitle={(data.avg_daily_models ?? 0) > 0 ? `${formatNumber(data.avg_daily_models ?? 0)} served daily` : `${formatNumber(data.total_serving_dbus)} DBUs`}
              infoTooltip="Distinct model-serving endpoints active at any point during the period. The daily trend shows unique endpoints per day, which is typically lower: the same endpoint counts once per day vs. once for the entire period."
              color="bg-orange-100"
              isLoading={isLoading || isFetching}
              unavailableReason={servingUnavailable}
              onClick={!servingUnavailable && startDate && endDate ? () => handleKPIClick("models_served", "Daily Models Served") : undefined}
              icon={
                <svg className="h-6 w-6 text-lava" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                </svg>
              }
            />
          )}

          <KPICard
            title="Unique Active Users"
            value={formatNumber(data.unique_query_users)}
            subtitle={data.local_billing_available === false
              ? "SQL query executors in selected source"
              : `${formatNumber(data.unique_job_owners ?? 0)} job owners`}
            isLoading={isLoading || isFetching}
            infoTooltip="Distinct SQL query executors in the selected period (matches the daily trend). Job owners shown separately: some users may appear in both groups."
            color="bg-orange-100"
            unavailableReason={queryUsersUnavailable}
            onClick={!queryUsersUnavailable && startDate && endDate ? () => handleKPIClick("total_users", "Daily Active Users") : undefined}
            icon={
              <svg className="h-6 w-6 text-lava" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
              </svg>
            }
          />

          <KPICard
            title="Usage Stickiness"
            value={stickinessPct !== null ? `${stickinessPct}%` : "N/A"}
            subtitle="DAU / total users ratio"
            infoTooltip="Avg daily active users divided by total unique users in the period. Higher = more habitual usage. 20%+ is strong engagement; 10% suggests occasional usage."
            color={stickinessPct !== null ? "bg-orange-100" : "bg-gray-100"}
            isLoading={isLoading || isFetching}
            unavailableReason={stickinessUnavailable}
            onClick={!stickinessUnavailable && stickinessPct !== null && startDate && endDate ? () => handleKPIClick("stickiness", "Daily Usage Stickiness") : undefined}
            icon={
              <svg className={`h-6 w-6 ${stickinessPct !== null ? "text-lava" : "text-gray-500"}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
              </svg>
            }
          />
        </div>
      </div>

      {/* Spend Changes & Trends */}
      <SpendAnomalies data={spendAnomalies} isLoading={anomaliesLoading} />

      {/* Platform KPI Trend Modal */}
      {selectedKPI && startDate && endDate && (
        <KPITrendModal
          variant="platform"
          kpi={selectedKPI.kpi}
          kpiLabel={selectedKPI.label}
          isOpen={!!selectedKPI}
          onClose={() => setSelectedKPI(null)}
          startDate={startDate}
          endDate={endDate}
          workspaceIds={workspaceIds}
          queryKeyPrefix="kpis-platform-kpi-trend"
        />
      )}
    </div>
  );
}
