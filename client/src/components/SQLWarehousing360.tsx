import { useEffect, useMemo, useState, useRef } from "react";
import { formatIdentity, useSpNameMap } from "@/utils/identity";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useFeatureAvailability } from "@/hooks/useFeatureAvailability";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
  Legend,
  BarChart,
  Bar,
  LabelList,
} from "recharts";
import type { BarRectangleItem } from "recharts";
import { format, parseISO } from "date-fns";
import type { GranularBreakdownResponse, DBSQLDashboardBundle } from "@/types/billing";
import { KPITrendModal } from "./KPITrendModal";
import { LoadingPanels, Spinner } from "./Spinner";
import { formatCurrency, formatKpiCurrency, formatNumber } from "@/utils/formatters";
import { C, seriesColor } from "@/theme";
import { PageHero, Chip, InfoPanel, SourceCapabilityNotice } from "@/components/brand";
import { Dialog } from "@/components/ui/Dialog";
import { InfoPopover as InfoTooltip } from "@/components/ui/InfoPopover";
import { FloatingMenu } from "@/components/ui/FloatingMenu";
import { KPICard } from "@/components/ui/KPICard";
import {
  buildFilteredUrl,
  getActiveSourceScopeKey,
  getWorkspaceScopeKey,
} from "@/hooks/useBillingData";

interface SQLWarehousing360Props {
  sqlBreakdownData: GranularBreakdownResponse | undefined;
  queryData: DBSQLDashboardBundle | undefined;
  isLoading: boolean;
  isError?: boolean;
  topQueriesData?: import("@/types/billing").TopQueriesResponse;
  topQueriesLoading?: boolean;
  host?: string | null;
  startDate?: string;
  endDate?: string;
  workspaceIds?: string[];
  workspaceNameMap?: Record<string, string>;
}

// Colors for query source types
const SOURCE_TYPE_COLORS: Record<string, string> = {
  "GENIE SPACE": C.s5,
  "AI/BI DASHBOARD": C.s2,
  "LEGACY DASHBOARD": C.s5,
  "SQL QUERY": C.s3,
  "NOTEBOOK": C.s4,
  "JOB": C.lava,
  "ALERT": C.s1,
  Unknown: C.slate,
};

function QuerySourceBadge({ sourceType }: { sourceType: string }) {
  const color = SOURCE_TYPE_COLORS[sourceType] || C.slate;
  return (
    <span
      className="inline-flex max-w-44 truncate rounded-full px-2 py-1 text-xs font-medium"
      style={{ backgroundColor: `color-mix(in srgb, ${color} 12%, transparent)`, color }}
      title={sourceType}
    >
      {sourceType}
    </span>
  );
}

const COST_TOOLTIP_TEXT = "Costs are estimates: the warehouse's billed DBU-hours are divided across all queries in the period, weighted by task duration. A fast query running during a low-activity window can inherit a large share of the hour's cost.";

// Stable module-level formatters for the Top Users bar chart: passing inline
// arrows to Recharts churns identity every render and re-triggers animations,
// which is what caused the labels to flicker.
const USER_BAR_TICK_FMT = (v: number | string) => formatCurrency(typeof v === "number" ? v : Number(v));
const USER_BAR_TOOLTIP_FMT = (value: unknown) => formatCurrency(value as number);
const USER_BAR_TOOLTIP_LABEL_FMT = (label: unknown) => `User: ${label}`;
const USER_BAR_LABEL_FMT = (v: unknown) => `$${Math.round(v as number).toLocaleString()}`;
const USER_BAR_LABEL_STYLE = { fontSize: 11, fill: C.slate };

function OptimizeTablePagination({
  currentPage,
  totalPages,
  totalItems,
  pageSize,
  itemLabel,
  onPageChange,
}: {
  currentPage: number;
  totalPages: number;
  totalItems: number;
  pageSize: number;
  itemLabel: string;
  onPageChange: (page: number) => void;
}) {
  if (totalItems === 0) return null;
  const start = (currentPage - 1) * pageSize + 1;
  const end = Math.min(currentPage * pageSize, totalItems);
  return (
    <div className="mt-4 flex items-center justify-between border-t border-gray-200 pt-4">
      <p
        className="text-sm text-gray-700"
        aria-label={`Showing ${start} to ${end} of ${totalItems} ${itemLabel}`}
      >
        Showing <span className="font-medium">{start}</span> to{" "}
        <span className="font-medium">{end}</span> of{" "}
        <span className="font-medium">{totalItems}</span> {itemLabel}
      </p>
      <div className="flex items-center gap-2">
        <button
          type="button"
          onClick={() => onPageChange(Math.max(1, currentPage - 1))}
          disabled={currentPage === 1}
          className="rounded border border-gray-300 px-3 py-1 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50"
        >
          Previous
        </button>
        <span className="px-1 text-sm text-gray-500">
          Page {currentPage} of {totalPages}
        </span>
        <button
          type="button"
          onClick={() => onPageChange(Math.min(totalPages, currentPage + 1))}
          disabled={currentPage === totalPages}
          className="rounded border border-gray-300 px-3 py-1 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50"
        >
          Next
        </button>
      </div>
    </div>
  );
}

const formatDuration = (seconds: number) => {
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  if (seconds < 3600) return `${(seconds / 60).toFixed(1)}m`;
  return `${(seconds / 3600).toFixed(1)}h`;
};

const formatDate = (dateStr: string) => {
  try {
    return format(parseISO(dateStr), "MMM d");
  } catch {
    return dateStr;
  }
};

type SortField = "cost" | "dbus" | "duration_seconds" | "executed_by";
type SortDirection = "asc" | "desc";

type UserBarDatum = {
  rawUser: string;
  user: string;
};

function isUserBarDatum(value: unknown): value is UserBarDatum {
  if (!value || typeof value !== "object") return false;
  const candidate = value as Record<string, unknown>;
  return typeof candidate.rawUser === "string" && typeof candidate.user === "string";
}

interface SourceQuery {
  statement_id: string;
  query_source_type: string;
  executed_by: string;
  statement_preview: string;
  duration_seconds: number;
  cost: number;
  dbus: number;
  query_profile_url: string | null;
  source_url: string | null;
}

async function loadSourceQueries(
  sourceType: string,
  startDate?: string,
  endDate?: string,
  workspaceIds?: string[],
): Promise<SourceQuery[]> {
  const params = new URLSearchParams({ source_type: sourceType, limit: "5" });
  if (startDate) params.set("start_date", startDate);
  if (endDate) params.set("end_date", endDate);
  const response = await fetch(
    buildFilteredUrl("/api/dbsql/top-queries-by-source", params, workspaceIds),
  );
  if (!response.ok) {
    throw new Error(`Source query request failed with ${response.status}`);
  }
  const result = await response.json();
  return result.queries || [];
}

export function SQLWarehousing360({ queryData, isLoading, isError, topQueriesData, topQueriesLoading, host, startDate, endDate, workspaceIds, workspaceNameMap }: SQLWarehousing360Props) {
  const spNameMap = useSpNameMap();
  const [sortField, setSortField] = useState<SortField>("cost");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const [queriesPage, setQueriesPage] = useState(1);
  const [showHistoricalQueries, setShowHistoricalQueries] = useState(false);
  const { tableGranted } = useFeatureAvailability();
  const queryHistoryGranted = tableGranted("system.query.history");
  const [selectedKPI, setSelectedKPI] = useState<{kpi: string; label: string; variant?: "billing" | "platform"} | null>(null);
  const [selectedSource, setSelectedSource] = useState<string | null>(null);
  const [querySourceFilters, setQuerySourceFilters] = useState<string[] | null>(null);
  const [querySourceDropdownOpen, setQuerySourceDropdownOpen] = useState(false);
  const querySourceDropdownRef = useRef<HTMLDivElement>(null);
  const [querySearch, setQuerySearch] = useState("");
  const queryClient = useQueryClient();
  const wsKey = getWorkspaceScopeKey(workspaceIds);
  const sourceKey = getActiveSourceScopeKey();

  const { data: sourceQueries = [], isLoading: sourceQueriesLoading } = useQuery({
    queryKey: [
      "dbsql",
      "top-queries-by-source",
      selectedSource,
      startDate,
      endDate,
      wsKey,
      sourceKey,
    ],
    queryFn: () => loadSourceQueries(selectedSource!, startDate, endDate, workspaceIds),
    enabled: !!selectedSource,
    staleTime: 5 * 60 * 1000,
  });

  // Prefetch top queries for ALL source types when data loads
  const sourceBreakdowns = queryData?.by_source?.sources;
  useEffect(() => {
    const prefetchSourceTypes = sourceBreakdowns?.map((source) => source.query_source_type) ?? [];
    if (!startDate || !endDate || prefetchSourceTypes.length === 0) return;
    for (const sourceType of prefetchSourceTypes) {
      void queryClient.prefetchQuery({
        queryKey: [
          "dbsql",
          "top-queries-by-source",
          sourceType,
          startDate,
          endDate,
          wsKey,
          sourceKey,
        ],
        queryFn: () => loadSourceQueries(sourceType, startDate, endDate, workspaceIds),
        staleTime: 5 * 60 * 1000,
      });
    }
  }, [startDate, endDate, sourceBreakdowns, queryClient, wsKey, sourceKey, workspaceIds]);

  const handleSourceClick = (sourceType: string) => {
    setSelectedSource(sourceType);
  };

  // Pre-warm trend queries so modals open instantly
  useEffect(() => {
    if (!startDate || !endDate) return;
    for (const kpi of ["sql_queries", "sql_users", "avg_query_duration"]) {
      queryClient.prefetchQuery({
        queryKey: ["sql-platform-kpi-trend", kpi, startDate, endDate, "daily", wsKey, sourceKey],
        queryFn: async () => {
          const params = new URLSearchParams({ kpi, start_date: startDate, end_date: endDate, granularity: "daily" });
          params.set("tab", "sql");
          const res = await fetch(buildFilteredUrl("/api/billing/platform-kpi-trend", params, workspaceIds));
          if (!res.ok) throw new Error("prefetch failed");
          return res.json();
        },
        staleTime: 5 * 60 * 1000,
      });
    }
  }, [startDate, endDate, wsKey, sourceKey, workspaceIds, queryClient]);

  // Info box minimize state with localStorage persistence
  const MINIMIZE_KEY = "cost-obs-minimize-sql-info";
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


  const [selectedUser, setSelectedUser] = useState<{ raw: string; display: string } | null>(null);

  const { data: userQueriesData, isLoading: userQueriesLoading } = useQuery<{
    available: boolean;
    queries: Array<{
      statement_id: string | null;
      query_source_type: string;
      executed_by: string;
      warehouse_id: string | null;
      workspace_id: string | null;
      statement_preview: string;
      duration_seconds: number;
      cost: number;
      dbus: number;
      query_profile_url: string | null;
      source_url: string | null;
      start_time: string | null;
    }>;
    total_spend: number;
    query_count: number;
  }>({
    queryKey: ["dbsql", "queries-by-user", selectedUser?.raw, startDate, endDate, wsKey, sourceKey],
    queryFn: () => {
      const params = new URLSearchParams({ user: selectedUser!.raw });
      if (startDate) params.set("start_date", startDate);
      if (endDate) params.set("end_date", endDate);
      return fetch(buildFilteredUrl("/api/dbsql/queries-by-user", params, workspaceIds)).then(r => r.json());
    },
    enabled: !!selectedUser,
    staleTime: 5 * 60 * 1000,
  });

  const [userQueryStaleCutoff] = useState(() => Date.now() - 25 * 24 * 60 * 60 * 1000);
  const freshUserQueries = (userQueriesData?.queries ?? [])
    .filter(q => !q.start_time || new Date(q.start_time).getTime() >= userQueryStaleCutoff)
    .sort((a, b) => b.cost - a.cost);
  const staleUserQueries = (userQueriesData?.queries ?? [])
    .filter(q => q.start_time && new Date(q.start_time).getTime() < userQueryStaleCutoff)
    .sort((a, b) => b.cost - a.cost);

  const userBarData = (() => {
    if (!queryData?.by_user?.users) return [];
    const byUser: Record<string, { user: string; rawUser: string; total_spend: number; query_count: number }> = {};
    for (const u of queryData.by_user.users) {
      if (!byUser[u.executed_by]) {
        byUser[u.executed_by] = { user: u.executed_by, rawUser: u.executed_by, total_spend: 0, query_count: 0 };
      }
      byUser[u.executed_by].total_spend += u.total_spend;
      byUser[u.executed_by].query_count += u.query_count;
    }
    return Object.values(byUser)
      .sort((a, b) => b.total_spend - a.total_spend)
      .slice(0, 10)
      .map(u => ({ ...u, user: formatIdentity(u.user, spNameMap) }));
  })();

  const timeseriesData = (() => {
    if (!queryData?.timeseries?.timeseries) return [];
    return queryData.timeseries.timeseries.map((point) => ({
      ...point,
      date: formatDate(point.date as string),
    }));
  })();

  const querySourceTypes = (() => {
    if (!topQueriesData?.queries) return [];
    const types = new Set(topQueriesData.queries.map((q) => q.query_source_type));
    return Array.from(types).sort();
  })();
  const activeQuerySourceFilters = querySourceFilters ?? querySourceTypes;

  const isHistoricalQuery = (q: { executed_by: string; statement_preview: string }) =>
    !q.executed_by || q.executed_by === "Unknown" || q.statement_preview === "N/A";
  const allQueries = topQueriesData?.queries || [];
  const historicalQueryCount = allQueries.filter(isHistoricalQuery).length;

  const filteredQueries = (() => {
    if (!topQueriesData?.queries) return [];
    let queries = [...topQueriesData.queries];
    if (!showHistoricalQueries) {
      queries = queries.filter((q) => !isHistoricalQuery(q));
    }
    if (activeQuerySourceFilters.length > 0) {
      queries = queries.filter((q) => activeQuerySourceFilters.includes(q.query_source_type));
    }
    queries.sort((a, b) => {
      let aVal: number | string = 0;
      let bVal: number | string = 0;
      switch (sortField) {
        case "cost":
          aVal = a.cost;
          bVal = b.cost;
          break;
        case "dbus":
          aVal = a.dbus;
          bVal = b.dbus;
          break;
        case "duration_seconds":
          aVal = a.duration_seconds;
          bVal = b.duration_seconds;
          break;
        case "executed_by":
          aVal = a.executed_by.toLowerCase();
          bVal = b.executed_by.toLowerCase();
          break;
      }
      if (typeof aVal === "string" && typeof bVal === "string") {
        return sortDirection === "asc" ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
      }
      return sortDirection === "asc" ? (aVal as number) - (bVal as number) : (bVal as number) - (aVal as number);
    });
    return queries;
  })();

  const searchedQueries = querySearch
    ? filteredQueries.filter(q =>
        (q.executed_by || "").toLowerCase().includes(querySearch.toLowerCase()) ||
        (q.query_source_type || "").toLowerCase().includes(querySearch.toLowerCase()) ||
        (q.statement_preview || "").toLowerCase().includes(querySearch.toLowerCase())
      )
    : filteredQueries;
  const queryTotalPages = Math.ceil(searchedQueries.length / 10);
  const queryStart = (queriesPage - 1) * 10;
  const sortedQueries = searchedQueries.slice(queryStart, queryStart + 10);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDirection("desc");
    }
    setQueriesPage(1);
  };

  if (isError) {
    return (
      <div className="rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-700">
        Query cost data could not be loaded. Try refreshing the SQL tab.
      </div>
    );
  }

  if (isLoading || queryData == null) {
    return <LoadingPanels sections={[
      "Query Spend Summary",
      "Spend by Source",
      "Warehouse Spend",
      "SKU Breakdown",
      "Top Users by Query Spend",
      "Top Queries",
    ]} />;
  }

  if (queryData.available === false) {
    const sourceUnsupported = queryData.error_code === "SOURCE_SCOPE_UNSUPPORTED"
      || queryData.reason === "shared_scope_unsupported";
    if (sourceUnsupported) {
      return (
        <SourceCapabilityNotice
          title="SQL detail is not included in this source"
          description="The selected source publishes account totals but not SQL query and warehouse attribution."
          requiredAggregates={[
            "daily_query_stats",
            "sql_tool_attribution",
            "dbsql_cost_per_query",
          ]}
        />
      );
    }
    return (
      <div className="rounded-lg border border-gray-200 bg-gray-50 p-6">
        <div className="flex items-start gap-3">
          <svg className="mt-0.5 h-5 w-5 shrink-0 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <div>
            <p className="text-sm font-medium text-gray-700">
              Query-level cost attribution is not available
            </p>
            <p className="mt-1 text-sm text-gray-500">
              This tab needs <code className="rounded bg-gray-200 px-1 text-xs">system.query.history</code> access for the app service principal,
              and the query-cost table from setup. Copy the GRANT SQL from Settings, Permissions, run it as a metastore admin, then rebuild tables from Settings, Config.
            </p>
          </div>
        </div>
      </div>
    );
  }

  const summary = queryData.summary;
  const sourceTypes = queryData.timeseries?.source_types || [];
  const warehouseTypeTimeseries = (queryData as typeof queryData & {
    warehouse_type_timeseries?: {
      timeseries?: unknown[];
      warehouse_types?: string[];
      sku_breakdown?: Array<{ sku_name: string; total_spend: number }>;
    };
  }).warehouse_type_timeseries;
  const sqlSkuBreakdown = warehouseTypeTimeseries?.sku_breakdown ?? [];
  const warehouseRows = queryData.by_warehouse?.warehouses ?? [];
  const hasWarehouseSizeData = warehouseRows.some(
    (warehouse) => warehouse.warehouse_size && warehouse.warehouse_size !== "UNKNOWN",
  );
  const regionScope = queryData.region_scope;
  const queryDetailUnavailable = Boolean(
    regionScope?.limited && regionScope.in_region_workspace_count === 0,
  );

  return (
    <div className="space-y-6">
      {/* Region-scope banner: system.compute / system.query are region-scoped, so
          SQL/warehouse detail only covers workspaces in this metastore's region even
          though account-wide billing (spend totals) spans all regions. */}
      {regionScope?.limited && (
        <SourceCapabilityNotice
          title="Query detail is unavailable for some selected workspaces"
          description={`${regionScope.missing_workspace_count} selected workspace${regionScope.missing_workspace_count === 1 ? "" : "s"} with ${formatKpiCurrency(regionScope.missing_region_sql_spend ?? 0)} in SQL billing spend ${regionScope.missing_workspace_count === 1 ? "is" : "are"} outside this app's metastore region. The billing-spend chart includes those workspaces; query attribution, counts, users, and duration cover only ${regionScope.in_region_workspace_count} in-region workspace${regionScope.in_region_workspace_count === 1 ? "" : "s"}.`}
          requiredAggregates={["daily_query_stats", "sql_tool_attribution", "dbsql_cost_per_query"]}
        />
      )}

      {/* Query-level Cost Attribution */}
      <InfoPanel
            title="SQL tab methodology"
            minimized={infoMinimized}
            onToggle={handleMinimizeToggle}
          >
            <ul className="grid grid-cols-1 gap-1 sm:grid-cols-2">
              <li><strong style={{ color: C.ink }}>Spend by Source:</strong> Click any source (Genie, AI/BI, SQL Editor, Jobs, Notebooks) to drill into the top queries from that source</li>
              <li><strong style={{ color: C.ink }}>Warehouse Spend:</strong> Breakdown by warehouse type and utilization patterns</li>
              <li><strong style={{ color: C.ink }}>SKU Breakdown:</strong> Spend split across Serverless, Pro, Classic, and other SQL SKUs</li>
              <li><strong style={{ color: C.ink }}>Top Users by Query Spend:</strong> Human users and service principals ranked by SQL query cost</li>
            </ul>
          </InfoPanel>

          <PageHero
            icon={
              <svg fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 3v2m6-2v2M9 19v2m6-2v2M5 9H3m2 6H3m18-6h-2m2 6h-2M7 19h10a2 2 0 002-2V7a2 2 0 00-2-2H7a2 2 0 00-2 2v10a2 2 0 002 2zM9 9h6v6H9V9z" />
              </svg>
            }
            title="SQL"
            subtitle={
              <>
                SQL-level cost attribution and warehouse analytics
                {workspaceIds && workspaceIds.length > 0 && (
                  <Chip kind="workspace">
                    {workspaceIds.length === 1 ? (workspaceNameMap?.[workspaceIds[0]] || workspaceIds[0]) : `${workspaceIds.length} workspaces`}
                  </Chip>
                )}
              </>
            }
          />

          {/* Stale data warning: shown when MV exists but has no data in the selected range */}
          {summary?.total_queries === 0 && (summary?.data_range?.total_rows ?? 0) > 0 && (() => {
            const earliest = summary.data_range?.earliest_date;
            const latest = summary.data_range?.latest_date;
            const rangeOutside = earliest && latest && startDate && endDate
              && (endDate < earliest || startDate > latest);
            return (
            <div className="rounded-lg border border-amber-200 bg-amber-50 p-4">
              <div className="flex items-start gap-3">
                <svg className="mt-0.5 h-5 w-5 shrink-0 text-amber-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
                </svg>
                <div>
                  <p className="text-sm font-medium text-amber-800">No data in selected range</p>
                  <p className="mt-1 text-sm text-amber-700">
                    {rangeOutside ? (
                      <>The Query data runs from <strong>{earliest}</strong> to <strong>{latest}</strong>: adjust the date range to see data.</>
                    ) : (
                      <>No SQL warehouse queries found for the selected {workspaceIds?.length ? `${workspaceIds.length} workspace${workspaceIds.length > 1 ? 's' : ''}` : 'filters'} in this date range. Try selecting all workspaces or a different date range.</>
                    )}
                  </p>
                </div>
              </div>
            </div>
            );
          })() }

          {/* Summary Cards */}
          {(() => {
            // Show unavailable state instead of fake 0 when query.history is explicitly denied
            // or when summary data is null (no data returned despite query succeeding).
            const summaryUnavailable = queryHistoryGranted === false
              ? "query.history grant required: run SP grants to fix"
              : (summary == null ? "No summary data returned" : undefined);

            if (summaryUnavailable) {
              return (
                <div className="rounded-lg border border-gray-200 bg-gray-50 p-4 text-sm text-gray-500">
                  <span className="font-medium text-gray-700">Query summary unavailable</span>
                  <span className="ml-2">: {summaryUnavailable}</span>
                </div>
              );
            }
            const availableSummary = summary!;

            return (
          <div className="co-kpi-grid grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <KPICard
              title={queryDetailUnavailable ? "SQL Billing Spend" : "Total Query Spend"}
              value={queryDetailUnavailable
                ? formatKpiCurrency(regionScope?.billing_sql_spend ?? 0)
                : formatKpiCurrency(availableSummary.total_spend ?? 0)}
              subtitle={queryDetailUnavailable
                ? "Account billing total; query attribution is unavailable"
                : `${formatNumber(availableSummary.total_dbus ?? 0)} DBUs · over ${startDate && endDate ? Math.round((new Date(endDate).getTime() - new Date(startDate).getTime()) / 86400000) + 1 : "?"} days`}
              onActivate={startDate && endDate ? () => setSelectedKPI({kpi: "sql_spend", label: queryDetailUnavailable ? "Daily SQL Billing Spend" : "Daily SQL Spend Trend", variant: "billing"}) : undefined}
              ariaLabel={queryDetailUnavailable ? "See SQL Billing Spend trend" : "See Total Query Spend trend"}
              icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>}
            />
            <KPICard
              title="Total Queries"
              value={queryDetailUnavailable ? "N/A" : formatNumber(availableSummary.total_queries ?? 0)}
              subtitle={queryDetailUnavailable ? "Query history is not available in these regions" : (() => {
                const days = startDate && endDate ? Math.round((new Date(endDate).getTime() - new Date(startDate).getTime()) / 86400000) + 1 : null;
                const avgPerDay = days ? Math.round((availableSummary.total_queries ?? 0) / days) : null;
                return `${avgPerDay != null ? formatNumber(avgPerDay) + " avg/day · " : ""}${formatCurrency(availableSummary.avg_cost_per_query ?? 0)}/query`;
              })()}
              onActivate={!queryDetailUnavailable && startDate && endDate ? () => setSelectedKPI({kpi: "sql_queries", label: "Daily SQL Queries", variant: "platform"}) : undefined}
              ariaLabel="See Total Queries trend"
              icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h3m-3 4h3m-6-4h.01M9 16h.01" /></svg>}
            />
            <KPICard
              title="Unique SQL Users"
              value={queryDetailUnavailable ? "N/A" : formatNumber(availableSummary.unique_users ?? 0)}
              subtitle={queryDetailUnavailable ? "Query history is not available in these regions" : `across ${formatNumber(availableSummary.unique_warehouses ?? 0)} SQL warehouses`}
              onActivate={!queryDetailUnavailable && startDate && endDate ? () => setSelectedKPI({kpi: "sql_users", label: "Daily SQL Users", variant: "platform"}) : undefined}
              ariaLabel="See Unique SQL Users trend"
              icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" /></svg>}
            />
            <KPICard
              title="Query Duration"
              value={queryDetailUnavailable ? "N/A" : formatDuration(availableSummary.avg_duration_seconds ?? 0)}
              subtitle={queryDetailUnavailable ? "Query history is not available in these regions" : "average per query"}
              onActivate={!queryDetailUnavailable && startDate && endDate ? () => setSelectedKPI({kpi: "avg_query_duration", label: "Query Duration"}) : undefined}
              ariaLabel="See Query Duration trend"
              icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>}
            />
          </div>
            );
          })()}

          {selectedKPI && startDate && endDate && (
            <KPITrendModal
              variant={selectedKPI.variant ?? "platform"}
              kpi={selectedKPI.kpi}
              kpiLabel={selectedKPI.label}
              isOpen={!!selectedKPI}
              onClose={() => setSelectedKPI(null)}
              startDate={startDate}
              endDate={endDate}
              workspaceIds={workspaceIds}
              queryKeyPrefix={selectedKPI.variant === "billing" ? "sql-kpi-trend" : "sql-platform-kpi-trend"}
            />
          )}

          {/* Daily Query Costs + Top Users: side by side */}
          {(timeseriesData.length > 0 || userBarData.length > 0) && (
          <div className={`grid grid-cols-1 gap-6 ${timeseriesData.length > 0 && userBarData.length > 0 ? "lg:grid-cols-2" : ""}`}>
            {/* Timeseries Chart */}
            {timeseriesData.length > 0 && (
            <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
              <h3 className="mb-4 text-lg font-semibold text-gray-900">Query Spend by Source</h3>
              <ResponsiveContainer width="100%" height={300}>
                  <AreaChart data={timeseriesData}>
                    <XAxis dataKey="date" stroke={C.muted} fontSize={12} tickMargin={8} />
                    <YAxis tickFormatter={(v) => formatCurrency(v)} stroke={C.muted} fontSize={12} tickMargin={8} />
                    <Tooltip
                      formatter={(value) => formatCurrency(value as number)}
                      labelFormatter={(label) => `Date: ${label}`}
                    />
                    <Legend wrapperStyle={{ fontSize: 12 }} />
                    {sourceTypes.map((type, idx) => (
                      <Area
                        key={type}
                        type="monotone"
                        dataKey={type}
                        stackId="1"
                        stroke={SOURCE_TYPE_COLORS[type] || seriesColor(idx)}
                        fill={SOURCE_TYPE_COLORS[type] || seriesColor(idx)}
                        fillOpacity={0.6}
                      />
                    ))}
                  </AreaChart>
              </ResponsiveContainer>
            </div>
            )}

            {/* Top Users Bar Chart */}
            {userBarData.length > 0 && (
            <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
              <div className="mb-4 flex items-center justify-between">
                <h3 className="text-lg font-semibold text-gray-900">Top Users by Query Spend</h3>
                <span className="text-xs font-medium" style={{ color: C.lava }}>Click a bar to drill down ↓</span>
              </div>
              <ResponsiveContainer width="100%" height={300}>
                  <BarChart
                    data={userBarData}
                    layout="vertical"
                    margin={{ left: 0, right: 70 }}
                    style={{ cursor: "pointer" }}
                  >
                    <XAxis type="number" tickFormatter={USER_BAR_TICK_FMT} stroke={C.muted} fontSize={12} tickMargin={8} />
                    <YAxis
                      type="category"
                      dataKey="user"
                      width={160}
                      stroke={C.muted}
                      fontSize={12}
                      tickMargin={8}
                    />
                    <Tooltip
                      formatter={USER_BAR_TOOLTIP_FMT}
                      labelFormatter={USER_BAR_TOOLTIP_LABEL_FMT}
                    />
                    <Bar
                      dataKey="total_spend"
                      radius={[0, 4, 4, 0]}
                      isAnimationActive={false}
                      onClick={(entry: BarRectangleItem) => {
                        const payload: unknown = entry.payload;
                        const user = isUserBarDatum(entry)
                          ? entry
                          : isUserBarDatum(payload)
                            ? payload
                            : null;
                        if (user) {
                          setSelectedUser({ raw: user.rawUser, display: user.user });
                        }
                      }}
                    >
                      {userBarData.map((_entry, idx) => (
                        <Cell key={idx} fill={seriesColor(idx)} />
                      ))}
                      <LabelList dataKey="total_spend" position="right" formatter={USER_BAR_LABEL_FMT} style={USER_BAR_LABEL_STYLE} />
                    </Bar>
                  </BarChart>
              </ResponsiveContainer>
              <div className="flex flex-wrap gap-2">
                {userBarData.map((user) => (
                  <button
                    key={user.rawUser}
                    type="button"
                    onClick={() => setSelectedUser({ raw: user.rawUser, display: user.user })}
                    className="sr-only rounded border border-gray-200 px-2 py-1 text-xs text-gray-600 focus:not-sr-only focus-visible:outline-none focus-visible:shadow-(--focus)"
                  >
                    View queries for {user.user}
                  </button>
                ))}
              </div>
            </div>
            )}
          </div>
          )}

          {/* Billing spend and warehouse inventory charts */}
          {((warehouseTypeTimeseries?.timeseries?.length ?? 0) > 0 || sqlSkuBreakdown.length > 0 || hasWarehouseSizeData) && (
          <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
          {(warehouseTypeTimeseries?.timeseries?.length ?? 0) > 0 && (
          <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
              <div className="mb-4">
                <h3 className="text-lg font-semibold text-gray-900">SQL Billing Spend by Warehouse Type</h3>
                {regionScope?.limited && (
                  <p className="mt-1 text-xs text-gray-500">Includes billing from selected workspaces outside this app&apos;s query-history region.</p>
                )}
              </div>
              {(() => {
                const whTypeTs = warehouseTypeTimeseries;
                const tsData = whTypeTs?.timeseries || [];
                const whTypes: string[] = whTypeTs?.warehouse_types || [];
                const typeColors: Record<string, string> = {
                  SERVERLESS: C.s3,
                  PRO: C.s5,
                  CLASSIC: C.s4,
                  UNCLASSIFIED: C.slate,
                };
                return (
                  <ResponsiveContainer width="100%" height={300}>
                    <AreaChart data={tsData} margin={{ top: 5, right: 10, left: 10, bottom: 5 }}>
                      <XAxis
                        dataKey="date"
                        tickFormatter={(d) => {
                          try { return format(parseISO(d), "MMM d"); } catch { return d; }
                        }}
                        stroke={C.muted} fontSize={11}
                      />
                      <YAxis tickFormatter={(v) => formatCurrency(v)} stroke={C.muted} fontSize={11} width={70} />
                      <Tooltip
                        formatter={(value: number | undefined) => formatCurrency(value ?? 0)}
                        labelFormatter={(label) => {
                          try { return format(parseISO(label as string), "MMM d, yyyy"); } catch { return label as string; }
                        }}
                        contentStyle={{ backgroundColor: C.card, border: `1px solid ${C.hairline}`, borderRadius: "8px" }}
                      />
                      <Legend wrapperStyle={{ fontSize: 12 }} />
                      {whTypes.map((wt) => (
                        <Area
                          key={wt}
                          type="monotone"
                          dataKey={wt}
                          stroke={typeColors[wt] || C.slate}
                          fill={typeColors[wt] || C.slate}
                          fillOpacity={0.15}
                          strokeWidth={2}
                        />
                      ))}
                    </AreaChart>
                  </ResponsiveContainer>
                );
              })()}
          </div>
          )}

          {sqlSkuBreakdown.length > 0 && (
            <div className="rounded-lg border bg-white p-6" style={{ borderColor: C.hairline }}>
              <h3 className="mb-1 text-lg font-semibold text-gray-900">SQL Billing Spend by SKU</h3>
              <p className="mb-4 text-xs text-gray-500">Billing-derived and available even when regional query history is not.</p>
              <ResponsiveContainer width="100%" height={Math.max(260, sqlSkuBreakdown.length * 34)}>
                <BarChart data={sqlSkuBreakdown} layout="vertical" margin={{ left: 10, right: 56 }}>
                  <XAxis type="number" tickFormatter={(value) => formatCurrency(value)} stroke={C.muted} fontSize={11} />
                  <YAxis
                    type="category"
                    dataKey="sku_name"
                    width={120}
                    stroke={C.muted}
                    fontSize={10}
                    tickFormatter={(value) => value.length > 20 ? `${value.slice(0, 18)}…` : value}
                  />
                  <Tooltip
                    formatter={(value: number | undefined) => formatCurrency(value ?? 0)}
                    contentStyle={{ backgroundColor: C.card, border: `1px solid ${C.hairline}`, borderRadius: "8px" }}
                  />
                  <Bar dataKey="total_spend" name="SQL Billing Spend" fill={C.s3} radius={[0, 4, 4, 0]}>
                    <LabelList dataKey="total_spend" position="right" formatter={(value) => formatCurrency(Number(value ?? 0))} style={{ fontSize: 10, fill: C.slate }} />
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Warehouse Count by Size */}
          {hasWarehouseSizeData && (
          <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline, overflow: 'visible' }}>
              {(() => {
                const bySize: Record<string, number> = {};
                for (const w of queryData?.by_warehouse?.warehouses || []) {
                  const s = w.warehouse_size || "UNKNOWN";
                  if (s === "UNKNOWN") continue;
                  bySize[s] = (bySize[s] || 0) + 1;
                }
                const chartData = Object.entries(bySize)
                  .sort((a, b) => b[1] - a[1])
                  .map(([name, count]) => ({ name: name.replace(/_/g, " "), count }));

                return (
                  <>
                    <h3 className="mb-4 text-lg font-semibold text-gray-900">Warehouse Count by Size</h3>
                    <ResponsiveContainer width="100%" height={300}>
                        <BarChart data={chartData} layout="vertical" margin={{ left: 20, right: 40 }}>
                          <XAxis type="number" stroke={C.muted} fontSize={12} tickMargin={8} />
                          <YAxis type="category" dataKey="name" width={80} stroke={C.muted} fontSize={12} tickMargin={8} />
                          <Tooltip contentStyle={{ backgroundColor: C.card, border: `1px solid ${C.hairline}`, borderRadius: "8px" }} />
                          <Bar dataKey="count" name="Warehouses" radius={[0, 4, 4, 0]}>
                            {chartData.map((_, idx) => (
                              <Cell key={idx} fill={seriesColor(idx)} />
                            ))}
                            <LabelList dataKey="count" position="right" style={{ fontSize: 11, fill: C.slate }} />
                          </Bar>
                        </BarChart>
                    </ResponsiveContainer>
                  </>
                );
              })()}
          </div>
          )}
          </div>
          )}

          {/* Query Source Breakdown: full width */}
          {(queryData.by_source?.sources?.length ?? 0) > 0 && (
          <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
              <h3 className="mb-4 text-lg font-semibold text-gray-900">Query Source Breakdown</h3>
              <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
                          Source Type
                        </th>
                        <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">
                          Query Count
                        </th>
                        <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">
                          Total Spend
                        </th>
                        <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">
                          Avg Cost/Query
                        </th>
                        <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">
                          Share
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200 bg-white">
                      {(queryData.by_source?.sources ?? []).map((source) => (
                        <tr key={source.query_source_type} className="hover:bg-gray-50">
                          <td className="whitespace-nowrap px-4 py-3">
                            <button
                              type="button"
                              onClick={() => handleSourceClick(source.query_source_type)}
                              className="flex items-center gap-2 rounded text-left focus-visible:outline-none focus-visible:shadow-(--focus)"
                            >
                              <QuerySourceBadge sourceType={source.query_source_type} />
                              <svg aria-hidden="true" className="h-3 w-3 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                              </svg>
                            </button>
                          </td>
                          <td className="whitespace-nowrap px-4 py-3 text-right text-sm text-gray-500">
                            {formatNumber(source.query_count)}
                          </td>
                          <td className="whitespace-nowrap px-4 py-3 text-right text-sm font-medium text-gray-900">
                            {formatCurrency(source.total_spend)}
                          </td>
                          <td className="whitespace-nowrap px-4 py-3 text-right text-sm text-gray-500">
                            {formatCurrency(source.avg_cost_per_query)}
                          </td>
                          <td className="whitespace-nowrap px-4 py-3 text-right">
                            <div className="flex items-center justify-end gap-2">
                              <div className="h-2 w-16 overflow-hidden rounded-full bg-gray-200">
                                <div
                                  className="h-full rounded-full"
                                  style={{
                                    width: `${source.percentage}%`,
                                    backgroundColor: SOURCE_TYPE_COLORS[source.query_source_type] || C.slate,
                                  }}
                                />
                              </div>
                              <span className="text-sm text-gray-500">{(source.percentage ?? 0).toFixed(1)}%</span>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
              </div>
          </div>
          )}

          {/* Top Expensive Queries Table */}
          {(topQueriesLoading || allQueries.length > 0) && (
          <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
            {/* Single toolbar row: title · show historical · source pills · search */}
            <div className="mb-4 flex flex-wrap items-center gap-2">
              <h3 className="mr-2 text-lg font-semibold text-gray-900 shrink-0">Most Expensive Queries</h3>
              {historicalQueryCount > 0 && (
                <label className="flex shrink-0 items-center gap-1.5 text-xs text-gray-500 cursor-pointer">
                  <input type="checkbox" checked={showHistoricalQueries}
                    onChange={(e) => { setShowHistoricalQueries(e.target.checked); setQueriesPage(1); }}
                    className="rounded border-gray-300 text-orange-600 focus:ring-orange-500" />
                  Show historical ({historicalQueryCount})
                  <InfoTooltip className="ml-0.5" label="About historical queries" text="Queries with unknown users or unavailable previews" stopClick />
                </label>
              )}
              <div ref={querySourceDropdownRef} className="relative ml-auto flex items-center gap-2 shrink-0">
                {querySourceDropdownOpen && (
                  <div className="fixed inset-0 z-10" onClick={() => setQuerySourceDropdownOpen(false)} />
                )}
                <div className="relative">
                  <button
                    onClick={() => setQuerySourceDropdownOpen((o) => !o)}
                    className={`flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-medium transition-colors ${activeQuerySourceFilters.length > 0 && activeQuerySourceFilters.length < querySourceTypes.length ? "border-lava text-lava" : "border-gray-300 text-gray-700 hover:bg-gray-50"}`}
                  >
                    <span className="max-w-[140px] truncate">
                      {activeQuerySourceFilters.length === 0 || activeQuerySourceFilters.length === querySourceTypes.length
                        ? "Sources"
                        : activeQuerySourceFilters.length === 1
                        ? activeQuerySourceFilters[0]
                        : `${activeQuerySourceFilters.length} Sources`}
                    </span>
                    <svg
                      className={`ml-0.5 h-4 w-4 shrink-0 text-gray-500 transition-transform ${querySourceDropdownOpen ? "rotate-180" : ""}`}
                      fill="none" viewBox="0 0 24 24" stroke="currentColor"
                    >
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                    </svg>
                  </button>
                  {querySourceDropdownOpen && (
                    <FloatingMenu anchorRef={querySourceDropdownRef} className="min-w-[200px] max-h-64 overflow-y-auto rounded-lg border border-gray-200 bg-white shadow-lg">
                      <div className="sticky top-0 flex items-center justify-between border-b border-gray-100 bg-white px-3 py-2">
                        <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Sources</span>
                        <div className="flex items-center gap-2 text-xs">
                          <button onClick={(e) => { e.stopPropagation(); setQuerySourceFilters([...querySourceTypes]); setQueriesPage(1); }} className="text-gray-500 hover:text-gray-800">All</button>
                          <span className="text-gray-300">·</span>
                          <button onClick={(e) => { e.stopPropagation(); setQuerySourceFilters([]); setQueriesPage(1); }} className="text-gray-500 hover:text-gray-800">Clear</button>
                        </div>
                      </div>
                      {querySourceTypes.map((type) => {
                        return (
                          <button
                            key={type}
                            onClick={() => {
                              setQuerySourceFilters((previous) => {
                                const current = previous ?? querySourceTypes;
                                return current.includes(type)
                                  ? current.filter((value) => value !== type)
                                  : [...current, type];
                              });
                              setQueriesPage(1);
                            }}
                            className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-xs hover:bg-gray-50"
                          >
                            <div className={`flex h-4 w-4 shrink-0 items-center justify-center rounded border ${activeQuerySourceFilters.includes(type) ? "border-orange-500 bg-orange-500" : "border-gray-300"}`}>
                              {activeQuerySourceFilters.includes(type) && <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>}
                            </div>
                            <span className="truncate text-gray-700">{type}</span>
                          </button>
                        );
                      })}
                    </FloatingMenu>
                  )}
                </div>
                <div className="relative shrink-0">
                  <svg className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-500" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                  </svg>
                  <input
                    type="text"
                    placeholder="Search queries..."
                    value={querySearch}
                    onChange={(e) => { setQuerySearch(e.target.value); setQueriesPage(1); }}
                    className="w-44 rounded-full border border-gray-200 bg-white py-1.5 pl-9 pr-4 text-xs placeholder:text-gray-400 focus:border-lava focus:outline-none focus:ring-1 focus:ring-lava"
                  />
                </div>
              </div>
            </div>
            {topQueriesLoading && sortedQueries.length === 0 ? (
              <div className="flex h-32 flex-col items-center justify-center gap-2">
                <Spinner size="md" />
                <span className="text-sm text-gray-500">Loading top queries…</span>
              </div>
            ) : sortedQueries.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
                        Source
                      </th>
                      <th
                        className="cursor-pointer px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500 hover:text-gray-700"
                        onClick={() => handleSort("executed_by")}
                      >
                        User {sortField === "executed_by" && (sortDirection === "asc" ? "↑" : "↓")}
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
                        <span className="flex items-center gap-1">
                          Query Preview
                          <InfoTooltip text="If queries show as <Redacted>, this app does not have access to system.query.history. Grant SELECT on this table to the app's service principal." />
                        </span>
                      </th>
                      <th
                        className="cursor-pointer px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500 hover:text-gray-700"
                        onClick={() => handleSort("duration_seconds")}
                      >
                        Duration {sortField === "duration_seconds" && (sortDirection === "asc" ? "↑" : "↓")}
                      </th>
                      <th
                        className="cursor-pointer px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500 hover:text-gray-700"
                        onClick={() => handleSort("cost")}
                      >
                        <span className="inline-flex items-center justify-end gap-1 whitespace-nowrap">
                          Cost {sortField === "cost" && (sortDirection === "asc" ? "↑" : "↓")}
                          <InfoTooltip text={COST_TOOLTIP_TEXT} stopClick />
                        </span>
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200 bg-white">
                    {sortedQueries.map((query, idx) => (
                      <tr key={query.statement_id || idx} className="hover:bg-gray-50">
                        <td className="whitespace-nowrap px-4 py-3">
                          <QuerySourceBadge sourceType={query.query_source_type} />
                        </td>
                        <td className="px-4 py-3">
                          <span className="inline-flex rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-700 max-w-40 truncate" title={query.executed_by}>
                            {formatIdentity(query.executed_by, spNameMap)}
                          </span>
                        </td>
                        <td className="max-w-md px-4 py-3 text-sm text-gray-500">
                          {(() => {
                            let qHistUrl: string | null = host ? `${host}/sql/history` : null;
                            if (!qHistUrl && query.query_profile_url) { try { qHistUrl = new URL(query.query_profile_url).origin + "/sql/history"; } catch { /* ignore */ } }
                            return qHistUrl ? (
                              <a href={qHistUrl} target="_blank" rel="noopener noreferrer" className="block truncate font-mono text-xs text-blue-600 hover:text-blue-800 hover:underline" title="Open Query History">
                                {query.statement_preview}
                              </a>
                            ) : (
                              <div className="truncate font-mono text-xs">{query.statement_preview}</div>
                            );
                          })()}
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 text-right text-sm text-gray-500">
                          {formatDuration(query.duration_seconds)}
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 text-right text-sm font-medium text-gray-900">
                          {formatCurrency(query.cost)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {queryTotalPages > 1 && (
                  <div className="mt-4 flex items-center justify-between border-t border-gray-200 pt-4">
                    <p className="text-sm text-gray-700">
                      Showing <span className="font-medium">{queryStart + 1}</span> to <span className="font-medium">{Math.min(queryStart + 10, searchedQueries.length)}</span> of <span className="font-medium">{searchedQueries.length}</span>
                    </p>
                    <div className="flex gap-2">
                      <button onClick={() => setQueriesPage(p => Math.max(1, p - 1))} disabled={queriesPage === 1}
                        className="rounded border border-gray-300 px-3 py-1 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50">Previous</button>
                      <button onClick={() => setQueriesPage(p => Math.min(queryTotalPages, p + 1))} disabled={queriesPage === queryTotalPages}
                        className="rounded border border-gray-300 px-3 py-1 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50">Next</button>
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div className="flex h-40 items-center justify-center text-gray-500">
                No query data available
              </div>
            )}
          </div>
          )}

      {/* Source Drilldown Modal: rendered via portal to avoid stacking context issues */}
      {selectedSource && (
        <Dialog
          open
          onClose={() => setSelectedSource(null)}
          title="Top 5 Queries"
          subtitle={<QuerySourceBadge sourceType={selectedSource} />}
          className="max-w-5xl"
          closeLabel="Close source query drilldown"
        >
            {sourceQueriesLoading ? (
              <div className="flex h-40 items-center justify-center">
                <Spinner size="md" />
              </div>
            ) : sourceQueries.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">User</th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
                        <span className="flex items-center gap-1">
                          Query Preview
                          <InfoTooltip text="If queries show as <Redacted>, this app does not have access to system.query.history. Grant SELECT on this table to the app's service principal." />
                        </span>
                      </th>
                      <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Duration</th>
                      <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">
                        <span className="inline-flex items-center justify-end gap-1 whitespace-nowrap">
                          Cost <InfoTooltip text={COST_TOOLTIP_TEXT} />
                        </span>
                      </th>
                      <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">History</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200">
                    {sourceQueries.map((q, idx) => {
                      const srcHistUrl: string | null = host ? `${host}/sql/history` : (() => { try { return q.query_profile_url ? new URL(q.query_profile_url).origin + "/sql/history" : null; } catch { return null; } })();
                      return (
                      <tr key={q.statement_id || idx} className="hover:bg-gray-50">
                        <td className="px-4 py-3">
                          <span className="inline-flex rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-700 max-w-40 truncate" title={q.executed_by}>
                            {formatIdentity(q.executed_by, spNameMap)}
                          </span>
                        </td>
                        <td className="max-w-sm px-4 py-3 text-sm text-gray-500">
                          <div className="truncate font-mono text-xs">{q.statement_preview}</div>
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 text-right text-sm text-gray-500">
                          {formatDuration(q.duration_seconds)}
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 text-right text-sm font-medium text-gray-900">
                          {formatCurrency(q.cost)}
                        </td>
                        <td className="whitespace-nowrap px-4 py-3 text-right">
                          {srcHistUrl
                            ? <a href={srcHistUrl} target="_blank" rel="noopener noreferrer" className="text-xs text-lava hover:underline">History ↗</a>
                            : <span className="text-xs text-gray-500">N/A</span>
                          }
                        </td>
                      </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="flex h-40 items-center justify-center text-gray-500">
                No queries found for this source type
              </div>
            )}
        </Dialog>
      )}
      {/* User Query Drilldown Modal */}
      {selectedUser && (
        <Dialog
          open
          onClose={() => setSelectedUser(null)}
          title={`Queries: ${selectedUser.display}`}
          subtitle={userQueriesData?.total_spend != null
            ? `${userQueriesData.query_count} queries · ${formatCurrency(userQueriesData.total_spend)} total`
            : undefined}
          className="max-w-4xl"
          closeLabel={`Close queries for ${selectedUser.display}`}
        >
            {userQueriesLoading ? (
              <div className="flex h-48 items-center justify-center">
                <Spinner size="md" />
              </div>
            ) : (userQueriesData?.queries?.length ?? 0) > 0 ? (
              <div className="overflow-x-auto max-h-[60vh] overflow-y-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50 sticky top-0">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Time</th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Source</th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Query</th>
                      <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Duration</th>
                      <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">
                        <span className="inline-flex items-center justify-end gap-1 whitespace-nowrap">
                          Cost <InfoTooltip text={COST_TOOLTIP_TEXT} />
                        </span>
                      </th>
                      <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">History</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200">
                    {freshUserQueries.map((q, idx) => {
                      const histUrl: string | null = host ? `${host}/sql/history` : (() => { try { return q.query_profile_url ? new URL(q.query_profile_url).origin + "/sql/history" : null; } catch { return null; } })();
                      return (
                        <tr key={q.statement_id || idx} className="hover:bg-gray-50">
                          <td className="whitespace-nowrap px-4 py-3 text-xs text-gray-500">
                            {q.start_time ? (() => { try { return format(new Date(q.start_time), "MMM d, HH:mm"); } catch { return q.start_time; } })() : "N/A"}
                          </td>
                          <td className="whitespace-nowrap px-4 py-3">
                            <QuerySourceBadge sourceType={q.query_source_type} />
                          </td>
                          <td className="max-w-xs px-4 py-3">
                            <div className="truncate font-mono text-xs text-gray-500" title={q.statement_preview}>{q.statement_preview || "N/A"}</div>
                          </td>
                          <td className="whitespace-nowrap px-4 py-3 text-right text-sm text-gray-500">{formatDuration(q.duration_seconds)}</td>
                          <td className="whitespace-nowrap px-4 py-3 text-right text-sm font-medium text-gray-900">{formatCurrency(q.cost)}</td>
                          <td className="whitespace-nowrap px-4 py-3 text-right">
                            {histUrl
                              ? <a href={histUrl} target="_blank" rel="noopener noreferrer" className="text-xs text-lava hover:underline">History ↗</a>
                              : <span className="text-xs text-gray-500">N/A</span>}
                          </td>
                        </tr>
                      );
                    })}
                    {staleUserQueries.length > 0 && (
                      <>
                        <tr>
                          <td colSpan={6} className="bg-gray-50 px-4 py-2 text-xs font-medium uppercase tracking-wider text-gray-500">
                            Historical: profile links may be expired (25+ days ago)
                          </td>
                        </tr>
                        {staleUserQueries.map((q, idx) => {
                          const historyUrl: string | null = host ? `${host}/sql/history` : (() => { try { return q.query_profile_url ? new URL(q.query_profile_url).origin + "/sql/history" : null; } catch { return null; } })();
                          return (
                            <tr key={q.statement_id || idx} className="opacity-60 hover:bg-gray-50">
                              <td className="whitespace-nowrap px-4 py-3 text-xs text-gray-500">
                                {q.start_time ? (() => { try { return format(new Date(q.start_time), "MMM d, HH:mm"); } catch { return q.start_time; } })() : "N/A"}
                              </td>
                              <td className="whitespace-nowrap px-4 py-3">
                                <QuerySourceBadge sourceType={q.query_source_type} />
                              </td>
                              <td className="max-w-xs px-4 py-3">
                                <div className="truncate font-mono text-xs text-gray-500" title={q.statement_preview}>{q.statement_preview || "N/A"}</div>
                              </td>
                              <td className="whitespace-nowrap px-4 py-3 text-right text-sm text-gray-500">{formatDuration(q.duration_seconds)}</td>
                              <td className="whitespace-nowrap px-4 py-3 text-right text-sm font-medium text-gray-900">{formatCurrency(q.cost)}</td>
                              <td className="whitespace-nowrap px-4 py-3 text-right">
                                <div className="flex flex-col items-end gap-1">
                                  <span className="text-xs text-gray-500" title="Databricks query history retention is ~30 days. Profile links for queries older than ~25 days may no longer be accessible.">Expired</span>
                                  {q.statement_id && (
                                    <button onClick={() => navigator.clipboard.writeText(q.statement_id!)} className="font-mono text-xs text-gray-500 hover:text-gray-700" title={`Copy statement ID: ${q.statement_id}`}>
                                      {q.statement_id.slice(0, 8)}… ⎘
                                    </button>
                                  )}
                                  {historyUrl && (
                                    <a href={historyUrl} target="_blank" rel="noopener noreferrer" className="text-xs text-blue-500 hover:underline">History ↗</a>
                                  )}
                                </div>
                              </td>
                            </tr>
                          );
                        })}
                      </>
                    )}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="flex h-48 items-center justify-center text-gray-500">
                No queries found for this user in the selected date range
              </div>
            )}
        </Dialog>
      )}
    </div>
  );
}

export function OptimizeMethodologyPanel() {
  const MINIMIZE_KEY = "cost-obs-minimize-optimize-info";
  const [minimized, setMinimized] = useState(() =>
    typeof window !== "undefined" ? localStorage.getItem(MINIMIZE_KEY) === "true" : false
  );
  const toggle = (v: boolean) => {
    setMinimized(v);
    if (v) {
      localStorage.setItem(MINIMIZE_KEY, "true");
    } else {
      localStorage.removeItem(MINIMIZE_KEY);
    }
  };
  return (
    <InfoPanel title="Optimize tab methodology" minimized={minimized} onToggle={toggle}>
      <p className="mb-2 font-medium">Idle Time</p>
      <ul className="list-inside list-disc space-y-1">
        <li><strong>Uptime</strong>: Derived from <code className="rounded bg-white/60 px-1 text-xs">system.compute.warehouse_events</code> using the time between START and STOP lifecycle events</li>
        <li><strong>Active query time</strong>: Sum of query durations from <code className="rounded bg-white/60 px-1 text-xs">system.query.history</code> for the same warehouse and window</li>
        <li><strong>Idle time</strong>: Uptime minus active query time, floored at zero</li>
        <li><strong>Estimated idle spend</strong>: Billed spend prorated by the idle share of uptime</li>
        <li>Serverless warehouses are excluded because they scale per query and do not emit start and stop events</li>
      </ul>
      <p className="mb-2 mt-3 font-medium">Rightsizing</p>
      <ul className="list-inside list-disc space-y-1">
        <li><strong>Over scaled</strong>: Multiple clusters were available, but median concurrency never exceeded one</li>
        <li><strong>Oversized</strong>: Query duration and data scanned suggest that a smaller warehouse may be sufficient</li>
        <li>Recommendations use warehouse events and query history over the selected date range</li>
      </ul>
    </InfoPanel>
  );
}

export interface WarehouseHealthData {
  available: boolean;
  reason?: string;
  error_code?: string;
  recommendations: Array<{
    warehouse_id: string;
    warehouse_name: string | null;
    warehouse_size: string | null;
    workspace_id: string;
    recommendation_type: string;
    recommendation_text: string;
  }>;
  warehouses_analyzed: number;
}

export function WarehouseRightsizingView({
  host,
  data: warehouseHealth,
  isLoading: healthLoading,
  isError: healthError,
}: {
  host?: string | null;
  data: WarehouseHealthData | undefined;
  isLoading: boolean;
  isError: boolean;
}) {
  const HEALTH_ISSUE_OPTIONS = [
    { value: "OVER_SCALED", label: "Over-Scaled" },
    { value: "OVERSIZED", label: "Oversized" },
  ];
  const [healthIssueFilter, setHealthIssueFilter] = useState<string[]>(() => HEALTH_ISSUE_OPTIONS.map(o => o.value));
  const [healthSearch, setHealthSearch] = useState("");
  const [healthPage, setHealthPage] = useState(1);
  const HEALTH_PAGE_SIZE = 10;
  const [healthIssueDropdownOpen, setHealthIssueDropdownOpen] = useState(false);
  const healthIssueDropdownRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!healthIssueDropdownOpen) return;
    const handler = (e: MouseEvent) => {
      if (healthIssueDropdownRef.current && !healthIssueDropdownRef.current.contains(e.target as Node)) {
        setHealthIssueDropdownOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [healthIssueDropdownOpen]);

  if (!healthLoading && !healthError && warehouseHealth?.available && warehouseHealth.recommendations.length === 0) {
    return null;
  }

  return (
    <div className="rounded-lg border border-gray-200 bg-white p-6">
      <div className="mb-4 flex items-center justify-between">
        <div>
          <h3 className="text-base font-semibold text-gray-900 flex items-center">
            Warehouse Rightsizing
            <InfoTooltip text="Two categories of rightsizing opportunities. Over-scaled: warehouse scaled to 2+ clusters over the last 30 days but peak query concurrency stayed below capacity: consider reducing max_num_clusters. Oversized: a Large or bigger warehouse with average queue time < 15s and median query duration < 3 minutes: consider downsizing one tier." />
          </h3>
        </div>
        <div className="flex items-center gap-3">
          {warehouseHealth && (
            <span className="text-xs text-gray-500">{warehouseHealth.warehouses_analyzed} warehouse{warehouseHealth.warehouses_analyzed !== 1 ? "s" : ""} analyzed</span>
          )}
          {warehouseHealth?.recommendations?.length ? (
            <>
              <div className="relative" ref={healthIssueDropdownRef}>
                <button
                  onClick={() => { setHealthIssueDropdownOpen((o) => !o); }}
                  className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium transition-colors ${healthIssueFilter.length > 0 && healthIssueFilter.length < HEALTH_ISSUE_OPTIONS.length ? "border-lava text-lava" : "border-gray-300 bg-white text-gray-700 hover:bg-gray-50"}`}
                >
                  {healthIssueFilter.length === 0 || healthIssueFilter.length === HEALTH_ISSUE_OPTIONS.length ? "Issues" : healthIssueFilter.length === 1 ? (HEALTH_ISSUE_OPTIONS.find(o => o.value === healthIssueFilter[0])?.label || healthIssueFilter[0]) : `${healthIssueFilter.length} Issues`}
                  <svg className={`h-3 w-3 transition-transform ${healthIssueDropdownOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" /></svg>
                </button>
                {healthIssueDropdownOpen && (
                  <FloatingMenu anchorRef={healthIssueDropdownRef} className="min-w-[180px] max-h-64 overflow-y-auto rounded-lg border border-gray-200 bg-white shadow-lg">
                    <div className="sticky top-0 flex items-center justify-between border-b border-gray-100 bg-white px-3 py-2">
                      <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Issues</span>
                      <div className="flex items-center gap-2 text-xs">
                        <button onClick={(e) => { e.stopPropagation(); setHealthIssueFilter(HEALTH_ISSUE_OPTIONS.map(o => o.value)); setHealthPage(1); }} className="text-gray-500 hover:text-gray-800">All</button>
                        <span className="text-gray-300">·</span>
                        <button onClick={(e) => { e.stopPropagation(); setHealthIssueFilter([]); setHealthPage(1); }} className="text-gray-500 hover:text-gray-800">Clear</button>
                      </div>
                    </div>
                    {HEALTH_ISSUE_OPTIONS.map((opt) => (
                      <button key={opt.value} onClick={() => { setHealthIssueFilter((prev) => prev.includes(opt.value) ? prev.filter(x => x !== opt.value) : [...prev, opt.value]); setHealthPage(1); }}
                        className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-xs hover:bg-gray-50">
                        <div className={`flex h-4 w-4 shrink-0 items-center justify-center rounded border ${healthIssueFilter.includes(opt.value) ? "border-orange-500 bg-orange-500" : "border-gray-300"}`}>
                          {healthIssueFilter.includes(opt.value) && <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>}
                        </div>
                        <span className="truncate text-gray-700">{opt.label}</span>
                      </button>
                    ))}
                  </FloatingMenu>
                )}
              </div>
              <div className="relative">
                <svg className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-500" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                </svg>
                <input
                  type="text"
                  placeholder="Search warehouses..."
                  value={healthSearch}
                  onChange={(e) => { setHealthSearch(e.target.value); setHealthPage(1); }}
                  className="w-44 rounded-full border border-gray-200 bg-white py-1.5 pl-9 pr-4 text-xs placeholder:text-gray-400 focus:border-lava focus:outline-none focus:ring-1 focus:ring-lava"
                />
              </div>
            </>
          ) : null}
        </div>
      </div>

      {healthLoading ? (
        <div className="flex h-32 flex-col items-center justify-center gap-2">
          <Spinner size="md" />
          <span className="text-sm text-gray-500">Loading rightsizing recommendations…</span>
        </div>
      ) : healthError ? (
        <SourceCapabilityNotice
          title="Rightsizing recommendations are temporarily unavailable"
          description="The warehouse-health query did not complete. Refresh this tab to retry."
        />
      ) : warehouseHealth?.available === false && warehouseHealth.reason === "shared_scope_unsupported" ? (
        <SourceCapabilityNotice
          title="Rightsizing detail is not included in this source"
          description="No current shared aggregate provides warehouse-event grain. The source must publish the aggregate below."
          requiredAggregates={["warehouse_health_summary"]}
        />
      ) : !warehouseHealth?.available || !warehouseHealth.recommendations.length ? (
        <div className="rounded-lg p-4 text-sm" style={{ background: C.oatPage, color: warehouseHealth?.available === false ? C.slate : C.greenInk }}>
          {warehouseHealth?.available === false
            ? "Warehouse health data unavailable. Requires system.compute.warehouse_events access."
            : (
              <span className="inline-flex items-center gap-2">
                <svg className="h-4 w-4 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" /></svg>
                No rightsizing recommendations: all warehouses appear appropriately sized.
              </span>
            )}
        </div>
      ) : (() => {
        const badgeColor: Record<string, string> = {
          IDLE_RUNNING: "bg-red-100 text-red-700",
          OVER_SCALED: "bg-amber-100 text-amber-700",
          OVERSIZED: "bg-orange-100 text-orange-700",
        };
        const badgeLabel: Record<string, string> = {
          IDLE_RUNNING: "Idle Running",
          OVER_SCALED: "Over-Scaled",
          OVERSIZED: "Oversized",
        };
        const filtered = warehouseHealth.recommendations
          .filter((r) => r.recommendation_type !== "IDLE_RUNNING")
          .filter((r) => healthIssueFilter.length === 0 || healthIssueFilter.includes(r.recommendation_type))
          .filter((r) => !healthSearch || (r.warehouse_name || r.warehouse_id).toLowerCase().includes(healthSearch.toLowerCase()));
        const totalPages = Math.max(1, Math.ceil(filtered.length / HEALTH_PAGE_SIZE));
        const safePage = Math.min(healthPage, totalPages);
        const pageRecs = filtered.slice((safePage - 1) * HEALTH_PAGE_SIZE, safePage * HEALTH_PAGE_SIZE);
        return (
          <>
            <div className="overflow-x-auto rounded-lg border border-gray-200">
              <table className="min-w-full divide-y divide-gray-200 text-sm">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Warehouse</th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Size</th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Issue</th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Recommendation</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200 bg-white">
                  {pageRecs.length === 0 && (
                    <tr>
                      <td colSpan={4} className="px-4 py-8 text-center text-sm text-gray-500">
                        No warehouses match the current filters.
                      </td>
                    </tr>
                  )}
                  {pageRecs.map((rec, i) => (
                    <tr key={`${rec.warehouse_id}-${rec.recommendation_type}-${i}`} className="hover:bg-gray-50">
                      <td className="px-4 py-3 font-medium text-gray-900">
                        {host ? (
                          <a
                            href={`${host}/sql/warehouses/${rec.warehouse_id}/edit`}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-blue-600 hover:underline"
                          >
                            {rec.warehouse_name || rec.warehouse_id}
                          </a>
                        ) : (
                          rec.warehouse_name || rec.warehouse_id
                        )}
                      </td>
                      <td className="px-4 py-3 text-gray-500">{rec.warehouse_size || "N/A"}</td>
                      <td className="px-4 py-3">
                        <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${badgeColor[rec.recommendation_type] || "bg-gray-100 text-gray-700"}`}>
                          {badgeLabel[rec.recommendation_type] || rec.recommendation_type}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-gray-600 text-xs max-w-sm">{rec.recommendation_text}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <OptimizeTablePagination
              currentPage={safePage}
              totalPages={totalPages}
              totalItems={filtered.length}
              pageSize={HEALTH_PAGE_SIZE}
              itemLabel={filtered.length === 1 ? "recommendation" : "recommendations"}
              onPageChange={setHealthPage}
            />
          </>
        );
      })()}
    </div>
  );
}

const fmt$ = (v: number) =>
  v >= 1000 ? `$${(v / 1000).toFixed(1)}K` : `$${v.toFixed(0)}`;

const fmtHours = (minutes: number) => {
  const h = Math.floor(minutes / 60);
  const m = Math.round(minutes % 60);
  if (h === 0) return `${m}m`;
  if (m === 0) return `${h}h`;
  return `${h}h ${m}m`;
};

export interface WarehouseIdleTimeData {
  available: boolean;
  reason?: string;
  error_code?: string;
  serverless_detected: boolean;
  error?: string;
  warehouses: Array<{
    warehouse_id: string;
    warehouse_name: string;
    warehouse_size: string;
    warehouse_type: string;
    workspace_id: string;
    uptime_source?: string;
    total_running_minutes: number;
    busy_union_minutes: number;
    idle_minutes: number;
    idle_pct: number;
    warm_hold_minutes: number;
    keep_alive_score: number;
    auto_stop_mins: number;
    max_num_clusters: number;
    total_spend: number;
    estimated_idle_spend: number | null;
    low_confidence: boolean;
  }>;
}

export function WarehouseIdleTimeView({
  host,
  data,
  isLoading,
  isError,
}: {
  host?: string | null;
  data: WarehouseIdleTimeData | undefined;
  isLoading: boolean;
  isError: boolean;
}) {
  const [idlePage, setIdlePage] = useState(1);
  const [idleSearch, setIdleSearch] = useState("");
  const [idleSizeFilter, setIdleSizeFilter] = useState<string[]>([]);
  const [idleTypeFilter, setIdleTypeFilter] = useState<string[]>([]);
  const idleSizeSeen = useRef<Set<string>>(new Set());
  const idleTypeSeen = useRef<Set<string>>(new Set());
  const [idleSizeDropdownOpen, setIdleSizeDropdownOpen] = useState(false);
  const [idleTypeDropdownOpen, setIdleTypeDropdownOpen] = useState(false);
  const idleSizeDropdownRef = useRef<HTMLDivElement>(null);
  const idleTypeDropdownRef = useRef<HTMLDivElement>(null);
  const IDLE_PAGE_SIZE = 10;

  const availableIdleSizes = useMemo(
    () => Array.from(new Set((data?.warehouses || []).map(w => w.warehouse_size))).filter(Boolean).sort() as string[],
    [data?.warehouses],
  );
  // Normalize warehouse_type to the two labels the dropdown actually shows
  // (Serverless vs Classic). Raw values include 'CLASSIC', 'PRO', 'SERVERLESS';
  // Pro is a classic-family variant and should not appear as a duplicate row.
  const availableIdleTypes = useMemo(
    () => Array.from(new Set((data?.warehouses || []).map(w => w.warehouse_type))).filter(Boolean) as string[],
    [data?.warehouses],
  );

  useEffect(() => {
    const seen = idleSizeSeen.current;
    const fresh = availableIdleSizes.filter(x => !seen.has(x));
    if (fresh.length === 0) return;
    setIdleSizeFilter(prev => Array.from(new Set([...prev, ...fresh])));
    fresh.forEach(x => seen.add(x));
  }, [availableIdleSizes]);

  useEffect(() => {
    const seen = idleTypeSeen.current;
    const fresh = availableIdleTypes.filter(x => !seen.has(x));
    if (fresh.length === 0) return;
    setIdleTypeFilter(prev => Array.from(new Set([...prev, ...fresh])));
    fresh.forEach(x => seen.add(x));
  }, [availableIdleTypes]);

  useEffect(() => {
    if (!idleSizeDropdownOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (idleSizeDropdownRef.current && !idleSizeDropdownRef.current.contains(e.target as Node)) {
        setIdleSizeDropdownOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [idleSizeDropdownOpen]);

  useEffect(() => {
    if (!idleTypeDropdownOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (idleTypeDropdownRef.current && !idleTypeDropdownRef.current.contains(e.target as Node)) {
        setIdleTypeDropdownOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [idleTypeDropdownOpen]);

  if (!isLoading && !isError && data?.available && data.warehouses.length === 0) {
    return null;
  }

  return (
    <div className="rounded-lg border border-gray-200 bg-white p-6">
      <div className="mb-4 flex items-start justify-between gap-4">
        <div>
          <h3 className="text-base font-semibold text-gray-900 flex items-center">
            Top Warehouses by Idle Time
            <InfoTooltip text="Idle time = warehouse uptime minus busy time (union of query intervals: bounded by wall-clock even under concurrency). Est. Idle Spend is only computed for CLASSIC single-cluster warehouses because serverless bills per-query with warm-hold at a reduced rate, and multi-cluster warehouses have concurrent cluster billing that wall-clock uptime can't reconstruct. For serverless rows, look at Warm-Hold (minutes held ready between queries, capped at auto_stop_mins) and the 'low conf.' badge: the actionable knob is auto_stop_mins, not raw idle %." />
          </h3>
        </div>
        {data?.available && data.warehouses.length > 0 && (() => {
          const distinctSizes = Array.from(new Set(data.warehouses.map(w => w.warehouse_size))).filter(Boolean).sort();
          const distinctTypes = Array.from(new Set(data.warehouses.map(w => w.warehouse_type))).filter(Boolean);
          return (
            <div className="flex items-center gap-2 shrink-0">
              {distinctSizes.length > 0 && (
                <div className="relative" ref={idleSizeDropdownRef}>
                  <button
                    onClick={() => { setIdleSizeDropdownOpen(o => !o); setIdleTypeDropdownOpen(false); }}
                    className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium transition-colors ${idleSizeFilter.length > 0 && idleSizeFilter.length < distinctSizes.length ? "border-lava text-lava" : "border-gray-300 bg-white text-gray-700 hover:bg-gray-50"}`}
                  >
                    {idleSizeFilter.length === 0 || idleSizeFilter.length === distinctSizes.length ? "Sizes" : idleSizeFilter.length === 1 ? idleSizeFilter[0] : `${idleSizeFilter.length} Sizes`}
                    <svg className={`h-3 w-3 transition-transform ${idleSizeDropdownOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" /></svg>
                  </button>
                  {idleSizeDropdownOpen && (
                    <FloatingMenu anchorRef={idleSizeDropdownRef} className="min-w-[180px] max-h-64 overflow-y-auto rounded-lg border border-gray-200 bg-white shadow-lg">
                      <div className="sticky top-0 flex items-center justify-between border-b border-gray-100 bg-white px-3 py-2">
                        <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Sizes</span>
                        <div className="flex items-center gap-2 text-xs">
                          <button onClick={(e) => { e.stopPropagation(); setIdleSizeFilter([...distinctSizes]); setIdlePage(1); }} className="text-gray-500 hover:text-gray-800">All</button>
                          <span className="text-gray-300">·</span>
                          <button onClick={(e) => { e.stopPropagation(); setIdleSizeFilter([]); setIdlePage(1); }} className="text-gray-500 hover:text-gray-800">Clear</button>
                        </div>
                      </div>
                      {distinctSizes.map(s => (
                        <button key={s} onClick={() => { setIdleSizeFilter((prev) => prev.includes(s) ? prev.filter(x => x !== s) : [...prev, s]); setIdlePage(1); }}
                          className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-xs hover:bg-gray-50">
                          <div className={`flex h-4 w-4 shrink-0 items-center justify-center rounded border ${idleSizeFilter.includes(s) ? "border-orange-500 bg-orange-500" : "border-gray-300"}`}>
                            {idleSizeFilter.includes(s) && <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>}
                          </div>
                          <span className="truncate text-gray-700">{s}</span>
                        </button>
                      ))}
                    </FloatingMenu>
                  )}
                </div>
              )}
              {distinctTypes.length > 0 && (
                <div className="relative" ref={idleTypeDropdownRef}>
                  <button
                    onClick={() => { setIdleTypeDropdownOpen(o => !o); setIdleSizeDropdownOpen(false); }}
                    className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium transition-colors ${idleTypeFilter.length > 0 && idleTypeFilter.length < distinctTypes.length ? "border-lava text-lava" : "border-gray-300 bg-white text-gray-700 hover:bg-gray-50"}`}
                  >
                    {idleTypeFilter.length === 0 || idleTypeFilter.length === distinctTypes.length ? "Types" : idleTypeFilter.length === 1 ? (idleTypeFilter[0] === "SERVERLESS" ? "Serverless" : "Classic") : `${idleTypeFilter.length} Types`}
                    <svg className={`h-3 w-3 transition-transform ${idleTypeDropdownOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" /></svg>
                  </button>
                  {idleTypeDropdownOpen && (
                    <FloatingMenu anchorRef={idleTypeDropdownRef} className="min-w-[180px] max-h-64 overflow-y-auto rounded-lg border border-gray-200 bg-white shadow-lg">
                      <div className="sticky top-0 flex items-center justify-between border-b border-gray-100 bg-white px-3 py-2">
                        <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Types</span>
                        <div className="flex items-center gap-2 text-xs">
                          <button onClick={(e) => { e.stopPropagation(); setIdleTypeFilter([...distinctTypes]); setIdlePage(1); }} className="text-gray-500 hover:text-gray-800">All</button>
                          <span className="text-gray-300">·</span>
                          <button onClick={(e) => { e.stopPropagation(); setIdleTypeFilter([]); setIdlePage(1); }} className="text-gray-500 hover:text-gray-800">Clear</button>
                        </div>
                      </div>
                      {distinctTypes.map(t => (
                        <button key={t} onClick={() => { setIdleTypeFilter((prev) => prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t]); setIdlePage(1); }}
                          className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-xs hover:bg-gray-50">
                          <div className={`flex h-4 w-4 shrink-0 items-center justify-center rounded border ${idleTypeFilter.includes(t) ? "border-orange-500 bg-orange-500" : "border-gray-300"}`}>
                            {idleTypeFilter.includes(t) && <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>}
                          </div>
                          <span className="truncate text-gray-700">{t === "SERVERLESS" ? "Serverless" : "Classic"}</span>
                        </button>
                      ))}
                    </FloatingMenu>
                  )}
                </div>
              )}
              <div className="relative">
                <svg className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-500" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                </svg>
                <input
                  type="text"
                  placeholder="Search warehouses..."
                  value={idleSearch}
                  onChange={(e) => { setIdleSearch(e.target.value); setIdlePage(1); }}
                  className="w-44 rounded-full border border-gray-200 bg-white py-1.5 pl-9 pr-4 text-xs placeholder:text-gray-400 focus:border-lava focus:outline-none focus:ring-1 focus:ring-lava"
                />
              </div>
            </div>
          );
        })()}
      </div>

      {isLoading ? (
        <div className="flex h-32 flex-col items-center justify-center gap-2">
          <Spinner size="md" />
          <span className="text-sm text-gray-500">Loading idle-time analysis…</span>
        </div>
      ) : isError ? (
        <SourceCapabilityNotice
          title="Idle time analysis is temporarily unavailable"
          description="The warehouse idle-time query did not complete. Refresh this tab to retry."
        />
      ) : data?.available === false && data.reason === "shared_scope_unsupported" ? (
        <SourceCapabilityNotice
          title="Idle-time detail is not included in this source"
          description="No current shared aggregate provides warehouse lifecycle grain. The source must publish the aggregate below."
          requiredAggregates={["warehouse_idle_summary"]}
        />
      ) : !data?.available || !data.warehouses.length ? (
        <div className="rounded-lg border border-gray-100 bg-gray-50 p-4 text-sm text-gray-500">
          {data?.available === false
            ? data.error
              ? `Idle time query failed: ${data.error}`
              : "Idle time data unavailable. Requires access to system.compute.warehouse_events and system.query.history."
            : data?.serverless_detected
            ? "Idle time via lifecycle events is not available for Serverless SQL Warehouses. Serverless warehouses scale per-query and do not emit start/stop events."
            : "No warehouse uptime data found for this date range."}
        </div>
      ) : (() => {
        const filteredWarehouses = data.warehouses
          .filter(w => idleSizeFilter.length === 0 || idleSizeFilter.includes(w.warehouse_size))
          .filter(w => idleTypeFilter.length === 0 || idleTypeFilter.includes(w.warehouse_type))
          .filter(w => !idleSearch || w.warehouse_name.toLowerCase().includes(idleSearch.toLowerCase()));
        const totalIdlePages = Math.max(1, Math.ceil(filteredWarehouses.length / IDLE_PAGE_SIZE));
        const safeIdlePage = Math.min(idlePage, totalIdlePages);
        const pageWarehouses = filteredWarehouses.slice((safeIdlePage - 1) * IDLE_PAGE_SIZE, safeIdlePage * IDLE_PAGE_SIZE);
        return (
          <>
            <div className="overflow-x-auto rounded-lg border border-gray-200">
              <table className="min-w-full divide-y divide-gray-200 text-sm">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Warehouse</th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Size</th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Type</th>
                    <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Uptime</th>
                    <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Idle Time</th>
                    <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Idle %</th>
                    <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Total Spend</th>
                    <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Est. Idle Spend</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200 bg-white">
                  {pageWarehouses.length === 0 && (
                    <tr>
                      <td colSpan={8} className="px-4 py-8 text-center text-sm text-gray-500">
                        No warehouses match the current filters.
                      </td>
                    </tr>
                  )}
                  {pageWarehouses.map((wh, i) => (
                    <tr key={`${wh.warehouse_id}-${i}`} className="hover:bg-gray-50">
                      <td className="px-4 py-3 font-medium text-gray-900">
                        {host ? (
                          <a href={`${host}/sql/warehouses/${wh.warehouse_id}/edit`} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
                            {wh.warehouse_name}
                          </a>
                        ) : wh.warehouse_name}
                      </td>
                      <td className="px-4 py-3 text-left text-sm text-gray-500">{wh.warehouse_size}</td>
                      <td className="px-4 py-3 text-left">
                        <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${wh.warehouse_type === 'SERVERLESS' ? 'bg-cyan-100 text-cyan-700' : 'bg-gray-100 text-gray-600'}`}>
                          {wh.warehouse_type === 'SERVERLESS' ? 'Serverless' : 'Classic'}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right text-gray-700">
                        <div className="flex items-center justify-end gap-1.5">
                          <span>{fmtHours(wh.total_running_minutes)}</span>
                          {wh.uptime_source === "billing" && (
                            <span
                              className="inline-flex rounded-full border border-amber-200 bg-amber-50 px-1.5 py-0.5 text-[10px] font-medium text-amber-700"
                              title="Approximate: derived from hourly billing buckets because no lifecycle events were emitted in the window. Sub-hour bursts inflate uptime, which in turn inflates idle time and idle spend."
                            >
                              est.
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="px-4 py-3 text-right text-gray-700">{fmtHours(wh.idle_minutes)}</td>
                      <td className="px-4 py-3 text-right">
                        <div className="flex items-center justify-end gap-1.5">
                          <span
                            className="inline-flex rounded-full px-2 py-0.5 text-xs font-medium"
                            style={wh.idle_pct >= 80 ? { background: C.maroonTint, color: C.maroon } : wh.idle_pct >= 50 ? { background: C.amberTint, color: C.amberInk } : { background: C.oatMed, color: C.slate }}
                          >
                            {wh.idle_pct.toFixed(1)}%
                          </span>
                          {wh.low_confidence && (
                            <span
                              className="inline-flex rounded-full border border-amber-200 bg-amber-50 px-1.5 py-0.5 text-[10px] font-medium text-amber-700"
                              title="Low confidence: serverless with wall-clock uptime above 95% of the window. Almost certainly a keep-alive probe firing under auto_stop_mins, not literal continuous compute. Look at Warm-Hold instead."
                            >
                              low conf.
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="px-4 py-3 text-right text-gray-700">{fmt$(wh.total_spend)}</td>
                      <td className={`px-4 py-3 text-right ${wh.estimated_idle_spend == null ? "text-gray-400" : wh.uptime_source === "billing" ? "text-red-500" : "font-medium text-red-600"}`}>
                        {wh.estimated_idle_spend != null ? (
                          wh.uptime_source === "billing" ? (
                            <span title="Approximate: uptime for this warehouse comes from billing buckets, so the idle-spend proration is directionally noisy.">
                              ~{fmt$(wh.estimated_idle_spend)}
                            </span>
                          ) : (
                            fmt$(wh.estimated_idle_spend)
                          )
                        ) : (
                          <span
                            title={
                              wh.warehouse_type === "SERVERLESS"
                                ? `Serverless: dollar attribution suppressed. Serverless bills per-query with warm-hold at a reduced rate, not full-rate wall-clock. Warm-hold: ${fmtHours(wh.warm_hold_minutes)} at ${wh.auto_stop_mins}m auto_stop_mins. Keep-alive score: ${wh.keep_alive_score.toFixed(1)}%.`
                                : wh.max_num_clusters > 1
                                ? `Multi-cluster (up to ${wh.max_num_clusters} clusters): wall-clock cluster_count > 0 can't reconstruct concurrent cluster billing. Warm-hold: ${fmtHours(wh.warm_hold_minutes)}.`
                                : wh.uptime_source === "billing"
                                ? "Lifecycle events missing: uptime denominator uncertain, dollar attribution suppressed."
                                : "Suppressed: see Warm-Hold and auto_stop_mins for the actionable metric."
                            }
                          >
                            N/A
                          </span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <OptimizeTablePagination
              currentPage={safeIdlePage}
              totalPages={totalIdlePages}
              totalItems={filteredWarehouses.length}
              pageSize={IDLE_PAGE_SIZE}
              itemLabel={filteredWarehouses.length === 1 ? "warehouse" : "warehouses"}
              onPageChange={setIdlePage}
            />
          </>
        );
      })()}
    </div>
  );
}
