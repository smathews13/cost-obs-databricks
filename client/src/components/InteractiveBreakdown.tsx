import { Fragment, memo, useEffect, useMemo, useRef, useState } from "react";
import type { InteractiveBreakdownResponse } from "@/types/billing";
import { formatCurrency, workspaceUrl } from "@/utils/formatters";
import { StatusIndicator } from "./StatusIndicator";
import { formatIdentity, useSpNameMap } from "@/utils/identity";
import { C } from "@/theme";
import { Spinner } from "./Spinner";
import { InfoPopover } from "./ui/InfoPopover";
import { FloatingMenu } from "./ui/FloatingMenu";
import { SortableHeader } from "./ui/SortableHeader";

interface InteractiveBreakdownProps {
  data: InteractiveBreakdownResponse | undefined;
  isLoading: boolean;
  host: string | null | undefined;
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

function getClusterUrl(host: string | null | undefined, clusterId: string, workspaceId: string | null): string | null {
  if (!host || !clusterId) return null;
  const workspaceParam = workspaceId ? `?o=${workspaceId}` : '';
  return workspaceUrl(host, `/compute/interactive${workspaceParam}`);
}

function getNotebookUrl(host: string | null | undefined, notebookPath: string, workspaceId: string | null): string | null {
  if (!host || !notebookPath) return null;
  const workspaceParam = workspaceId ? `?o=${workspaceId}` : '';
  return workspaceUrl(host, `/editor/notebooks/${notebookPath.replace(/^\//, '')}${workspaceParam}`);
}

type SortField = "user" | "notebook_path" | "cluster_id" | "total_spend" | "total_dbus" | "days_active";
type SortDirection = "asc" | "desc";
type ViewMode = "by-user" | "by-cluster" | "by-notebook";

interface AggregatedInteractiveItem {
  key: string;
  workspace_id: string;
  cluster_state: string | null;
  cluster_name: string | null;
  user: string | null;
  total_dbus: number;
  total_spend: number;
  days_active: number;
  count: number;
  _topUserSpend: number;
  percentage: number;
}

function isHistoricalItem(item: AggregatedInteractiveItem): boolean {
  return !item.cluster_name;
}

export const InteractiveBreakdown = memo(function InteractiveBreakdown({ data, isLoading, host }: InteractiveBreakdownProps) {
  const spNameMap = useSpNameMap();
  const [sortField, setSortField] = useState<SortField>("total_spend");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const [viewMode, setViewMode] = useState<ViewMode>("by-user");
  const [currentPage, setCurrentPage] = useState(1);
  const [search, setSearch] = useState("");
  const [showHistorical, setShowHistorical] = useState(false);
  const [viewDropdownOpen, setViewDropdownOpen] = useState(false);
  const viewDropdownRef = useRef<HTMLDivElement>(null);
  const itemsPerPage = 10;

  useEffect(() => {
    if (!viewDropdownOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (viewDropdownRef.current && !viewDropdownRef.current.contains(e.target as Node)) {
        setViewDropdownOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [viewDropdownOpen]);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDirection("desc");
    }
    setCurrentPage(1);
  };

  const derived = useMemo(() => {
    const items = data?.items ?? [];
    const grouped = new Map<string, {
      key: string;
      workspace_id: string;
      cluster_state: string | null;
      cluster_name: string | null;
      user: string | null;
      total_dbus: number;
      total_spend: number;
      days_active: number;
      count: number;
      _topUserSpend: number;
    }>();

    for (const item of items) {
      let key: string;
      if (viewMode === "by-user") {
        key = item.user || "(Unknown User)";
      } else if (viewMode === "by-cluster") {
        key = item.cluster_id || "(Unknown Cluster)";
      } else {
        key = item.notebook_path || "(No Notebook)";
      }

      const existing = grouped.get(key);
      if (existing) {
        existing.total_dbus += item.total_dbus;
        existing.total_spend += item.total_spend;
        existing.days_active = Math.max(existing.days_active, item.days_active);
        existing.count += 1;
        if (!existing.cluster_state && item.cluster_state) {
          existing.cluster_state = item.cluster_state;
        }
        if (!existing.cluster_name && item.cluster_name) {
          existing.cluster_name = item.cluster_name;
        }
        // Track highest-spending user for this notebook
        if (item.user && item.total_spend > existing._topUserSpend) {
          existing.user = item.user;
          existing._topUserSpend = item.total_spend;
        }
      } else {
        grouped.set(key, {
          key,
          workspace_id: item.workspace_id,
          cluster_state: item.cluster_state || null,
          cluster_name: item.cluster_name || null,
          user: item.user || null,
          total_dbus: item.total_dbus,
          total_spend: item.total_spend,
          days_active: item.days_active,
          count: 1,
          _topUserSpend: item.total_spend,
        });
      }
    }

    const aggregatedData: AggregatedInteractiveItem[] = Array.from(grouped.values()).map((g) => ({
      ...g,
      percentage: (data?.total_spend ?? 0) > 0
        ? (g.total_spend / (data?.total_spend ?? 0)) * 100
        : 0,
    }));

    const historicalCount = viewMode === "by-cluster"
      ? aggregatedData.filter(isHistoricalItem).length
      : 0;
    const baseFiltered = aggregatedData.filter(
      (item) => (
        item.key !== "(Unknown User)"
        && item.key !== "(Unknown Cluster)"
        && item.key !== "(No Notebook)"
      ) && (viewMode !== "by-cluster" || showHistorical || !isHistoricalItem(item)),
    );
    const searchLower = search.toLowerCase();
    const filteredData = search
      ? baseFiltered.filter((item) =>
          item.key.toLowerCase().includes(searchLower)
          || (item.cluster_name || "").toLowerCase().includes(searchLower)
          || (item.user || "").toLowerCase().includes(searchLower)
        )
      : baseFiltered;
    const sortedData = [...filteredData].sort((a, b) => {
      const modifier = sortDirection === "asc" ? 1 : -1;
      if (sortField === "user" || sortField === "notebook_path" || sortField === "cluster_id") {
        return a.key.localeCompare(b.key) * modifier;
      }
      const aVal = sortField === "total_spend" ? a.total_spend :
                   sortField === "total_dbus" ? a.total_dbus : a.days_active;
      const bVal = sortField === "total_spend" ? b.total_spend :
                   sortField === "total_dbus" ? b.total_dbus : b.days_active;
      return (aVal - bVal) * modifier;
    });

    return {
      historicalCount,
      sortedData,
      totalDbu: filteredData.reduce((sum, item) => sum + item.total_dbus, 0),
      totalSpend: filteredData.reduce((sum, item) => sum + item.total_spend, 0),
      uniqueUsers: new Set(items.map((item) => item.user).filter(Boolean)).size,
      uniqueClusters: new Set(items.map((item) => item.cluster_id).filter(Boolean)).size,
      uniqueNotebooks: new Set(items.map((item) => item.notebook_path).filter(Boolean)).size,
    };
  }, [data, search, showHistorical, sortDirection, sortField, viewMode]);

  const {
    historicalCount,
    sortedData,
    totalDbu,
    totalSpend,
    uniqueClusters,
    uniqueNotebooks,
    uniqueUsers,
  } = derived;
  const totalPages = Math.ceil(sortedData.length / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const paginatedData = useMemo(
    () => sortedData.slice(startIndex, endIndex),
    [endIndex, sortedData, startIndex],
  );

  if (isLoading) {
    return (
      <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
        <div className="flex h-48 flex-col items-center justify-center gap-3">
          <Spinner size="lg" />
          <p className="text-sm text-gray-500">Loading interactive compute...</p>
        </div>
      </div>
    );
  }

  if (data?.availability === "unavailable" || data?.available === false) {
    return (
      <div className="rounded-lg border bg-white p-5" style={{ borderColor: C.hairline }}>
        <h3 className="text-base font-semibold text-gray-900">Interactive Compute Leaderboard</h3>
        <p className="mt-2 text-sm text-amber-700">
          Interactive compute detail is temporarily unavailable. Retry shortly.
        </p>
      </div>
    );
  }

  if (data?.error) {
    return (
      <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
        <h3 className="mb-4 text-lg font-semibold text-gray-900">Interactive Compute Leaderboard</h3>
        <p className="text-sm text-amber-600">{data.error}</p>
      </div>
    );
  }

  if (!data || data.items.length === 0) {
    return null;
  }

  return (
    <div className="animate-fade-in rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
      <div className="mb-4">
        <div className="flex flex-wrap items-center gap-2">
          <h3 className="text-lg font-semibold text-gray-900 shrink-0 flex items-center gap-1.5">
            Interactive Compute Leaderboard
            <InfoPopover
              className=""
              label="About Interactive Compute Leaderboard"
              size="compact"
              panelClassName="w-64"
              text={'Interactive compute (also known as "All Purpose" compute) usage from notebooks, IDEs, and interactive sessions. Does not include automated jobs or streaming pipelines: those are tracked in the ETL Leaderboard below.'}
            />
          </h3>
          <span className="text-sm text-gray-500 shrink-0">{uniqueUsers} users · {uniqueClusters} clusters · {uniqueNotebooks} notebooks</span>
          <div className="ml-auto flex shrink-0 items-center gap-2">
            <div ref={viewDropdownRef} className="relative">
              <button
                onClick={() => setViewDropdownOpen(o => !o)}
                className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium transition-colors ${viewMode !== "by-user" ? "border-lava text-lava" : "border-gray-300 bg-white text-gray-700 hover:border-gray-400"}`}
              >
                {viewMode === "by-user" ? "By User" : viewMode === "by-cluster" ? "By Cluster" : "By Notebook"}
                <svg className={`h-3 w-3 transition-transform ${viewDropdownOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                </svg>
              </button>
              {viewDropdownOpen && (
                <FloatingMenu anchorRef={viewDropdownRef} className="w-44 rounded-lg border border-gray-200 bg-white shadow-lg">
                  <div className="sticky top-0 flex items-center border-b border-gray-100 bg-white px-3 py-2">
                    <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Group by</span>
                  </div>
                  {(["by-user", "by-cluster", "by-notebook"] as const).map((v) => {
                    const label = v === "by-user" ? "By User" : v === "by-cluster" ? "By Cluster" : "By Notebook";
                    return (
                      <button
                        key={v}
                        onClick={() => { setViewMode(v); setViewDropdownOpen(false); setCurrentPage(1); setSearch(""); }}
                        className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-xs hover:bg-gray-50"
                      >
                        <div className={`flex h-4 w-4 shrink-0 items-center justify-center rounded border ${viewMode === v ? "border-orange-500 bg-orange-500" : "border-gray-300"}`}>
                          {viewMode === v && <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>}
                        </div>
                        <span className="text-gray-700">{label}</span>
                      </button>
                    );
                  })}
                </FloatingMenu>
              )}
            </div>
            {historicalCount > 0 && (
              <label className="flex shrink-0 items-center gap-1.5 text-xs text-gray-500 cursor-pointer">
                <input
                  type="checkbox"
                  checked={showHistorical}
                  onChange={(e) => { setShowHistorical(e.target.checked); setCurrentPage(1); }}
                  className="rounded border-gray-300 text-orange-600 focus:ring-orange-500"
                />
                Show historical ({historicalCount})
                <InfoPopover className="ml-0.5" label="About historical clusters" text="Clusters whose names could not be resolved: likely terminated or from inaccessible workspaces" stopClick />
              </label>
            )}
            <div className="relative w-44 shrink-0">
              <svg className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-500" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <input
                type="text"
                placeholder={viewMode === "by-user" ? "Search users..." : viewMode === "by-notebook" ? "Search notebooks..." : "Search clusters..."}
                value={search}
                onChange={(e) => { setSearch(e.target.value); setCurrentPage(1); }}
                className="w-full rounded-full border border-gray-200 bg-white py-1.5 pl-9 pr-4 text-xs placeholder:text-gray-400 focus:border-lava focus:outline-none focus:ring-1 focus:ring-lava"
              />
            </div>
          </div>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <SortableHeader
                field={viewMode === "by-user" ? "user" : viewMode === "by-cluster" ? "cluster_id" : "notebook_path"}
                activeField={sortField}
                direction={sortDirection}
                onSort={(field) => handleSort(field as SortField)}
                className="px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500"
              >
                {viewMode === "by-user" ? "User" : viewMode === "by-cluster" ? "Cluster" : "Notebook"}
              </SortableHeader>
              {viewMode === "by-notebook" && (
                <th className="px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">
                  User
                </th>
              )}
              <SortableHeader field="total_spend" activeField={sortField} direction={sortDirection} onSort={(field) => handleSort(field as SortField)} align="right" className="px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Spend</SortableHeader>
              <SortableHeader field="total_dbus" activeField={sortField} direction={sortDirection} onSort={(field) => handleSort(field as SortField)} align="right" className="px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">DBUs</SortableHeader>
              <SortableHeader field="days_active" activeField={sortField} direction={sortDirection} onSort={(field) => handleSort(field as SortField)} align="right" className="px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Days</SortableHeader>
              <th className="px-3 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">
                %
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200 bg-white">
            {paginatedData.length === 0 && (
              <tr>
                <td colSpan={viewMode === "by-notebook" ? 6 : 5} className="px-4 py-10 text-center text-sm text-gray-500">
                  No interactive compute rows match the current filters.
                </td>
              </tr>
            )}
            {paginatedData.map((item, idx) => {
              // Get the appropriate URL based on view mode
              const url = viewMode === "by-cluster"
                ? getClusterUrl(host, item.key, item.workspace_id)
                : viewMode === "by-notebook"
                ? getNotebookUrl(host, item.key, item.workspace_id)
                : null;

              // For notebook view, show just the notebook name (last path segment)
              // For cluster view, show cluster_name if available
              // For user view, use formatIdentity (handles SPs and emails)
              const displayName = viewMode === "by-cluster"
                ? (item.cluster_name || item.key)
                : viewMode === "by-notebook" && item.key !== "(No Notebook)"
                ? item.key.split("/").pop() || item.key
                : viewMode === "by-user"
                ? formatIdentity(item.key, spNameMap)
                : item.key;

              return (
                <tr key={`${item.key}-${idx}`} className="hover:bg-gray-50">
                  <td className="px-3 py-3">
                    <div className="flex flex-col items-start gap-1">
                      {url ? (
                        <a
                          href={url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="group flex max-w-md items-center gap-1 truncate text-sm font-medium text-lava hover:text-lava-hover"
                          title={item.key}
                        >
                          <span className="truncate">{displayName}</span>
                          <svg className="h-3 w-3 flex-shrink-0 opacity-0 transition-opacity group-hover:opacity-100" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                          </svg>
                        </a>
                      ) : viewMode === "by-user" ? (
                        <span className="inline-block rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-700 max-w-36 truncate" title={item.key}>
                          {displayName}
                        </span>
                      ) : (
                        <div className="max-w-md truncate text-sm font-medium text-gray-900" title={item.key}>
                          {displayName}
                        </div>
                      )}
                      <div className="flex items-center gap-2">
                        {viewMode === "by-cluster" && isHistoricalItem(item) && (
                          <span className="inline-flex rounded-full bg-gray-100 px-2 py-0.5 text-[10px] font-medium text-gray-500">Historical</span>
                        )}
                        {viewMode === "by-cluster" && item.cluster_state && (
                          <StatusIndicator status={item.cluster_state} type="cluster" />
                        )}
                        {viewMode === "by-cluster" && item.cluster_name && item.cluster_name !== item.key && (
                          <span className="text-xs text-gray-500">{item.key}</span>
                        )}
                      </div>
                    </div>
                  </td>
                  {viewMode === "by-notebook" && (
                    <td className="px-3 py-3">
                      {item.user ? (
                        <span className="inline-flex rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-700 max-w-36 truncate" title={item.user}>
                          {formatIdentity(item.user, spNameMap)}
                        </span>
                      ) : (
                        <span className="text-gray-500">N/A</span>
                      )}
                    </td>
                  )}
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm font-medium text-gray-900">
                    {formatCurrency(item.total_spend)}
                  </td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm text-gray-600">
                    {formatNumber(item.total_dbus)}
                  </td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm text-gray-600">
                    {item.days_active}
                  </td>
                  <td className="whitespace-nowrap px-3 py-3 text-right text-sm text-gray-500">
                    {item.percentage.toFixed(1)}%
                  </td>
                </tr>
              );
            })}
          </tbody>
          <tfoot className="bg-gray-50">
            <tr>
              <td className="px-3 py-3 text-sm font-medium text-gray-700" colSpan={viewMode === "by-notebook" ? 2 : 1}>
                Total ({sortedData.length} {viewMode === "by-user" ? "users" : viewMode === "by-cluster" ? "clusters" : "notebooks"})
              </td>
              <td className="whitespace-nowrap px-3 py-3 text-right text-sm font-bold text-gray-900">
                {formatCurrency(totalSpend)}
              </td>
              <td className="whitespace-nowrap px-3 py-3 text-right text-sm font-medium text-gray-700">
                {formatNumber(totalDbu)}
              </td>
              <td colSpan={2}></td>
            </tr>
          </tfoot>
        </table>
        {totalPages > 1 && (
          <div className="mt-4 flex items-center justify-between border-t border-gray-200 pt-4">
            <p className="text-sm text-gray-700">
              Showing <span className="font-medium">{startIndex + 1}</span> to{" "}
              <span className="font-medium">{Math.min(endIndex, sortedData.length)}</span> of{" "}
              <span className="font-medium">{sortedData.length}</span> {viewMode === "by-user" ? "users" : viewMode === "by-cluster" ? "clusters" : "notebooks"}
            </p>
            <div className="flex gap-2">
              <button
                onClick={() => setCurrentPage(Math.max(1, currentPage - 1))}
                disabled={currentPage === 1}
                className="rounded border border-gray-300 px-3 py-1 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50"
              >
                Previous
              </button>
              {Array.from({ length: totalPages }, (_, i) => i + 1)
                .filter((page) => {
                  // Show first, last, current, and pages around current
                  return (
                    page === 1 ||
                    page === totalPages ||
                    (page >= currentPage - 1 && page <= currentPage + 1)
                  );
                })
                .map((page, idx, arr) => {
                  const prevPage = arr[idx - 1];
                  const showEllipsis = prevPage && page - prevPage > 1;
                  return (
                    <Fragment key={page}>
                      {showEllipsis && (
                        <span className="px-2 py-1 text-gray-500">...</span>
                      )}
                      <button
                        onClick={() => setCurrentPage(page)}
                        className={`rounded px-3 py-1 text-sm font-medium ${
                          currentPage === page
                            ? "text-white"
                            : "border border-gray-300 text-gray-700 hover:bg-gray-50"
                        }`}
                        style={currentPage === page ? { backgroundColor: C.lava } : undefined}
                      >
                        {page}
                      </button>
                    </Fragment>
                  );
                })}
              <button
                onClick={() => setCurrentPage(Math.min(totalPages, currentPage + 1))}
                disabled={currentPage === totalPages}
                className="rounded border border-gray-300 px-3 py-1 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50"
              >
                Next
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
});
