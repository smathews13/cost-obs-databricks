import { useMemo, useState, useEffect, useRef, useCallback, memo } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Cell,
  ResponsiveContainer,
  LabelList,
} from "recharts";
import type { SKUBreakdownResponse, WorkspaceBreakdown } from "@/types/billing";
import { Spinner } from "./Spinner";
import { formatCurrency } from "@/utils/formatters";
import { VirtualizedList } from "./VirtualizedList";
import { C, seriesColor } from "@/theme";
import { FloatingMenu } from "@/components/ui/FloatingMenu";
import { SourceCapabilityNotice } from "@/components/brand";
import { buildFilteredUrl } from "@/hooks/useBillingData";

interface SKUBreakdownProps {
  data: SKUBreakdownResponse | undefined;
  isLoading: boolean;
  workspaces?: WorkspaceBreakdown[];
  dateRange?: { startDate: string; endDate: string };
  workspaceNameMap?: Record<string, string>;
}

const fmtCurrency = (v: unknown) => formatCurrency(v as number);
const fmtTooltip = (value: number | undefined) => formatCurrency(value ?? 0);
const fmtTooltipLabel = (label: unknown) => `SKU: ${label}`;
const fmtYTick = (v: string) => (v.length > 22 ? v.substring(0, 20) + "…" : v);
const TOOLTIP_STYLE = { backgroundColor: C.card, border: `1px solid ${C.hairline}`, borderRadius: "8px" } as const;
const LABEL_STYLE = { fontSize: 11, fill: C.slate } as const;

interface FilterResult {
  key: string;
  data?: SKUBreakdownResponse;
  error: boolean;
}

interface WsRowProps {
  wsId: string;
  wsName: string;
  selected: boolean;
  onToggle: (id: string) => void;
  historical?: boolean;
}
const WsRow = memo(function WsRow({ wsId, wsName, selected, onToggle, historical }: WsRowProps) {
  return (
    <button
      onClick={() => onToggle(wsId)}
      className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-xs hover:bg-gray-50"
    >
      <div className={`flex h-4 w-4 shrink-0 items-center justify-center rounded border ${selected ? "border-orange-500 bg-orange-500" : "border-gray-300"}`}>
        {selected && <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>}
      </div>
      <span className="truncate text-gray-700">{wsName}</span>
      {historical && (
        <span className="ml-auto shrink-0 rounded border border-gray-200 bg-gray-100 px-1.5 py-0.5 text-[9px] font-medium text-gray-600" title="This workspace no longer exists in the account. Its data is historical.">
          historical
        </span>
      )}
    </button>
  );
});

export function SKUBreakdown({ data, isLoading, workspaces, dateRange, workspaceNameMap }: SKUBreakdownProps) {
  const [workspaceFilters, setWorkspaceFilters] = useState<string[]>([]);
  const workspaceFiltersSeen = useRef<Set<string>>(new Set());
  const [filterResult, setFilterResult] = useState<FilterResult>({
    key: "",
    error: false,
  });
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [wsSearch, setWsSearch] = useState("");
  const dropdownRef = useRef<HTMLDivElement>(null);

  const allWorkspaceIds = useMemo(
    () => (workspaces || []).map((ws) => String(ws.workspace_id)),
    [workspaces],
  );

  // Sync-add: unseen workspaces get added to the filter automatically, preserving user
  // unselections of items already seen. First load acts as default-all init.
  useEffect(() => {
    const seen = workspaceFiltersSeen.current;
    const fresh = allWorkspaceIds.filter((x) => !seen.has(x));
    if (fresh.length === 0) return;
    setWorkspaceFilters((prev) => Array.from(new Set([...prev, ...fresh])));
    fresh.forEach((x) => seen.add(x));
  }, [allWorkspaceIds]);

  // Empty and complete selections use the already scoped parent payload. Any
  // partial selection requests exactly those workspace IDs.
  const isWorkspaceFilterActive = workspaceFilters.length > 0 && workspaceFilters.length < allWorkspaceIds.length;
  const selectedWorkspaceScope = useMemo(
    () => isWorkspaceFilterActive ? [...workspaceFilters].sort() : [],
    [isWorkspaceFilterActive, workspaceFilters],
  );
  const filterKey = `${selectedWorkspaceScope.join(",") || "all"}|${dateRange?.startDate ?? ""}|${dateRange?.endDate ?? ""}`;

  useEffect(() => {
    if (selectedWorkspaceScope.length === 0) return;

    let cancelled = false;
    const params = new URLSearchParams();
    if (dateRange?.startDate) params.set("start_date", dateRange.startDate);
    if (dateRange?.endDate) params.set("end_date", dateRange.endDate);
    params.set("workspace_ids", selectedWorkspaceScope.join(","));

    fetch(buildFilteredUrl("/api/billing/sku-breakdown", params, selectedWorkspaceScope))
      .then((res) => {
        if (!res.ok) throw new Error(`SKU spend request failed with ${res.status}`);
        return res.json();
      })
      .then((json: SKUBreakdownResponse) => {
        if (!cancelled) {
          setFilterResult({ key: filterKey, data: json, error: false });
        }
      })
      .catch(() => {
        if (!cancelled) setFilterResult({ key: filterKey, error: true });
      });

    return () => {
      cancelled = true;
    };
  }, [selectedWorkspaceScope, dateRange?.startDate, dateRange?.endDate, filterKey]);

  const closeDropdown = useCallback(() => {
    setDropdownOpen(false);
    setWsSearch("");
  }, []);

  useEffect(() => {
    if (!dropdownOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        closeDropdown();
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [closeDropdown, dropdownOpen]);

  const filteredData = filterResult.key === filterKey ? filterResult.data : undefined;
  const filterLoading = selectedWorkspaceScope.length > 0 && filterResult.key !== filterKey;
  const filterError = selectedWorkspaceScope.length > 0 && filterResult.key === filterKey && filterResult.error;
  const displayData = selectedWorkspaceScope.length === 0 ? data : filteredData;
  const showLoading = isLoading || filterLoading;

  const selectedWorkspaceName = useMemo(() => {
    if (workspaceFilters.length !== 1 || !workspaces) return null;
    const wsId = workspaceFilters[0];
    const ws = workspaces.find((w) => String(w.workspace_id) === wsId);
    return workspaceNameMap?.[wsId] || (ws ? (ws.workspace_name || String(ws.workspace_id)) : wsId);
  }, [workspaceFilters, workspaces, workspaceNameMap]);

  // Stable toggle callback so memoized WsRow doesn't re-render every keystroke elsewhere.
  const toggleWs = useCallback((wsId: string) => {
    setWorkspaceFilters((prev) => (prev.includes(wsId) ? prev.filter((x) => x !== wsId) : [...prev, wsId]));
  }, []);
  const selectedSet = useMemo(() => new Set(workspaceFilters), [workspaceFilters]);

  const wsItems = useMemo(
    () => (workspaces || []).map((ws) => {
      const wsId = String(ws.workspace_id);
      const resolvedName = workspaceNameMap?.[wsId] || ws.workspace_name || null;
      return { wsId, wsName: resolvedName || wsId, historical: ws.historical ?? !resolvedName };
    }),
    [workspaces, workspaceNameMap],
  );
  const filteredWsItems = useMemo(() => {
    if (!wsSearch.trim()) return wsItems;
    const q = wsSearch.toLowerCase();
    return wsItems.filter((it) => it.wsName.toLowerCase().includes(q) || it.wsId.toLowerCase().includes(q));
  }, [wsItems, wsSearch]);

  const workspaceSelector = workspaces && workspaces.length > 1 ? (
    <div className="relative" ref={dropdownRef}>
      <button
        onClick={() => {
          if (dropdownOpen) closeDropdown();
          else setDropdownOpen(true);
        }}
        className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium transition-colors ${isWorkspaceFilterActive ? "border-lava text-lava" : "border-gray-300 text-gray-700 hover:bg-gray-50"}`}
      >
        {workspaceFilters.length === 1
          ? (selectedWorkspaceName || workspaceFilters[0])
          : isWorkspaceFilterActive
          ? `${workspaceFilters.length} Workspaces`
          : "Workspace"}
        <svg className={`h-3 w-3 transition-transform ${dropdownOpen ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {dropdownOpen && (
        <FloatingMenu anchorRef={dropdownRef} className="min-w-[220px] rounded-lg border border-gray-200 bg-white shadow-lg">
          <div className="sticky top-0 flex items-center justify-between border-b border-gray-100 bg-white px-3 py-2">
            <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Workspace</span>
            <div className="flex items-center gap-2 text-xs">
              <button onClick={(e) => { e.stopPropagation(); setWorkspaceFilters((workspaces || []).map(ws => String(ws.workspace_id))); }} className="text-gray-500 hover:text-gray-800">All</button>
              <span className="text-gray-300">·</span>
              <button onClick={(e) => { e.stopPropagation(); setWorkspaceFilters([]); }} className="text-gray-500 hover:text-gray-800">Clear</button>
            </div>
          </div>
          <div className="border-b border-gray-100 p-2">
            <input
              type="text"
              value={wsSearch}
              onChange={(e) => setWsSearch(e.target.value)}
              placeholder="Search workspaces..."
              className="w-full rounded border border-gray-200 px-2 py-1 text-xs focus:border-lava focus:outline-none focus:ring-1 focus:ring-lava"
              autoFocus
            />
          </div>
          {filteredWsItems.length === 0 ? (
            <div className="px-3 py-2 text-xs text-gray-500">No matching workspaces</div>
          ) : (
            <VirtualizedList
              items={filteredWsItems}
              itemHeight={36}
              maxHeight={256}
              getKey={(it) => it.wsId}
              renderItem={(it) => (
                <WsRow wsId={it.wsId} wsName={it.wsName} historical={it.historical} selected={selectedSet.has(it.wsId)} onToggle={toggleWs} />
              )}
            />
          )}
        </FloatingMenu>
      )}
    </div>
  ) : null;

  const barData = useMemo(() => {
    if (!displayData?.skus?.length) return [];
    return [...displayData.skus]
      .sort((a, b) => b.total_spend - a.total_spend)
      .slice(0, 10)
      .map((sku) => {
        const stripped = (sku.product ?? "").replace(/^(PREMIUM_|STANDARD_|ENTERPRISE_)/i, "");
        const parts = stripped.split("_");
        const label = parts.length > 4 ? parts.slice(0, 4).join(" ") + "…" : stripped.replace(/_/g, " ");
        return { name: label, total_spend: sku.total_spend };
      });
  }, [displayData]);

  if (showLoading) {
    return (
      <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
        <div className="mb-4 flex items-center justify-between">
          <h3 className="text-lg font-semibold text-gray-900">Spend by SKU</h3>
          {workspaceSelector}
        </div>
        <div className="flex h-48 flex-col items-center justify-center gap-3">
          <Spinner size="lg" />
          <p className="text-sm text-gray-500">Loading SKU breakdown...</p>
        </div>
      </div>
    );
  }

  if (data?.availability === "unavailable" || data?.available === false) {
    const sourceUnsupported = data.error_code === "SOURCE_SCOPE_UNSUPPORTED"
      || data.reason === "shared_scope_unsupported";
    return (
      <div className="rounded-lg border bg-white p-5" style={{ borderColor: C.hairline }}>
        <h3 className="text-base font-semibold text-gray-900">Spend by SKU</h3>
        <div className="mt-3">
          <SourceCapabilityNotice
            title={sourceUnsupported ? "SKU detail is not included in this source" : "SKU detail is temporarily unavailable"}
            description={sourceUnsupported
              ? "No current shared aggregate provides SKU-grain spend. The source must publish the aggregate below."
              : data.reason_detail || "Retry shortly."}
            requiredAggregates={sourceUnsupported ? ["daily_sku_summary"] : []}
          />
        </div>
      </div>
    );
  }

  if (filterError) {
    return (
      <div className="rounded-lg bg-white p-6 border" style={{ borderColor: C.hairline }}>
        <div className="mb-4 flex items-center justify-between">
          <h3 className="text-lg font-semibold text-gray-900">Spend by SKU</h3>
          {workspaceSelector}
        </div>
        <div className="flex h-48 items-center justify-center text-sm text-amber-700">
          SKU spend could not be loaded for the selected workspace.
        </div>
      </div>
    );
  }

  if (!data?.skus?.length) {
    return null;
  }

  if (!displayData || !displayData.skus?.length) {
    return (
      <div className="rounded-lg bg-white p-6 border" style={{ borderColor: C.hairline }}>
        <div className="mb-4 flex items-center justify-between">
          <h3 className="text-lg font-semibold text-gray-900">Spend by SKU</h3>
          {workspaceSelector}
        </div>
        <div className="flex h-48 items-center justify-center text-sm text-gray-500">
          No SKU spend matches the selected workspace filters.
        </div>
      </div>
    );
  }

  return (
    <div className="animate-fade-in rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
      <div className="mb-4 flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold text-gray-900">Spend by SKU</h3>
          {selectedWorkspaceName && (
            <p className="text-sm text-orange-600 font-medium mt-0.5">
              Filtered to: {selectedWorkspaceName}
            </p>
          )}
          {workspaceFilters.length > 1 && isWorkspaceFilterActive && (
            <p className="text-xs text-amber-600 mt-1">Showing aggregate view: select one workspace to filter by workspace</p>
          )}
        </div>
        {workspaceSelector}
      </div>
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={barData} layout="vertical" margin={{ left: -25, right: 70 }}>
          <XAxis type="number" tickFormatter={fmtCurrency} stroke={C.muted} fontSize={12} tickMargin={8} />
          <YAxis
            type="category"
            dataKey="name"
            width={175}
            stroke={C.muted}
            fontSize={11}
            tickMargin={2}
            tickFormatter={fmtYTick}
          />
          <Tooltip
            formatter={fmtTooltip}
            labelFormatter={fmtTooltipLabel}
            contentStyle={TOOLTIP_STYLE}
          />
          <Bar dataKey="total_spend" name="Spend" radius={[0, 4, 4, 0]} isAnimationActive={false}>
            {barData.map((_entry, idx) => (
              <Cell key={idx} fill={seriesColor(idx)} />
            ))}
            <LabelList dataKey="total_spend" position="right" formatter={fmtCurrency} style={LABEL_STYLE} />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
