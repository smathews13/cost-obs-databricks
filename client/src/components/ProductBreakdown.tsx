import { memo, useState, useEffect, useMemo, useRef, useCallback } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
  LabelList,
} from "recharts";
import type { ProductBreakdownResponse, WorkspaceBreakdown } from "@/types/billing";
import { Spinner } from "./Spinner";
import { formatCurrencyCompact as formatCurrency } from "@/utils/formatters";
import { VirtualizedList } from "./VirtualizedList";
import { C, seriesColor } from "@/theme";
import { FloatingMenu } from "@/components/ui/FloatingMenu";

// Hoisted formatters: see SKUBreakdown for rationale.
const fmtCurrency = (v: unknown) => formatCurrency(v as number);
const fmtTooltip = (value: number | undefined) => formatCurrency(value ?? 0);
const fmtTooltipLabel = (label: unknown) => `Product: ${label}`;
const fmtYTick = (v: string) => (v.length > 18 ? v.substring(0, 18) + "..." : v);
const TOOLTIP_STYLE = { backgroundColor: C.card, border: `1px solid ${C.hairline}`, borderRadius: "8px" } as const;
const LABEL_STYLE = { fontSize: 11, fill: C.slate } as const;
const EMPTY_PRODUCT_DATA = {
  products: [],
  total_spend: 0,
  start_date: "",
  end_date: "",
} as ProductBreakdownResponse;

interface FilterResult {
  key: string;
  data?: ProductBreakdownResponse;
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

interface ProductBreakdownProps {
  data: ProductBreakdownResponse | undefined;
  isLoading: boolean;
  workspaces?: WorkspaceBreakdown[];
  dateRange?: { startDate: string; endDate: string };
  workspaceNameMap?: Record<string, string>;
}

const CATEGORY_COLORS: Record<string, string> = {
  "SQL - DBSQL": C.s2,
  "SQL - Genie": C.s2,
  SQL: C.s2,
  "ETL - Batch": C.s3,
  "ETL - Streaming": C.s3,
  Interactive: C.s4,
  Serverless: C.s3,
  "Model Serving": C.s1,
  "AI Search": C.s5,
  "Fine-Tuning": C.s4,
  "AI Functions": C.s5,
  Other: C.s5,
};

export const ProductBreakdown = memo(function ProductBreakdown({ data, isLoading, workspaces, dateRange, workspaceNameMap }: ProductBreakdownProps) {
  const allWsIds = useMemo(
    () => (workspaces ?? []).map((w) => String(w.workspace_id)),
    [workspaces],
  );
  const [workspaceSelection, setWorkspaceSelection] = useState<string[] | null>(null);
  // Null means the user has not applied a workspace filter. It must stay
  // account-wide even when workspace metadata is late, partial, or unavailable.
  const selectedWorkspaces = useMemo(
    () => workspaceSelection ?? [],
    [workspaceSelection],
  );
  const [filterResult, setFilterResult] = useState<FilterResult>({
    key: "",
    error: false,
  });
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [wsSearch, setWsSearch] = useState("");
  const dropdownRef = useRef<HTMLDivElement>(null);

  const hasExplicitWorkspaceFilter = workspaceSelection !== null;
  const isAll = !hasExplicitWorkspaceFilter || (
    allWsIds.length > 0
    && selectedWorkspaces.length === allWsIds.length
    && allWsIds.every((id) => selectedWorkspaces.includes(id))
  );
  const isPartial = hasExplicitWorkspaceFilter
    && selectedWorkspaces.length > 0
    && !isAll;
  const isEmpty = hasExplicitWorkspaceFilter && selectedWorkspaces.length === 0;
  const needsFilteredData = !isAll && !isEmpty;

  const wsKey = useMemo(() => [...selectedWorkspaces].sort().join(','), [selectedWorkspaces]);
  const filterKey = `${wsKey}|${dateRange?.startDate ?? ""}|${dateRange?.endDate ?? ""}`;

  useEffect(() => {
    if (!needsFilteredData) return;

    let cancelled = false;

    const fetchOne = async (wsId: string) => {
      const params = new URLSearchParams();
      if (dateRange?.startDate) params.set("start_date", dateRange.startDate);
      if (dateRange?.endDate) params.set("end_date", dateRange.endDate);
      params.set("workspace_id", wsId);
      const response = await fetch(`/api/billing/by-product?${params}`);
      if (!response.ok) throw new Error(`Product spend request failed with ${response.status}`);
      return response.json();
    };

    if (selectedWorkspaces.length === 1) {
      fetchOne(selectedWorkspaces[0])
        .then((json: ProductBreakdownResponse) => {
          if (!cancelled) setFilterResult({ key: filterKey, data: json, error: false });
        })
        .catch(() => {
          if (!cancelled) setFilterResult({ key: filterKey, error: true });
        });
    } else {
      // Merge per-workspace results by product category
      Promise.all(selectedWorkspaces.map(fetchOne))
        .then((results: ProductBreakdownResponse[]) => {
          if (cancelled) return;
          const merged: Record<string, { total_spend: number; total_dbus: number; workspace_count: number }> = {};
          for (const r of results) {
            for (const p of r.products || []) {
              const key = p.category;
              if (!merged[key]) merged[key] = { total_spend: 0, total_dbus: 0, workspace_count: 0 };
              merged[key].total_spend += p.total_spend || 0;
              merged[key].total_dbus += p.total_dbus || 0;
              merged[key].workspace_count += p.workspace_count || 0;
            }
          }
          const total = Object.values(merged).reduce((s, x) => s + x.total_spend, 0);
          setFilterResult({ key: filterKey, error: false, data: {
            products: Object.entries(merged).map(([category, v]) => ({
              category,
              total_spend: v.total_spend,
              total_dbus: v.total_dbus,
              workspace_count: v.workspace_count,
              percentage: total > 0 ? (v.total_spend / total) * 100 : 0,
            })).sort((a, b) => b.total_spend - a.total_spend),
            total_spend: total,
            start_date: "",
            end_date: "",
          } as ProductBreakdownResponse });
        })
        .catch(() => {
          if (!cancelled) setFilterResult({ key: filterKey, error: true });
        });
    }

    return () => { cancelled = true; };
  }, [dateRange?.startDate, dateRange?.endDate, filterKey, needsFilteredData, selectedWorkspaces]);

  // Close dropdown on outside click; reset search on close so reopening starts fresh
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
  const filterLoading = needsFilteredData && filterResult.key !== filterKey;
  const filterError = needsFilteredData && filterResult.key === filterKey && filterResult.error;
  const displayData = useMemo(
    () => (isEmpty ? EMPTY_PRODUCT_DATA : isAll ? data : filteredData),
    [data, filteredData, isAll, isEmpty],
  );
  const showLoading = isLoading || filterLoading;

  const selectedWorkspaceName = useMemo(() => {
    if (selectedWorkspaces.length !== 1 || !workspaces) return null;
    const wsId = selectedWorkspaces[0];
    const ws = workspaces.find((w) => String(w.workspace_id) === wsId);
    return workspaceNameMap?.[wsId] || (ws ? (ws.workspace_name || String(ws.workspace_id)) : wsId);
  }, [selectedWorkspaces, workspaces, workspaceNameMap]);

  const toggleWs = useCallback((wsId: string) => {
    setWorkspaceSelection((current) => {
      const previous = current ?? allWsIds;
      return previous.includes(wsId)
        ? previous.filter((id) => id !== wsId)
        : [...previous, wsId];
    });
  }, [allWsIds]);
  const selectedSet = useMemo(
    () => new Set(workspaceSelection ?? allWsIds),
    [allWsIds, workspaceSelection],
  );
  const wsItems = useMemo(
    () => (workspaces || []).map((ws) => {
      const wsId = String(ws.workspace_id);
      const resolvedName = workspaceNameMap?.[wsId] || ws.workspace_name || null;
      return {
        wsId,
        wsName: resolvedName || String(ws.workspace_id),
        historical: ws.historical ?? !resolvedName,
      };
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
        className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium transition-colors ${isPartial ? "border-lava text-lava" : "border-gray-300 text-gray-700 hover:bg-gray-50"}`}
      >
        {selectedWorkspaceName
          ? selectedWorkspaceName
          : isPartial
          ? `${selectedWorkspaces.length} workspaces`
          : "Workspace"}
        <svg className={`h-3 w-3 transition-transform ${dropdownOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {dropdownOpen && (
        <FloatingMenu anchorRef={dropdownRef} className="w-72 rounded-lg border border-gray-200 bg-white shadow-lg">
          <div className="sticky top-0 flex items-center justify-between border-b border-gray-100 bg-white px-3 py-2">
            <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Workspace</span>
            <div className="flex items-center gap-2 text-xs">
              <button onClick={(e) => { e.stopPropagation(); setWorkspaceSelection([...allWsIds]); }} className="text-gray-500 hover:text-gray-800">All</button>
              <span className="text-gray-300">·</span>
              <button onClick={(e) => { e.stopPropagation(); setWorkspaceSelection([]); }} className="text-gray-500 hover:text-gray-800">Clear</button>
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

  // Memoize before early returns so hook count is stable across renders.
  const chartData = useMemo(
    () =>
      [...(displayData?.products ?? [])]
        .sort((a, b) => b.total_spend - a.total_spend)
        .map((p) => ({
          name: p.category,
          total_spend: p.total_spend,
          percentage: p.percentage,
        })),
    [displayData],
  );

  if (showLoading) {
    return (
      <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
        <div className="mb-4 flex items-center justify-between">
          <h3 className="text-lg font-semibold text-gray-900">Spend by Product</h3>
          {workspaceSelector}
        </div>
        <div className="flex h-48 flex-col items-center justify-center gap-3">
          <Spinner size="lg" />
          <p className="text-sm text-gray-500">Loading product breakdown...</p>
        </div>
      </div>
    );
  }

  if (filterError) {
    return (
      <div className="rounded-lg bg-white p-6 border" style={{ borderColor: C.hairline }}>
        <div className="mb-4 flex items-center justify-between">
          <h3 className="text-lg font-semibold text-gray-900">Spend by Product</h3>
          {workspaceSelector}
        </div>
        <div className="flex h-48 items-center justify-center text-sm text-amber-700">
          Product spend could not be loaded for the selected workspaces.
        </div>
      </div>
    );
  }

  if (!data?.products?.length) {
    return null;
  }

  if (!displayData || displayData.products.length === 0) {
    return (
      <div className="rounded-lg bg-white p-6 border" style={{ borderColor: C.hairline }}>
        <div className="mb-4 flex items-center justify-between">
          <h3 className="text-lg font-semibold text-gray-900">Spend by Product</h3>
          {workspaceSelector}
        </div>
        <div className="flex h-48 items-center justify-center text-sm text-gray-500">
          No product spend matches the selected workspace filters.
        </div>
      </div>
    );
  }

  return (
    <div className="animate-fade-in rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline, overflow: 'visible' }}>
      <div className="mb-4 flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold text-gray-900">Spend by Product</h3>
          {selectedWorkspaceName && (
            <p className="text-sm text-orange-600 font-medium mt-0.5">
              Filtered to: {selectedWorkspaceName}
            </p>
          )}
        </div>
        {workspaceSelector}
      </div>
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={chartData} layout="vertical" margin={{ left: 0, right: 70 }}>
          <XAxis type="number" tickFormatter={fmtCurrency} stroke={C.muted} fontSize={12} tickMargin={8} />
          <YAxis
            type="category"
            dataKey="name"
            width={100}
            stroke={C.muted}
            fontSize={12}
            tickMargin={8}
            tickFormatter={fmtYTick}
          />
          <Tooltip
            formatter={fmtTooltip}
            labelFormatter={fmtTooltipLabel}
            contentStyle={TOOLTIP_STYLE}
          />
          <Bar dataKey="total_spend" name="Spend" radius={[0, 4, 4, 0]} isAnimationActive={false}>
            {chartData.map((entry, idx) => (
              <Cell key={entry.name} fill={CATEGORY_COLORS[entry.name] || seriesColor(idx)} />
            ))}
            <LabelList dataKey="total_spend" position="right" formatter={fmtCurrency} style={LABEL_STYLE} />
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
});
