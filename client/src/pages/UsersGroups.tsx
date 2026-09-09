import { useState, useEffect, useRef } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  Cell, LabelList,
} from "recharts";
import { KPITrendModal } from "@/components/KPITrendModal";
import { Bot } from "lucide-react";
import { LoadingPanels } from "@/components/Spinner";
import { formatCurrency, formatKpiCurrency, formatNumber } from "@/utils/formatters";

import type { UserSpend, UsersGroupsBundle } from "@/hooks/useBillingData";
import {
  buildAnonymizedIdentityMap,
  formatIdentity,
  isServicePrincipal,
  useSpNameMap,
} from "@/utils/identity";
import { C, productColor, seriesColor } from "@/theme";
import { PageHero, Chip, InfoPanel, SourceCapabilityNotice } from "@/components/brand";
import { Dialog } from "@/components/ui/Dialog";
import { InfoPopover as InfoTooltip } from "@/components/ui/InfoPopover";
import { FloatingMenu } from "@/components/ui/FloatingMenu";
import { KPICard } from "@/components/ui/KPICard";

// ── User Detail Modal ─────────────────────────────────────────────────────────

function UserDetailModal({ user, displayName, onClose }: { user: UserSpend; displayName: string; onClose: () => void }) {
  return (
    <Dialog
      open
      onClose={onClose}
      title={displayName}
      subtitle={`${user.active_days} active days · ${user.workspace_count} workspace${user.workspace_count !== 1 ? "s" : ""}`}
      className="max-w-lg"
      bodyClassName="max-h-[70vh] space-y-5 overflow-y-auto p-6"
      closeLabel={`Close details for ${displayName}`}
    >
          <div className="grid grid-cols-2 gap-3">
            <div className="rounded-lg border border-gray-100 bg-gray-50 p-3">
              <p className="text-xs text-gray-500">Total spend</p>
              <p className="text-lg font-bold text-gray-900">{formatCurrency(user.total_spend)}</p>
            </div>
            <div className="rounded-lg border border-gray-100 bg-gray-50 p-3">
              <p className="text-xs text-gray-500">Share of total</p>
              <p className="text-lg font-bold text-gray-900">{(user.percentage ?? 0).toFixed(1)}%</p>
            </div>
            <div className="rounded-lg border border-gray-100 bg-gray-50 p-3">
              <p className="text-xs text-gray-500">Total DBUs</p>
              <p className="text-lg font-bold text-gray-900">{(user.total_dbus ?? 0).toFixed(0)}</p>
              <p className="text-xs text-gray-500 mt-0.5">{formatCurrency(user.total_spend)} spend</p>
            </div>
            <div className="rounded-lg border border-gray-100 bg-gray-50 p-3">
              <p className="text-xs text-gray-500">Primary product</p>
              <p className="text-sm font-semibold text-gray-900 mt-1">{user.primary_product}</p>
            </div>
          </div>

          {user.products.length > 0 && (
            <div>
              <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">Spend by product</h4>
              <div className="space-y-2">
                {user.products.sort((a, b) => b.spend - a.spend).map(p => {
                  const pct = user.total_spend > 0 ? (p.spend / user.total_spend) * 100 : 0;
                  return (
                    <div key={p.product}>
                      <div className="flex justify-between text-xs mb-1">
                        <span className="text-gray-600">{p.product}</span>
                        <span className="font-medium text-gray-800">{formatCurrency(p.spend)}</span>
                      </div>
                      <div className="h-1.5 w-full rounded-full bg-gray-100">
                        <div className="h-1.5 rounded-full" style={{ width: `${pct}%`, backgroundColor: productColor(p.product) }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

    </Dialog>
  );
}


// ── Product Drill-down ────────────────────────────────────────────────────────

function ProductDrilldown({ topUsers, displayIdentity }: { topUsers: UserSpend[]; displayIdentity: (email: string) => string }) {
  const [selectedProduct, setSelectedProduct] = useState<string | null>(null);

  const productTotals: Record<string, number> = {};
  topUsers.forEach(u => u.products.forEach(p => {
    productTotals[p.product] = (productTotals[p.product] || 0) + p.spend;
  }));
  const sorted = Object.entries(productTotals).sort((a, b) => b[1] - a[1]);
  const total = sorted.reduce((s, [, v]) => s + v, 0);

  // Top 5 users for the selected product
  const top5 = selectedProduct
    ? topUsers
        .map(u => ({ email: u.user_email, spend: u.products.find(p => p.product === selectedProduct)?.spend ?? 0 }))
        .filter(u => u.spend > 0)
        .sort((a, b) => b.spend - a.spend)
        .slice(0, 5)
    : [];

  return (
    <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
      <div className="mb-4 flex items-center justify-between">
        <h3 className="text-lg font-medium text-gray-900">User Spend by Product</h3>
        <span className="text-xs font-medium" style={{ color: C.lava }}>Click a row to drill down ↓</span>
      </div>
      <div className="space-y-2.5">
        {sorted.map(([product, spend]) => {
          const pct = total > 0 ? (spend / total) * 100 : 0;
          return (
            <div key={product}>
              <button
                className="w-full text-left group"
                onClick={() => setSelectedProduct(product)}
              >
                <div className="flex justify-between text-xs mb-1">
                  <span className="font-medium text-gray-600 group-hover:text-gray-900">
                    {product}
                    <span className="ml-1 text-gray-500 text-[10px]" aria-hidden="true">↗</span>
                  </span>
                  <span className="font-medium text-gray-800">{formatCurrency(spend)} <span className="text-gray-500">({pct.toFixed(1)}%)</span></span>
                </div>
                <div className="h-2 w-full rounded-full bg-gray-100">
                  <div className="h-2 rounded-full transition-all" style={{ width: `${pct}%`, backgroundColor: productColor(product) }} />
                </div>
              </button>
            </div>
          );
        })}
      </div>
      <Dialog
        open={selectedProduct !== null}
        onClose={() => setSelectedProduct(null)}
        title={selectedProduct ? `Top users: ${selectedProduct}` : "Top users"}
        subtitle="Highest-spending users for this product"
        className="max-w-lg"
        closeLabel="Close product drilldown"
      >
        {top5.length > 0 ? (
          <ol className="space-y-2">
            {top5.map((user, index) => (
              <li key={user.email} className="flex items-center justify-between rounded-lg bg-gray-50 px-3 py-2 text-sm">
                <span className="min-w-0 truncate text-gray-700">
                  <span className="mr-2 text-gray-500">{index + 1}.</span>
                  {displayIdentity(user.email)}
                </span>
                <span className="ml-3 shrink-0 font-medium text-gray-800">{formatCurrency(user.spend)}</span>
              </li>
            ))}
          </ol>
        ) : (
          <p className="text-sm text-gray-500">No user spend is available for this product.</p>
        )}
      </Dialog>
    </div>
  );
}


// ── Main page ─────────────────────────────────────────────────────────────────

interface Props {
  startDate: string;
  endDate: string;
  data: UsersGroupsBundle | undefined;
  isLoading: boolean;
  isError?: boolean;
  onRetry: () => void;
  anonymizeUsers?: boolean;
  workspaceIds?: string[];
  workspaceNameMap?: Record<string, string>;
}

const PAGE_SIZE = 10;

export default function UsersGroups({
  startDate,
  endDate,
  data,
  isLoading,
  isError = false,
  onRetry,
  anonymizeUsers = false,
  workspaceIds,
  workspaceNameMap,
}: Props) {
  const spNameMap = useSpNameMap();
  const [selectedUser, setSelectedUser] = useState<UserSpend | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState<Array<"users" | "sps">>(["users", "sps"]);
  const [typeFilterOpen, setTypeFilterOpen] = useState(false);
  const typeFilterRef = useRef<HTMLDivElement>(null);
  const [page, setPage] = useState(0);
  const [selectedProducts, setProductFilter] = useState<string[] | null>(null);
  const [productFilterOpen, setProductFilterOpen] = useState(false);
  const productFilterRef = useRef<HTMLDivElement>(null);
  const [selectedKPI, setSelectedKPI] = useState<{kpi: string; label: string; variant?: "billing" | "platform"} | null>(null);
  const [infoMinimized, setInfoMinimized] = useState(() => {
    if (typeof window !== "undefined") return localStorage.getItem("cost-obs-minimize-users-info") === "true";
    return false;
  });
  const handleMinimizeToggle = (v: boolean) => {
    setInfoMinimized(v);
    if (v) localStorage.setItem("cost-obs-minimize-users-info", "true");
    else localStorage.removeItem("cost-obs-minimize-users-info");
  };

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (typeFilterRef.current && !typeFilterRef.current.contains(e.target as Node)) {
        setTypeFilterOpen(false);
      }
      if (productFilterRef.current && !productFilterRef.current.contains(e.target as Node)) {
        setProductFilterOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const summary = data?.summary;
  const topUsers = data?.top_users ?? [];
  const uniqueProducts = Array.from(new Set(topUsers.map(u => u.primary_product).filter(Boolean))).sort() as string[];
  const productFilter = selectedProducts ?? uniqueProducts;

  const daysDiff = startDate && endDate
    ? Math.round((new Date(endDate).getTime() - new Date(startDate).getTime()) / (1000 * 60 * 60 * 24)) + 1
    : 30;
  const rankedUsers = [...topUsers]
    .filter((user) => (user.total_spend ?? 0) > 0)
    .sort((a, b) => b.total_spend - a.total_spend);
  const powerUsers = rankedUsers.slice(
    0,
    rankedUsers.length > 0 ? Math.max(1, Math.ceil(rankedUsers.length * 0.1)) : 0,
  );
  const powerUsersSpend = powerUsers.reduce((acc, u) => acc + (u.total_spend ?? 0), 0);

  // Stable anon index map: human users sorted by spend get User 1, User 2, …
  const anonMap = anonymizeUsers ? buildAnonymizedIdentityMap(topUsers) : new Map<string, string>();
  const displayUser = (email: string) =>
    anonymizeUsers && anonMap.has(email) ? anonMap.get(email)! : formatIdentity(email, spNameMap);

  const filtered = topUsers
    .filter(u => {
      if (searchQuery && !displayUser(u.user_email).toLowerCase().includes(searchQuery.toLowerCase())) return false;
      const isSP = isServicePrincipal(u.user_email);
      const typeKey: "users" | "sps" = isSP ? "sps" : "users";
      if (!typeFilter.includes(typeKey)) return false;
      if (!productFilter.includes(u.primary_product)) return false;
      return true;
    })
    .sort((a, b) => b.total_spend - a.total_spend);

  const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
  const paginated = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  // Bar chart: top 15 users (always from unfiltered top users)
  // Use raw email as the Recharts category key to avoid duplicate-key issues for SPs
  // Pre-format labels so Recharts category axis shows abbreviated SP names directly
  const seenLabels = new Set<string>();
  const barData = topUsers.slice(0, 15).map(u => {
    let label = displayUser(u.user_email);
    if (seenLabels.has(label)) {
      let n = 2;
      while (seenLabels.has(`${label} (${n})`)) n++;
      label = `${label} (${n})`;
    }
    seenLabels.add(label);
    return { user: label, rawEmail: u.user_email, spend: u.total_spend };
  });

  if (isLoading) {
    return <LoadingPanels sections={[
      "Top Users by Spend",
      "User Spend by Product",
      "User Activity",
      "Service Principal Activity",
    ]} />;
  }

  if (isError || data?.availability === "unavailable" || data?.available === false) {
    const sourceUnsupported = data?.reason === "identity_detail_unavailable_for_shared_sources"
      || data?.error_code === "SOURCE_SCOPE_UNSUPPORTED";
    if (sourceUnsupported) {
      return (
        <SourceCapabilityNotice
          title="User detail is not included in this source"
          description="The selected source has account totals but no user or service-principal identity grain. This aggregate is not currently published by the source."
          requiredAggregates={["daily_user_attribution"]}
        />
      );
    }
    return (
      <SourceCapabilityNotice
        title="User data is temporarily unavailable"
        description="User summary data is temporarily unavailable. Retry shortly."
        onRetry={onRetry}
      />
    );
  }

  return (
    <div className="space-y-6">
      {data?.availability === "partial" && (
        <div className="rounded-md border border-amber-200 bg-amber-50 px-4 py-2 text-sm text-amber-800">
          Top users and totals are available. Some optional activity or product detail could not be loaded.
        </div>
      )}
      <InfoPanel
        title="Users tab methodology"
        minimized={infoMinimized}
        onToggle={handleMinimizeToggle}
        minimizeLabel="Don't show again"
      >
        <ul className="list-inside list-disc space-y-1">
          <li><strong>Attribution model</strong>: Spend is attributed via <code>identity_metadata.run_as</code> in <code>system.billing.usage</code>, which identifies the user or service principal that triggered the workload.</li>
          <li><strong>Service principals</strong>: Jobs and automated pipelines run as service principals. High automated spend is normal; focus on human spend for personal cost governance.</li>
          <li><strong>Active users</strong>: Distinct identities with any DBU spend in the period, not just SQL query users.</li>
          <li><strong>Cost governance</strong>: Set user spend alerts to notify individuals or managers when spend exceeds a threshold.</li>
          <li><strong>Reducing costs</strong>: Review top spenders for long running interactive clusters, idle warehouses, or redundant notebook sessions.</li>
        </ul>
      </InfoPanel>

      <PageHero
        dateRange={{ startDate, endDate }}
        icon={
          <svg fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
          </svg>
        }
        title="Users"
        subtitle={
          <>
            User spend attribution and top consumers by product
            {workspaceIds && workspaceIds.length > 0 && (
              <Chip kind="workspace" label="Workspace(s)">
                {workspaceIds.length === 1 ? (workspaceNameMap?.[workspaceIds[0]] || workspaceIds[0]) : `${workspaceIds.length} workspaces`}
              </Chip>
            )}
          </>
        }
      />

      {<>
      {/* Summary Cards */}
      <div className="co-kpi-grid grid grid-cols-2 gap-4 lg:grid-cols-4">
        <KPICard
          title="Unique Active Users"
          value={summary?.user_count != null ? formatNumber(summary.user_count) : "N/A"}
          subtitle={`across ${summary?.workspace_count ?? "N/A"} workspaces`}
          infoText="Distinct users (humans and service principals) with any DBU spend in the selected date range, across all products."
          onActivate={summary && startDate && endDate ? () => setSelectedKPI({kpi: "active_users", label: "Daily Unique Active Users"}) : undefined}
          ariaLabel="See Unique Active Users trend"
          icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" /></svg>}
        />
        <KPICard
          title="Daily User Spend"
          value={summary ? formatKpiCurrency(summary.avg_spend_per_user) : "N/A"}
          subtitle={`Per-user spend over ${daysDiff} days`}
          infoText="Total list-price spend in the date range divided by the number of distinct active users. Includes all products."
          onActivate={summary && startDate && endDate ? () => setSelectedKPI({kpi: "avg_spend_per_user", label: "Daily Per-User Spend"}) : undefined}
          ariaLabel="See Daily User Spend trend"
          icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" /></svg>}
        />
        <KPICard
          title="Power Users"
          value={powerUsers.length}
          subtitle={`${formatCurrency(powerUsersSpend)} spend over ${daysDiff} days`}
          infoText="The top 10% of users ranked by spend in the selected period. This adapts to large accounts where no single user represents 10% of total spend."
          onActivate={powerUsers.length > 0 && summary && startDate && endDate ? () => setSelectedKPI({kpi: "power_user_spend", label: "Power User Daily Spend"}) : undefined}
          ariaLabel="See Power Users trend"
          icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 3v4M3 5h4M6 17v4m-2-2h4m5-16l2.286 6.857L21 12l-5.714 2.143L13 21l-2.286-6.857L5 12l5.714-2.143L13 3z" /></svg>}
        />
        <KPICard
          title="User Spend Growth"
          value={summary?.spend_growth_pct != null ? `${summary.spend_growth_pct >= 0 ? "+" : ""}${summary.spend_growth_pct}%` : "N/A"}
          valueClassName={summary?.spend_growth_pct != null ? (summary.spend_growth_pct >= 0 ? "text-red-600" : "text-green-600") : ""}
          subtitle={`${summary?.user_count != null ? formatNumber(summary.user_count) : "N/A"} total users · ${daysDiff} days`}
          infoText="Compares total user spend in the first half of the selected date range to the second half. Positive = spend increased over the period."
          onActivate={summary && startDate && endDate ? () => setSelectedKPI({kpi: "user_spend", label: "Daily User Spend"}) : undefined}
          ariaLabel="See User Spend Growth trend"
          icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" /></svg>}
        />
      </div>

      {selectedKPI && startDate && endDate && (
        <KPITrendModal
          variant={selectedKPI.variant ?? "billing"}
          kpi={selectedKPI.kpi}
          kpiLabel={selectedKPI.label}
          isOpen={!!selectedKPI}
          onClose={() => setSelectedKPI(null)}
          startDate={startDate}
          endDate={endDate}
          workspaceIds={workspaceIds}
          queryKeyPrefix={selectedKPI.variant === "platform"
            ? "users-groups-platform-kpi-trend"
            : "users-groups-kpi-trend"}
        />
      )}

      {/* Charts row */}
      {topUsers.length > 0 && (
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        {/* Top users bar chart */}
        <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
          <div className="mb-4 flex items-center justify-between">
            <h3 className="text-lg font-medium text-gray-900">Top Users by Spend</h3>
            <span className="text-xs font-medium" style={{ color: C.lava }}>Click a bar to drill down ↓</span>
          </div>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={barData} layout="vertical" margin={{ left: 8, right: 24, top: 0, bottom: 0 }}>
              <XAxis type="number" tickFormatter={v => formatCurrency(v)} stroke={C.muted} fontSize={12} tickMargin={8} />
              <YAxis type="category" dataKey="user" width={140} stroke={C.muted} fontSize={12} tickMargin={8} interval={0} />
              <Tooltip formatter={(v: number | undefined) => formatCurrency(v ?? 0)} />
              <Bar dataKey="spend" radius={[0, 4, 4, 0]} isAnimationActive={false} onClick={(d: unknown) => {
                const rawEmail = (d as { rawEmail?: string }).rawEmail;
                const u = topUsers.find(u => u.user_email === rawEmail);
                if (u) setSelectedUser(u);
              }} style={{ cursor: "pointer" }}>
                {barData.map((_, i) => (
                  <Cell key={i} fill={seriesColor(i)} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Spend by product */}
        <ProductDrilldown topUsers={topUsers} displayIdentity={displayUser} />
      </div>
      )}

      {/* User growth charts: always last 6 months */}
      {data?.user_growth && data.user_growth.length > 1 && (
        <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
          <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
            <h3 className="text-lg font-medium text-gray-900 mb-1 flex items-center">
              Monthly Active Users
              <InfoTooltip text="Distinct users (humans + service principals) with any DBU spend in that calendar month. Always shows the last 6 months regardless of the date filter above." />
            </h3>
            <p className="text-xs text-gray-500 mb-4">Last 6 months</p>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={data!.user_growth} margin={{ left: 0, right: 16, top: 20, bottom: 0 }}>
                <XAxis dataKey="month" stroke={C.muted} fontSize={12} tickMargin={8} tickFormatter={m => { const months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]; const parts = m.split("-"); return months[parseInt(parts[1], 10) - 1] || m; }} />
                <YAxis stroke={C.muted} fontSize={12} tickMargin={4} allowDecimals={false} />
                <Tooltip labelFormatter={l => String(l)} />
                <Bar dataKey="active_users" name="Active users" fill={C.lava} radius={[4, 4, 0, 0]} isAnimationActive={false}>
                  <LabelList dataKey="active_users" position="top" style={{ fontSize: 10, fill: C.slate }} />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
          <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
            <h3 className="text-lg font-medium text-gray-900 mb-1 flex items-center">
              Monthly User Growth
              <InfoTooltip text="Counts distinct users whose earliest recorded DBU spend falls within each calendar month: i.e., users appearing for the first time that month. Always shows the last 6 months regardless of the date filter above." />
            </h3>
            <p className="text-xs text-gray-500 mb-4">New users appearing for the first time each month: last 6 months</p>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={data!.user_growth} margin={{ left: 0, right: 16, top: 20, bottom: 0 }}>
                <XAxis dataKey="month" stroke={C.muted} fontSize={12} tickMargin={8} tickFormatter={m => { const months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]; const parts = m.split("-"); return months[parseInt(parts[1], 10) - 1] || m; }} />
                <YAxis stroke={C.muted} fontSize={12} tickMargin={4} allowDecimals={false} />
                <Tooltip labelFormatter={l => String(l)} />
                <Bar dataKey="new_users" name="New users" fill={C.s2} radius={[4, 4, 0, 0]} isAnimationActive={false}>
                  <LabelList dataKey="new_users" position="top" style={{ fontSize: 10, fill: C.slate }} />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* User table */}
      {topUsers.length > 0 && (
      <div className="rounded-xl border border-gray-200 bg-white ">
        <div className="flex flex-wrap items-center justify-between gap-3 px-5 py-4 border-b border-gray-100">
          <h2 className="text-base font-semibold text-gray-900">User Leaderboard</h2>
          <div className="flex flex-wrap items-center gap-2">
            {/* Type filter */}
            <div className="relative" ref={typeFilterRef}>
              <button
                onClick={() => setTypeFilterOpen(o => !o)}
                className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium transition-colors ${typeFilter.length > 0 && typeFilter.length < 2 ? "border-lava text-lava" : "border-gray-300 text-gray-700 hover:bg-gray-50"}`}
              >
                {typeFilter.length === 0
                  ? "Type"
                  : typeFilter.length === 2
                  ? "Type"
                  : typeFilter[0] === "users"
                  ? "Users"
                  : "Service Principals"}
                <svg className={`h-3 w-3 transition-transform ${typeFilterOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" /></svg>
              </button>
              {typeFilterOpen && (
                <FloatingMenu anchorRef={typeFilterRef} align="start" className="w-52 rounded-lg border border-gray-200 bg-white shadow-lg">
                  <div className="sticky top-0 flex items-center justify-between border-b border-gray-100 bg-white px-3 py-2">
                    <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Type</span>
                    <div className="flex items-center gap-2 text-xs">
                      <button onClick={(e) => { e.stopPropagation(); setTypeFilter(["users", "sps"]); setPage(0); }} className="text-gray-500 hover:text-gray-800">All</button>
                      <span className="text-gray-300">·</span>
                      <button onClick={(e) => { e.stopPropagation(); setTypeFilter([]); setPage(0); }} className="text-gray-500 hover:text-gray-800">Clear</button>
                    </div>
                  </div>
                  {(["users", "sps"] as const).map(t => (
                    <button
                      key={t}
                      onClick={() => { setTypeFilter(prev => prev.includes(t) ? prev.filter(x => x !== t) : [...prev, t]); setPage(0); }}
                      className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-xs hover:bg-gray-50"
                    >
                      <div className={`flex h-4 w-4 shrink-0 items-center justify-center rounded border ${typeFilter.includes(t) ? "border-orange-500 bg-orange-500" : "border-gray-300"}`}>
                        {typeFilter.includes(t) && <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>}
                      </div>
                      <span className="text-gray-700">{t === "users" ? "Users" : "Service Principals"}</span>
                    </button>
                  ))}
                </FloatingMenu>
              )}
            </div>
            {/* Product filter */}
            <div className="relative" ref={productFilterRef}>
              <button
                onClick={() => setProductFilterOpen(o => !o)}
                className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium transition-colors ${productFilter.length > 0 && productFilter.length < uniqueProducts.length ? "border-lava text-lava" : "border-gray-300 text-gray-700 hover:bg-gray-50"}`}
              >
                {productFilter.length === 0
                  ? "Product"
                  : productFilter.length === 1
                  ? productFilter[0]
                  : productFilter.length === uniqueProducts.length
                  ? "Product"
                  : `${productFilter.length} products`}
                <svg className={`h-3 w-3 transition-transform ${productFilterOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" /></svg>
              </button>
              {productFilterOpen && (
                <FloatingMenu anchorRef={productFilterRef} align="start" className="w-52 overflow-y-auto rounded-lg border border-gray-200 bg-white shadow-lg" style={{ maxHeight: 260 }}>
                  <div className="sticky top-0 flex items-center justify-between border-b border-gray-100 bg-white px-3 py-2">
                    <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Product</span>
                    <div className="flex items-center gap-2 text-xs">
                      <button onClick={(e) => { e.stopPropagation(); setProductFilter([...uniqueProducts]); setPage(0); }} className="text-gray-500 hover:text-gray-800">All</button>
                      <span className="text-gray-300">·</span>
                      <button onClick={(e) => { e.stopPropagation(); setProductFilter([]); setPage(0); }} className="text-gray-500 hover:text-gray-800">Clear</button>
                    </div>
                  </div>
                  {uniqueProducts.map(p => (
                    <button
                      key={p}
                      onClick={() => {
                        setProductFilter((previous) => {
                          const current = previous ?? uniqueProducts;
                          return current.includes(p)
                            ? current.filter((value) => value !== p)
                            : [...current, p];
                        });
                        setPage(0);
                      }}
                      className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-xs hover:bg-gray-50"
                    >
                      <div className={`flex h-4 w-4 shrink-0 items-center justify-center rounded border ${productFilter.includes(p) ? "border-orange-500 bg-orange-500" : "border-gray-300"}`}>
                        {productFilter.includes(p) && <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>}
                      </div>
                      <span className="truncate text-gray-700">{p}</span>
                    </button>
                  ))}
                </FloatingMenu>
              )}
            </div>
            <div className="relative w-44">
              <svg className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-500" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <input
                type="text"
                placeholder="Search users..."
                value={searchQuery}
                onChange={e => { setSearchQuery(e.target.value); setPage(0); }}
                className="w-full rounded-full border border-gray-200 bg-white py-1.5 pl-9 pr-4 text-xs placeholder:text-gray-400 focus:border-lava focus:outline-none focus:ring-1 focus:ring-lava"
              />
            </div>
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100 bg-gray-50 text-xs text-gray-500">
                <th className="px-5 py-3 text-left font-medium">User</th>
                <th className="px-4 py-3 text-right font-medium">Share</th>
                <th className="px-4 py-3 text-right font-medium">Spend</th>
                <th className="px-4 py-3 text-right font-medium">DBUs</th>
                <th className="px-4 py-3 text-right font-medium">Active days</th>
                <th className="px-4 py-3 text-left font-medium">Primary product</th>
                <th className="px-4 py-3" />
              </tr>
            </thead>
            <tbody>
              {paginated.map((u, i) => {
                const globalIdx = page * PAGE_SIZE + i;
                const sp = isServicePrincipal(u.user_email);
                return (
                  <tr key={u.user_email} className="border-b border-gray-50 hover:bg-gray-50 transition-colors">
                    <td className="px-5 py-3">
                      <div className="flex items-center gap-2.5">
                        <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full text-white text-xs font-semibold" style={{ backgroundColor: seriesColor(globalIdx) }}>
                          {sp ? <Bot className="h-4 w-4" aria-label="Service principal" /> : (anonMap.get(u.user_email)?.replace("User ", "") ?? u.user_email.charAt(0).toUpperCase())}
                        </div>
                        <div className="min-w-0">
                          <span className="text-gray-800 font-medium truncate max-w-55 block">{displayUser(u.user_email)}</span>
                        </div>
                      </div>
                    </td>
                    <td className="px-4 py-3 text-right">
                      <div className="flex items-center justify-end gap-2">
                        <div className="w-16 h-1.5 rounded-full bg-gray-100">
                          <div className="h-1.5 rounded-full" style={{ width: `${Math.min(u.percentage, 100)}%`, backgroundColor: seriesColor(globalIdx) }} />
                        </div>
                        <span className="text-gray-500 text-xs w-10 text-right">{(u.percentage ?? 0).toFixed(1)}%</span>
                      </div>
                    </td>
                    <td className="px-4 py-3 text-right text-gray-600">{formatCurrency(u.total_spend)}</td>
                    <td className="px-4 py-3 text-right text-gray-600">{formatNumber(u.total_dbus ?? 0)}</td>
                    <td className="px-4 py-3 text-right text-gray-600">{u.active_days}</td>
                    <td className="px-4 py-3">
                      <Chip>{u.primary_product}</Chip>
                    </td>
                    <td className="px-4 py-3">
                      <button onClick={() => setSelectedUser(u)} className="text-xs text-gray-500 hover:text-gray-700 underline">
                        Details
                      </button>
                    </td>
                  </tr>
                );
              })}
              {filtered.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-5 py-8 text-center text-sm text-gray-500">
                    {searchQuery || typeFilter.length < 2 || productFilter.length < uniqueProducts.length ? "No users match your filters." : "No user spend data found for this date range."}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        {filtered.length > 0 && (
          <div className="flex items-center justify-between px-5 py-3 border-t border-gray-100">
            <span className="text-xs text-gray-500">
              {filtered.length} result{filtered.length !== 1 ? "s" : ""}
            </span>
            {totalPages > 1 && (
              <div className="flex items-center gap-1">
                <button
                  onClick={() => setPage(p => Math.max(0, p - 1))}
                  disabled={page === 0}
                  className="rounded-lg border border-gray-200 px-2.5 py-1 text-xs text-gray-600 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  ‹ Prev
                </button>
                <span className="px-2 text-xs text-gray-500">{page + 1} / {totalPages}</span>
                <button
                  onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
                  disabled={page >= totalPages - 1}
                  className="rounded-lg border border-gray-200 px-2.5 py-1 text-xs text-gray-600 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  Next ›
                </button>
              </div>
            )}
          </div>
        )}
      </div>
      )}

      {selectedUser && (
        <UserDetailModal user={selectedUser} displayName={displayUser(selectedUser.user_email)} onClose={() => setSelectedUser(null)} />
      )}
      </>}
    </div>
  );
}
