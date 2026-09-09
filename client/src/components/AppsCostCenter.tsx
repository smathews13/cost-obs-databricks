import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from "recharts";
import type { AppsDashboardBundle, AppsApp, AppsConnectedArtifact } from "@/types/billing";
import { KPITrendModal } from "./KPITrendModal";
import { VirtualizedList } from "./VirtualizedList";
import { LoadingPanels } from "./Spinner";
import { formatIdentity } from "@/utils/identity";
import { formatCurrency, formatKpiCurrency, formatNumber } from "@/utils/formatters";
import { getAppFallbackColor, getAppInitials } from "@/utils/apps";
import { C } from "@/theme";
import { PageHero, Chip, InfoPanel, SourceCapabilityNotice } from "@/components/brand";
import { InfoPopover } from "@/components/ui/InfoPopover";
import { FloatingMenu } from "@/components/ui/FloatingMenu";
import { KPICard } from "@/components/ui/KPICard";
import {
  buildFilteredUrl,
  getActiveSourceScopeKey,
  getWorkspaceScopeKey,
} from "@/hooks/useBillingData";

interface AppsCostCenterProps {
  data: AppsDashboardBundle | undefined;
  isLoading: boolean;
  isError?: boolean;
  error?: Error | null;
  onRetry?: () => void;
  host?: string | null;
  startDate?: string;
  endDate?: string;
  workspaceIds?: string[];
  workspaceNameMap?: Record<string, string>;
}

const PIE_COLORS = {
  active: C.s3,   // green
  inactive: C.s4, // amber
  historical: C.muted, // gray
};

function workspaceBaseUrl(host?: string | null): string | null {
  const candidate = host?.trim();
  if (!candidate) return null;
  try {
    const url = new URL(candidate.includes("://") ? candidate : `https://${candidate}`);
    if (url.protocol !== "https:" || url.username || url.password) return null;
    return url.origin;
  } catch {
    return null;
  }
}

function servicePrincipalAdminUrl(
  hostBase: string | null,
  servicePrincipalId?: string | null,
): string | null {
  const id = String(servicePrincipalId ?? "").trim();
  if (!hostBase || !/^\d+$/.test(id)) return null;
  return `${hostBase}/settings/identity-and-access/service-principals/${encodeURIComponent(id)}`;
}

function AppThumbnail({
  app,
  size,
}: {
  app: AppsApp;
  size: number;
}) {
  const [failed, setFailed] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const fallbackColor = getAppFallbackColor(app.app_id || app.app_name);

  if (app.metadata?.thumbnail_url && !failed) {
    return (
      <span
        className="relative block shrink-0 overflow-hidden rounded-md"
        style={{
          width: size,
          height: size,
          aspectRatio: "1 / 1",
          backgroundColor: fallbackColor,
        }}
      >
        <img
          src={app.metadata.thumbnail_url}
          alt={`${app.app_name} icon`}
          loading="lazy"
          decoding="async"
          width={size}
          height={size}
          className={`h-full w-full object-cover transition-opacity ${loaded ? "opacity-100" : "opacity-0"}`}
          onLoad={() => setLoaded(true)}
          onError={() => setFailed(true)}
        />
      </span>
    );
  }

  return (
    <div
      className="flex items-center justify-center rounded-md text-white"
      style={{
        backgroundColor: fallbackColor,
        width: size,
        height: size,
        aspectRatio: "1 / 1",
      }}
      aria-label={`${app.app_name} fallback icon`}
    >
      <span className="font-bold select-none" style={{ fontSize: Math.max(14, size * 0.4) }}>
        {getAppInitials(app.app_name, app.app_id)}
      </span>
    </div>
  );
}

const displayState = (value?: string | null) =>
  value ? value.replace(/_/g, " ").toLowerCase().replace(/\b\w/g, c => c.toUpperCase()) : "Unknown";

const formatTimestamp = (value?: string | null) => {
  if (!value) return "N/A";
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime())
    ? value
    : parsed.toLocaleString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
        hour: "numeric",
        minute: "2-digit",
      });
};

function StateBadge({ label, state }: { label: string; state?: string | null }) {
  const normalized = (state || "").toUpperCase();
  const tone =
    ["RUNNING", "ACTIVE", "SUCCEEDED"].includes(normalized)
      ? "bg-green-100 text-green-700"
      : ["FAILED", "ERROR", "CRASHED", "UNAVAILABLE"].includes(normalized)
        ? "bg-red-100 text-red-700"
        : ["DEPLOYING", "STARTING", "UPDATING", "IN_PROGRESS"].includes(normalized)
          ? "bg-blue-100 text-blue-700"
          : "bg-gray-100 text-gray-700";
  return (
    <span className={`inline-flex rounded-full px-2 py-0.5 text-[10px] font-medium ${tone}`}>
      {label}: {displayState(state)}
    </span>
  );
}

function MetadataValue({ label, value }: { label: string; value?: string | number | null }) {
  return (
    <div>
      <dt className="text-[10px] font-semibold uppercase tracking-wide text-gray-500">{label}</dt>
      <dd className="mt-0.5 wrap-break-word text-xs text-gray-800">{value || value === 0 ? value : "N/A"}</dd>
    </div>
  );
}

export function AppsCostCenter({ data, isLoading, isError, onRetry, host, startDate, endDate, workspaceIds, workspaceNameMap }: AppsCostCenterProps) {
  const MINIMIZE_KEY = "cost-obs-minimize-apps-info";

  const [infoMinimized, setInfoMinimized] = useState(() => {
    if (typeof window !== "undefined") {
      return localStorage.getItem(MINIMIZE_KEY) === "true";
    }
    return false;
  });

  const [selectedKPI, setSelectedKPI] = useState<{kpi: string; label: string} | null>(null);
  const [selectedAppId, setSelectedAppId] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [showHistoricalApps, setShowHistoricalApps] = useState(false);
  const [selectedWorkspaces, setSelectedWorkspaces] = useState<string[]>([]);
  const selectedWorkspacesSeen = useRef<Set<string>>(new Set());
  const [wsFilterOpen, setWsFilterOpen] = useState(false);
  const [wsFilterSearch, setWsFilterSearch] = useState("");
  const wsFilterRef = useRef<HTMLDivElement>(null);
  const [appsPagination, setAppsPagination] = useState({ filterKey: "", page: 1 });
  const APPS_PAGE_SIZE = 40;
  const [artifactTypeFilters, setArtifactTypeFilters] = useState<string[]>([]);
  const artifactTypeSeen = useRef<Set<string>>(new Set());
  const artifactAppSeen = useRef<Set<string>>(new Set());
  const [artifactTypeDropdownOpen, setArtifactTypeDropdownOpen] = useState(false);
  const artifactTypeDropdownRef = useRef<HTMLDivElement>(null);
  const [artifactAppFilter, setArtifactAppFilter] = useState<string[]>([]);
  const [artifactAppFilterOpen, setArtifactAppFilterOpen] = useState(false);
  const [artifactAppFilterSearch, setArtifactAppFilterSearch] = useState("");
  const artifactAppFilterRef = useRef<HTMLDivElement>(null);
  const [artifactSearch, setArtifactSearch] = useState("");
  const [artifactPage, setArtifactPage] = useState(1);
  const artifactsPerPage = 10;

  // Close workspace filter dropdown on outside click
  useEffect(() => {
    if (!wsFilterOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (!(e.target as HTMLElement).closest("[data-ws-filter-dropdown]")) {
        setWsFilterOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [wsFilterOpen]);

  // Close artifact app filter dropdown on outside click
  useEffect(() => {
    if (!artifactAppFilterOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (!(e.target as HTMLElement).closest("[data-artifact-app-dropdown]")) {
        setArtifactAppFilterOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [artifactAppFilterOpen]);

  const availableArtifactTypes = useMemo(() => {
    const arts = data?.connected_artifacts || [];
    return [...new Set(arts.map((a: AppsConnectedArtifact) => a.artifact_type))].filter((t): t is string => !!t && t !== 'UNKNOWN' && t !== 'Unknown').sort();
  }, [data?.connected_artifacts]);
  const availableArtifactApps = useMemo(() => {
    const arts = data?.connected_artifacts || [];
    return [...new Set(arts.map((a: AppsConnectedArtifact) => a.app_name).filter(Boolean))].sort() as string[];
  }, [data?.connected_artifacts]);

  useEffect(() => {
    const seen = artifactTypeSeen.current;
    const fresh = availableArtifactTypes.filter(x => !seen.has(x));
    if (fresh.length === 0) return;
    setArtifactTypeFilters(prev => Array.from(new Set([...prev, ...fresh])));
    fresh.forEach(x => seen.add(x));
  }, [availableArtifactTypes]);

  useEffect(() => {
    const seen = artifactAppSeen.current;
    const fresh = availableArtifactApps.filter(x => !seen.has(x));
    if (fresh.length === 0) return;
    setArtifactAppFilter(prev => Array.from(new Set([...prev, ...fresh])));
    fresh.forEach(x => seen.add(x));
  }, [availableArtifactApps]);

  // Close artifact type filter dropdown on outside click
  useEffect(() => {
    if (!artifactTypeDropdownOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (!(e.target as HTMLElement).closest("[data-artifact-type-dropdown]")) {
        setArtifactTypeDropdownOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [artifactTypeDropdownOpen]);

  const handleToggleWorkspace = useCallback((ws: string) => {
    setSelectedWorkspaces(prev =>
      prev.includes(ws) ? prev.filter(w => w !== ws) : [...prev, ws]
    );
  }, []);

  // Pre-warm trend queries so modals open instantly (uses apps-specific endpoint)
  const queryClient = useQueryClient();
  const wsKey = getWorkspaceScopeKey(workspaceIds);
  const sourceKey = getActiveSourceScopeKey();
  useEffect(() => {
    if (!startDate || !endDate) return;
    for (const kpi of ["apps_spend", "apps_dbus", "apps_count", "apps_avg_cost_per_app"]) {
      queryClient.prefetchQuery({
        queryKey: ["apps-kpi-trend", kpi, startDate, endDate, "daily", wsKey, sourceKey],
        queryFn: async () => {
          const params = new URLSearchParams({ kpi, start_date: startDate, end_date: endDate, granularity: "daily" });
          const res = await fetch(buildFilteredUrl("/api/apps/kpi-trend", params, workspaceIds));
          if (!res.ok) throw new Error("prefetch failed");
          return res.json();
        },
        staleTime: 5 * 60 * 1000,
      });
    }
  }, [startDate, endDate, wsKey, sourceKey, workspaceIds, queryClient]);

  const handleMinimizeToggle = (checked: boolean) => {
    setInfoMinimized(checked);
    if (checked) {
      localStorage.setItem(MINIMIZE_KEY, "true");
    } else {
      localStorage.removeItem(MINIMIZE_KEY);
    }
  };

  // Resolve workspace names: prefer billing-wide name map (most complete), then
  // backend name, then a formatted "Workspace <id>" so filter rows never show
  // a bare workspace ID when the name is missing (matches the top-nav pattern).
  const workspaces = data?.workspaces;
  const resolveWsName = useCallback((wsId: string) => {
    if (workspaceNameMap?.[wsId]) return workspaceNameMap[wsId];
    const backendWs = workspaces?.find(w => w.id === wsId);
    if (backendWs?.name && backendWs.name !== wsId) return backendWs.name;
    return `Workspace ${wsId}`;
  }, [workspaces, workspaceNameMap]);

  // Available workspaces for filtering (resolved to names)
  const availableWorkspaces = useMemo(() => {
    if (!workspaces) return [];
    return workspaces
      // Drop null/"None"/"null" ids so a stringified null never becomes an option.
      .filter(ws => ws.id != null && !["", "none", "null"].includes(String(ws.id).trim().toLowerCase()))
      .map(ws => {
        const name = resolveWsName(ws.id);
        // No real name resolved (fell back to "Workspace <id>") => the workspace
        // no longer exists in the account: mark it historical.
        return { id: ws.id, name, historical: name === `Workspace ${ws.id}` };
      })
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [workspaces, resolveWsName]);

  // Sync-add: unseen workspace IDs get added to the filter automatically.
  useEffect(() => {
    const seen = selectedWorkspacesSeen.current;
    const fresh = availableWorkspaces.map(ws => ws.id).filter(id => !seen.has(id));
    if (fresh.length === 0) return;
    setSelectedWorkspaces(prev => Array.from(new Set([...prev, ...fresh])));
    fresh.forEach(id => seen.add(id));
  }, [availableWorkspaces]);

  const selectedWorkspaceIds = useMemo(
    () => new Set(selectedWorkspaces),
    [selectedWorkspaces],
  );
  const historicalAppCount = data?.apps?.apps?.filter(
    (app) => !app.is_registered || app.status === "historical",
  ).length ?? 0;

  // Filter apps by search query and workspace
  const filteredApps = (() => {
    if (!data?.apps?.apps) return [];
    // Empty or all-selected → treat as "show all" so Clear doesn't silently
    // hide every row and the default "all workspaces" state doesn't drop apps
    // that have no workspace_names (registry-only rows with no billing yet).
    // Only apply the workspace filter when there is an actual partial
    // selection, and pass through apps without workspace data (their
    // workspace_names is [] whenever there's no APPS billing to map from).
    const isPartial = selectedWorkspaces.length > 0
      && selectedWorkspaces.length < availableWorkspaces.length;
    const workspaceScopedApps = !isPartial
      ? data.apps.apps
      : data.apps.apps.filter(a =>
          (!a.workspaces?.length && !a.workspace_names?.length)
          || a.workspaces?.some(ws => selectedWorkspaceIds.has(ws.id))
          || a.workspace_names?.some(ws => selectedWorkspaceIds.has(ws))
        );
    const apps = showHistoricalApps
      ? workspaceScopedApps
      : workspaceScopedApps.filter(
          (app) => app.is_registered && app.status !== "historical",
        );
    if (!searchQuery.trim()) return apps;
    const q = searchQuery.toLowerCase();
    return apps.filter(
      (a) =>
        a.app_name.toLowerCase().includes(q)
        || a.app_id.toLowerCase().includes(q)
        || a.metadata?.description?.toLowerCase().includes(q)
        || a.metadata?.creator?.toLowerCase().includes(q)
    );
  })();
  const selectedApp = selectedAppId
    ? data?.apps?.apps?.find((app) => app.app_id === selectedAppId) ?? null
    : null;
  const selectedUnavailable = Boolean(
    selectedAppId && data?.apps?.apps && !selectedApp,
  );

  const appsFilterKey = `${showHistoricalApps}\u0000${searchQuery}\u0000${selectedWorkspaces.join("\u0000")}`;
  const requestedAppsPage = appsPagination.filterKey === appsFilterKey ? appsPagination.page : 1;
  const totalAppsPages = Math.ceil(filteredApps.length / APPS_PAGE_SIZE);
  const effectiveAppsPage = Math.min(requestedAppsPage, Math.max(1, totalAppsPages));
  const paginatedApps = filteredApps.slice((effectiveAppsPage - 1) * APPS_PAGE_SIZE, effectiveAppsPage * APPS_PAGE_SIZE);

  // Build pie chart data: Active vs Inactive vs Historical (unregistered)
  const pieData = useMemo(() => {
    if (!data?.apps) return [];
    const appsData = data.apps;
    const slices: { name: string; value: number; fill: string; count: number }[] = [];

    if (appsData.active_count > 0) {
      slices.push({
        name: "Active",
        value: appsData.active_count,
        fill: PIE_COLORS.active,
        count: appsData.active_count,
      });
    }
    if (appsData.inactive_count > 0) {
      slices.push({
        name: "Inactive",
        value: appsData.inactive_count,
        fill: PIE_COLORS.inactive,
        count: appsData.inactive_count,
      });
    }
    if ((appsData.unregistered_summary?.count ?? 0) > 0) {
      slices.push({
        name: "Historical",
        value: appsData.unregistered_summary?.count ?? 0,
        fill: PIE_COLORS.historical,
        count: appsData.unregistered_summary?.count ?? 0,
      });
    }
    return slices;
  }, [data?.apps]);

  // Daily timeseries (raw from API, matches date picker range)
  const dailyTimeseries = data?.timeseries?.timeseries ?? [];

  if (isLoading && !data && !isError) {
    return <LoadingPanels sections={[
      "Apps Spend Over Time",
      "Apps by Spend",
      "Connected Resources",
    ]} />;
  }

  if (data?.availability === "unavailable" || data?.available === false) {
    const sourceUnsupported = data.error_code === "SOURCE_SCOPE_UNSUPPORTED"
      || data.reason === "shared_scope_unsupported";
    if (sourceUnsupported) {
      return (
        <SourceCapabilityNotice
          title="Apps data is not included in this source"
          description="The selected source publishes account spend summaries but not app-level billing or runtime metadata."
          requiredAggregates={["daily_apps_summary"]}
        />
      );
    }
    return (
      <SourceCapabilityNotice
        title="Apps data is temporarily unavailable"
        description="The background producer did not finish. Retry shortly."
        onRetry={onRetry}
      />
    );
  }

  if (isError && !data) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 p-6">
        <div className="flex flex-col items-center justify-center gap-3 py-4">
          <p className="text-base font-medium text-red-800">Failed to load Apps data</p>
          <p className="text-center text-sm text-red-700">
            Apps data is temporarily unavailable. Retry shortly.
          </p>
          {onRetry && (
            <button
              onClick={onRetry}
              className="mt-1 rounded-md px-4 py-2 text-sm font-medium text-white"
              style={{ backgroundColor: C.lava }}
            >
              Retry
            </button>
          )}
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="rounded-lg border border-yellow-200 bg-yellow-50 p-6">
        <div className="flex flex-col items-center justify-center gap-2 py-4">
          <p className="text-base font-medium text-yellow-800">No Apps cost data available</p>
          <p className="text-sm text-yellow-700">Try expanding the date range, or check that Databricks Apps are deployed and active</p>
        </div>
      </div>
    );
  }

  const summary = data.summary;
  const appsData = data.apps;
  const unregisteredSummary = appsData.unregistered_summary;

  const hostBase = workspaceBaseUrl(host);

  /** Live app endpoint URL (the running frontend). */
  const liveEndpoint = (app: AppsApp) => app.app_url || null;

  /** Backend deployment page in the Databricks workspace. */
  const deploymentUrl = (app: AppsApp) =>
    hostBase ? `${hostBase}/apps-v2/app/${encodeURIComponent(app.app_name)}/overview` : null;

  const selectedMetadata = selectedApp?.metadata;
  const selectedDeployment = selectedMetadata?.deployment;
  const selectedBindings = selectedApp?.resource_bindings || [];
  const selectedWorkspaceLabels = selectedApp
    ? (
        selectedApp.workspaces?.map(ws => ws.name || resolveWsName(ws.id))
        || selectedApp.workspace_names?.map(resolveWsName)
        || []
      )
    : [];

  return (
    <div className="animate-fade-in space-y-6">
      <InfoPanel
        title="Apps tab methodology"
        minimized={infoMinimized}
        onToggle={handleMinimizeToggle}
      >
        <ul className="list-inside list-disc space-y-1">
          <li><strong>Databricks Apps</strong>: Custom web applications deployed and hosted on Databricks</li>
          <li><strong>Active apps</strong>: Registered apps currently reported as running by Databricks</li>
          <li><strong>Inactive apps</strong>: Registered apps not currently running; recent compute usage is used only when live status is unavailable</li>
          <li><strong>Historical apps</strong>: Billing entries with no matching deployed app (deleted or from other workspaces)</li>
          <li>Costs tracked per app from <code className="rounded bg-white/60 px-1">system.billing.usage</code></li>
        </ul>
      </InfoPanel>

      <PageHero
        dateRange={{ startDate, endDate }}
        icon={
          <svg fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z" />
          </svg>
        }
        title="Apps"
        subtitle={
          <>
            Databricks Apps compute cost attribution and trends
            {workspaceIds && workspaceIds.length > 0 && (
              <Chip kind="workspace" label="Workspace(s)">
                {workspaceIds.length === 1 ? (workspaceNameMap?.[workspaceIds[0]] || workspaceIds[0]) : `${workspaceIds.length} workspaces`}
              </Chip>
            )}
          </>
        }
      />

      {/* Summary Cards with click-to-trend */}
      <div className="co-kpi-grid grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <KPICard
          title="Total App Spend"
          value={formatKpiCurrency(summary.total_spend)}
          subtitle={`over ${summary.days_in_range} days`}
          onActivate={startDate && endDate ? () => setSelectedKPI({kpi: "apps_spend", label: "Daily App Spend"}) : undefined}
          ariaLabel="See Total App Spend trend"
          icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>}
        />
        <KPICard
          title="Total App DBUs"
          value={formatNumber(summary.total_dbus)}
          subtitle={`over ${summary.days_in_range} days`}
          onActivate={startDate && endDate ? () => setSelectedKPI({kpi: "apps_dbus", label: "Daily App DBUs"}) : undefined}
          ariaLabel="See Total App DBUs trend"
          icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 7h6m0 10v-3m-3 3h.01M9 17h.01M9 14h.01M12 14h.01M15 11h.01M12 11h.01M9 11h.01M7 21h10a2 2 0 002-2V5a2 2 0 00-2-2H7a2 2 0 00-2 2v14a2 2 0 002 2z" /></svg>}
        />
        <KPICard
          title="Active Apps"
          value={formatNumber(summary.active_app_count)}
          valueTestId="active-apps-kpi-value"
          subtitle={`currently running · ${summary.workspace_count} ${summary.workspace_count === 1 ? "workspace" : "workspaces"}`}
          infoText="The card shows registered apps currently reported as running by Databricks. Its trend drilldown shows daily apps with billed compute activity because historical runtime-state snapshots are not available."
          onActivate={startDate && endDate ? () => setSelectedKPI({kpi: "apps_count", label: "Daily Apps with Compute Activity"}) : undefined}
          ariaLabel="See Active Apps trend"
          icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z" /></svg>}
        />
        <KPICard
          title="Per-App Spend"
          value={formatKpiCurrency(summary.avg_cost_per_app ?? 0)}
          subtitle="daily average"
          onActivate={startDate && endDate ? () => setSelectedKPI({kpi: "apps_avg_cost_per_app", label: "Daily Per-App Spend"}) : undefined}
          ariaLabel="See Per-App Spend trend"
          icon={<svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" /></svg>}
        />
      </div>

      {selectedKPI && startDate && endDate && (
        <KPITrendModal
          kpi={selectedKPI.kpi}
          kpiLabel={selectedKPI.label}
          isOpen={!!selectedKPI}
          onClose={() => setSelectedKPI(null)}
          startDate={startDate}
          endDate={endDate}
          variant="apps"
        />
      )}

      {/* App Status Breakdown + Spend Over Time: side by side */}
      {(pieData.length > 0 || dailyTimeseries.length > 0) && (
      <div className={`grid grid-cols-1 gap-6 ${pieData.length > 0 && dailyTimeseries.length > 0 ? "lg:grid-cols-2" : ""}`}>
        {/* App Status Breakdown: Pie Chart */}
        {pieData.length > 0 && (
          <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
            <h3 className="mb-4 flex items-center text-lg font-semibold text-gray-900">
              App Status Breakdown
              <InfoPopover text="Uses the same registered-app population and live Databricks Apps status as the Active Apps KPI. Billing activity is only a fallback when live status is unavailable." />
            </h3>
            <div className="flex flex-col items-center gap-6 md:flex-row md:items-start">
              <ResponsiveContainer width="100%" height={250} className="max-w-xs">
                <PieChart>
                  <Pie isAnimationActive={false}
                    data={pieData}
                    cx="50%"
                    cy="50%"
                    innerRadius={55}
                    outerRadius={90}
                    paddingAngle={2}
                    dataKey="value"
                    nameKey="name"
                    label={false}
                    labelLine={false}
                  >
                    {pieData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.fill} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value: number | undefined) => formatNumber(value ?? 0)} />
                </PieChart>
              </ResponsiveContainer>
              <div className="flex flex-col gap-3 text-sm">
                <div className="flex items-center gap-3">
                  <div className="h-3 w-3 rounded-full" style={{ backgroundColor: PIE_COLORS.active }} />
                  <div>
                    <span data-testid="active-apps-breakdown-count">
                      <Chip kind="serverless">{formatNumber(appsData.active_count)} Active</Chip>
                    </span>
                    <p className="text-xs text-gray-500">Registered apps currently running</p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <div className="h-3 w-3 rounded-full" style={{ backgroundColor: PIE_COLORS.inactive }} />
                  <div>
                    <Chip kind="historical">{formatNumber(appsData.inactive_count)} Inactive</Chip>
                    <p className="text-xs text-gray-500">Registered apps not currently running</p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <div className="h-3 w-3 rounded-full" style={{ backgroundColor: PIE_COLORS.historical }} />
                  <div>
                    <Chip kind="workspace">{formatNumber(unregisteredSummary.count)} Historical</Chip>
                    <p className="text-xs text-gray-500">Deleted or unregistered: exist in billing system tables only</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Spend Over Time: daily */}
        {dailyTimeseries.length > 0 && (
        <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
          <h3 className="mb-4 text-lg font-semibold text-gray-900">Apps Spend Over Time</h3>
          <ResponsiveContainer width="100%" height={250}>
              <AreaChart data={dailyTimeseries} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
                <XAxis
                  dataKey="date"
                  tickFormatter={(date) => {
                    const d = new Date(date);
                    return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
                  }}
                  stroke={C.muted}
                  fontSize={12}
                  tickMargin={8}
                />
                <YAxis
                  tickFormatter={(value) => formatCurrency(value)}
                  width={70}
                  stroke={C.muted}
                  fontSize={12}
                />
                <Tooltip
                  formatter={(value: number | string | undefined) => formatCurrency(Number(value ?? 0))}
                  labelFormatter={(label) => {
                    const d = new Date(label);
                    return d.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" });
                  }}
                />
                <Area isAnimationActive={false}
                  type="monotone"
                  dataKey="Total"
                  stroke={C.lava}
                  fill={C.lava}
                  fillOpacity={0.15}
                  strokeWidth={2}
                />
              </AreaChart>
          </ResponsiveContainer>
        </div>
        )}
      </div>
      )}

      {/* App Grid: each app is a clickable tile */}
      {(appsData.apps.length > 0 || selectedUnavailable) && (
      <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
        <div className="mb-4 flex items-center justify-between gap-3">
          <h3 className="shrink-0 text-lg font-semibold text-gray-900">
            Apps by Spend
            <span className="ml-2 text-sm font-normal text-gray-500">
              ({filteredApps.length} app{filteredApps.length !== 1 ? "s" : ""})
            </span>
          </h3>
          <div className="flex items-center gap-2">
            {historicalAppCount > 0 && (
              <label className="flex shrink-0 cursor-pointer items-center gap-1.5 whitespace-nowrap text-xs text-gray-500">
                <input
                  type="checkbox"
                  checked={showHistoricalApps}
                  onChange={(event) => {
                    setShowHistoricalApps(event.target.checked);
                    setAppsPagination({ filterKey: "", page: 1 });
                  }}
                  className="rounded border-gray-300 text-orange-600 focus:ring-orange-500"
                />
                Show historical ({historicalAppCount})
                <InfoPopover
                  className="ml-0.5"
                  label="About historical apps"
                  text="Apps present in billing history but no longer registered or accessible in the workspace."
                  stopClick
                />
              </label>
            )}
            {/* Workspace filter */}
            {availableWorkspaces.length > 1 && (
              <div ref={wsFilterRef} className="relative" data-ws-filter-dropdown>
                <button
                  onClick={() => { setWsFilterOpen(!wsFilterOpen); setWsFilterSearch(""); }}
                  className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium transition-colors ${selectedWorkspaces.length > 0 && selectedWorkspaces.length < availableWorkspaces.length ? "border-lava text-lava" : "border-gray-300 text-gray-700 hover:bg-gray-50"}`}
                >
                  {selectedWorkspaces.length === 0 || selectedWorkspaces.length === availableWorkspaces.length
                    ? "Workspace"
                    : selectedWorkspaces.length === 1
                    ? resolveWsName(selectedWorkspaces[0])
                    : `${selectedWorkspaces.length} Workspaces`}
                  <svg className={`h-3 w-3 transition-transform ${wsFilterOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" /></svg>
                </button>
                {wsFilterOpen && (
                  <FloatingMenu anchorRef={wsFilterRef} className="w-72 rounded-lg border border-gray-200 bg-white shadow-lg">
                    <div className="p-2">
                      <input
                        type="text"
                        value={wsFilterSearch}
                        onChange={(e) => setWsFilterSearch(e.target.value)}
                        placeholder="Search workspaces..."
                        className="w-full rounded-md border border-gray-300 px-3 py-1.5 text-sm focus:border-orange-500 focus:outline-none focus:ring-1 focus:ring-orange-500"
                        autoFocus
                      />
                    </div>
                    <div>
                      <div className="sticky top-0 flex items-center justify-between border-b border-gray-100 bg-white px-3 py-2">
                        <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Workspaces</span>
                        <div className="flex items-center gap-2 text-xs">
                          <button onClick={(e) => { e.stopPropagation(); setSelectedWorkspaces(availableWorkspaces.map(ws => ws.id)); }} className="text-gray-500 hover:text-gray-800">All</button>
                          <span className="text-gray-300">·</span>
                          <button onClick={(e) => { e.stopPropagation(); setSelectedWorkspaces([]); }} className="text-gray-500 hover:text-gray-800">Clear</button>
                        </div>
                      </div>
                      {(() => {
                        const filtered = availableWorkspaces.filter(ws => !wsFilterSearch || ws.name.toLowerCase().includes(wsFilterSearch.toLowerCase()));
                        if (filtered.length === 0) {
                          return <div className="px-3 py-2 text-sm text-gray-500">No matching workspaces</div>;
                        }
                        return (
                          <VirtualizedList
                            items={filtered}
                            itemHeight={40}
                            maxHeight={240}
                            getKey={(ws) => ws.id}
                            renderItem={(ws) => (
                              <button
                                onClick={() => handleToggleWorkspace(ws.id)}
                                className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-sm hover:bg-gray-50"
                              >
                                <div className={`flex h-4 w-4 shrink-0 items-center justify-center rounded border ${selectedWorkspaces.includes(ws.id) ? "border-orange-500 bg-orange-500" : "border-gray-300"}`}>
                                  {selectedWorkspaces.includes(ws.id) && (
                                    <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" />
                                    </svg>
                                  )}
                                </div>
                                <span className="truncate text-xs text-gray-700">{ws.name}</span>
                                {ws.historical && (
                                  <span className="ml-auto shrink-0 rounded border border-gray-200 bg-gray-100 px-1.5 py-0.5 text-[9px] font-medium text-gray-600" title="This workspace no longer exists in the account. Its data is historical.">
                                    historical
                                  </span>
                                )}
                              </button>
                            )}
                          />
                        );
                      })()}
                    </div>
                  </FloatingMenu>
                )}
              </div>
            )}
            {/* Search */}
            <div className="relative">
              <svg className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-500" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Search apps..."
                className="w-44 rounded-full border border-gray-200 bg-white py-1.5 pl-9 pr-4 text-xs placeholder:text-gray-400 focus:border-lava focus:outline-none focus:ring-1 focus:ring-lava"
              />
              {searchQuery && (
                <button
                  onClick={() => setSearchQuery("")}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-600"
                >
                  <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              )}
            </div>
            {selectedApp && (
              <button
                onClick={() => setSelectedAppId(null)}
                className="text-xs text-gray-500 hover:text-gray-700"
              >
                ← Back to grid
              </button>
            )}
          </div>
        </div>
        {/* Workspace filter pills: only shown for partial selections (all-selected is the default).
            Hard cap and scroll to prevent unbounded pill sprawl with 50+ workspaces. */}
        {selectedWorkspaces.length > 0 && selectedWorkspaces.length < availableWorkspaces.length && (
          selectedWorkspaces.length > 12 ? (
            <div className="mb-4 flex items-center gap-2 text-xs text-gray-600">
              <span className="rounded-full bg-orange-50 px-2.5 py-0.5 font-medium text-lava">
                {selectedWorkspaces.length} of {availableWorkspaces.length} workspaces selected
              </span>
              <span className="inline-flex items-center gap-2">
                <button onClick={() => setSelectedWorkspaces([...availableWorkspaces.map(w => w.id)])} className="text-gray-500 hover:text-gray-700 underline">All</button>
                <span className="text-gray-300">·</span>
                <button onClick={() => setSelectedWorkspaces([])} className="text-gray-500 hover:text-gray-700 underline">Clear</button>
              </span>
            </div>
          ) : (
            <div className="mb-4 flex max-h-20 flex-wrap items-center gap-1.5 overflow-y-auto pr-1">
              {selectedWorkspaces.map(wsId => (
                <span key={wsId} className="inline-flex max-w-[220px] items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-medium text-white" style={{ backgroundColor: C.lava }}>
                  <span className="truncate">{resolveWsName(wsId)}</span>
                  <button onClick={() => handleToggleWorkspace(wsId)} className="ml-0.5 inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center rounded-full hover:bg-white/20">
                    <svg className="h-2.5 w-2.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                    </svg>
                  </button>
                </span>
              ))}
              <span className="inline-flex items-center gap-2 text-xs">
                <button onClick={() => setSelectedWorkspaces([...availableWorkspaces.map(w => w.id)])} className="text-gray-500 hover:text-gray-700">All</button>
                <span className="text-gray-300">·</span>
                <button onClick={() => setSelectedWorkspaces([])} className="text-gray-500 hover:text-gray-700">Clear</button>
              </span>
            </div>
          )
        )}
        {selectedUnavailable && (
          <div role="status" className="mb-4 flex items-center justify-between gap-3 rounded-md border border-yellow-200 bg-yellow-50 px-3 py-2 text-xs text-yellow-800">
            <span>The selected app is no longer available in the current date, workspace, or source scope.</span>
            <button type="button" onClick={() => setSelectedAppId(null)} className="font-semibold underline">Dismiss</button>
          </div>
        )}

        {/* Detail panel: shown when an app is selected */}
        {selectedApp && (
          <div className="mb-6 animate-fade-in rounded-lg border border-gray-200 bg-gray-50 p-5">
            <div className="flex items-start justify-between">
              <div className="flex items-center gap-3">
                <AppThumbnail
                  key={`${selectedApp.app_id}:${selectedApp.metadata?.thumbnail_url ?? ""}`}
                  app={selectedApp}
                  size={56}
                />
                <div className="min-w-0">
                  <h4 className="text-base font-semibold text-gray-900">{selectedApp.app_name}</h4>
                  {selectedApp.app_name !== selectedApp.app_id && (
                    <p className="text-[10px] text-gray-500 font-mono">{selectedApp.app_id}</p>
                  )}
                  <div className="mt-1 flex flex-wrap items-center gap-1.5">
                    {selectedMetadata?.app_status && (
                      <StateBadge label="App" state={selectedMetadata.app_status.state} />
                    )}
                    {selectedMetadata?.compute_status && (
                      <StateBadge label="Compute" state={selectedMetadata.compute_status.state} />
                    )}
                    {selectedDeployment && (
                      <StateBadge label={selectedDeployment.pending ? "Pending deploy" : "Deploy"} state={selectedDeployment.state} />
                    )}
                  </div>
                  <div className="mt-1.5 flex flex-wrap items-center gap-3">
                    {liveEndpoint(selectedApp) && (
                      <a
                        href={liveEndpoint(selectedApp)!}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-xs text-lava hover:underline"
                      >
                        Live App Endpoint →
                      </a>
                    )}
                    {deploymentUrl(selectedApp) && (
                      <a
                        href={deploymentUrl(selectedApp)!}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-xs text-blue-600 hover:underline"
                      >
                        Manage App →
                      </a>
                    )}
                    {selectedMetadata?.git?.repository_url && (
                      <a
                        href={selectedMetadata.git.repository_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-xs text-blue-600 hover:underline"
                      >
                        Source Repository →
                      </a>
                    )}
                  </div>
                </div>
              </div>
              <button
                onClick={() => setSelectedAppId(null)}
                className="rounded p-1 text-gray-500 hover:bg-gray-200 hover:text-gray-600"
              >
                <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>

            {selectedMetadata?.description && (
              <p className="mt-3 max-w-4xl text-sm leading-relaxed text-gray-700">
                {selectedMetadata.description}
              </p>
            )}

            {selectedMetadata?.availability && selectedMetadata.availability !== "available" && (
              <div className="mt-3 rounded-md border border-yellow-200 bg-yellow-50 px-3 py-2 text-xs text-yellow-800">
                {selectedMetadata.availability === "partial"
                  ? "Some current app metadata is unavailable. Billing history and the metadata returned by the app registry are still shown."
                  : "This app is no longer available in the Apps registry. Billing history is preserved, but live app metadata and links may be unavailable."}
              </div>
            )}

            <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
              <div className="rounded-md bg-white p-3 border border-gray-200">
                <p className="text-xs text-gray-500">Spend</p>
                <p className="text-lg font-semibold text-gray-900">{formatCurrency(selectedApp.total_spend)}</p>
              </div>
              <div className="rounded-md bg-white p-3 border border-gray-200">
                <p className="text-xs text-gray-500">DBUs</p>
                <p className="text-lg font-semibold text-gray-900">{formatNumber(selectedApp.total_dbus)}</p>
              </div>
              <div className="rounded-md bg-white p-3 border border-gray-200">
                <p className="text-xs text-gray-500">Days Active</p>
                <p className="text-lg font-semibold text-gray-900">{selectedApp.days_active}</p>
              </div>
              <div className="rounded-md bg-white p-3 border border-gray-200">
                <p className="text-xs text-gray-500">Last Usage</p>
                <p className="text-lg font-semibold text-gray-900">
                  {selectedApp.last_usage_date
                    ? new Date(selectedApp.last_usage_date).toLocaleDateString("en-US", { month: "short", day: "numeric" })
                    : "N/A"}
                </p>
              </div>
            </div>

            {(selectedMetadata || selectedWorkspaceLabels.length > 0) && (
              <div className="mt-4 grid gap-4 lg:grid-cols-2">
                <div className="rounded-md border border-gray-200 bg-white p-4">
                  <h5 className="mb-3 text-xs font-semibold uppercase tracking-wide text-gray-500">App & Compute</h5>
                  <dl className="grid grid-cols-2 gap-x-4 gap-y-3">
                    <MetadataValue label="Owner / creator" value={selectedMetadata?.creator ? formatIdentity(selectedMetadata.creator) : null} />
                    <MetadataValue label="Last updated by" value={selectedMetadata?.updater ? formatIdentity(selectedMetadata.updater) : null} />
                    <MetadataValue label="Compute size" value={selectedMetadata?.compute_size ? displayState(selectedMetadata.compute_size) : null} />
                    <MetadataValue
                      label="Active instances"
                      value={selectedMetadata?.compute_status?.instances ?? selectedMetadata?.app_status?.instances}
                    />
                    <MetadataValue label="App updated" value={formatTimestamp(selectedMetadata?.update_time)} />
                    <MetadataValue label="Workspace" value={selectedWorkspaceLabels.join(", ") || null} />
                  </dl>
                  {(selectedMetadata?.compute_status?.message || selectedMetadata?.app_status?.message) && (
                    <p className="mt-3 border-t border-gray-100 pt-2 text-[11px] text-gray-600">
                      {selectedMetadata.compute_status?.message || selectedMetadata.app_status?.message}
                    </p>
                  )}
                </div>

                <div className="rounded-md border border-gray-200 bg-white p-4">
                  <h5 className="mb-3 text-xs font-semibold uppercase tracking-wide text-gray-500">Deployment & Source</h5>
                  <dl className="grid grid-cols-2 gap-x-4 gap-y-3">
                    <MetadataValue label="Deployed by" value={selectedDeployment?.creator ? formatIdentity(selectedDeployment.creator) : null} />
                    <MetadataValue label="Deployment updated" value={formatTimestamp(selectedDeployment?.update_time)} />
                    <MetadataValue label="Deployment mode" value={selectedDeployment?.mode ? displayState(selectedDeployment.mode) : null} />
                    <MetadataValue label="Git reference" value={selectedMetadata?.git?.branch || selectedMetadata?.git?.tag || null} />
                    <MetadataValue label="Commit" value={selectedMetadata?.git?.commit ? selectedMetadata.git.commit.slice(0, 12) : null} />
                    <MetadataValue label="Space" value={selectedMetadata?.space || null} />
                  </dl>
                  {selectedMetadata?.source_code_path && (
                    <div className="mt-3 border-t border-gray-100 pt-2">
                      <p className="text-[10px] font-semibold uppercase tracking-wide text-gray-500">Source path</p>
                      <p className="mt-0.5 break-all font-mono text-[11px] text-gray-700">{selectedMetadata.source_code_path}</p>
                    </div>
                  )}
                  {selectedDeployment?.message && (
                    <p className="mt-2 text-[11px] text-gray-600">{selectedDeployment.message}</p>
                  )}
                </div>
              </div>
            )}

            {selectedBindings.length > 0 && (
              <div className="mt-4">
                <h5 className="mb-2 text-xs font-semibold uppercase tracking-wide text-gray-500">Resource Bindings</h5>
                <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                  {selectedBindings.map((binding, index) => (
                    <div key={`${binding.type}-${binding.name}-${index}`} className="rounded-md border border-gray-200 bg-white px-3 py-2">
                      <div className="flex items-center justify-between gap-2">
                        <span className="truncate text-xs font-medium text-gray-800">{binding.name || "Unnamed resource"}</span>
                        <span className="shrink-0 rounded bg-gray-100 px-1.5 py-0.5 text-[9px] font-medium text-gray-600">
                          {displayState(binding.type)}
                        </span>
                      </div>
                      {binding.description && (
                        <p className="mt-1 truncate text-[10px] text-gray-500" title={binding.description}>{binding.description}</p>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* SKU Cost Breakdown */}
            {selectedApp.sku_breakdown && selectedApp.sku_breakdown.length > 0 && (
              <div className="mt-4">
                <h5 className="mb-2 text-xs font-semibold uppercase tracking-wide text-gray-500">Cost Breakdown by SKU</h5>
                <div className="space-y-2">
                  {selectedApp.sku_breakdown.map((sku) => (
                    <div key={sku.sku_name} className="rounded-md border border-gray-200 bg-white px-3 py-2">
                      <div className="flex items-center justify-between">
                        <span className="text-xs font-medium text-gray-800">{sku.sku_name}</span>
                        <span className="text-xs font-semibold text-gray-900">{formatCurrency(sku.total_spend)}</span>
                      </div>
                      <div className="mt-1 flex items-center gap-2">
                        <div className="h-1.5 flex-1 rounded-full bg-gray-100">
                          <div
                            className="h-1.5 rounded-full"
                            style={{ width: `${Math.min(sku.percentage, 100)}%`, backgroundColor: C.lava }}
                          />
                        </div>
                        <span className="text-[10px] text-gray-500">{sku.percentage.toFixed(1)}%</span>
                      </div>
                      <p className="mt-0.5 text-[10px] text-gray-500">{formatNumber(sku.total_dbus)} DBUs</p>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className="mt-3 flex items-center gap-4 text-xs text-gray-500">
              <span>{selectedApp.percentage.toFixed(1)}% of total spend</span>
              <span>Workspace count: {selectedApp.workspace_count}</span>
              {(!selectedApp.is_registered || selectedApp.status === "historical") && (
                <span className="rounded border border-gray-200 bg-gray-100 px-1.5 py-0.5 text-gray-600">Historical billing record: app deleted or inaccessible</span>
              )}
            </div>
          </div>
        )}

        {/* Tile grid */}
        {filteredApps.length > 0 ? (
          <div className="grid grid-cols-3 gap-3 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-8">
            {paginatedApps.map((app) => {
              const isSelected = selectedApp?.app_id === app.app_id;
              const isResolved = app.app_name !== app.app_id;

              // Scale icon size linearly based on spend without overpowering the tile.
              const maxSpend = filteredApps[0]?.total_spend || 1;
              const minSize = 28;
              const maxSize = 48;
              const ratio = maxSpend > 0 ? app.total_spend / maxSpend : 0;
              const iconSize = Math.round(minSize + ratio * (maxSize - minSize));

              return (
                <button
                  key={app.app_id}
                  onClick={() => {
                    setSelectedAppId(isSelected ? null : app.app_id);
                  }}
                  className={`group relative flex flex-col items-center justify-center rounded-lg border-2 p-2.5 transition-all hover:shadow-md ${
                    isSelected
                      ? "border-lava shadow-md scale-105"
                      : "border-gray-200 hover:border-gray-300"
                  }`}
                  title={`${app.app_name}${isResolved ? ` (${app.app_id})` : ""}\n${formatCurrency(app.total_spend)} · ${app.days_active}d active`}
                >
                  {(!app.is_registered || app.status === "historical") && (
                    <span className="absolute right-1.5 top-1.5 rounded border border-gray-200 bg-gray-100 px-1.5 py-0.5 text-[8px] font-semibold uppercase tracking-wide text-gray-600">
                      Historical
                    </span>
                  )}
                  <div className="transition-transform group-hover:scale-110">
                    <AppThumbnail
                      key={`${app.app_id}:${app.metadata?.thumbnail_url ?? ""}`}
                      app={app}
                      size={iconSize}
                    />
                  </div>
                  {/* App name */}
                  <span className="mt-1.5 w-full truncate text-center text-[10px] font-medium text-gray-700">
                    {app.app_name}
                  </span>
                  {/* Spend label */}
                  <span className="text-[9px] text-gray-500">
                    {formatCurrency(app.total_spend)}
                  </span>
                </button>
              );
            })}
          </div>
        ) : searchQuery ? (
          <div className="flex h-32 items-center justify-center text-gray-500">
            No apps matching "{searchQuery}"
          </div>
        ) : (
          <div className="flex h-32 items-center justify-center text-gray-500">
            No app spend found for the current filters
          </div>
        )}

        {/* Pagination */}
        {filteredApps.length > 0 && (
          <div className="mt-4 flex items-center justify-between">
            <span className="text-xs text-gray-500">
              {filteredApps.length} app{filteredApps.length !== 1 ? "s" : ""}
              {searchQuery ? ` matching "${searchQuery}"` : ""}
            </span>
            {totalAppsPages > 1 && (
              <div className="flex items-center gap-1">
                <button
                  onClick={() => setAppsPagination({
                    filterKey: appsFilterKey,
                    page: Math.max(1, effectiveAppsPage - 1),
                  })}
                  disabled={effectiveAppsPage === 1}
                  className="rounded border border-gray-200 px-2.5 py-1 text-xs text-gray-600 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  ‹ Prev
                </button>
                <span className="px-2 text-xs text-gray-500">{effectiveAppsPage} / {totalAppsPages}</span>
                <button
                  onClick={() => setAppsPagination({
                    filterKey: appsFilterKey,
                    page: Math.min(totalAppsPages, effectiveAppsPage + 1),
                  })}
                  disabled={effectiveAppsPage >= totalAppsPages}
                  className="rounded border border-gray-200 px-2.5 py-1 text-xs text-gray-600 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  Next ›
                </button>
              </div>
            )}
          </div>
        )}
      </div>
      )}

      {/* Connected Resources & Artifacts */}
      {data?.connected_artifacts && data.connected_artifacts.length > 0 && (() => {
        const artifactTypes = [...new Set(data.connected_artifacts.map((a: AppsConnectedArtifact) => a.artifact_type))].filter((t: string) => t && t !== 'UNKNOWN' && t !== 'Unknown').sort();
        const allAppNames = [...new Set(data.connected_artifacts.map((a: AppsConnectedArtifact) => a.app_name).filter(Boolean))].sort() as string[];
        let filteredArtifacts = artifactTypeFilters.length === 0
          ? data.connected_artifacts
          : data.connected_artifacts.filter((a: AppsConnectedArtifact) => artifactTypeFilters.includes(a.artifact_type));
        if (artifactAppFilter.length > 0) {
          filteredArtifacts = filteredArtifacts.filter((a: AppsConnectedArtifact) => artifactAppFilter.includes(a.app_name || ''));
        }
        if (artifactSearch) {
          const q = artifactSearch.toLowerCase();
          filteredArtifacts = filteredArtifacts.filter((a: AppsConnectedArtifact) =>
            a.app_name?.toLowerCase().includes(q) ||
            a.artifact_name?.toLowerCase().includes(q) ||
            a.artifact_type?.toLowerCase().includes(q) ||
            a.artifact_description?.toLowerCase().includes(q)
          );
        }
        const filteredAppNames = allAppNames.filter(n => !artifactAppFilterSearch || n.toLowerCase().includes(artifactAppFilterSearch.toLowerCase()));
        const totalArtifactPages = Math.ceil(filteredArtifacts.length / artifactsPerPage);
        const safePage = Math.min(artifactPage, totalArtifactPages || 1);
        const paginatedArtifacts = filteredArtifacts.slice((safePage - 1) * artifactsPerPage, safePage * artifactsPerPage);

        return (
          <div className="rounded-lg bg-white p-6 border " style={{ borderColor: C.hairline }}>
            {/* Single toolbar row: title · filters · search */}
            <div className="mb-4 flex flex-wrap items-center gap-2">
              <div className="mr-2 shrink-0">
                <h3 className="text-lg font-semibold text-gray-900">Connected Resources</h3>
              </div>

              <div className="ml-auto flex shrink-0 items-center gap-2">

              {/* App filter dropdown */}
              <div ref={artifactAppFilterRef} className="relative" data-artifact-app-dropdown>
                <button
                  onClick={() => { setArtifactAppFilterOpen(v => !v); setArtifactAppFilterSearch(""); }}
                  className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium transition-colors ${artifactAppFilter.length > 0 && artifactAppFilter.length < allAppNames.length ? "border-lava text-lava" : "border-gray-300 bg-white text-gray-700 hover:border-gray-400"}`}
                >
                  App
                  <svg className={`h-3 w-3 transition-transform ${artifactAppFilterOpen ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
                {artifactAppFilterOpen && (
                  <FloatingMenu anchorRef={artifactAppFilterRef} className="w-64 max-h-72 overflow-y-auto rounded-lg border border-gray-200 bg-white shadow-lg">
                    <div className="sticky top-0 flex items-center justify-between border-b border-gray-100 bg-white px-3 py-2">
                      <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">App</span>
                      <div className="flex items-center gap-2 text-xs">
                        <button onClick={(e) => { e.stopPropagation(); setArtifactAppFilter([...allAppNames]); setArtifactPage(1); }} className="text-gray-500 hover:text-gray-800">All</button>
                        <span className="text-gray-300">·</span>
                        <button onClick={(e) => { e.stopPropagation(); setArtifactAppFilter([]); setArtifactPage(1); }} className="text-gray-500 hover:text-gray-800">Clear</button>
                      </div>
                    </div>
                    <div className="p-2">
                      <input
                        autoFocus
                        type="text"
                        value={artifactAppFilterSearch}
                        onChange={e => setArtifactAppFilterSearch(e.target.value)}
                        placeholder="Search apps..."
                        className="w-full rounded border border-gray-200 px-2 py-1.5 text-xs focus:border-lava focus:outline-none focus:ring-1 focus:ring-lava"
                      />
                    </div>
                    {filteredAppNames.length === 0 ? (
                      <p className="px-3 py-2 text-xs text-gray-500">No apps found</p>
                    ) : (
                      <VirtualizedList
                        items={filteredAppNames}
                        itemHeight={36}
                        maxHeight={240}
                        getKey={(n) => n}
                        renderItem={(name) => (
                          <button
                            onClick={() => {
                              setArtifactAppFilter(prev => prev.includes(name) ? prev.filter(x => x !== name) : [...prev, name]);
                              setArtifactPage(1);
                            }}
                            className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-xs hover:bg-gray-50"
                          >
                            <div className={`flex h-4 w-4 shrink-0 items-center justify-center rounded border ${artifactAppFilter.includes(name) ? "border-orange-500 bg-orange-500" : "border-gray-300"}`}>
                              {artifactAppFilter.includes(name) && <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>}
                            </div>
                            <span className="truncate text-gray-700">{name}</span>
                          </button>
                        )}
                      />
                    )}
                  </FloatingMenu>
                )}
              </div>

              {/* Resource type multi-select dropdown */}
              <div ref={artifactTypeDropdownRef} className="relative" data-artifact-type-dropdown>
                <button
                  onClick={() => setArtifactTypeDropdownOpen(v => !v)}
                  className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium transition-colors ${artifactTypeFilters.length > 0 && artifactTypeFilters.length < artifactTypes.length ? "border-lava text-lava" : "border-gray-300 bg-white text-gray-700 hover:border-gray-400"}`}
                >
                  {artifactTypeFilters.length === 0
                    ? 'Resource type'
                    : artifactTypeFilters.length === 1
                    ? artifactTypeFilters[0].replace(/_/g, ' ')
                    : artifactTypeFilters.length === artifactTypes.length
                    ? 'Resource type'
                    : `${artifactTypeFilters.length} types`}
                  <svg className={`h-3 w-3 transition-transform ${artifactTypeDropdownOpen ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
                {artifactTypeDropdownOpen && (
                  <FloatingMenu anchorRef={artifactTypeDropdownRef} className="w-56 max-h-72 overflow-y-auto rounded-lg border border-gray-200 bg-white shadow-lg">
                    <div className="sticky top-0 flex items-center justify-between border-b border-gray-100 bg-white px-3 py-2">
                      <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-500">Types</span>
                      <div className="flex items-center gap-2 text-xs">
                        <button onClick={(e) => { e.stopPropagation(); setArtifactTypeFilters([...(artifactTypes as string[])]); setArtifactPage(1); }} className="text-gray-500 hover:text-gray-800">All</button>
                        <span className="text-gray-300">·</span>
                        <button onClick={(e) => { e.stopPropagation(); setArtifactTypeFilters([]); setArtifactPage(1); }} className="text-gray-500 hover:text-gray-800">Clear</button>
                      </div>
                    </div>
                    {artifactTypes.map((type: string) => {
                      const count = data.connected_artifacts.filter((a: AppsConnectedArtifact) => a.artifact_type === type).length;
                      const checked = artifactTypeFilters.includes(type);
                      return (
                        <button
                          key={type}
                          onClick={() => {
                            setArtifactTypeFilters(prev => prev.includes(type) ? prev.filter(t => t !== type) : [...prev, type]);
                            setArtifactPage(1);
                          }}
                          className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-xs hover:bg-gray-50"
                        >
                          <div className={`flex h-4 w-4 shrink-0 items-center justify-center rounded border ${checked ? "border-orange-500 bg-orange-500" : "border-gray-300"}`}>
                            {checked && <svg className="h-3 w-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>}
                          </div>
                          <span className="flex-1 truncate text-gray-700">{type.replace(/_/g, ' ')}</span>
                          <span className="text-gray-500">({count})</span>
                        </button>
                      );
                    })}
                  </FloatingMenu>
                )}
              </div>

              {/* Search */}
              <div className="relative shrink-0">
                <svg className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-500" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                </svg>
                <input
                  type="text"
                  value={artifactSearch}
                  onChange={(e) => { setArtifactSearch(e.target.value); setArtifactPage(1); }}
                  placeholder="Search resources..."
                  className="w-44 rounded-full border border-gray-200 bg-white py-1.5 pl-9 pr-4 text-xs placeholder:text-gray-400 focus:border-lava focus:outline-none focus:ring-1 focus:ring-lava"
                />
              </div>

              </div>{/* end ml-auto group */}
            </div>

            <div className="overflow-x-auto">
              <table className="min-w-full">
                <thead className="bg-gray-50">
                  <tr className="border-b border-gray-200">
                    <th className="px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Resource</th>
                    <th className="px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Resource Name</th>
                    <th className="px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Resource Type</th>
                    <th className="px-3 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Details</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {paginatedArtifacts.length === 0 && (
                    <tr>
                      <td colSpan={4} className="px-4 py-10 text-center text-sm text-gray-500">
                        No connected resources match the current filters.
                      </td>
                    </tr>
                  )}
                  {paginatedArtifacts.map((artifact: AppsConnectedArtifact, idx: number) => {
                    const artifactUrl = hostBase ? (
                      artifact.artifact_type === 'SERVING_ENDPOINT' ? `${hostBase}/ml/endpoints/${artifact.artifact_name}` :
                      artifact.artifact_type === 'SQL_WAREHOUSE' ? `${hostBase}/sql/warehouses/${artifact.artifact_name}` :
                      artifact.artifact_type === 'JOB' ? `${hostBase}/jobs/${artifact.artifact_name}` :
                      artifact.artifact_type === 'SECRET' ? `${hostBase}/secrets/scopes` :
                      artifact.artifact_type === 'SERVICE_PRINCIPAL' ? servicePrincipalAdminUrl(hostBase, artifact.artifact_id) :
                      null
                    ) : null;

                    const na = (v: string | null | undefined) => (!v || v === 'Unknown' || v === 'UNKNOWN') ? 'N/A' : v;
                    const displayType = na(artifact.artifact_type);
                    const appBackendUrl = hostBase && artifact.app_name
                      ? `${hostBase}/apps-v2/app/${encodeURIComponent(artifact.app_name)}/overview`
                      : null;

                    const isSP = artifact.artifact_type === 'SERVICE_PRINCIPAL';
                    const displayName = isSP ? formatIdentity(artifact.artifact_name) : na(artifact.artifact_name);

                    return (
                      <tr key={`${artifact.app_id}-${artifact.artifact_name}-${idx}`} className="hover:bg-gray-50">
                        <td className="whitespace-nowrap px-3 py-3 text-sm font-medium">
                          {appBackendUrl ? (
                            <a href={appBackendUrl} target="_blank" rel="noopener noreferrer" className="group flex items-center gap-1 text-lava hover:text-lava-hover">
                              <span>{na(artifact.app_name)}</span>
                              <svg className="h-3 w-3 shrink-0 opacity-0 transition-opacity group-hover:opacity-100" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                              </svg>
                            </a>
                          ) : (
                            <span className="text-gray-900">{na(artifact.app_name)}</span>
                          )}
                        </td>
                        <td className="px-3 py-3 text-sm">
                          <div className="flex flex-col gap-0.5">
                            {artifactUrl ? (
                              <a href={artifactUrl} target="_blank" rel="noopener noreferrer" className="group flex items-center gap-1 font-medium text-lava hover:text-lava-hover">
                                <span>{displayName}</span>
                                <svg
                                  data-testid={isSP ? "service-principal-external-link" : undefined}
                                  aria-hidden="true"
                                  className="h-3 w-3 shrink-0 opacity-60 transition-opacity group-hover:opacity-100"
                                  fill="none"
                                  viewBox="0 0 24 24"
                                  stroke="currentColor"
                                >
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                                </svg>
                              </a>
                            ) : (
                              <span className="text-gray-700">{displayName}</span>
                            )}
                          </div>
                        </td>
                        <td className="px-3 py-3">
                          <span className={`inline-flex items-center whitespace-nowrap rounded-full px-2 py-0.5 text-xs font-medium ${
                            artifact.artifact_type === 'SERVING_ENDPOINT' ? 'bg-blue-50 text-blue-700' :
                            artifact.artifact_type === 'SQL_WAREHOUSE' ? 'bg-blue-100 text-blue-700' :
                            artifact.artifact_type === 'SECRET' ? 'bg-yellow-100 text-yellow-700' :
                            artifact.artifact_type === 'JOB' ? 'bg-green-100 text-green-700' :
                            artifact.artifact_type === 'SERVICE_PRINCIPAL' ? 'bg-orange-100 text-orange-700' :
                            artifact.artifact_type === 'LAKEBASE' ? 'bg-cyan-50 text-cyan-700' :
                            'bg-gray-100 text-gray-700'
                          }`}>
                            {displayType.replace(/_/g, ' ')}
                          </span>
                        </td>
                        <td className="px-3 py-3 text-sm text-gray-500">{na(artifact.artifact_description)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {/* Pagination */}
            {totalArtifactPages > 1 && (
              <div className="mt-4 flex items-center justify-between border-t border-gray-200 pt-4">
                <p className="text-sm text-gray-500">
                  Showing {(safePage - 1) * artifactsPerPage + 1} to {Math.min(safePage * artifactsPerPage, filteredArtifacts.length)} of {filteredArtifacts.length}
                </p>
                <div className="flex gap-2">
                  <button
                    onClick={() => setArtifactPage(Math.max(1, safePage - 1))}
                    disabled={safePage === 1}
                    className="rounded border border-gray-300 px-3 py-1 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    Previous
                  </button>
                  {Array.from({ length: totalArtifactPages }, (_, i) => i + 1)
                    .filter((p) => p === 1 || p === totalArtifactPages || (p >= safePage - 1 && p <= safePage + 1))
                    .map((p, idx, arr) => {
                      const prev = arr[idx - 1];
                      const showEllipsis = prev && p - prev > 1;
                      return (
                        <span key={p} className="flex items-center">
                          {showEllipsis && <span className="px-2 py-1 text-gray-500">...</span>}
                          <button
                            onClick={() => setArtifactPage(p)}
                            className={`rounded px-3 py-1 text-sm font-medium ${
                              safePage === p ? 'text-white' : 'border border-gray-300 text-gray-700 hover:bg-gray-50'
                            }`}
                            style={safePage === p ? { backgroundColor: C.lava } : undefined}
                          >
                            {p}
                          </button>
                        </span>
                      );
                    })}
                  <button
                    onClick={() => setArtifactPage(Math.min(totalArtifactPages, safePage + 1))}
                    disabled={safePage === totalArtifactPages}
                    className="rounded border border-gray-300 px-3 py-1 text-sm font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    Next
                  </button>
                </div>
              </div>
            )}
          </div>
        );
      })()}
      {data?.connected_resources_available !== false
        && (!data?.connected_artifacts || data.connected_artifacts.length === 0) && (
        <div className="rounded-lg border bg-white p-6" style={{ borderColor: C.hairline }}>
          <h3 className="text-lg font-semibold text-gray-900">Connected Resources</h3>
          <p className="mt-3 text-sm text-gray-500">
            {data?.partial_reasons?.app_details || data?.partial_reasons?.app_registry
              ? "Connected resource metadata is still loading. Refresh this tab shortly."
              : "No connected resources were reported by the Databricks Apps API."}
          </p>
        </div>
      )}

    </div>
  );
}
