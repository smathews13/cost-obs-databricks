import React, { useState, useEffect, useMemo, useRef, useCallback, useId, lazy, Suspense } from "react";
import { QueryClient, QueryClientProvider, useQuery, useQueryClient } from "@tanstack/react-query";
import { TabRefreshRegion } from "@/components/TabRefreshRegion";
import { SetupWizard } from "@/components/SetupWizard";
import { SummaryCards } from "@/components/SummaryCards";
import { SpendChart } from "@/components/SpendChart";
import { ProductBreakdown } from "@/components/ProductBreakdown";
import { WorkspaceTable } from "@/components/WorkspaceTable";
import { PipelineObjectsTable } from "@/components/PipelineObjectsTable";
import { DateRangePicker } from "@/components/DateRangePicker";
import { WorkspaceFilter } from "@/components/WorkspaceFilter";
import { SourceLabelFilter } from "@/components/SourceLabelFilter";
import { SKUBreakdown } from "@/components/SKUBreakdown";
import { ExportDialog, type ExportSections, type ExportFormat } from "@/components/ExportDialog";
import { SettingsDialog } from "@/components/SettingsDialog";
import { PricingProvider, usePricing } from "@/context/PricingContext";
import { SpNameMapContext } from "@/utils/identity";
import { Footer } from "@/components/Footer";
import { UserMenu } from "@/components/UserMenu";
import { DeploymentBadgeFromApi } from "@/components/DeploymentBadge";
import { applyInfraPricing } from "@/utils/cloudCosts";
import {
  WarehouseGuidanceBanner,
  WarehouseHealthCheckBanner,
} from "@/components/WarehouseGuidanceBanner";
import { Bot, Settings } from "lucide-react";

// Retry a dynamic import on failure. First retry handles transient network blips.
// If the second attempt also fails (stale deployment: browser has old index.html with
// outdated chunk hashes that 404 after a redeploy), force a hard page reload to pick up
// the new assets. A sessionStorage flag prevents reload loops if assets are genuinely broken.
function lazyWithRetry<T>(factory: () => Promise<T>): Promise<T> {
  return factory().catch(() =>
    factory().catch((err) => {
      if (!sessionStorage.getItem("_chunk_reload")) {
        sessionStorage.setItem("_chunk_reload", "1");
        window.location.reload();
      }
      throw err;
    })
  );
}

// Lazy-loaded tab views: chunks download on first render
const InteractiveBreakdown = lazy(() => lazyWithRetry(() => import("@/components/InteractiveBreakdown").then(m => ({ default: m.InteractiveBreakdown }))));
const CloudCostsView = lazy(() => lazyWithRetry(() => import("@/components/CloudCostsView").then(m => ({ default: m.CloudCostsView }))));
const PlatformKPIsView = lazy(() => lazyWithRetry(() => import("@/components/PlatformKPIsView").then(m => ({ default: m.PlatformKPIsView }))));
const AIMLCostCenter = lazy(() => lazyWithRetry(() => import("@/components/AIMLCostCenter").then(m => ({ default: m.AIMLCostCenter }))));
const AppsCostCenter = lazy(() => lazyWithRetry(() => import("@/components/AppsCostCenter").then(m => ({ default: m.AppsCostCenter }))));
const TaggingHub = lazy(() => lazyWithRetry(() => import("@/components/TaggingHub").then(m => ({ default: m.TaggingHub }))));
const SQLWarehousing360 = lazy(() => lazyWithRetry(() => import("@/components/SQLWarehousing360").then(m => ({ default: m.SQLWarehousing360 }))));
const WarehouseRightsizingView = lazy(() => lazyWithRetry(() => import("@/components/SQLWarehousing360").then(m => ({ default: m.WarehouseRightsizingView }))));
const WarehouseIdleTimeView = lazy(() => lazyWithRetry(() => import("@/components/SQLWarehousing360").then(m => ({ default: m.WarehouseIdleTimeView }))));
const OptimizeMethodologyPanel = lazy(() => lazyWithRetry(() => import("@/components/SQLWarehousing360").then(m => ({ default: m.OptimizeMethodologyPanel }))));
const UsersGroups = lazy(() => lazyWithRetry(() => import("@/pages/UsersGroups")));

import {
  useAccountInfo,
  useAWSActualCosts,
  useAzureActualCosts,
  useGCPActualCosts,
  useDashboardBundleFast,
  useSqlBreakdown,
  usePipelineObjects,
  useInteractiveBreakdown,
  useSKUBreakdown,
  getDefaultDateRange,
  useAIMLDashboardBundle,
  useAppsDashboardBundle,
  useTaggingDashboardBundle,
  useDBSQLQueryCosts,
  useDBSQLTopQueries,
  useInfraBundle,
  useKPIsBundle,
  useUsersGroupsBundle,
  getActiveSourceLabels,
  responsePayloadIssue,
} from "@/hooks/useBillingData";
import type { DateRange, WorkspaceBreakdown } from "@/types/billing";
import { generateArchitectureReport } from "@/utils/architectureReport";
import { generateCostReport } from "@/utils/pdfExport";
import { generateCostCSV } from "@/utils/csvExport";
import { C } from "@/theme";
import { CostObsLockup, VersionPill, PageHero, Chip, InfoPanel } from "@/components/brand";
import { LoadingPanels, Spinner } from "@/components/Spinner";
import {
  buildExportScopeKey,
  cancelExportPreparationQueries,
  cancelRunningSubmitAndPollForTab,
  isTabDataRequested,
} from "@/utils/tabDemand";
import { isDashboardQuery, refreshSourceScopeData, refreshTabData, TAB_LOADING_SECTIONS } from "@/utils/tabRefresh";
import {
  WAREHOUSE_WARM_SESSION_KEY,
  fetchWarehouseHealth,
  nextWarehouseWarmState,
  shouldGateDashboard,
  shouldShowDbuSkeleton,
  shouldRequestWarehouseProbe,
  warehouseHealthPollInterval,
  type WarehouseHealth,
} from "@/utils/warehouseGuidance";
import {
  hydrateSettingsFromServer,
  loadAppSettings,
  loadTabVisibility,
  persistAppSettings,
  persistTabVisibility,
  settingsAreEqual,
  tabVisibilityIsEqual,
  type AppSettings,
  type TabVisibility,
  type UnifiedSettings,
} from "@/utils/settingsHydration";

// Keep the Databricks Apps pod warm while the tab is open.
// Cold starts take 30s to 1min; a lightweight ping every 4 min prevents idle suspension.
function useKeepAlive() {
  useEffect(() => {
    const ping = () => fetch("/api/ping", { method: "GET" }).catch(() => {});
    const interval = setInterval(ping, 4 * 60 * 1000);
    const onVisible = () => { if (document.visibilityState === "visible") ping(); };
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      clearInterval(interval);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, []);
}

type ViewTab = keyof TabVisibility;

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30 * 60 * 1000, // 30 minutes - data doesn't change often
      gcTime: 60 * 60 * 1000, // 1 hour cache
      refetchOnWindowFocus: false,
      refetchOnReconnect: false,
      retry: 1,
    },
  },
});

interface User {
  email: string;
  name: string;
  role?: "admin" | "consumer";
}

function AccountIdentifier({ value }: { value: string }) {
  const tooltipId = useId();

  return (
    <span className="account-id-tooltip relative mt-[3px] min-w-0">
      <span
        tabIndex={0}
        aria-describedby={tooltipId}
        className="account-id-tooltip-trigger block max-w-[150px] truncate rounded-[4px] bg-white/[.09] px-[7px] py-[3px] text-[11px] text-[#E9EFED] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#FF3621]/35"
        style={{ fontFamily: "var(--mono)" }}
      >
        {value}
      </span>
      <span
        id={tooltipId}
        role="tooltip"
        className="account-id-tooltip-content pointer-events-none absolute left-0 top-full z-50 mt-2 max-w-[min(360px,80vw)] rounded-[6px] bg-[#0B2026] px-2.5 py-1.5 text-[11px] leading-snug text-white shadow-lg"
        style={{ fontFamily: "var(--mono)" }}
      >
        {value}
      </span>
    </span>
  );
}

function DBUMethodologyPanel() {
  const minimizeKey = "cost-obs-minimize-dbu-info";
  const [minimized, setMinimized] = useState(() =>
    typeof window !== "undefined" ? localStorage.getItem(minimizeKey) === "true" : false
  );
  const handleToggle = (value: boolean) => {
    setMinimized(value);
    if (value) localStorage.setItem(minimizeKey, "true");
    else localStorage.removeItem(minimizeKey);
  };

  return (
    <InfoPanel title="DBU Overview tab methodology" minimized={minimized} onToggle={handleToggle}>
      <ul className="list-inside list-disc space-y-1">
        <li><strong>DBUs</strong> are Databricks Units reported as usage quantity in <code className="rounded bg-white/60 px-1">system.billing.usage</code>.</li>
        <li><strong>Spend</strong> multiplies DBUs by the effective SKU price: account pricing when enabled and available, otherwise current list pricing.</li>
        <li>Date and workspace filters apply throughout this tab; cloud infrastructure charges are reported separately under Cloud Costs.</li>
      </ul>
    </InfoPanel>
  );
}

function AccountPricingBanner() {
  const { useAccountPrices, discountPercent, skuCount, available } = usePricing();
  if (!useAccountPrices) return null;
  return (
    <div className="flex items-center justify-center gap-2 px-4 py-1.5 text-xs font-medium text-white" style={{ backgroundColor: C.s3 }}>
      <svg className="h-3.5 w-3.5 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
      {available
        ? `Account prices active: ${discountPercent.toFixed(1)}% discount applied across ${skuCount} SKUs (from system.billing.account_prices)`
        : "Account prices mode active: system.billing.account_prices not available, showing list prices"}
    </div>
  );
}

function SpGrantsBanner({ onOpenSettings }: { onOpenSettings: () => void }) {
  const [dismissed, setDismissed] = useState(() => sessionStorage.getItem("coc-sp-grants-dismissed") === "1");

  const { data: authStatus } = useQuery<{ user_token_active: boolean; identity: string } | null>({
    queryKey: ["settings-auth-status"],
    queryFn: () => fetch("/api/settings/auth-status").then(r => r.json()).catch(() => null),
    staleTime: 60_000,
  });

  const spMode = authStatus && !authStatus.user_token_active && authStatus.identity === "service_principal";

  const { data: billingAccess } = useQuery<{ ok: boolean; reason?: string; warehouse_id?: string; sp_client_id?: string } | null>({
    queryKey: ["settings-billing-access"],
    queryFn: () => fetch("/api/settings/billing-access").then(r => r.json()).catch(() => null),
    staleTime: 5 * 60_000,
    enabled: !!spMode,
  });

  const isWarehouseIssue = billingAccess?.reason === "warehouse_access";
  const isGrantsIssue = billingAccess?.reason === "grants_missing";

  if (dismissed || !spMode || !billingAccess || billingAccess.ok !== false || (!isWarehouseIssue && !isGrantsIssue)) return null;

  return (
    <div className="flex items-center justify-between gap-4 px-4 py-2.5 bg-amber-50 border-b border-amber-200">
      <div className="flex items-center gap-2 min-w-0">
        <svg className="h-4 w-4 shrink-0 text-amber-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
        </svg>
        {isWarehouseIssue ? (
          <span className="text-xs text-amber-800">
            <strong>SP missing warehouse access</strong>: the service principal{billingAccess.sp_client_id ? ` (${billingAccess.sp_client_id})` : ""} cannot use the SQL warehouse.
            A workspace admin must run:{" "}
            <code className="rounded bg-amber-100 px-1 font-mono">
              GRANT CAN_USE ON WAREHOUSE {billingAccess.warehouse_id || "<warehouse_id>"} TO `{billingAccess.sp_client_id || "<sp_client_id>"}`
            </code>
          </span>
        ) : (
          <span className="text-xs text-amber-800">
            <strong>SP grants missing</strong>: the service principal lacks system table access after the last git deploy.
            Re-run the Permissions setup to restore access.
          </span>
        )}
      </div>
      <div className="flex items-center gap-2 shrink-0">
        <button
          onClick={() => { onOpenSettings(); }}
          className="text-xs font-medium px-3 py-1.5 rounded"
          style={{ background: C.lava, color: "#fff" }}
        >
          {isWarehouseIssue ? "Open Settings → Permissions" : "Re-run Permissions"}
        </button>
        <button
          onClick={() => { sessionStorage.setItem("coc-sp-grants-dismissed", "1"); setDismissed(true); }}
          className="text-xs text-amber-600 hover:text-amber-800 px-1"
          aria-label="Dismiss"
        >
          ✕
        </button>
      </div>
    </div>
  );
}

function Dashboard() {
  useKeepAlive();
  const [appSettings, setAppSettings] = useState<AppSettings>(loadAppSettings);
  const { data: runtimeSettings } = useQuery<UnifiedSettings | null>({
    queryKey: ["unified-settings"],
    queryFn: () => fetch("/api/settings").then((response) => response.ok ? response.json() : null).catch(() => null),
    staleTime: 60 * 1000,
  });
  const defaultRange = getDefaultDateRange(appSettings.defaultDateRangeDays);
  const [dateRange, setDateRange] = useState<DateRange>(defaultRange);
  const [activeTab, setActiveTab] = useState<ViewTab>(() => {
    // Start on the configured default landing tab, falling back to the first visible
    // tab when that tab is hidden or unknown.
    const v = loadTabVisibility();
    const want = appSettings.defaultLandingTab as ViewTab;
    if (want && v[want as keyof TabVisibility]) return want;
    const first = (Object.keys(v) as ViewTab[]).find((k) => v[k as keyof TabVisibility]);
    return first ?? "dbu";
  });
  const [showExportDialog, setShowExportDialog] = useState(false);
  const [sourceScopeVersion, setSourceScopeVersion] = useState(0);
  const [preparedExportScope, setPreparedExportScope] = useState<string | null>(null);
  const [exportPreparingScope, setExportPreparingScope] = useState<string | null>(null);
  const [exportPreparationArmed, setExportPreparationArmed] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [selectedWorkspaceIds, setSelectedWorkspaceIds] = useState<string[]>([]);
  const [tabVisibility, setTabVisibility] = useState<TabVisibility>(loadTabVisibility);
  const appSettingsRef = useRef(appSettings);
  const tabVisibilityRef = useRef(tabVisibility);
  const settingsHydratedRef = useRef(false);
  appSettingsRef.current = appSettings;
  tabVisibilityRef.current = tabVisibility;

  useEffect(() => {
    if (!runtimeSettings) return;
    const previousSettings = appSettingsRef.current;
    const previousVisibility = tabVisibilityRef.current;
    const hydrated = hydrateSettingsFromServer(
      runtimeSettings,
      previousSettings,
      previousVisibility,
    );
    const initialHydration = !settingsHydratedRef.current;
    settingsHydratedRef.current = true;

    persistAppSettings(hydrated.appSettings);
    persistTabVisibility(hydrated.tabVisibility);
    appSettingsRef.current = hydrated.appSettings;
    tabVisibilityRef.current = hydrated.tabVisibility;
    if (!settingsAreEqual(previousSettings, hydrated.appSettings)) {
      setAppSettings(hydrated.appSettings);
    }
    if (!tabVisibilityIsEqual(previousVisibility, hydrated.tabVisibility)) {
      setTabVisibility(hydrated.tabVisibility);
    }
    if (initialHydration) {
      setDateRange(getDefaultDateRange(hydrated.appSettings.defaultDateRangeDays));
    }
    setActiveTab((current) => {
      const preferred = hydrated.appSettings.defaultLandingTab as ViewTab;
      if (initialHydration && hydrated.tabVisibility[preferred]) return preferred;
      if (hydrated.tabVisibility[current]) return current;
      return (Object.keys(hydrated.tabVisibility) as ViewTab[])
        .find((tab) => hydrated.tabVisibility[tab]) ?? "dbu";
    });
  }, [runtimeSettings]);

  // true = show wizard, false = show dashboard.
  // True until /api/setup/status resolves: blocks the dashboard from rendering.
  // Skip for returning users (local cache says done) so they never see a loading gate.
  const [setupCheckPending, setSetupCheckPending] = useState<boolean>(
    () => localStorage.getItem("coc-setup-complete") !== "true" &&
          sessionStorage.getItem("coc-setup-complete") !== "true"
  );
  const [showSetupWizard, setShowSetupWizard] = useState<boolean>(false);
  // Set when user closes wizard without completing: shows the incomplete banner on dashboard.
  const [setupIncomplete, setSetupIncomplete] = useState(false);
  // Stored so onLaunchWizard can abort the in-flight status check and prevent it from
  // overriding the manually-triggered wizard with a stale "ready" response.
  const setupStatusAbortRef = useRef<AbortController | null>(null);
  const rqClient = useQueryClient();
  const [explicitRefreshingTab, setExplicitRefreshingTab] = useState<ViewTab | null>(null);
  const [refreshErrors, setRefreshErrors] = useState<Partial<Record<ViewTab, string>>>({});
  const previousActiveTabRef = useRef(activeTab);

  useEffect(() => {
    const previousTab = previousActiveTabRef.current;
    previousActiveTabRef.current = activeTab;
    if (previousTab !== activeTab) {
      void cancelRunningSubmitAndPollForTab(rqClient, previousTab);
    }
  }, [activeTab, rqClient]);

  const handleTabRefresh = async () => {
    if (explicitRefreshingTab !== null) return;
    const tab = activeTab;
    setExplicitRefreshingTab(tab);
    setRefreshErrors((current) => ({ ...current, [tab]: undefined }));
    try {
      await refreshTabData(rqClient, tab);
    } catch (error) {
      console.error(`Failed to refresh ${tab} tab`, error);
      const detail = error instanceof Error ? error.message : "Unknown error";
      setRefreshErrors((current) => ({
        ...current,
        [tab]: `This tab could not be refreshed: ${detail}`,
      }));
    } finally {
      setExplicitRefreshingTab(null);
    }
  };

  const handleSourceApplied = useCallback(async () => {
    const tab = activeTab;
    setSourceScopeVersion((version) => version + 1);
    setExplicitRefreshingTab(tab);
    try {
      await refreshSourceScopeData(rqClient, tab);
    } catch (error) {
      console.error(`Failed to apply source filter to ${tab} tab`, error);
      throw error;
    } finally {
      setExplicitRefreshingTab(null);
    }
  }, [activeTab, rqClient]);

  // On every load, verify setup status with the server.
  // 60s timeout: allows for cold App pod start.
  useEffect(() => {
    const controller = new AbortController();
    setupStatusAbortRef.current = controller;
    const timeout = setTimeout(() => controller.abort(), 60_000);
    const prevCompleted = () =>
      localStorage.getItem("coc-setup-complete") === "true" ||
      sessionStorage.getItem("coc-setup-complete") === "true";

    fetch("/api/setup/status", { signal: controller.signal })
      .then((r) => r.json())
      .then(async (status) => {
        clearTimeout(timeout);
        if (status?.status === "ready") {
          localStorage.setItem("coc-setup-complete", "true");
          sessionStorage.setItem("coc-setup-complete", "true");
          setShowSetupWizard(false);
        } else if (status?.status === "setup_required") {
          // Only show wizard on a definitive "setup_required": not on transient states
          // like "initializing". Avoids wizard flash during cold start or mid-build polling.
          if (!prevCompleted()) {
            localStorage.removeItem("coc-setup-complete");
            sessionStorage.removeItem("coc-setup-complete");
            // The durable administrator claim must happen before any setup
            // mutation. The server verifies the forwarded Apps OAuth identity
            // and atomically allows only the first caller to claim ownership.
            const bootstrap = await fetch("/api/setup/bootstrap-admin", {
              method: "POST",
              signal: controller.signal,
            });
            setShowSetupWizard(bootstrap.ok);
          }
        }
        setSetupCheckPending(false);
        // "initializing" and other transient states: no wizard change, but unblock dashboard
      })
      .catch(() => {
        clearTimeout(timeout);
        setSetupCheckPending(false);
        // Network error, timeout, or non-JSON: trust local cache rather than flashing wizard.
        if (!prevCompleted()) {
          setShowSetupWizard(true);
        }
      });

    return () => { clearTimeout(timeout); controller.abort(); };
  }, []);

  const handleSetupComplete = () => {
    localStorage.setItem("coc-setup-complete", "true");
    sessionStorage.setItem("coc-setup-complete", "true");
    // Mark setup complete server-side (survives page refresh, cleared on redeploy)
    fetch("/api/setup/complete", { method: "POST" }).catch(() => {});
    setShowSetupWizard(false);
    rqClient.invalidateQueries();
  };

  // On first load after each new deploy, reset all info banner minimize flags so users
  // see best-practice guidance at least once. After that, their collapse preference persists.
  useEffect(() => {
    const BANNER_RESET_VERSION = "2026-03-12";
    const BANNER_RESET_KEY = "coc-banner-reset-v";
    if (localStorage.getItem(BANNER_RESET_KEY) !== BANNER_RESET_VERSION) {
      [
        "cost-obs-minimize-tagging-info",
        "cost-obs-minimize-sql-info",
        "cost-obs-minimize-infra-info",
        "cost-obs-minimize-aiml-info",
        "cost-obs-minimize-kpis-info",
      ].forEach((k) => localStorage.removeItem(k));
      localStorage.setItem(BANNER_RESET_KEY, BANNER_RESET_VERSION);
    }
  }, []);

  // Auto-refresh interval based on settings
  useEffect(() => {
    if (appSettings.refreshIntervalMinutes <= 0) return;
    const interval = setInterval(() => {
      rqClient.invalidateQueries();
    }, appSettings.refreshIntervalMinutes * 60 * 1000);
    return () => clearInterval(interval);
  }, [appSettings.refreshIntervalMinutes, rqClient]);

  // Density → compact-mode CSS class on root
  useEffect(() => {
    document.documentElement.classList.toggle("compact-mode", appSettings.density === "compact");
  }, [appSettings.density]);

  // Theme → dark-mode CSS class on root ("system" follows prefers-color-scheme).
  useEffect(() => {
    const apply = () => {
      const dark = appSettings.theme === "dark" ||
        (appSettings.theme === "system" && window.matchMedia("(prefers-color-scheme: dark)").matches);
      document.documentElement.classList.toggle("dark-mode", dark);
    };
    apply();
    if (appSettings.theme === "system") {
      const mq = window.matchMedia("(prefers-color-scheme: dark)");
      mq.addEventListener("change", apply);
      return () => mq.removeEventListener("change", apply);
    }
  }, [appSettings.theme]);

  const { data: user } = useQuery<User>({
    queryKey: ["user"],
    queryFn: async () => {
      const response = await fetch("/api/user/me");
      if (!response.ok) throw new Error("Failed to fetch user");
      return response.json();
    },
  });

  const { data: accountInfo } = useAccountInfo();

  const { data: authStatus } = useQuery<{
    user_token_active: boolean;
    identity: "user_oauth" | "service_principal";
    locked_to_sp: boolean;
    has_sql_scope: boolean | null;
    sp_client_id?: string;
    sp_display_name?: string;
    sp_user_name?: string;
  } | null>({
    queryKey: ["settings-auth-status"],
    queryFn: () => fetch("/api/settings/auth-status").then(r => r.json()).catch(() => null),
    staleTime: 60 * 1000,
  });

  const {
    applyPricing,
    multiplier: pricingMultiplier,
    useAccountPrices,
    available: accountPricesAvailable,
  } = usePricing();

  // Central warehouse warming poller: single source of truth for warehouse state.
  // Normal checks are REST-only. If the initial cold gate remains after two polls,
  // one throttled SQL probe wakes/verifies the warehouse without releasing every
  // dashboard query at once. Missing bindings remain a definitive unavailable state.
  const initialWarehouseEverWarm =
    sessionStorage.getItem(WAREHOUSE_WARM_SESSION_KEY) === "1";
  const warehouseEverWarmRef = useRef(initialWarehouseEverWarm);
  const warehouseColdPollsRef = useRef(0);
  const warehouseLastProbeAtRef = useRef<number | null>(null);
  const [warehouseEverWarm, setWarehouseEverWarm] = useState(
    initialWarehouseEverWarm,
  );
  const {
    data: warehouseStatus,
    isError: warehouseHealthCheckFailed,
    isFetching: warehouseHealthChecking,
    refetch: retryWarehouseHealthCheck,
  } = useQuery<WarehouseHealth>({
    queryKey: ["health", "sql-warehouse"],
    queryFn: async () => {
      const now = Date.now();
      const requestProbe = shouldRequestWarehouseProbe(
        warehouseColdPollsRef.current,
        warehouseEverWarmRef.current,
        warehouseLastProbeAtRef.current,
        now,
      );
      if (requestProbe) warehouseLastProbeAtRef.current = now;

      const status = await fetchWarehouseHealth(requestProbe);
      if (status.status === "warm") {
        warehouseEverWarmRef.current = true;
        warehouseColdPollsRef.current = 0;
      } else if (status.status === "warming_up") {
        warehouseColdPollsRef.current += 1;
      } else {
        warehouseColdPollsRef.current = 0;
      }
      return status;
    },
    retry: 2,
    retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 4000),
    // Keep retrying even if the endpoint itself transiently returns a network/5xx
    // error. An error allows normal data queries, so it never creates a cold gate.
    refetchInterval: (query) => warehouseHealthPollInterval(
      query.state.data?.status,
      query.state.status === "error",
    ),
    staleTime: 0,
  });
  const warehouseWarming = warehouseStatus?.status === "warming_up";
  // True only when we've confirmed the warehouse is accepting queries.
  // Undefined (not yet fetched) is treated as NOT ready: prevents firing 15+ SQL
  // queries against a cold warehouse before we know its state. Once bounded health
  // retries fail, allow real app queries to prove connectivity instead of blocking
  // an otherwise healthy app on a non-authoritative status endpoint.
  const warehouseReady = warehouseStatus?.status === "warm";
  const warehouseQueriesAllowed = warehouseReady || warehouseHealthCheckFailed;

  useEffect(() => {
    setWarehouseEverWarm((current) => {
      const next = nextWarehouseWarmState(current, warehouseStatus?.status);
      warehouseEverWarmRef.current = next;
      if (next && !current) sessionStorage.setItem(WAREHOUSE_WARM_SESSION_KEY, "1");
      return next;
    });
  }, [warehouseStatus?.status]);

  // SettingsConfig writes true here when a rebuild starts, false when it finishes.
  // Suppresses the cold-start screen during rebuilds: the warehouse waking up
  // was caused by the rebuild itself, not a cold app load.
  const { data: rebuildInProgress = false } = useQuery<boolean>({
    queryKey: ["rebuild-in-progress"],
    queryFn: () => false,
    staleTime: Infinity,
    gcTime: Infinity,
  });

  // Clear the stale-chunk reload guard once the app is healthy. Tab chunks remain
  // on demand: React.lazy downloads each one only when its tab is first opened.
  useEffect(() => {
    if (!warehouseReady) return;
    sessionStorage.removeItem("_chunk_reload");
  }, [warehouseReady]);

  const exportScopeKey = buildExportScopeKey(
    dateRange.startDate,
    dateRange.endDate,
    selectedWorkspaceIds,
    sourceScopeVersion,
    (Object.keys(tabVisibility) as ViewTab[]).filter((tab) => tabVisibility[tab]),
  );
  const exportPreparationRequested = showExportDialog && exportPreparingScope === exportScopeKey;
  const requested = (tab: ViewTab) =>
    warehouseQueriesAllowed && isTabDataRequested(tab, activeTab, exportPreparationRequested, tabVisibility);
  const dbuRequested = requested("dbu");
  const sqlRequested = requested("sql");
  const infraRequested = requested("infra");
  const optimizerRequested = requested("optimizer");
  const kpisRequested = requested("kpis");
  const aimlRequested = requested("aiml");
  const appsRequested = requested("apps");
  const taggingRequested = requested("tagging");
  const usersRequested = requested("users-groups");

  // Fast bundle for the DBU tab and report exports (uses materialized views).
  const { data: bundle, isLoading: bundleLoading, isError: bundleError } = useDashboardBundleFast(
    dateRange,
    selectedWorkspaceIds.length ? selectedWorkspaceIds : undefined,
    dbuRequested,
  );

  // Extract data from fast bundle: apply pricing multiplier when account prices are active
  const summary = useMemo(() => {
    const s = bundle?.summary;
    if (!s || pricingMultiplier === 1.0) return s;
    return {
      ...s,
      total_spend: applyPricing(s.total_spend ?? 0),
      total_dbus: s.total_dbus, // DBUs don't scale with price
    };
  }, [bundle?.summary, pricingMultiplier, applyPricing]);

  const products = useMemo(() => {
    const p = bundle?.products;
    if (!p || pricingMultiplier === 1.0) return p;
    return {
      ...p,
      products: p.products?.map((prod) => ({
        ...prod,
        total_spend: applyPricing(prod.total_spend ?? 0),
      })),
    };
  }, [bundle?.products, pricingMultiplier, applyPricing]);

  const workspaces = useMemo(() => {
    const w = bundle?.workspaces;
    if (!w || pricingMultiplier === 1.0) return w;
    return {
      ...w,
      workspaces: w.workspaces?.map((ws) => ({
        ...ws,
        total_spend: applyPricing(ws.total_spend ?? 0),
      })),
    };
  }, [bundle?.workspaces, pricingMultiplier, applyPricing]);

  const timeseries = useMemo(() => {
    const t = bundle?.timeseries;
    if (!t || pricingMultiplier === 1.0) return t;
    return {
      ...t,
      timeseries: t.timeseries?.map((row) => {
        const scaled: typeof row = { ...row };
        for (const key of Object.keys(row)) {
          if (key !== "date" && typeof row[key] === "number") {
            scaled[key] = applyPricing(row[key] as number);
          }
        }
        return scaled;
      }),
    };
  }, [bundle?.timeseries, pricingMultiplier, applyPricing]);

  const _wsIds = selectedWorkspaceIds.length ? selectedWorkspaceIds : undefined;

  // Each tab now opts into only the queries it owns. React Query keeps settled data
  // cached, so revisiting a tab is instant until its stale-time expires.
  const { data: sqlBreakdown, isLoading: sqlLoading, isError: sqlError } = useSqlBreakdown(dateRange, _wsIds, sqlRequested);
  const { data: pipelineObjects, isLoading: pipelineLoading, isError: pipelineError } = usePipelineObjects(dateRange, _wsIds, dbuRequested);
  const { data: interactiveBreakdown, isLoading: interactiveLoading, isError: interactiveError } = useInteractiveBreakdown(dateRange, _wsIds, dbuRequested);
  const { data: skuBreakdown, isLoading: skuLoading, isError: skuError } = useSKUBreakdown(dateRange, _wsIds, dbuRequested);

  const { data: infraBundle, isLoading: infraBundleLoading, isError: infraBundleError } = useInfraBundle(dateRange, _wsIds, infraRequested);
  const infraCosts = infraBundle?.infra_costs;
  const infraCostsTimeseries = infraBundle?.infra_timeseries;
  const infraPricingMultiplier =
    useAccountPrices && accountPricesAvailable ? pricingMultiplier : 1;
  const pricedInfraCosts = useMemo(
    () => applyInfraPricing(infraCosts, infraPricingMultiplier),
    [infraCosts, infraPricingMultiplier],
  );

  const { data: kpisBundle, isLoading: kpisBundleLoading, isFetching: kpisBundleFetching, isError: kpisBundleError } = useKPIsBundle(dateRange, _wsIds, kpisRequested);
  const spendAnomalies = kpisBundle?.anomalies;
  const platformKPIs = kpisBundle?.kpis;
  const anomaliesLoading = kpisBundleLoading;
  const kpisLoading = kpisBundleLoading;

  const {
    data: aimlData,
    isLoading: aimlLoading,
    isError: aimlError,
    error: aimlErrorObj,
    refetch: refetchAiml,
  } = useAIMLDashboardBundle(dateRange, _wsIds, aimlRequested);
  const {
    data: appsData,
    isLoading: appsLoading,
    isError: appsError,
    error: appsErrorObj,
    refetch: refetchApps,
  } = useAppsDashboardBundle(dateRange, _wsIds, appsRequested);
  const { data: taggingData, isLoading: taggingLoading, isError: taggingError } = useTaggingDashboardBundle(dateRange, _wsIds, taggingRequested);

  // Cloud actual costs: no workspace filter
  const { data: awsActualData, isLoading: awsActualLoading, isError: awsActualError } = useAWSActualCosts(dateRange, infraRequested);
  const { data: azureActualData, isLoading: azureActualLoading, isError: azureActualError } = useAzureActualCosts(dateRange, infraRequested);
  const { data: gcpActualData, isLoading: gcpActualLoading, isError: gcpActualError } = useGCPActualCosts(dateRange, infraRequested);

  const { data: dbsqlData, isLoading: dbsqlLoading, isError: dbsqlError } = useDBSQLQueryCosts(dateRange, _wsIds, sqlRequested);
  const { data: dbsqlTopQueriesData, isLoading: dbsqlTopQueriesLoading, isError: dbsqlTopQueriesError } = useDBSQLTopQueries(dateRange, _wsIds, sqlRequested);
  const { data: usersGroupsData, isLoading: usersGroupsLoading, isError: usersGroupsError } = useUsersGroupsBundle(dateRange, _wsIds, usersRequested);

  // Optimizer queries run when its tab or the report exporter requests them.
  const { data: optimizeRightsizingData, isLoading: optimizeRightsizingLoading, isError: optimizeRightsizingError } = useQuery<{
    available: boolean;
    warehouses_analyzed: number;
    recommendations: Array<{
      warehouse_id: string;
      warehouse_name: string | null;
      warehouse_size: string | null;
      workspace_id: string;
      recommendation_type: string;
      recommendation_text: string;
    }>;
  }>({
    queryKey: ["warehouse-health"],
    queryFn: async () => {
      const response = await fetch("/api/sql/warehouse-health");
      if (!response.ok) throw new Error(`Warehouse health request failed with ${response.status}`);
      return response.json();
    },
    staleTime: 30 * 60 * 1000,
    retry: false,
    // Deferred: only fire when the Optimize tab is actually opened. Previously
    // this prefetched at Dashboard mount and could starve concurrent bundles.
    enabled: optimizerRequested,
  });
  const { data: optimizeIdleData, isLoading: optimizeIdleLoading, isError: optimizeIdleError } = useQuery<{
    available: boolean;
    serverless_detected: boolean;
    warehouses: Array<{
      warehouse_id: string;
      warehouse_name: string;
      warehouse_size: string;
      warehouse_type: string;
      workspace_id: string;
      total_running_minutes: number;
      total_query_minutes: number;
      idle_minutes: number;
      idle_pct: number;
      total_spend: number;
      estimated_idle_spend: number;
    }>;
  }>({
    queryKey: ["warehouse-idle-time", dateRange.startDate, dateRange.endDate, _wsIds?.join(",")],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (dateRange.startDate) params.set("start_date", dateRange.startDate);
      if (dateRange.endDate) params.set("end_date", dateRange.endDate);
      if (_wsIds?.length) params.set("workspace_ids", _wsIds.join(","));
      const response = await fetch(`/api/sql/warehouse-health/idle-time?${params}`);
      if (!response.ok) throw new Error(`Warehouse idle-time request failed with ${response.status}`);
      return response.json();
    },
    staleTime: 30 * 60 * 1000,
    retry: false,
    // Deferred: only fire when the Optimize tab is actually opened.
    enabled: optimizerRequested,
  });

  const tabLoading: Record<ViewTab, boolean> = {
    dbu: bundleLoading || skuLoading || pipelineLoading || interactiveLoading,
    sql: sqlLoading || dbsqlLoading || dbsqlTopQueriesLoading,
    infra: infraBundleLoading || awsActualLoading || azureActualLoading || gcpActualLoading,
    optimizer: optimizeRightsizingLoading || optimizeIdleLoading,
    kpis: kpisBundleLoading,
    aiml: aimlLoading,
    apps: appsLoading,
    tagging: taggingLoading,
    "users-groups": usersGroupsLoading,
  };
  const firstPayloadIssue = (
    ...payloads: Array<[unknown, boolean?]>
  ): string | undefined => {
    for (const [payload, requireAvailable = false] of payloads) {
      const issue = responsePayloadIssue(payload, requireAvailable);
      if (issue) return issue;
    }
    return undefined;
  };
  const reportPayloadIssues: Partial<Record<ViewTab, string>> = {
    dbu: firstPayloadIssue(
      [bundle],
      [bundle?.summary, true],
      [bundle?.products, true],
      [bundle?.workspaces, true],
      [skuBreakdown, true],
      [pipelineObjects, true],
      [interactiveBreakdown, true],
    ),
    sql: firstPayloadIssue(
      [sqlBreakdown, true],
      [dbsqlData, true],
      [dbsqlTopQueriesData, true],
    ),
    infra: firstPayloadIssue(
      [infraBundle],
      [infraCosts, true],
      [infraCostsTimeseries, true],
    ),
    optimizer: firstPayloadIssue(
      [optimizeRightsizingData, true],
      [optimizeIdleData, true],
    ),
    kpis: firstPayloadIssue(
      [kpisBundle],
      [spendAnomalies, true],
      [platformKPIs, true],
    ),
    aiml: firstPayloadIssue([aimlData, true]),
    apps: firstPayloadIssue([appsData, true]),
    tagging: firstPayloadIssue([taggingData, true]),
    "users-groups": firstPayloadIssue([usersGroupsData, true]),
  };
  const tabErrors: Partial<Record<ViewTab, string>> = {
    dbu: reportPayloadIssues.dbu || (bundleError || skuError || pipelineError || interactiveError ? "DBU Overview data failed to load." : undefined),
    sql: reportPayloadIssues.sql || (sqlError || dbsqlError || dbsqlTopQueriesError ? "SQL data failed to load." : undefined),
    infra: reportPayloadIssues.infra || (infraBundleError || awsActualError || azureActualError || gcpActualError ? "Cloud Costs data failed to load." : undefined),
    optimizer: reportPayloadIssues.optimizer || (optimizeRightsizingError || optimizeIdleError ? "Optimize data failed to load." : undefined),
    kpis: reportPayloadIssues.kpis || (kpisBundleError ? "Platform KPI data failed to load." : undefined),
    aiml: reportPayloadIssues.aiml || (aimlError ? "AI/ML data failed to load." : undefined),
    apps: reportPayloadIssues.apps || (appsError ? "Apps data failed to load." : undefined),
    tagging: reportPayloadIssues.tagging || (taggingError ? "Tagging data failed to load." : undefined),
    "users-groups": reportPayloadIssues["users-groups"] || (usersGroupsError ? "Users data failed to load." : undefined),
  };
  const activeTabInitialLoading = !warehouseQueriesAllowed || tabLoading[activeTab];
  const showActiveTabLoading = activeTabInitialLoading || explicitRefreshingTab === activeTab;
  const exportDataLoading = exportPreparationRequested &&
    (Object.keys(tabVisibility) as ViewTab[]).some((tab) => tabVisibility[tab] && tabLoading[tab]);
  const exportHasErrors = exportPreparationRequested &&
    (Object.keys(tabVisibility) as ViewTab[]).some((tab) => tabVisibility[tab] && Boolean(tabErrors[tab]));

  useEffect(() => {
    if (!exportPreparationRequested) return;
    if (!exportPreparationArmed) {
      setExportPreparationArmed(true);
      return;
    }
    if (exportDataLoading || exportHasErrors) return;
    setPreparedExportScope(exportScopeKey);
    setExportPreparingScope(null);
  }, [exportDataLoading, exportHasErrors, exportPreparationArmed, exportPreparationRequested, exportScopeKey]);

  const previousExportScopeRef = useRef(exportScopeKey);
  useEffect(() => {
    if (previousExportScopeRef.current === exportScopeKey) return;
    previousExportScopeRef.current = exportScopeKey;
    setPreparedExportScope(null);
    if (!showExportDialog) return;
    void cancelExportPreparationQueries(rqClient);
    setExportPreparingScope(exportScopeKey);
    setExportPreparationArmed(false);
  }, [exportScopeKey, rqClient, showExportDialog]);

  const openExportDialog = () => {
    const hasVisibleErrors = (Object.keys(tabVisibility) as ViewTab[])
      .some((tab) => tabVisibility[tab] && Boolean(tabErrors[tab]));
    const needsPreparation = preparedExportScope !== exportScopeKey || hasVisibleErrors;
    setExportPreparingScope(needsPreparation ? exportScopeKey : null);
    setExportPreparationArmed(!needsPreparation);
    setShowExportDialog(true);
  };

  const closeExportDialog = useCallback(() => {
    setShowExportDialog(false);
    setExportPreparingScope(null);
    setExportPreparationArmed(false);
    void cancelExportPreparationQueries(rqClient, activeTab);
  }, [activeTab, rqClient]);

  const retryFailedExportData = useCallback(async () => {
    await rqClient.refetchQueries({
      type: "active",
      predicate: (query) => query.state.status === "error" && isDashboardQuery(query.queryKey),
    });
  }, [rqClient]);

  // Workspace list for the filter dropdown: SQL-backed, only fire when warehouse is ready.
  const { data: wsListData, isLoading: wsListLoading } = useQuery<{ workspaces: { id: string; name: string; historical?: boolean }[] }>({
    queryKey: ["billing", "workspaces"],
    queryFn: () => fetch("/api/billing/workspaces").then(r => r.json()),
    staleTime: Infinity,
    enabled: warehouseQueriesAllowed,
  });
  const workspaceNameMap = useMemo(() => {
    const map: Record<string, string> = {};
    // wsListData has real account-level names; only store entries where a real name exists
    wsListData?.workspaces?.forEach((w) => { if (w.name) map[w.id] = w.name; });
    // Fill any gaps from billing data (workspace_name sometimes populated here too)
    workspaces?.workspaces?.forEach((w: WorkspaceBreakdown) => {
      if (!map[w.workspace_id] && w.workspace_name) map[w.workspace_id] = w.workspace_name;
    });
    return map;
  }, [wsListData?.workspaces, workspaces?.workspaces]);

  // Overlay the merged name map so the top-nav filter picks up billing-derived
  // names when /api/billing/workspaces returns an id-only record.
  const wsFilterList = useMemo(
    () => (wsListData?.workspaces ?? []).map(w => {
      const name = workspaceNameMap[w.id] || w.name || null;
      // Trust the server's `historical` (computed from the true resolved name), so the
      // "workspace display names" toggle nulling names never mislabels a live workspace.
      return { workspace_id: w.id, workspace_name: name, historical: w.historical ?? !name };
    }),
    [wsListData?.workspaces, workspaceNameMap],
  );

  // Service-principal display-name map: refetches after 10 min so if the first
  // SCIM call missed (e.g. permission granted later, backend was in cold-start),
  // consumers recover without a hard refresh.
  const { data: spListData } = useQuery<{ map: Record<string, string> }>({
    queryKey: ["user", "service-principals"],
    queryFn: () => fetch("/api/user/service-principals").then(r => r.json()),
    staleTime: 10 * 60 * 1000,
  });
  // Memoize so consumers of SpNameMapContext get a stable reference while
  // spListData is undefined (otherwise every parent render churns the context).
  const spNameMap = useMemo(() => spListData?.map ?? {}, [spListData?.map]);

  // Settings data: all prefetched in the background after the main bundle loads.
  // `enabled` gates each query on `!!bundle` so settings requests don't race the
  // critical-path billing queries on cold start.
  const _settingsReady = !!bundle;
  useQuery({ queryKey: ["user-permissions"],      enabled: _settingsReady, queryFn: async () => { const r = await fetch("/api/settings/user-permissions"); if (!r.ok) throw new Error("Failed"); return r.json(); }, staleTime: 5 * 60 * 1000 });
  useQuery({ queryKey: ["app-config"],             enabled: _settingsReady, queryFn: async () => { const r = await fetch("/api/settings/config"); if (!r.ok) throw new Error("Failed"); return r.json(); }, staleTime: 5 * 60 * 1000 });
  // settings-install-report: same endpoint as app-config but separate key used by SettingsDebugger
  useQuery({ queryKey: ["settings-install-report"], enabled: _settingsReady, queryFn: async () => { const r = await fetch("/api/settings/config"); return r.ok ? r.json() : null; }, staleTime: 5 * 60 * 1000 });
  useQuery({ queryKey: ["warehouses"],             enabled: _settingsReady, queryFn: async () => { const r = await fetch("/api/settings/warehouses"); if (!r.ok) throw new Error("Failed"); return r.json(); }, staleTime: 5 * 60 * 1000 });
  useQuery({ queryKey: ["cloud-provider"],         enabled: _settingsReady, queryFn: async () => { const r = await fetch("/api/settings/cloud-provider"); if (!r.ok) throw new Error("Failed"); return r.json(); }, staleTime: 30 * 60 * 1000 });
  useQuery({ queryKey: ["cloud-connections"],      enabled: _settingsReady, queryFn: async () => { const r = await fetch("/api/settings/cloud-connections"); if (!r.ok) throw new Error("Failed"); return r.json(); }, staleTime: 5 * 60 * 1000 });
  useQuery({ queryKey: ["settings-account-prices"], enabled: _settingsReady, queryFn: async () => { const r = await fetch("/api/settings/account-prices"); return r.ok ? r.json() : { available: false, prices: [], source: null, count: 0 }; }, staleTime: 5 * 60 * 1000 });
  useQuery({ queryKey: ["settings-catalog"],       enabled: _settingsReady, queryFn: async () => { const r = await fetch("/api/settings/catalog"); return r.ok ? r.json() : null; }, staleTime: 5 * 60 * 1000 });
  useQuery({ queryKey: ["settings-auth-status"],   enabled: _settingsReady, queryFn: async () => { const r = await fetch("/api/settings/auth-status"); return r.ok ? r.json() : null; }, staleTime: 5 * 60 * 1000 });
  useQuery({ queryKey: ["settings-schedule"],      enabled: _settingsReady, queryFn: async () => { const r = await fetch("/api/settings/schedule"); return r.ok ? r.json() : null; }, staleTime: 5 * 60 * 1000 });
  useQuery({ queryKey: ["setup-workspace-filter"], enabled: _settingsReady, queryFn: async () => { const r = await fetch("/api/setup/workspace-filter"); return r.ok ? r.json() : null; }, staleTime: 5 * 60 * 1000 });
  useQuery({ queryKey: ["billing", "account"],     enabled: _settingsReady, queryFn: async () => { const r = await fetch("/api/billing/account"); return r.ok ? r.json() : null; }, staleTime: Infinity });
  // settings-tables-status is NOT pre-fetched: it runs SQL against every app table
  // and returns stale false-negatives if it fires before the background MV build completes.

  // Memoize infra data transformations to avoid re-creating arrays on every render
  const infraViewData = useMemo(() => pricedInfraCosts ? {
    clusters: (pricedInfraCosts.clusters || []).map(c => ({
      cluster_id: c.cluster_id,
      cluster_name: c.cluster_name,
      driver_instance_type: c.driver_instance_type,
      worker_instance_type: c.worker_instance_type,
      cluster_source: c.cluster_source,
      total_dbu_hours: c.total_dbu_hours,
      databricks_spend: c.databricks_spend,
      days_active: c.days_active,
      percentage: c.percentage,
      workspace_id: c.workspace_id || "",
      workspace_name: c.workspace_name ?? null,
      state: null,
    })),
    instance_families: pricedInfraCosts.instance_families,
    total_estimated_cost: pricedInfraCosts.total_estimated_cost,
    total_databricks_spend: pricedInfraCosts.total_databricks_spend,
    total_dbu_hours: pricedInfraCosts.total_dbu_hours,
    total_cluster_count: pricedInfraCosts.total_cluster_count,
    detail_limit: pricedInfraCosts.detail_limit,
    detail_truncated: pricedInfraCosts.detail_truncated,
    full_first_usage_date: pricedInfraCosts.full_first_usage_date,
    full_last_usage_date: pricedInfraCosts.full_last_usage_date,
    billing_summary: pricedInfraCosts.billing_summary,
    start_date: pricedInfraCosts.start_date,
    end_date: pricedInfraCosts.end_date,
    disclaimer: pricedInfraCosts.disclaimer,
    error: pricedInfraCosts.error,
  } : undefined, [pricedInfraCosts]);

  const infraViewTimeseries = useMemo(() => infraCostsTimeseries ? {
    timeseries: (infraCostsTimeseries.timeseries || []).map(t => ({
      date: t.date,
      "Cluster DBUs": t.total_dbu_hours,
    })),
    categories: ["Cluster DBUs"],
    start_date: infraCostsTimeseries.start_date,
    end_date: infraCostsTimeseries.end_date,
  } : undefined, [infraCostsTimeseries]);

  const handleExport = (sections: ExportSections, format: ExportFormat) => {
    const workspaceFilter = _wsIds?.length
      ? { ids: _wsIds, names: _wsIds.map((id: string) => workspaceNameMap[id] || id) }
      : { ids: [] };
    const exportContext = {
      anonymizeUsers: appSettings.anonymizeUsers,
      companyName: appSettings.companyName,
      sourceLabels: getActiveSourceLabels(),
      cloudProvider: accountInfo?.cloud,
    };

    if (format === "csv") {
      generateCostCSV(
        {
          summary,
          products,
          workspaces,
          skus: skuBreakdown,
          pipelineObjects,
          interactiveBreakdown,
          aiml: aimlData,
          apps: appsData,
          tagging: taggingData,
          users: usersGroupsData,
          query360: dbsqlData ?? undefined,
        },
        sections,
        { start: dateRange.startDate, end: dateRange.endDate },
        workspaceFilter,
        exportContext,
      );
      return;
    }
    void handleExportPDF(sections, workspaceFilter);
  };

  const handleExportPDF = async (sections: ExportSections, workspaceFilter?: { ids: string[]; names?: string[] }) => {
    try {
      await generateCostReport(
        {
        summary,
        products,
        workspaces,
        skus: skuBreakdown,
        anomalies: spendAnomalies,
        pipelineObjects,
        interactiveBreakdown,
        awsCosts: pricedInfraCosts ? {
          clusters: (pricedInfraCosts.clusters ?? []).map(c => ({
            cluster_id: c.cluster_id,
            cluster_name: c.cluster_name,
            driver_instance_type: c.driver_instance_type,
            worker_instance_type: c.worker_instance_type,
            cluster_source: c.cluster_source,
            total_dbu_hours: c.total_dbu_hours,
            databricks_spend: c.databricks_spend,
            days_active: c.days_active,
            percentage: c.percentage,
            workspace_id: c.workspace_id || "",
            state: null,
          })),
          instance_families: pricedInfraCosts.instance_families,
          total_estimated_cost: pricedInfraCosts.total_estimated_cost,
          total_databricks_spend: pricedInfraCosts.total_databricks_spend,
          total_dbu_hours: pricedInfraCosts.total_dbu_hours,
          start_date: pricedInfraCosts.start_date,
          end_date: pricedInfraCosts.end_date,
          disclaimer: pricedInfraCosts.disclaimer,
          error: pricedInfraCosts.error,
        } : undefined,
        aiml: aimlData,
        apps: appsData,
        tagging: taggingData,
        platformKPIs,
        query360: dbsqlData ?? undefined,
        users: usersGroupsData,
        optimize: (optimizeRightsizingData || optimizeIdleData)
          ? { rightsizing: optimizeRightsizingData, idle: optimizeIdleData }
          : undefined,
        dateRange: {
          start: dateRange.startDate,
          end: dateRange.endDate,
        },
        workspaceFilter,
        context: {
          anonymizeUsers: appSettings.anonymizeUsers,
          companyName: appSettings.companyName,
          sourceLabels: getActiveSourceLabels(),
          cloudProvider: accountInfo?.cloud,
        },
        },
        sections
      );
    } catch (error) {
      console.error("PDF export failed", error);
      window.alert("The PDF could not be generated. Please try again.");
    }
  };

  const handleArchitectureExport = async () => {
    await generateArchitectureReport();
  };

  if (showSetupWizard) {
    return (
      <div className="flex min-h-screen items-center justify-center" style={{ backgroundColor: C.oatPage }}>
        <SetupWizard
          onComplete={handleSetupComplete}
          onClose={() => { setShowSetupWizard(false); setSetupIncomplete(true); }}
        />
      </div>
    );
  }

  if (setupCheckPending) {
    return (
      <div className="flex min-h-screen flex-col items-center justify-center gap-4" style={{ backgroundColor: C.oatPage }}>
        <Spinner size="md" />
        <p className="text-sm font-medium" style={{ color: C.ink }}>Checking setup status…</p>
      </div>
    );
  }

  if (
    warehouseStatus?.status === "unavailable"
    && shouldGateDashboard(warehouseStatus.status, warehouseEverWarm, rebuildInProgress)
  ) {
    return (
      <div className="flex min-h-screen flex-col items-center justify-center gap-6" style={{ backgroundColor: C.oatPage }}>
        <div className="flex flex-col items-center gap-4 text-center">
          <svg className="h-10 w-10 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 9v2m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
          </svg>
          <h2 className="text-xl font-semibold" style={{ color: C.ink }}>SQL Warehouse unavailable</h2>
          <p className="text-sm" style={{ color: C.slate }}>The warehouse could not be reached. Check that your warehouse is running in the Databricks console.</p>
        </div>
      </div>
    );
  }

  if (
    warehouseWarming
    && shouldGateDashboard(warehouseStatus?.status, warehouseEverWarm, rebuildInProgress)
  ) {
    return (
      <div className="flex min-h-screen flex-col items-center justify-center gap-6" style={{ backgroundColor: C.oatPage }}>
        <div className="flex flex-col items-center gap-4 text-center">
          <div className="flex h-3 w-13 items-center justify-center">
            <Spinner size="xs" />
          </div>
          <h2 className="text-xl font-semibold" style={{ color: C.ink }}>SQL Warehouse is starting up</h2>
          <p className="text-sm" style={{ color: C.slate }}>Dashboard data will load automatically once the warehouse is ready.</p>
        </div>
      </div>
    );
  }

  return (
    <SpNameMapContext.Provider value={spNameMap}>
    <div className="min-h-screen" style={{ backgroundColor: C.oatPage }}>
      {/* Setup incomplete banner: non-dismissable, shown when wizard was closed without finishing */}
      {setupIncomplete && (
        <div className="flex items-center justify-between gap-4 px-4 py-2.5 border-b" style={{ backgroundColor: C.coralTint, borderColor: C.coralBrd }}>
          <div className="flex items-center gap-2 min-w-0">
            <svg className="h-4 w-4 shrink-0 text-orange-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
            </svg>
            <span className="text-xs font-medium text-orange-800">
              Setup incomplete: materialized views have not been created. Data may be slower or incomplete.
            </span>
          </div>
          <div className="flex shrink-0 items-center gap-2">
            <button
              onClick={() => {
                // Manually mark setup complete (tables already exist: e.g. the flag
                // was lost after a redeploy). Dismisses the banner without a rebuild.
                setSetupIncomplete(false);
                fetch("/api/setup/mark-complete", { method: "POST" }).catch(() => {});
              }}
              className="rounded-md border border-orange-300 px-3 py-1.5 text-xs font-medium text-orange-800 hover:bg-orange-100 transition-colors"
              title="Mark setup complete without rebuilding: use when the tables already exist"
            >
              Mark complete
            </button>
            <button
              onClick={() => {
                setSetupIncomplete(false);
                fetch("/api/setup/rerun", { method: "POST" }).catch(() => {});
                setShowSetupWizard(true);
              }}
              className="rounded-md px-3 py-1.5 text-xs font-semibold text-white transition-colors"
              style={{ backgroundColor: C.lava }}
            >
              Resume Setup
            </button>
          </div>
        </div>
      )}

      {/* Sticky top chrome: navy account bar + white title/tabs */}
      <div className="sticky top-0 z-30 shadow">
      {/* Account rail */}
      <div data-testid="account-rail" className="h-[52px] overflow-visible bg-[#1B3139] text-white">
        <div className="flex h-full w-full min-w-0 flex-nowrap items-center gap-[8px] px-[12px] min-[1280px]:gap-[14px] min-[1280px]:px-[20px]">
          <a
            href="https://www.databricks.com"
            target="_blank"
            rel="noreferrer"
            className="flex shrink-0 items-center gap-[10px] rounded focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#FF3621]/35"
          >
            <img src="/brand/databricks-symbol-white.svg" alt="" className="h-[19px] w-[17px]" />
            <span className="hidden flex-col gap-[1px] min-[1180px]:flex">
              <span className="text-[8.5px] font-semibold tracking-[.14em] text-[#E9EFED]/60">BUILT ON</span>
              <span className="text-[14px] font-semibold leading-none text-white">Databricks</span>
            </span>
          </a>

          <DeploymentBadgeFromApi />

          <span className="hidden h-[22px] w-px shrink-0 bg-white/[.16] min-[1180px]:block" aria-hidden="true" />

          <div className="hidden shrink-0 flex-col leading-none min-[1100px]:flex">
            <span className="text-[9px] font-semibold tracking-[.1em] text-[#E9EFED]/55">ACCOUNT</span>
            <AccountIdentifier value={accountInfo?.account_id || accountInfo?.account_name || "Loading…"} />
          </div>

          <WorkspaceFilter
            workspaces={wsFilterList}
            selectedIds={selectedWorkspaceIds}
            onChange={setSelectedWorkspaceIds}
            isLoading={wsListLoading}
            variant="rail"
          />
              <SourceLabelFilter variant="rail" onApplied={handleSourceApplied} />

          <div className="min-w-[2px] flex-1" />

          {user && (
            <>
              {authStatus && authStatus.identity !== "user_oauth" && (
                <>
                  {authStatus.sp_display_name && (
                    <span className="hidden h-6 shrink-0 items-center gap-1 rounded-full px-2 text-[10px] font-semibold text-green-200 min-[1440px]:inline-flex" style={{ background: "rgba(34,197,94,0.2)", border: "1px solid rgba(134,239,172,0.22)" }} title={authStatus.sp_display_name}>
                      <span className="healthy-status-dot h-1.5 w-1.5 rounded-full bg-green-400" />
                      <span className="font-mono">{authStatus.sp_display_name.slice(0, 8)}</span>
                      <span className="opacity-60">ID</span>
                    </span>
                  )}
                  {(authStatus.sp_user_name || authStatus.sp_client_id) && (
                    <span className="hidden h-6 shrink-0 items-center gap-1 rounded-full px-2 text-[10px] font-semibold text-green-200 min-[1440px]:inline-flex" style={{ background: "rgba(34,197,94,0.2)", border: "1px solid rgba(134,239,172,0.22)" }} title={authStatus.sp_user_name || authStatus.sp_client_id}>
                      <span className="healthy-status-dot h-1.5 w-1.5 rounded-full bg-green-400" />
                      <span className="font-mono">{(authStatus.sp_user_name || authStatus.sp_client_id || "").slice(0, 8)}</span>
                      <Bot className="h-3.5 w-3.5 opacity-70" aria-label="Service principal" />
                    </span>
                  )}
                </>
              )}
              {warehouseStatus && (
                <span
                  className="inline-flex h-6 shrink-0 items-center gap-1 rounded-full px-1.5 text-[10px] font-semibold text-green-200 min-[1280px]:px-2"
                  style={{ background: "rgba(34,197,94,0.2)", border: "1px solid rgba(134,239,172,0.22)" }}
                  title={`SQL Warehouse: ${warehouseStatus.state ?? warehouseStatus.status}`}
                >
                  <span
                    className={`${warehouseStatus.status === "warm" ? "healthy-status-dot " : ""}h-1.5 w-1.5 rounded-full`}
                    style={{ background: warehouseStatus.status === "warm" ? "var(--status-dot)" : warehouseStatus.status === "warming_up" ? "var(--amber)" : "var(--maroon)" }}
                  />
                  <span className="hidden min-[1280px]:inline">
                    {warehouseStatus.status === "warm" ? "Active" : warehouseStatus.status === "warming_up" ? "Starting" : "Offline"}
                  </span>
                  <span className="hidden opacity-60 min-[1536px]:inline">SQL</span>
                </span>
              )}
              <UserMenu
                name={user.name}
                email={user.email}
                isAdmin={user.role === "admin"}
                workspaceHost={accountInfo?.host}
              />
              <div className="flex shrink-0 items-center gap-[4px]">
                <button
                  type="button"
                  onClick={openExportDialog}
                  aria-label="Export"
                  className="rail-control-border inline-flex h-[32px] w-[32px] shrink-0 items-center justify-center rounded-[8px] border bg-[#2272B4] px-0 text-[12.5px] font-semibold text-white transition-colors hover:bg-[#1B5F96] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#4D98D0]/50 focus-visible:ring-offset-1 focus-visible:ring-offset-[#1B3139] min-[1100px]:w-auto min-[1100px]:gap-1.5 min-[1100px]:px-[11px]"
                  title="Export"
                >
                  <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                  </svg>
                  <span className="hidden min-[1100px]:inline">Export</span>
                </button>
                <button
                  type="button"
                  onClick={() => setShowSettings(true)}
                  className="flex h-[32px] w-[32px] shrink-0 items-center justify-center rounded-[6px] text-white opacity-80 transition-colors hover:bg-white/[.10] hover:opacity-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#FF3621]/35 focus-visible:ring-offset-1 focus-visible:ring-offset-[#1B3139]"
                  title="App Settings"
                  aria-label="App Settings"
                >
                  <Settings size={17} strokeWidth={1.8} aria-hidden="true" />
                </button>
              </div>
            </>
          )}
        </div>
      </div>

      {warehouseEverWarm && warehouseStatus && !warehouseReady && (
        <div
          role="status"
          className={`flex items-center justify-center gap-2 border-b px-4 py-1.5 text-xs font-medium ${
            warehouseStatus.status === "warming_up"
              ? "border-amber-200 bg-amber-50 text-amber-900"
              : "border-red-200 bg-red-50 text-red-800"
          }`}
        >
          {warehouseStatus.status === "warming_up" && <Spinner size="xs" />}
          <span>
            {warehouseStatus.status === "warming_up"
              ? "SQL Warehouse is starting. Dashboard queries are paused; showing the last loaded data."
              : "SQL Warehouse is temporarily unavailable. Dashboard queries are paused; showing the last loaded data."}
          </span>
        </div>
      )}
      {warehouseHealthCheckFailed && (
        <WarehouseHealthCheckBanner
          isRetrying={warehouseHealthChecking}
          onRetry={() => { void retryWarehouseHealthCheck(); }}
        />
      )}
      {warehouseStatus && (
        <WarehouseGuidanceBanner
          key={warehouseStatus.warehouse_id ?? "unbound"}
          warehouse={warehouseStatus}
          workspaceHost={accountInfo?.host}
        />
      )}
      <AccountPricingBanner />
      <SpGrantsBanner onOpenSettings={() => setShowSettings(true)} />

      <header className="bg-white">
        <div className="mx-auto max-w-7xl px-4 pt-8 pb-4 sm:px-6 lg:px-8">
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2">
              <button type="button" onClick={() => setActiveTab("dbu")} title="Back to DBU Overview" className="cursor-pointer transition-opacity hover:opacity-80">
                <CostObsLockup />
              </button>
              <VersionPill />
            </div>
            <div className="ml-auto">
              <DateRangePicker value={dateRange} onChange={setDateRange} />
            </div>
          </div>
          {/* Tab Navigation */}
          <div className="mt-4 overflow-x-auto overflow-y-hidden" style={{ borderBottom: "1px solid var(--hairline)" }}>
            <nav className="-mb-px flex min-w-max justify-center space-x-7 [&_svg]:h-3.75 [&_svg]:w-3.75">
              {tabVisibility.dbu && (
              <button
                onClick={() => setActiveTab("dbu")}
                className={`whitespace-nowrap border-b-2 px-1 py-3 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-gray-400 ${
                  activeTab === "dbu"
                    ? "border-lava text-lava"
                    : "border-transparent text-slate hover:bg-oat-page hover:text-ink"
                }`}
              >
                <span className="mr-1.5 -mt-0.5 inline-flex h-4 items-center justify-center font-mono text-base font-bold">$</span>
                DBU Overview
              </button>
              )}
              {tabVisibility.sql && (
              <button
                onClick={() => setActiveTab("sql")}
                className={`whitespace-nowrap border-b-2 px-1 py-3 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-gray-400 ${
                  activeTab === "sql"
                    ? "border-lava text-lava"
                    : "border-transparent text-slate hover:bg-oat-page hover:text-ink"
                }`}
              >
                <svg className="mr-2 -mt-0.5 inline h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 3v2m6-2v2M9 19v2m6-2v2M5 9H3m2 6H3m18-6h-2m2 6h-2M7 19h10a2 2 0 002-2V7a2 2 0 00-2-2H7a2 2 0 00-2 2v10a2 2 0 002 2zM9 9h6v6H9V9z" />
                </svg>
                SQL
              </button>
              )}
              {tabVisibility.aiml && (
              <button
                onClick={() => setActiveTab("aiml")}
                className={`whitespace-nowrap border-b-2 px-1 py-3 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-gray-400 ${
                  activeTab === "aiml"
                    ? "border-lava text-lava"
                    : "border-transparent text-slate hover:bg-oat-page hover:text-ink"
                }`}
              >
                <svg className="mr-2 -mt-0.5 inline h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                </svg>
                AI/ML
              </button>
              )}
              {tabVisibility.apps && (
              <button
                onClick={() => setActiveTab("apps")}
                className={`whitespace-nowrap border-b-2 px-1 py-3 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-gray-400 ${
                  activeTab === "apps"
                    ? "border-lava text-lava"
                    : "border-transparent text-slate hover:bg-oat-page hover:text-ink"
                }`}
              >
                <svg className="mr-2 -mt-0.5 inline h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z" />
                </svg>
                Apps
              </button>
              )}
              {tabVisibility.tagging && (
              <button
                onClick={() => setActiveTab("tagging")}
                className={`whitespace-nowrap border-b-2 px-1 py-3 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-gray-400 ${
                  activeTab === "tagging"
                    ? "border-lava text-lava"
                    : "border-transparent text-slate hover:bg-oat-page hover:text-ink"
                }`}
              >
                <svg className="mr-2 -mt-0.5 inline h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 7h.01M7 3h5c.512 0 1.024.195 1.414.586l7 7a2 2 0 010 2.828l-7 7a2 2 0 01-2.828 0l-7-7A1.994 1.994 0 013 12V7a4 4 0 014-4z" />
                </svg>
                Tagging
              </button>
              )}
              {tabVisibility["users-groups"] && (
              <button
                onClick={() => setActiveTab("users-groups")}
                className={`whitespace-nowrap border-b-2 px-1 py-3 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-gray-400 ${
                  activeTab === "users-groups"
                    ? "border-lava text-lava"
                    : "border-transparent text-slate hover:bg-oat-page hover:text-ink"
                }`}
              >
                <svg className="mr-2 -mt-0.5 inline h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
                </svg>
                Users
              </button>
              )}
              {tabVisibility.kpis && (
              <button
                onClick={() => setActiveTab("kpis")}
                className={`whitespace-nowrap border-b-2 px-1 py-3 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-gray-400 ${
                  activeTab === "kpis"
                    ? "border-lava text-lava"
                    : "border-transparent text-slate hover:bg-oat-page hover:text-ink"
                }`}
              >
                <svg className="mr-2 -mt-0.5 inline h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                </svg>
                KPIs & Trends
              </button>
              )}
              {tabVisibility.infra && (
              <button
                onClick={() => setActiveTab("infra")}
                className={`whitespace-nowrap border-b-2 px-1 py-3 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-gray-400 ${
                  activeTab === "infra"
                    ? "border-lava text-lava"
                    : "border-transparent text-slate hover:bg-oat-page hover:text-ink"
                }`}
              >
                <svg className="mr-2 -mt-0.5 inline h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 15a4 4 0 004 4h9a5 5 0 10-.1-9.999 5.002 5.002 0 10-9.78 2.096A4.001 4.001 0 003 15z" />
                </svg>
                Cloud Costs
              </button>
              )}
              {tabVisibility.optimizer && (
              <button
                onClick={() => setActiveTab("optimizer")}
                className={`whitespace-nowrap border-b-2 px-1 py-3 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-gray-400 ${
                  activeTab === "optimizer"
                    ? "border-lava text-lava"
                    : "border-transparent text-slate hover:bg-oat-page hover:text-ink"
                }`}
              >
                <svg className="mr-2 -mt-0.5 inline h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4" />
                </svg>
                Optimize
              </button>
              )}
            </nav>
          </div>
        </div>
      </header>
      </div>{/* end sticky top chrome */}

      <main className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
        <div key={activeTab} className="animate-fade-in relative">
        <TabRefreshRegion
          isLoading={showActiveTabLoading}
          isRefreshing={explicitRefreshingTab !== null}
          loadingSections={TAB_LOADING_SECTIONS[activeTab]}
          onRefresh={handleTabRefresh}
          refreshError={refreshErrors[activeTab]}
        >
        <Suspense fallback={
          <LoadingPanels sections={TAB_LOADING_SECTIONS[activeTab]} />
        }>
        {activeTab === "dbu" ? (
          shouldShowDbuSkeleton(warehouseQueriesAllowed, bundleLoading) ? (
            <LoadingPanels sections={[
              "Spend Over Time",
              "Spend by Product",
              "Spend by SKU",
              "Workspace Breakdown",
              "Jobs and Pipelines",
              "Interactive Compute",
            ]} />
          ) : (
          <TabErrorBoundary tabName="DBU Overview">
          <div className="space-y-6">
            <DBUMethodologyPanel />
            {/* Header */}
            <PageHero
              icon={<span className="font-mono text-xl font-bold">$</span>}
              title="DBU Overview"
              subtitle={
                <>
                  Databricks Unit consumption and cost breakdown
                  {_wsIds && _wsIds.length > 0 && (
                    <Chip kind="workspace">
                      {_wsIds.length === 1 ? (workspaceNameMap[_wsIds[0]] || _wsIds[0]) : `${_wsIds.length} workspaces`}
                    </Chip>
                  )}
                </>
              }
            />


            <SummaryCards
              data={summary}
              isLoading={bundleLoading}
              startDate={dateRange.startDate}
              endDate={dateRange.endDate}
              workspaceIds={_wsIds}
            />

            <SpendChart data={timeseries} isLoading={bundleLoading} />

            <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
              <ProductBreakdown data={products} isLoading={bundleLoading} workspaces={workspaces?.workspaces} dateRange={dateRange} workspaceNameMap={workspaceNameMap} />
              <SKUBreakdown data={skuBreakdown} isLoading={skuLoading} workspaces={workspaces?.workspaces} dateRange={dateRange} workspaceNameMap={workspaceNameMap} />
            </div>

            <WorkspaceTable data={workspaces} isLoading={bundleLoading} host={accountInfo?.host} workspaceNameMap={workspaceNameMap} />

            <InteractiveBreakdown data={interactiveBreakdown} isLoading={interactiveLoading} host={accountInfo?.host} />

            <PipelineObjectsTable data={pipelineObjects} isLoading={pipelineLoading} host={accountInfo?.host} />
          </div>
          </TabErrorBoundary>
          )
        ) : activeTab === "infra" ? (
          <TabErrorBoundary tabName="Cloud Costs">
          <CloudCostsView
            data={infraViewData}
            isLoading={infraBundleLoading}
            timeseriesData={infraViewTimeseries}
            timeseriesLoading={infraBundleLoading}
            host={accountInfo?.host}
            actualData={awsActualData}
            actualLoading={awsActualLoading}
            azureActualData={azureActualData}
            azureActualLoading={azureActualLoading}
            gcpActualData={gcpActualData}
            gcpActualLoading={gcpActualLoading}
            infraData={pricedInfraCosts}
            infraLoading={infraBundleLoading}
            infraTimeseriesData={infraCostsTimeseries}
            infraTimeseriesLoading={infraBundleLoading}
            startDate={dateRange.startDate}
            endDate={dateRange.endDate}
            detectedCloud={accountInfo?.cloud || undefined}
            workspaceNameMap={workspaceNameMap}
            workspaceIds={_wsIds}
            accountPricingApplied={useAccountPrices && accountPricesAvailable}
          />
          </TabErrorBoundary>
        ) : activeTab === "optimizer" ? (
          <TabErrorBoundary tabName="Optimize">
          <div className="space-y-6">
            <OptimizeMethodologyPanel />
            <PageHero
              icon={
                <svg fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4" />
                </svg>
              }
              title="Optimize"
              subtitle="Rightsizing recommendations and cost optimization insights"
            />
            <WarehouseIdleTimeView host={accountInfo?.host} startDate={dateRange.startDate} endDate={dateRange.endDate} workspaceIds={_wsIds} />
            <WarehouseRightsizingView host={accountInfo?.host} />
          </div>
          </TabErrorBoundary>
        ) : activeTab === "kpis" ? (
          <TabErrorBoundary tabName="KPIs & Trends">
          <PlatformKPIsView
            data={platformKPIs}
            isLoading={kpisLoading}
            isFetching={kpisBundleFetching}
            spendAnomalies={spendAnomalies}
            anomaliesLoading={anomaliesLoading}
            startDate={dateRange.startDate}
            endDate={dateRange.endDate}
            workspaceIds={_wsIds}
            workspaceNameMap={workspaceNameMap}
          />
          </TabErrorBoundary>
        ) : activeTab === "aiml" ? (
          <TabErrorBoundary tabName="AI/ML">
          <AIMLCostCenter
            data={aimlData}
            isLoading={aimlLoading}
            isError={aimlError}
            error={aimlErrorObj}
            onRetry={() => void refetchAiml()}
            startDate={dateRange.startDate}
            endDate={dateRange.endDate}
            host={accountInfo?.host}
            workspaceIds={_wsIds}
            workspaceNameMap={workspaceNameMap}
          />
          </TabErrorBoundary>
        ) : activeTab === "apps" ? (
          <TabErrorBoundary tabName="Apps">
          <AppsCostCenter
            data={appsData}
            isLoading={appsLoading}
            isError={appsError}
            error={appsErrorObj}
            onRetry={() => void refetchApps()}
            host={accountInfo?.host}
            startDate={dateRange.startDate}
            endDate={dateRange.endDate}
            workspaceIds={_wsIds}
            workspaceNameMap={workspaceNameMap}
          />
          </TabErrorBoundary>
        ) : activeTab === "tagging" ? (
          <TabErrorBoundary tabName="Tagging">
          <TaggingHub
            data={taggingData}
            isLoading={taggingLoading}
            host={accountInfo?.host}
            startDate={dateRange.startDate}
            endDate={dateRange.endDate}
            workspaceIds={_wsIds}
            workspaceNameMap={workspaceNameMap}
          />
          </TabErrorBoundary>
        ) : activeTab === "sql" ? (
          <TabErrorBoundary tabName="SQL">
          <SQLWarehousing360
            sqlBreakdownData={sqlBreakdown}
            queryData={dbsqlData ?? undefined}
            isLoading={sqlLoading || dbsqlLoading}
            isError={dbsqlError}
            topQueriesData={dbsqlTopQueriesData}
            topQueriesLoading={dbsqlTopQueriesLoading}
            host={accountInfo?.host}
            startDate={dateRange.startDate}
            endDate={dateRange.endDate}
            workspaceIds={_wsIds}
            workspaceNameMap={workspaceNameMap}
          />
          </TabErrorBoundary>
        ) : activeTab === "users-groups" ? (
          <TabErrorBoundary tabName="Users">
          <UsersGroups
            startDate={dateRange.startDate}
            endDate={dateRange.endDate}
            dateRange={dateRange}
            anonymizeUsers={appSettings.anonymizeUsers}
            workspaceIds={_wsIds}
            workspaceNameMap={workspaceNameMap}
          />
          </TabErrorBoundary>
        ) : null}
        </Suspense>
        </TabRefreshRegion>
        </div>
      </main>

      <Footer />

      <ExportDialog
        isOpen={showExportDialog}
        onClose={closeExportDialog}
        onExport={handleExport}
        enableArchitectureView={appSettings.enableArchitectureView}
        onExportArchitecture={handleArchitectureExport}
        tabVisibility={tabVisibility}
        dataLoading={exportDataLoading}
        dataErrors={tabErrors}
        onRetryFailed={retryFailedExportData}
      />

      <SettingsDialog
        isOpen={showSettings}
        onClose={() => setShowSettings(false)}
        tabVisibility={tabVisibility}
        appSettings={appSettings}
        onTabVisibilityChange={(v) => {
          setTabVisibility(v);
          // If the active tab was hidden, switch to the first visible tab.
          if (!v[activeTab as keyof typeof v]) {
            const firstVisible = (Object.keys(v) as ViewTab[]).find((k) => v[k as keyof typeof v]);
            if (firstVisible) setActiveTab(firstVisible);
          }
        }}
        onSettingsChange={setAppSettings}
      />
    </div>
    </SpNameMapContext.Provider>
  );
}

class TabErrorBoundary extends React.Component<
  { children: React.ReactNode; tabName?: string },
  { error: Error | null }
> {
  state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) { return { error }; }
  render() {
    if (this.state.error) {
      return (
        <div className="flex flex-col items-center justify-center py-16 px-8 rounded-lg bg-white border " style={{ borderColor: C.hairline }}>
          <div className="text-3xl mb-3">⚠️</div>
          <h3 className="text-lg font-semibold text-gray-900 mb-1">
            {this.props.tabName ? `${this.props.tabName} encountered an error` : "Something went wrong"}
          </h3>
          <p className="text-sm text-gray-500 text-center max-w-md mb-4">
            This may happen when data is loading or system tables are not accessible. Other tabs should still work.
          </p>
          <button
            onClick={() => this.setState({ error: null })}
            className="px-4 py-2 rounded-lg text-sm font-medium text-white"
            style={{ backgroundColor: C.lava }}
          >
            Try Again
          </button>
          <details className="mt-4 text-xs text-gray-500">
            <summary className="cursor-pointer">Error details</summary>
            <pre className="mt-2 whitespace-pre-wrap">{this.state.error.message}</pre>
          </details>
        </div>
      );
    }
    return this.props.children;
  }
}

class ErrorBoundary extends React.Component<
  { children: React.ReactNode },
  { error: Error | null }
> {
  state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: 40, fontFamily: "sans-serif", background: "#0f172a", color: "#e2e8f0", minHeight: "100vh" }}>
          <h1 style={{ color: "#f97316", marginBottom: 16 }}>Something went wrong</h1>
          <p style={{ color: "#94a3b8", marginBottom: 24 }}>The app encountered an error. This usually happens when data is still loading or system tables are not accessible.</p>
          <button
            onClick={() => { this.setState({ error: null }); window.location.reload(); }}
            style={{ padding: "10px 24px", background: "#f97316", color: "#000", border: "none", borderRadius: 8, cursor: "pointer", fontWeight: 600, marginBottom: 24 }}
          >
            Reload App
          </button>
          <details style={{ marginTop: 16 }}>
            <summary style={{ cursor: "pointer", color: "#64748b" }}>Error details</summary>
            <pre style={{ whiteSpace: "pre-wrap", fontSize: 12, color: "#64748b", marginTop: 8 }}>
              {this.state.error.message}
              {"\n\n"}
              {this.state.error.stack}
            </pre>
          </details>
        </div>
      );
    }
    return this.props.children;
  }
}

function App() {
  return (
    <ErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <PricingProvider>
          <Dashboard />
        </PricingProvider>
      </QueryClientProvider>
    </ErrorBoundary>
  );
}

export default App;
