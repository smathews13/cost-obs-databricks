import { useQuery } from "@tanstack/react-query";
import type {
  AccountInfo,
  AWSCostsResponse,
  AWSActualDashboardBundle,
  AzureActualDashboardBundle,
  GCPActualDashboardBundle,
  BillingSummary,
  ProductBreakdownResponse,
  WorkspaceBreakdownResponse,
  TimeseriesResponse,
  GranularBreakdownResponse,
  PipelineObjectsResponse,
  InteractiveBreakdownResponse,
  SKUBreakdownResponse,
  SpendAnomaliesResponse,
  PlatformKPIsResponse,
  DashboardBundleFast,
  DateRange,
  AIMLDashboardBundle,
  AppsDashboardBundle,
  TaggingDashboardBundle,
  DBSQLDashboardBundle,
  InfraCostsResponse,
  InfraCostsTimeseriesResponse,
  InfraBundleResponse,
  KPIsBundleResponse,
} from "@/types/billing";

function formatLocalDate(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function getDefaultEnd(): Date {
  // Buffer by one day: today's cost data is incomplete/inaccurate
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  return yesterday;
}

// Billing data changes infrequently; 5 min staleTime prevents
// unnecessary refetches on tab focus / component remount.
const STALE_TIME = 5 * 60 * 1000;

export function responsePayloadIssue(
  payload: unknown,
  requireAvailable = false,
): string | undefined {
  if (!payload || typeof payload !== "object") return undefined;
  const record = payload as Record<string, unknown>;
  for (const key of ["error", "_error"] as const) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) return value;
    if (value && typeof value === "object") return JSON.stringify(value);
  }
  if (requireAvailable && record.available === false) {
    for (const key of ["reason_detail", "message", "reason"] as const) {
      const value = record[key];
      if (typeof value === "string" && value.trim()) return value;
    }
    return "Required data is unavailable.";
  }
  return undefined;
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const body = await response.json();
      if (body?.detail) detail = `${response.status}: ${body.detail}`;
    } catch { /* ignore parse errors */ }
    throw new Error(detail);
  }
  const data = await response.json() as T;
  const issue = responsePayloadIssue(data);
  if (issue) throw new Error(issue);
  return data;
}

// Active MV source-label selection, set by the top-nav SourceLabelFilter. Empty
// means "all sources" (no filter). Appended to every data URL so the backend can
// narrow MV reads by source; changing it invalidates queries to force a refetch.
let _activeSourceLabels: string[] = [];
export function setActiveSourceLabels(labels: string[]): void {
  _activeSourceLabels = Array.from(new Set(labels ?? [])).filter(Boolean).sort();
}

export function getActiveSourceLabels(): string[] {
  return [..._activeSourceLabels];
}

export function getActiveSourceScopeKey(): string {
  return _activeSourceLabels.join("\u0001");
}

export function getWorkspaceScopeKey(workspaceIds?: string[]): string {
  return workspaceIds?.length ? [...workspaceIds].sort().join(",") : "";
}

function scopedQueryKey(...parts: unknown[]) {
  return [...parts, getActiveSourceScopeKey()];
}

/**
 * Add the dashboard's active source/workspace scope to an API URL. Source labels
 * remain repeated query parameters so labels containing commas round-trip.
 */
export function buildFilteredUrl(
  endpoint: string,
  params: URLSearchParams = new URLSearchParams(),
  workspaceIds?: string[],
): string {
  const scoped = new URLSearchParams(params);
  for (const label of _activeSourceLabels) {
    scoped.append("source_labels", label);
  }
  const wsKey = getWorkspaceScopeKey(workspaceIds);
  if (wsKey) scoped.set("workspace_ids", wsKey);
  const queryString = scoped.toString();
  return queryString ? `${endpoint}?${queryString}` : endpoint;
}

function buildUrl(endpoint: string, dateRange?: DateRange): string {
  const params = new URLSearchParams();
  if (dateRange?.startDate) {
    params.set("start_date", dateRange.startDate);
  }
  if (dateRange?.endDate) {
    params.set("end_date", dateRange.endDate);
  }
  return buildFilteredUrl(endpoint, params);
}

function buildUrlWithWs(endpoint: string, dateRange?: DateRange, workspaceIds?: string[]): string {
  const params = new URLSearchParams();
  if (dateRange?.startDate) params.set("start_date", dateRange.startDate);
  if (dateRange?.endDate) params.set("end_date", dateRange.endDate);
  return buildFilteredUrl(endpoint, params, workspaceIds);
}


export function useBillingSummary(dateRange?: DateRange) {
  return useQuery<BillingSummary>({
    queryKey: scopedQueryKey("billing", "summary", dateRange),
    queryFn: () => fetchJson(buildUrl("/api/billing/summary", dateRange)),
    staleTime: STALE_TIME,
  });
}

export function useBillingByProduct(dateRange?: DateRange) {
  return useQuery<ProductBreakdownResponse>({
    queryKey: scopedQueryKey("billing", "by-product", dateRange),
    queryFn: () => fetchJson(buildUrl("/api/billing/by-product", dateRange)),
    staleTime: STALE_TIME,
  });
}

export function useBillingByWorkspace(dateRange?: DateRange) {
  return useQuery<WorkspaceBreakdownResponse>({
    queryKey: scopedQueryKey("billing", "by-workspace", dateRange),
    queryFn: () => fetchJson(buildUrl("/api/billing/by-workspace", dateRange)),
    staleTime: STALE_TIME,
  });
}

export function useBillingTimeseries(dateRange?: DateRange) {
  return useQuery<TimeseriesResponse>({
    queryKey: scopedQueryKey("billing", "timeseries", dateRange),
    queryFn: () => fetchJson(buildUrl("/api/billing/timeseries", dateRange)),
    staleTime: STALE_TIME,
  });
}

export function useSqlBreakdown(dateRange?: DateRange, workspaceIds?: string[], enabled: boolean = true) {
  return useQuery<GranularBreakdownResponse>({
    queryKey: scopedQueryKey("billing", "sql-breakdown", dateRange, getWorkspaceScopeKey(workspaceIds)),
    queryFn: () => fetchJson(buildUrlWithWs("/api/billing/sql-breakdown", dateRange, workspaceIds)),
    staleTime: STALE_TIME,
    enabled,
  });
}

export function useEtlBreakdown(dateRange?: DateRange, enabled: boolean = true) {
  return useQuery<GranularBreakdownResponse>({
    queryKey: scopedQueryKey("billing", "etl-breakdown", dateRange),
    queryFn: () => fetchJson(buildUrl("/api/billing/etl-breakdown", dateRange)),
    staleTime: STALE_TIME,
    enabled,
  });
}

export function usePipelineObjects(dateRange?: DateRange, workspaceIds?: string[], enabled: boolean = true) {
  return useQuery<PipelineObjectsResponse>({
    queryKey: scopedQueryKey("billing", "pipeline-objects", dateRange, getWorkspaceScopeKey(workspaceIds)),
    queryFn: () => fetchJson(buildUrlWithWs("/api/billing/pipeline-objects", dateRange, workspaceIds)),
    staleTime: STALE_TIME,
    enabled,
  });
}

export function useInteractiveBreakdown(dateRange?: DateRange, workspaceIds?: string[], enabled: boolean = true) {
  return useQuery<InteractiveBreakdownResponse>({
    queryKey: scopedQueryKey("billing", "interactive-breakdown", dateRange, getWorkspaceScopeKey(workspaceIds)),
    queryFn: () => fetchJson(buildUrlWithWs("/api/billing/interactive-breakdown", dateRange, workspaceIds)),
    staleTime: STALE_TIME,
    enabled,
  });
}

export function useAWSCosts(dateRange?: DateRange, enabled: boolean = true) {
  return useQuery<AWSCostsResponse>({
    queryKey: scopedQueryKey("billing", "aws-costs", dateRange),
    queryFn: () => fetchJson(buildUrl("/api/billing/aws-costs", dateRange)),
    staleTime: STALE_TIME,
    enabled,
  });
}

export function useAWSCostsTimeseries(dateRange?: DateRange, enabled: boolean = true) {
  return useQuery<TimeseriesResponse>({
    queryKey: scopedQueryKey("billing", "aws-costs-timeseries", dateRange),
    queryFn: () => fetchJson(buildUrl("/api/billing/aws-costs-timeseries", dateRange)),
    staleTime: STALE_TIME,
    enabled,
  });
}

/**
 * Multi-cloud infrastructure costs - automatically detects AWS or Azure
 */
export function useInfraCosts(dateRange?: DateRange, enabled: boolean = true) {
  return useQuery<InfraCostsResponse>({
    queryKey: scopedQueryKey("billing", "infra-costs", dateRange),
    queryFn: () => fetchJson(buildUrl("/api/billing/infra-costs", dateRange)),
    staleTime: STALE_TIME,
    enabled,
  });
}

/**
 * Multi-cloud infrastructure costs timeseries
 */
export function useInfraCostsTimeseries(dateRange?: DateRange, enabled: boolean = true) {
  return useQuery<InfraCostsTimeseriesResponse>({
    queryKey: scopedQueryKey("billing", "infra-costs-timeseries", dateRange),
    queryFn: () => fetchJson(buildUrl("/api/billing/infra-costs-timeseries", dateRange)),
    staleTime: STALE_TIME,
    enabled,
  });
}

export function useAccountInfo() {
  // Fast call: returns instantly from host URL detection (no SQL)
  const fast = useQuery<AccountInfo>({
    queryKey: ["billing", "account"],
    queryFn: () => fetchJson("/api/billing/account"),
    staleTime: Infinity,
  });

  // Slow call: backfills account_id from billing data (may take seconds)
  const details = useQuery<{ account_id: string | null; cloud: string | null }>({
    queryKey: ["billing", "account-details"],
    queryFn: () => fetchJson("/api/billing/account-details"),
    staleTime: Infinity,
    enabled: !!fast.data,
  });

  // Merge: fast data + account_id from slow query
  const merged = fast.data ? {
    ...fast.data,
    account_id: details.data?.account_id || fast.data.account_id,
    cloud: details.data?.cloud || fast.data.cloud,
  } : undefined;

  return { ...fast, data: merged as AccountInfo | undefined };
}

export function getDefaultDateRange(days: number = 30): DateRange {
  const inclusiveDays = Math.max(1, Math.trunc(days));
  const end = getDefaultEnd();
  const start = new Date(end);
  start.setDate(start.getDate() - (inclusiveDays - 1));
  return {
    startDate: formatLocalDate(start),
    endDate: formatLocalDate(end),
  };
}

export function useSKUBreakdown(dateRange?: DateRange, workspaceIds?: string[], enabled: boolean = true) {
  return useQuery<SKUBreakdownResponse>({
    queryKey: scopedQueryKey("billing", "sku-breakdown", dateRange, getWorkspaceScopeKey(workspaceIds)),
    queryFn: () => fetchJson(buildUrlWithWs("/api/billing/sku-breakdown", dateRange, workspaceIds)),
    staleTime: STALE_TIME,
    enabled,
  });
}

// ── Users & Groups types ──────────────────────────────────────────────────────

export interface UserSpend {
  user_email: string;
  total_spend: number;
  total_dbus: number;
  active_days: number;
  workspace_count: number;
  percentage: number;
  primary_product: string;
  products: { product: string; spend: number }[];
}

export interface UsersGroupsBundle {
  available?: boolean;
  availability?: "available" | "partial" | "unavailable";
  reason?: string;
  reason_detail?: string;
  summary: {
    user_count: number;
    workspace_count: number;
    total_spend: number;
    total_dbus: number;
    avg_spend_per_user: number;
    spend_growth_pct: number | null;
  };
  top_users: UserSpend[];
  timeseries: Array<{ date: string; [user: string]: string | number }>;
  timeseries_users: string[];
  by_workspace: { workspace_id: string; user_count: number; total_spend: number }[];
  user_growth: UserGrowthPoint[];
  start_date: string;
  end_date: string;
}

export function useUsersGroupsBundle(dateRange?: DateRange, workspaceIds?: string[], enabled: boolean = true) {
  return useQuery<UsersGroupsBundle>({
    queryKey: scopedQueryKey("users-groups", "bundle", dateRange, getWorkspaceScopeKey(workspaceIds)),
    queryFn: () => fetchJson(buildUrlWithWs("/api/users-groups/bundle", dateRange, workspaceIds)),
    staleTime: STALE_TIME,
    enabled,
  });
}

export interface UserGrowthPoint {
  month: string;
  active_users: number;
  new_users: number;
}

export function useUserGrowth() {
  return useQuery<{ data: UserGrowthPoint[] }>({
    queryKey: ["users-groups", "growth"],
    queryFn: () => fetchJson("/api/users-groups/user-growth"),
    staleTime: STALE_TIME,
  });
}

export interface ReportConfig {
  weekly_reports: Array<{
    id: string;
    email: string;
    name: string;
    send_day: string;
    enabled: boolean;
    created_at: string;
  }>;
  user_alerts: Array<{
    id: string;
    email: string;
    name: string;
    threshold_amount: number | null;
    spike_percent: number | null;
    enabled: boolean;
    created_at: string;
  }>;
}

export function useReportConfig() {
  return useQuery<ReportConfig>({
    queryKey: ["users-groups", "report-config"],
    queryFn: () => fetchJson("/api/users-groups/report-config"),
    staleTime: 60 * 1000,
  });
}

export function useSpendAnomalies(dateRange?: DateRange, enabled: boolean = true) {
  return useQuery<SpendAnomaliesResponse>({
    queryKey: scopedQueryKey("billing", "spend-anomalies", dateRange),
    queryFn: () => fetchJson(buildUrl("/api/billing/spend-anomalies", dateRange)),
    staleTime: STALE_TIME,
    enabled,
  });
}

export function usePlatformKPIs(dateRange?: DateRange, enabled: boolean = true) {
  return useQuery<PlatformKPIsResponse>({
    queryKey: scopedQueryKey("billing", "platform-kpis", dateRange),
    queryFn: () => fetchJson(buildUrl("/api/billing/platform-kpis", dateRange)),
    staleTime: STALE_TIME,
    enabled,
  });
}

/**
 * Fast dashboard bundle - optimized for quick initial page load.
 * Skips slow query.history joins for 5-10x faster load times.
 * Use this for initial load, then lazy-load detailed breakdowns.
 */
export function useDashboardBundleFast(dateRange?: DateRange, workspaceIds?: string[], enabled: boolean = true) {
  const wsKey = getWorkspaceScopeKey(workspaceIds);
  return useQuery<DashboardBundleFast>({
    queryKey: scopedQueryKey("billing", "dashboard-bundle-fast", dateRange, wsKey),
    queryFn: () => {
      const base = buildUrl("/api/billing/dashboard-bundle-fast", dateRange);
      const url = wsKey
        ? `${base}${base.includes("?") ? "&" : "?"}workspace_ids=${encodeURIComponent(wsKey)}`
        : base;
      return fetchJson(url);
    },
    staleTime: 5 * 60 * 1000,
    enabled,
  });
}

const POLL_INTERVAL_MS = 2000;
const POLL_TIMEOUT_MS = 3 * 60 * 1000;

interface SubmitAndPollOptions {
  timeoutMs?: number;
  defaultIntervalMs?: number;
}

function abortError(): DOMException {
  return new DOMException("The request was aborted", "AbortError");
}

function retryDelayMs(response: Response, defaultIntervalMs: number): number {
  const retryAfter = response.headers.get("Retry-After")?.trim();
  if (!retryAfter) return defaultIntervalMs;

  const seconds = Number(retryAfter);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return seconds * 1000;
  }

  const retryAt = Date.parse(retryAfter);
  return Number.isNaN(retryAt)
    ? defaultIntervalMs
    : Math.max(0, retryAt - Date.now());
}

function abortableDelay(delayMs: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(abortError());

  return new Promise((resolve, reject) => {
    const timer = window.setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, delayMs);
    const onAbort = () => {
      window.clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      reject(abortError());
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

function timeoutError(url: string, timeoutMs: number): Error {
  const duration = timeoutMs % 60_000 === 0
    ? `${timeoutMs / 60_000} minute${timeoutMs === 60_000 ? "" : "s"}`
    : `${Math.ceil(timeoutMs / 1000)} seconds`;
  return new Error(`Timed out waiting for ${url} after ${duration}. Please retry.`);
}

/**
 * Submit a bundle request and keep polling the same URL while the server returns
 * 202. One query invocation owns the whole poll lifecycle, so React Query can
 * deduplicate it by key and cancel both fetches and delays through its signal.
 */
export async function fetchSubmitAndPoll<T>(
  url: string,
  signal?: AbortSignal,
  options: SubmitAndPollOptions = {},
): Promise<T> {
  const timeoutMs = options.timeoutMs ?? POLL_TIMEOUT_MS;
  const defaultIntervalMs = options.defaultIntervalMs ?? POLL_INTERVAL_MS;
  if (signal?.aborted) throw abortError();

  const operation = new AbortController();
  const onAbort = () => operation.abort();
  signal?.addEventListener("abort", onAbort, { once: true });
  const timeout = window.setTimeout(() => operation.abort(), timeoutMs);

  try {
    while (true) {
      const response = await fetch(url, { signal: operation.signal });
      if (response.status === 202) {
        await abortableDelay(retryDelayMs(response, defaultIntervalMs), operation.signal);
        continue;
      }
      if (!response.ok) {
        let detail = `${response.status} ${response.statusText}`;
        try {
          const body = await response.json();
          if (body?.detail) detail = `${response.status}: ${body.detail}`;
        } catch { /* ignore parse errors */ }
        throw new Error(detail);
      }
      const data = await response.json() as T;
      const issue = responsePayloadIssue(data);
      if (issue) throw new Error(issue);
      return data;
    }
  } catch (error) {
    if (signal?.aborted) throw abortError();
    if (operation.signal.aborted) throw timeoutError(url, timeoutMs);
    throw error;
  } finally {
    window.clearTimeout(timeout);
    signal?.removeEventListener("abort", onAbort);
  }
}

/**
 * AI/ML 360 dashboard bundle: one abortable request polls until data is ready.
 */
export function useAIMLDashboardBundle(dateRange?: DateRange, workspaceIds?: string[], enabled: boolean = true) {
  return useQuery<AIMLDashboardBundle>({
    queryKey: scopedQueryKey("aiml", "dashboard-bundle", dateRange, getWorkspaceScopeKey(workspaceIds)),
    queryFn: ({ signal }) =>
      fetchSubmitAndPoll<AIMLDashboardBundle>(
        buildUrlWithWs("/api/aiml/dashboard-bundle", dateRange, workspaceIds),
        signal,
      ),
    enabled,
    retry: false,
  });
}

/**
 * Apps dashboard bundle: one abortable request polls until data is ready.
 */
export function useAppsDashboardBundle(dateRange?: DateRange, workspaceIds?: string[], enabled: boolean = true) {
  return useQuery<AppsDashboardBundle>({
    queryKey: scopedQueryKey("apps", "dashboard-bundle", dateRange, getWorkspaceScopeKey(workspaceIds)),
    queryFn: ({ signal }) =>
      fetchSubmitAndPoll<AppsDashboardBundle>(
        buildUrlWithWs("/api/apps/dashboard-bundle", dateRange, workspaceIds),
        signal,
      ),
    enabled,
    retry: false,
  });
}

/**
 * Tagging Hub dashboard bundle
 * @param dateRange - Date range for the query
 * @param enabled - Whether to enable the query (set false when tab not active)
 */
export function useTaggingDashboardBundle(dateRange?: DateRange, workspaceIds?: string[], enabled: boolean = true) {
  return useQuery<TaggingDashboardBundle>({
    queryKey: scopedQueryKey("tagging", "dashboard-bundle", dateRange, getWorkspaceScopeKey(workspaceIds)),
    queryFn: () => fetchJson(buildUrlWithWs("/api/tagging/dashboard-bundle", dateRange, workspaceIds)),
    staleTime: 5 * 60 * 1000,
    enabled,
  });
}

/**
 * AWS Actual Costs - from CUR 2.0 data when available
 * @param dateRange - Date range for the query
 * @param enabled - Whether to enable the query (set false when tab not active)
 */
export function useAWSActualCosts(dateRange?: DateRange, enabled: boolean = true) {
  return useQuery<AWSActualDashboardBundle>({
    queryKey: scopedQueryKey("aws-actual", "dashboard-bundle", dateRange),
    queryFn: () =>
      fetchJson(buildUrl("/api/aws-actual/dashboard-bundle", dateRange)),
    staleTime: 5 * 60 * 1000,
    enabled,
  });
}

export function useAzureActualCosts(dateRange?: DateRange, enabled: boolean = true) {
  return useQuery<AzureActualDashboardBundle>({
    queryKey: scopedQueryKey("azure-actual", "dashboard-bundle", dateRange),
    queryFn: () =>
      fetchJson(buildUrl("/api/azure-actual/dashboard-bundle", dateRange)),
    staleTime: 5 * 60 * 1000,
    enabled,
  });
}

export function useGCPActualCosts(dateRange?: DateRange, enabled: boolean = true) {
  return useQuery<GCPActualDashboardBundle>({
    queryKey: scopedQueryKey("gcp-actual", "dashboard-bundle", dateRange),
    queryFn: () =>
      fetchJson(buildUrl("/api/gcp-actual/dashboard-bundle", dateRange)),
    staleTime: 5 * 60 * 1000,
    enabled,
  });
}

/**
 * DBSQL 360 dashboard bundle: submit-and-poll within one abortable query.
 * After a deploy the cost-per-query table can take a couple of minutes to appear, so
 * available=false is polled briefly, then treated as a settled "not configured" result.
 */
const UNAVAILABLE_POLL_MS = 3 * 60 * 1000;
const dbsqlUnavailableSince = new Map<string, number>();

export function useDBSQLQueryCosts(dateRange?: DateRange, workspaceIds?: string[], enabled: boolean = true) {
  const result = useQuery<DBSQLDashboardBundle>({
    queryKey: scopedQueryKey("dbsql", "dashboard-bundle", dateRange, getWorkspaceScopeKey(workspaceIds)),
    queryFn: ({ signal }) =>
      fetchSubmitAndPoll<DBSQLDashboardBundle>(
        buildUrlWithWs("/api/dbsql/dashboard-bundle", dateRange, workspaceIds),
        signal,
      ),
    staleTime: 5 * 60 * 1000,
    enabled,
    retry: false,
    refetchInterval: (q) => {
      const key = JSON.stringify(q.queryKey);
      if (q.state.error) {
        dbsqlUnavailableSince.delete(key);
        return false;
      }
      if (q.state.data?.available === false) {
        const started = dbsqlUnavailableSince.get(key) ?? Date.now();
        dbsqlUnavailableSince.set(key, started);
        return Date.now() - started < UNAVAILABLE_POLL_MS ? POLL_INTERVAL_MS : false;
      }
      dbsqlUnavailableSince.delete(key);
      return false;
    },
  });
  return result;
}

export function useDBSQLTopQueries(dateRange?: DateRange, workspaceIds?: string[], enabled: boolean = true) {
  return useQuery<import("@/types/billing").TopQueriesResponse>({
    queryKey: scopedQueryKey("dbsql", "top-queries", dateRange, getWorkspaceScopeKey(workspaceIds)),
    queryFn: () =>
      fetchJson(buildUrlWithWs("/api/dbsql/top-queries", dateRange, workspaceIds)),
    staleTime: 5 * 60 * 1000,
    enabled,
  });
}

/**
 * Bundled infrastructure costs - fetches clusters, instance families, and timeseries
 * in a single request with server-side parallel execution.
 */
export function useInfraBundle(dateRange?: DateRange, workspaceIds?: string[], enabled: boolean = true) {
  return useQuery<InfraBundleResponse>({
    queryKey: scopedQueryKey("billing", "infra-bundle", dateRange, getWorkspaceScopeKey(workspaceIds)),
    queryFn: () => fetchJson(buildUrlWithWs("/api/billing/infra-bundle", dateRange, workspaceIds)),
    staleTime: 5 * 60 * 1000,
    enabled,
  });
}

/**
 * Bundled KPIs - fetches platform KPIs and spend anomalies
 * in a single request with server-side parallel execution.
 */
export function useKPIsBundle(dateRange?: DateRange, workspaceIds?: string[], enabled: boolean = true) {
  return useQuery<KPIsBundleResponse>({
    queryKey: scopedQueryKey("billing", "kpis-bundle", dateRange, getWorkspaceScopeKey(workspaceIds)),
    queryFn: () => fetchJson(buildUrlWithWs("/api/billing/kpis-bundle", dateRange, workspaceIds)),
    staleTime: 5 * 60 * 1000,
    enabled,
  });
}
