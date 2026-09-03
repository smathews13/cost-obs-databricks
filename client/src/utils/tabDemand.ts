import type { TabVisibility } from "@/utils/settingsHydration";
import type { Query, QueryClient } from "@tanstack/react-query";

export type DashboardTab = keyof TabVisibility;
export const DEFAULT_TAB_PRODUCER_LIMIT = 2;
export type TabDemandRefreshPhase = "waiting" | "fetching";

export interface TabDemandState {
  scopeKey: string;
  visited: DashboardTab[];
  active: DashboardTab[];
  queued: DashboardTab[];
  settled: DashboardTab[];
}

export function clearTabDemandRefreshPhases(
  current: Partial<Record<DashboardTab, TabDemandRefreshPhase>>,
  tabs: readonly DashboardTab[],
): Partial<Record<DashboardTab, TabDemandRefreshPhase>> {
  if (!tabs.some((tab) => current[tab])) return current;
  const next = { ...current };
  tabs.forEach((tab) => { delete next[tab]; });
  return next;
}

function uniqueTabs(tabs: readonly DashboardTab[]): DashboardTab[] {
  return Array.from(new Set(tabs));
}

function fillProducerSlots(
  active: DashboardTab[],
  queued: DashboardTab[],
  currentTab: DashboardTab,
  limit: number,
): Pick<TabDemandState, "active" | "queued"> {
  const nextActive = [...active];
  const nextQueued = [...queued];
  while (nextActive.length < limit && nextQueued.length > 0) {
    const currentIndex = nextQueued.indexOf(currentTab);
    const [next] = nextQueued.splice(currentIndex >= 0 ? currentIndex : 0, 1);
    if (!nextActive.includes(next)) nextActive.push(next);
  }
  return { active: nextActive, queued: nextQueued };
}

export function createTabDemandState(
  scopeKey: string,
  currentTab: DashboardTab,
): TabDemandState {
  return {
    scopeKey,
    visited: [currentTab],
    active: [currentTab],
    queued: [],
    settled: [],
  };
}

/**
 * Reconcile navigation/export demand without ever running more than `limit`
 * dashboard producers. A new scope requeues every visited visible tab, while
 * the current tab moves to the front of the waiting line.
 */
export function queueTabDemand(
  state: TabDemandState,
  {
    scopeKey,
    currentTab,
    visibleTabs,
    exportTabs = [],
  }: {
    scopeKey: string;
    currentTab: DashboardTab;
    visibleTabs: readonly DashboardTab[];
    exportTabs?: readonly DashboardTab[];
  },
  limit = DEFAULT_TAB_PRODUCER_LIMIT,
): TabDemandState {
  const visible = new Set(visibleTabs);
  const visited = uniqueTabs([...state.visited.filter((tab) => visible.has(tab)), currentTab]);
  const demanded = uniqueTabs([...visited, ...exportTabs.filter((tab) => visible.has(tab))]);

  if (state.scopeKey !== scopeKey) {
    const queued = demanded.filter((tab) => tab !== currentTab);
    const filled = fillProducerSlots([currentTab], queued, currentTab, limit);
    return { scopeKey, visited, settled: [], ...filled };
  }

  const settled = state.settled.filter((tab) => demanded.includes(tab));
  const active = state.active.filter(
    (tab) => demanded.includes(tab) && !settled.includes(tab),
  );
  const waiting = demanded.filter(
    (tab) => !settled.includes(tab) && !active.includes(tab),
  );
  const queued = uniqueTabs([
    ...(waiting.includes(currentTab) ? [currentTab] : []),
    ...state.queued.filter((tab) => waiting.includes(tab) && tab !== currentTab),
    ...waiting.filter((tab) => tab !== currentTab),
  ]);
  if (
    queued.includes(currentTab)
    && !active.includes(currentTab)
    && active.length >= limit
  ) {
    const oldestBackgroundIndex = active.findIndex((tab) => tab !== currentTab);
    if (oldestBackgroundIndex >= 0) {
      const [preempted] = active.splice(oldestBackgroundIndex, 1);
      const currentIndex = queued.indexOf(currentTab);
      queued.splice(currentIndex, 1);
      queued.unshift(currentTab);
      queued.push(preempted);
    }
  }
  const filled = fillProducerSlots(active, queued, currentTab, limit);
  return { scopeKey, visited, settled, ...filled };
}

export function settleTabDemand(
  state: TabDemandState,
  settledTabs: readonly DashboardTab[],
  currentTab: DashboardTab,
  limit = DEFAULT_TAB_PRODUCER_LIMIT,
): TabDemandState {
  const settled = uniqueTabs([...state.settled, ...settledTabs]);
  const active = state.active.filter((tab) => !settled.includes(tab));
  const queued = state.queued.filter((tab) => !settled.includes(tab));
  const filled = fillProducerSlots(active, queued, currentTab, limit);
  return { ...state, settled, ...filled };
}

/**
 * Mark selected tabs unresolved again. The current tab may take an active slot
 * immediately; any displaced producer remains queued and visually unresolved.
 */
export function requeueTabDemand(
  state: TabDemandState,
  tabs: readonly DashboardTab[],
  currentTab: DashboardTab,
  limit = DEFAULT_TAB_PRODUCER_LIMIT,
  markVisited = true,
): TabDemandState {
  const targets = uniqueTabs(tabs);
  if (targets.length === 0) return state;
  const settled = state.settled.filter((tab) => !targets.includes(tab));
  const visited = markVisited
    ? uniqueTabs([...state.visited, ...targets])
    : state.visited;
  const active = state.active.filter((tab) => !settled.includes(tab));
  let queued = uniqueTabs([
    ...(targets.includes(currentTab) && !active.includes(currentTab) ? [currentTab] : []),
    ...state.queued.filter((tab) => !settled.includes(tab)),
    ...targets.filter((tab) => tab !== currentTab && !active.includes(tab)),
  ]).filter((tab) => !active.includes(tab));

  if (
    targets.includes(currentTab)
    && !active.includes(currentTab)
    && active.length >= limit
  ) {
    const demoted = active.pop();
    if (demoted) queued = uniqueTabs([currentTab, demoted, ...queued]);
  }
  const filled = fillProducerSlots(active, queued, currentTab, limit);
  return { ...state, visited, settled, ...filled };
}

export function isTabProducerActive(
  state: TabDemandState,
  tab: DashboardTab,
): boolean {
  return state.active.includes(tab);
}

export function isTabDemandUnresolved(
  state: TabDemandState,
  tab: DashboardTab,
): boolean {
  return (
    (state.visited.includes(tab) || state.active.includes(tab) || state.queued.includes(tab))
    && !state.settled.includes(tab)
  );
}

export function buildExportScopeKey(
  startDate: string,
  endDate: string,
  workspaceIds: string[],
  sourceVersion: number,
  visibleTabs: DashboardTab[],
  includeHistoricalWorkspaces = true,
): string {
  return JSON.stringify([
    startDate,
    endDate,
    [...workspaceIds].sort(),
    sourceVersion,
    includeHistoricalWorkspaces,
    [...visibleTabs].sort(),
  ]);
}

export function isTabDataRequested(
  tab: DashboardTab,
  activeTab: DashboardTab,
  exportTabs: readonly DashboardTab[] = [],
  visitedTabs: ReadonlySet<DashboardTab> = new Set(),
): boolean {
  return activeTab === tab || exportTabs.includes(tab) || visitedTabs.has(tab);
}

const SUBMIT_AND_POLL_TAB_KEYS: Partial<Record<DashboardTab, readonly [string, string]>> = {
  apps: ["apps", "dashboard-bundle"],
  aiml: ["aiml", "dashboard-bundle"],
  sql: ["dbsql", "dashboard-bundle"],
};

export function isRunningSubmitAndPollQuery(tab: DashboardTab, query: Query): boolean {
  const ownerKey = SUBMIT_AND_POLL_TAB_KEYS[tab];
  return Boolean(
    ownerKey
    && query.queryKey[0] === ownerKey[0]
    && query.queryKey[1] === ownerKey[1]
    && query.state.fetchStatus === "fetching",
  );
}

/**
 * Stop only an in-flight submit-and-poll request when its tab is left. React
 * Query retains any settled data already in the cache.
 */
export async function cancelRunningSubmitAndPollForTab(
  queryClient: QueryClient,
  tab: DashboardTab,
): Promise<void> {
  if (!SUBMIT_AND_POLL_TAB_KEYS[tab]) return;
  await queryClient.cancelQueries({
    predicate: (query) => isRunningSubmitAndPollQuery(tab, query),
  });
}

export async function cancelExportPreparationQueries(
  queryClient: QueryClient,
  preserveTab?: DashboardTab,
): Promise<void> {
  const exportPollTabs = Object.keys(SUBMIT_AND_POLL_TAB_KEYS) as DashboardTab[];
  await queryClient.cancelQueries({
    predicate: (query) => exportPollTabs.some(
      (tab) => tab !== preserveTab && isRunningSubmitAndPollQuery(tab, query),
    ),
  });
}
