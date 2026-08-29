import { QueryClient, QueryObserver } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { TabRefreshRegion } from "@/components/TabRefreshRegion";
import {
  isQueryOwnedByTab,
  refreshSourceScopeData,
  refreshTabData,
  TAB_LOADING_SECTIONS,
} from "../tabRefresh";

describe("per-tab manual refresh", () => {
  it.each([
    ["dbu", ["billing", "dashboard-bundle-fast"]],
    ["sql", ["dbsql", "dashboard-bundle"]],
    ["infra", ["azure-actual", "dashboard-bundle"]],
    ["optimizer", ["warehouse-idle-time", "2026-01-01"]],
    ["kpis", ["kpis-platform-kpi-trend", "total_queries"]],
    ["aiml", ["aiml-kpi-trend", "aiml_spend"]],
    ["apps", ["apps-kpi-trend", "apps_spend"]],
    ["tagging", ["tagging-kpi-trend", "total_spend"]],
    ["users-groups", ["users-groups", "bundle"]],
  ] as const)("matches %s-owned queries", (tab, queryKey) => {
    expect(isQueryOwnedByTab(tab, queryKey)).toBe(true);
  });

  it("excludes global settings, account, and other-tab queries", () => {
    expect(isQueryOwnedByTab("dbu", ["unified-settings"])).toBe(false);
    expect(isQueryOwnedByTab("dbu", ["billing", "account"])).toBe(false);
    expect(isQueryOwnedByTab("dbu", ["dbsql", "dashboard-bundle"])).toBe(false);
    expect(isQueryOwnedByTab("infra", ["billing", "dashboard-bundle-fast"])).toBe(false);
    expect(isQueryOwnedByTab("sql", ["aiml-kpi-trend", "aiml_spend"])).toBe(false);
    expect(isQueryOwnedByTab("dbu", ["tagging-kpi-trend", "tagged_spend"])).toBe(false);
  });

  it.each([
    ["dbu", ["kpi-trend", "total_spend"]],
    ["sql", ["sql-platform-kpi-trend", "total_queries"]],
    ["infra", ["infra-kpi-trend", "infra_cost"]],
    ["kpis", ["kpis-platform-kpi-trend", "total_queries"]],
    ["aiml", ["aiml-kpi-trend", "aiml_spend"]],
    ["apps", ["apps-kpi-trend", "apps_spend"]],
    ["tagging", ["tagging-kpi-trend", "tagged_spend"]],
  ] as const)("assigns %s trend queries to one tab", (tab, queryKey) => {
    expect(isQueryOwnedByTab(tab, queryKey)).toBe(true);
    for (const other of ["dbu", "sql", "infra", "kpis", "aiml", "apps", "tagging"] as const) {
      if (other !== tab) expect(isQueryOwnedByTab(other, queryKey)).toBe(false);
    }
  });

  it("cancels and refetches only matching keys through the tab cache URL", async () => {
    const client = new QueryClient();
    const dbuQuery = client.getQueryCache().build(client, {
      queryKey: ["billing", "dashboard-bundle-fast"],
      queryFn: async () => ({}),
    });
    const settingsQuery = client.getQueryCache().build(client, {
      queryKey: ["unified-settings"],
      queryFn: async () => ({}),
    });
    const cancel = vi.spyOn(client, "cancelQueries").mockResolvedValue();
    const invalidate = vi.spyOn(client, "invalidateQueries").mockResolvedValue();
    const refetch = vi.spyOn(client, "refetchQueries").mockResolvedValue();
    const fetcher = vi.fn().mockResolvedValue({ ok: true, status: 200 });

    await refreshTabData(client, "dbu", fetcher);

    expect(fetcher).toHaveBeenCalledWith("/api/cache/clear?tab=dbu", { method: "POST" });
    const cancelOptions = cancel.mock.calls[0][0];
    const invalidateOptions = invalidate.mock.calls[0][0];
    const refetchOptions = refetch.mock.calls[0][0];
    expect(cancelOptions?.predicate?.(dbuQuery)).toBe(true);
    expect(cancelOptions?.predicate?.(settingsQuery)).toBe(false);
    expect(invalidateOptions?.refetchType).toBe("none");
    expect(invalidateOptions?.predicate?.(dbuQuery)).toBe(true);
    expect(invalidateOptions?.predicate?.(settingsQuery)).toBe(false);
    expect(refetchOptions?.type).toBe("active");
    expect(refetchOptions?.predicate?.(dbuQuery)).toBe(true);
    expect(refetchOptions?.predicate?.(settingsQuery)).toBe(false);
    expect(invalidate.mock.invocationCallOrder[0]).toBeLessThan(refetch.mock.invocationCallOrder[0]);
  });

  it("refreshes DBU immediately, leaves inactive tabs stale, then fetches them on demand", async () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: 60_000 } },
    });
    const dbuFetch = vi.fn(async () => ({ total_spend: 1 }));
    const appsFetch = vi.fn(async () => ({ apps: [] }));
    const dbuOptions = {
      queryKey: ["billing", "dashboard-bundle-fast", "scope-a"],
      queryFn: dbuFetch,
    };
    const appsOptions = {
      queryKey: ["apps", "dashboard-bundle", "scope-a"],
      queryFn: appsFetch,
    };
    const observer = new QueryObserver(client, dbuOptions);
    const unsubscribe = observer.subscribe(() => {});
    await client.ensureQueryData(dbuOptions);
    client.setQueryData(appsOptions.queryKey, { apps: ["stale"] });

    await refreshSourceScopeData(client, "dbu");

    expect(dbuFetch).toHaveBeenCalledTimes(2);
    expect(appsFetch).not.toHaveBeenCalled();
    expect(client.getQueryState(appsOptions.queryKey)?.isInvalidated).toBe(true);

    await client.fetchQuery(appsOptions);
    expect(appsFetch).toHaveBeenCalledTimes(1);
    unsubscribe();
  });

  it("keeps the button visible and replaces stale content with loading panels", async () => {
    render(
      <TabRefreshRegion
        isLoading
        isRefreshing
        loadingSections={TAB_LOADING_SECTIONS.optimizer}
        onRefresh={vi.fn()}
      >
        <div>Stale optimizer content</div>
      </TabRefreshRegion>,
    );

    expect(screen.getByRole("button", { name: "Refresh this tab" })).toBeVisible();
    expect(screen.queryByText("Stale optimizer content")).not.toBeInTheDocument();
    expect(screen.getByText("Idle Time Analysis")).toBeVisible();
    expect(screen.getByText("Rightsizing Recommendations")).toBeVisible();
  });

  it("describes the refresh as per-tab", async () => {
    render(
      <TabRefreshRegion
        isLoading={false}
        isRefreshing={false}
        loadingSections={[]}
        onRefresh={vi.fn()}
      >
        <div>Current content</div>
      </TabRefreshRegion>,
    );
    await userEvent.hover(screen.getByRole("button", { name: "Refresh this tab" }));
    expect(screen.getByText("Refresh this tab", { selector: "p" })).toBeVisible();
    expect(screen.getByText(/clears this tab's caches/)).toBeVisible();
  });

  it("keeps refresh failures visible and directly retryable", async () => {
    const onRefresh = vi.fn().mockResolvedValue(undefined);
    render(
      <TabRefreshRegion
        isLoading={false}
        isRefreshing={false}
        loadingSections={[]}
        onRefresh={onRefresh}
        refreshError="This tab could not be refreshed: warehouse unavailable"
      >
        <div>Current content</div>
      </TabRefreshRegion>,
    );

    expect(screen.getByRole("alert")).toHaveTextContent("warehouse unavailable");
    await userEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(onRefresh).toHaveBeenCalledOnce();
  });
});
