import { QueryClient } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { TabRefreshRegion } from "@/components/TabRefreshRegion";
import {
  isQueryOwnedByTab,
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
});
