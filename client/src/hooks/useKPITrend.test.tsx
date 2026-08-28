import type { ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { setActiveSourceLabels } from "./useBillingData";
import {
  useAppsKPITrend,
  useKPITrend,
  usePlatformKPITrend,
} from "./useKPITrend";

const responseBody = {
  kpi: "total_spend",
  granularity: "daily",
  data_points: [],
  summary: {
    period_start_value: 0,
    period_end_value: 0,
    change_amount: 0,
    change_percent: 0,
    min_value: 0,
    max_value: 0,
    avg_value: 0,
    trend: "flat",
  },
};

function setup() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  return { client, wrapper };
}

afterEach(() => {
  setActiveSourceLabels([]);
  vi.unstubAllGlobals();
});

describe("KPI trend scope", () => {
  it("includes source labels and sorted workspaces in billing trend URLs and keys", async () => {
    setActiveSourceLabels(["west,shared", "east"]);
    const fetchMock = vi.fn(async () => new Response(JSON.stringify(responseBody), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const { client, wrapper } = setup();

    const { result } = renderHook(
      () => useKPITrend("total_spend", "2026-08-01", "2026-08-28", "daily", ["2", "1"]),
      { wrapper },
    );
    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    const url = new URL(String(fetchMock.mock.calls[0][0]), "https://example.test");
    expect(url.searchParams.getAll("source_labels")).toEqual(["east", "west,shared"]);
    expect(url.searchParams.get("workspace_ids")).toBe("1,2");
    expect(url.searchParams.get("tab")).toBe("dbu");
    expect(client.getQueryCache().getAll()[0].queryKey).toEqual([
      "kpi-trend", "total_spend", "2026-08-01", "2026-08-28", "daily",
      "1,2", "east\u0001west,shared",
    ]);
  });

  it("scopes platform and Apps trends to their owner and current filters", async () => {
    setActiveSourceLabels(["west"]);
    const fetchMock = vi.fn(async () => new Response(JSON.stringify(responseBody), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const platform = setup();
    const apps = setup();

    const platformHook = renderHook(
      () => usePlatformKPITrend("total_queries", "2026-08-01", "2026-08-28", "daily", ["7"], "sql-platform-kpi-trend"),
      { wrapper: platform.wrapper },
    );
    const appsHook = renderHook(
      () => useAppsKPITrend("apps_spend", "2026-08-01", "2026-08-28", "daily", ["7"]),
      { wrapper: apps.wrapper },
    );
    await waitFor(() => {
      expect(platformHook.result.current.isSuccess).toBe(true);
      expect(appsHook.result.current.isSuccess).toBe(true);
    });

    const urls = fetchMock.mock.calls.map((call) => new URL(String(call[0]), "https://example.test"));
    const platformUrl = urls.find((url) => url.pathname.includes("platform-kpi-trend"));
    const appsUrl = urls.find((url) => url.pathname.includes("/api/apps/kpi-trend"));
    expect(platformUrl?.searchParams.get("tab")).toBe("sql");
    expect(platformUrl?.searchParams.getAll("source_labels")).toEqual(["west"]);
    expect(appsUrl?.searchParams.get("workspace_ids")).toBe("7");
    expect(appsUrl?.searchParams.getAll("source_labels")).toEqual(["west"]);
    expect(apps.client.getQueryCache().getAll()[0].queryKey).toContain("west");
  });
});
