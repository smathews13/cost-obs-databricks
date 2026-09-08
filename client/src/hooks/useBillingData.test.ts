import { createElement, type ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  actionableErrorMessage,
  fetchSubmitAndPoll,
  getDefaultDateRange,
  responsePayloadIssue,
  setActiveSourceLabels,
  setActiveSourceRouting,
  setIncludeHistoricalWorkspaceData,
  useAccountInfo,
  useAppsDashboardBundle,
  useCloudCostsBundle,
  useDashboardBundleFast,
  useDBSQLQueryCosts,
  useDBSQLTopQueries,
  useKPIsBundle,
  useSKUBreakdown,
  useUsersGroupsBundle,
} from "./useBillingData";

const fetchMock = vi.fn<typeof fetch>();

function jsonResponse(status: number, body?: unknown, headers?: HeadersInit): Response {
  return new Response(body === undefined ? null : JSON.stringify(body), {
    status,
    headers: {
      ...(body === undefined ? {} : { "Content-Type": "application/json" }),
      ...headers,
    },
  });
}

describe("default billing date ranges", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-30T12:00:00"));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([7, 14, 30, 90])(
    "returns exactly %i inclusive days ending yesterday",
    (days) => {
      const range = getDefaultDateRange(days);
      const elapsedDays = (
        Date.parse(`${range.endDate}T00:00:00Z`)
        - Date.parse(`${range.startDate}T00:00:00Z`)
      ) / 86_400_000;

      expect(range.endDate).toBe("2026-08-29");
      expect(elapsedDays + 1).toBe(days);
    },
  );
});

describe("fetchSubmitAndPoll", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("polls after the default delay until a 202 becomes 200", async () => {
    fetchMock
      .mockResolvedValueOnce(jsonResponse(202))
      .mockResolvedValueOnce(jsonResponse(200, { ready: true }));

    const request = fetchSubmitAndPoll<{ ready: boolean }>("/api/bundle");
    await vi.advanceTimersByTimeAsync(1999);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1);
    await expect(request).resolves.toEqual({ ready: true });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("honors Retry-After before polling again", async () => {
    fetchMock
      .mockResolvedValueOnce(jsonResponse(202, undefined, { "Retry-After": "1" }))
      .mockResolvedValueOnce(jsonResponse(200, { ready: true }));

    const request = fetchSubmitAndPoll<{ ready: boolean }>("/api/bundle");
    await vi.advanceTimersByTimeAsync(999);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1);
    await expect(request).resolves.toEqual({ ready: true });
  });

  it("aborts an in-progress poll without sending another request", async () => {
    fetchMock.mockResolvedValue(jsonResponse(202));
    const controller = new AbortController();
    const request = fetchSubmitAndPoll("/api/bundle", controller.signal);
    const rejection = expect(request).rejects.toMatchObject({ name: "AbortError" });

    await vi.advanceTimersByTimeAsync(0);
    controller.abort();

    await rejection;
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("settles with a clear error when polling times out", async () => {
    fetchMock.mockResolvedValue(jsonResponse(202));
    const sensitiveUrl = "/api/bundle?workspace_ids=123456789&token=private";
    const request = fetchSubmitAndPoll(sensitiveUrl, undefined, { timeoutMs: 5000 });
    const rejection = expect(request).rejects.toThrow(
      "The data request timed out after 5 seconds. Please retry.",
    );

    await vi.advanceTimersByTimeAsync(5000);
    await rejection;
    await expect(request).rejects.not.toThrow("123456789");
    await expect(request).rejects.not.toThrow("private");
  });

  it("settles immediately on a server error", async () => {
    fetchMock.mockResolvedValue(jsonResponse(500, { detail: "warehouse failed" }));

    await expect(fetchSubmitAndPoll("/api/bundle")).rejects.toThrow(
      "500: warehouse failed",
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("maps typed cloud failures to actionable copy without exposing raw details", async () => {
    fetchMock.mockResolvedValue(jsonResponse(503, {
      detail: {
        message: "host=https://private.example SQL=SELECT secret_value",
        error_code: "CLOUD_BUNDLE_FAILED",
        retryable: true,
      },
    }, { "Retry-After": "2" }));

    await expect(fetchSubmitAndPoll("/api/bundle")).rejects.toThrow(
      "503: Cloud cost data is temporarily unavailable. Retry, or check the SQL warehouse in Settings.",
    );
    expect(actionableErrorMessage("SQL_TIMEOUT", "fallback")).toMatch(
      /warehouse may still be starting/i,
    );
  });

  it.each(["error", "_error"])(
    "rejects HTTP 200 bundle payloads carrying %s",
    async (errorKey) => {
      fetchMock.mockResolvedValue(
        jsonResponse(200, { [errorKey]: "bundle failed" }),
      );

      await expect(fetchSubmitAndPoll("/api/bundle")).rejects.toThrow(
        "bundle failed",
      );
    },
  );
});

describe("Users bundle polling", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("polls a 202 producer and settles on the typed bundle response", async () => {
    const payload = {
      availability: "partial",
      partial_reasons: { timeseries: "SQL_TIMEOUT" },
      summary: { user_count: 2 },
      top_users: [],
      timeseries: [],
      timeseries_users: [],
      by_workspace: [],
      user_growth: [],
      start_date: "2026-08-01",
      end_date: "2026-08-30",
    };
    fetchMock
      .mockResolvedValueOnce(jsonResponse(202, { status: "pending" }, { "Retry-After": "1" }))
      .mockResolvedValueOnce(jsonResponse(200, payload));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = ({ children }: { children: ReactNode }) =>
      createElement(QueryClientProvider, { client }, children);

    const hook = renderHook(
      () => useUsersGroupsBundle(
        { startDate: "2026-08-01", endDate: "2026-08-30" },
        ["2", "1"],
      ),
      { wrapper },
    );
    await act(() => vi.advanceTimersByTimeAsync(0));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await act(() => vi.advanceTimersByTimeAsync(1000));
    await act(() => vi.advanceTimersByTimeAsync(1));

    expect(hook.result.current.isSuccess).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    const url = new URL(String(fetchMock.mock.calls[0][0]), "https://example.test");
    expect(url.pathname).toBe("/api/users-groups/bundle");
    expect(url.searchParams.get("workspace_ids")).toBe("1,2");
    expect(hook.result.current.data?.availability).toBe("partial");
  });
});

describe("Cloud Costs recovery", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
    vi.spyOn(Math, "random").mockReturnValue(0);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("uses one submit-and-poll request for the complete Cloud tab", async () => {
    const payload = {
      availability: "available",
      partial_reasons: {},
      infra_bundle: { infra_costs: {}, infra_timeseries: {} },
      aws_actual: { available: false },
      azure_actual: { available: false },
      gcp_actual: { available: false },
      start_date: "2026-08-01",
      end_date: "2026-08-28",
    };
    fetchMock
      .mockResolvedValueOnce(jsonResponse(202, undefined, { "Retry-After": "1" }))
      .mockResolvedValueOnce(jsonResponse(200, payload));
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const wrapper = ({ children }: { children: ReactNode }) =>
      createElement(QueryClientProvider, { client }, children);

    const hook = renderHook(
      () => useCloudCostsBundle(
        { startDate: "2026-08-01", endDate: "2026-08-28" },
        ["2", "1"],
        true,
      ),
      { wrapper },
    );
    await act(() => vi.advanceTimersByTimeAsync(0));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await act(() => vi.advanceTimersByTimeAsync(1000));
    await act(() => vi.advanceTimersByTimeAsync(1));
    expect(hook.result.current.isSuccess).toBe(true);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    for (const [request] of fetchMock.mock.calls) {
      const url = new URL(String(request), "https://example.test");
      expect(url.pathname).toBe("/api/billing/cloud-costs-bundle");
      expect(url.searchParams.get("workspace_ids")).toBe("1,2");
    }
    hook.unmount();
  });

  it("retries explicit capacity overload using Retry-After", async () => {
    const payload = {
      availability: "partial",
      partial_reasons: { gcp_actual: "SQL_OVERLOADED" },
      infra_bundle: { infra_costs: {}, infra_timeseries: {} },
      aws_actual: { available: false },
      azure_actual: { available: false },
      gcp_actual: { available: false },
      start_date: "2026-08-01",
      end_date: "2026-08-28",
    };
    fetchMock
      .mockResolvedValueOnce(jsonResponse(
        503,
        { detail: { message: "SQL capacity is full", error_code: "SQL_OVERLOADED" } },
        { "Retry-After": "2" },
      ))
      .mockResolvedValueOnce(jsonResponse(200, payload));
    const client = new QueryClient();
    const wrapper = ({ children }: { children: ReactNode }) =>
      createElement(QueryClientProvider, { client }, children);

    const hook = renderHook(() => useCloudCostsBundle(undefined, undefined, true), {
      wrapper,
    });
    await act(() => vi.advanceTimersByTimeAsync(0));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await act(() => vi.advanceTimersByTimeAsync(1999));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await act(() => vi.advanceTimersByTimeAsync(1));
    await act(() => vi.advanceTimersByTimeAsync(1));
    expect(hook.result.current.isSuccess).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    hook.unmount();
  });
});

describe("report payload readiness", () => {
  it("blocks required unavailable payloads but accepts truthful empty data", () => {
    expect(
      responsePayloadIssue(
        { available: false, message: "section unavailable" },
        true,
      ),
    ).toBe("section unavailable");
    expect(
      responsePayloadIssue(
        { available: true, availability: "empty", rows: [] },
        true,
      ),
    ).toBeUndefined();
    expect(responsePayloadIssue({ rows: [] }, true)).toBeUndefined();
  });

  it("surfaces error payloads regardless of availability fields", () => {
    expect(responsePayloadIssue({ error: "query failed" })).toBe("query failed");
    expect(responsePayloadIssue({ _error: "worker failed" })).toBe("worker failed");
  });
});

describe("dashboard source scope", () => {
  afterEach(() => {
    setActiveSourceLabels([]);
    setIncludeHistoricalWorkspaceData(true);
    vi.unstubAllGlobals();
  });

  it("separates current-only requests and tells the backend to exclude history", async () => {
    setIncludeHistoricalWorkspaceData(false);
    const request = vi.fn(async () => jsonResponse(200, {}));
    vi.stubGlobal("fetch", request);
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const wrapper = ({ children }: { children: ReactNode }) =>
      createElement(QueryClientProvider, { client }, children);

    const { result } = renderHook(
      () => useDashboardBundleFast(
        { startDate: "2026-08-01", endDate: "2026-08-28" },
        undefined,
        true,
      ),
      { wrapper },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    const url = new URL(String(request.mock.calls[0][0]), "https://example.test");
    expect(url.searchParams.get("include_historical_workspaces")).toBe("false");
    expect(client.getQueryCache().getAll()[0].queryKey).toContain("current-only");
  });

  it("includes source scope in active and on-demand tab query keys", async () => {
    setActiveSourceLabels(["shared-west"]);
    const fetchMock = vi.fn(async () => jsonResponse(200, {}));
    vi.stubGlobal("fetch", fetchMock);
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const wrapper = ({ children }: { children: ReactNode }) =>
      createElement(QueryClientProvider, { client }, children);

    const { result } = renderHook(() => {
      const dbu = useDashboardBundleFast(
        { startDate: "2026-08-01", endDate: "2026-08-28" },
        ["2", "1"],
        true,
      );
      useAppsDashboardBundle(
        { startDate: "2026-08-01", endDate: "2026-08-28" },
        ["2", "1"],
        false,
      );
      return dbu;
    }, { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    const keys = client.getQueryCache().getAll().map((query) => query.queryKey);
    expect(keys).toContainEqual([
      "billing",
      "dashboard-bundle-fast",
      { startDate: "2026-08-01", endDate: "2026-08-28" },
      "1,2",
      "shared-west",
    ]);
    expect(keys).toContainEqual([
      "apps",
      "dashboard-bundle",
      { startDate: "2026-08-01", endDate: "2026-08-28" },
      "1,2",
      "shared-west",
    ]);
    const url = new URL(String(fetchMock.mock.calls[0][0]), "https://example.test");
    expect(url.searchParams.getAll("source_labels")).toEqual(["shared-west"]);
    expect(url.searchParams.get("workspace_ids")).toBe("1,2");
  });

  it("separates scoped SQL drilldown requests in the cache", async () => {
    fetchMock.mockReset();
    fetchMock.mockImplementation(async () =>
      jsonResponse(200, { available: true, queries: [] }));
    vi.stubGlobal("fetch", fetchMock);
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const wrapper = ({ children }: { children: ReactNode }) =>
      createElement(QueryClientProvider, { client }, children);
    const range = { startDate: "2026-08-01", endDate: "2026-08-28" };

    setActiveSourceLabels(["shared-east"]);
    const east = renderHook(
      () => useDBSQLTopQueries(range, ["1"], true),
      { wrapper },
    );
    await waitFor(() => expect(east.result.current.isSuccess).toBe(true));

    setActiveSourceLabels(["shared-west"]);
    const west = renderHook(
      () => useDBSQLTopQueries(range, ["2"], true),
      { wrapper },
    );
    await waitFor(() => expect(west.result.current.isSuccess).toBe(true));

    const keys = client.getQueryCache().getAll().map((query) => query.queryKey);
    expect(keys).toContainEqual([
      "dbsql", "top-queries", range, "1", "shared-east",
    ]);
    expect(keys).toContainEqual([
      "dbsql", "top-queries", range, "2", "shared-west",
    ]);
    const urls = fetchMock.mock.calls.map(([url]) =>
      new URL(String(url), "https://example.test"));
    expect(urls[0].searchParams.getAll("source_labels")).toEqual(["shared-east"]);
    expect(urls[0].searchParams.get("workspace_ids")).toBe("1");
    expect(urls[1].searchParams.getAll("source_labels")).toEqual(["shared-west"]);
    expect(urls[1].searchParams.get("workspace_ids")).toBe("2");

    east.unmount();
    west.unmount();
  });

  it("routes SKU requests through a same-account source workspace", async () => {
    setActiveSourceLabels(["west4"]);
    setActiveSourceRouting(["local"], ["workspace-west"]);
    const request = vi.fn(async () => jsonResponse(200, { skus: [] }));
    vi.stubGlobal("fetch", request);
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const wrapper = ({ children }: { children: ReactNode }) =>
      createElement(QueryClientProvider, { client }, children);

    const { result } = renderHook(
      () => useSKUBreakdown(
        { startDate: "2026-08-01", endDate: "2026-08-28" },
        undefined,
        true,
      ),
      { wrapper },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    const url = new URL(String(request.mock.calls[0][0]), "https://example.test");
    expect(url.searchParams.getAll("source_labels")).toEqual(["local"]);
    expect(url.searchParams.get("workspace_ids")).toBe("workspace-west");
  });
});

describe("account display metadata", () => {
  it("prefers the resolved account display name over a numeric host label", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/api/billing/account-details")) {
        return jsonResponse(200, {
          account_id: "account-1",
          account_name: "fevm-cmegdemos",
          cloud: "GCP",
        });
      }
      return jsonResponse(200, {
        account_id: null,
        account_name: null,
        cloud: "GCP",
        host: `https://${"8".repeat(16)}.gcp.databricks.com`,
      });
    }));
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const wrapper = ({ children }: { children: ReactNode }) =>
      createElement(QueryClientProvider, { client }, children);

    const { result } = renderHook(() => useAccountInfo(), { wrapper });
    await waitFor(() => expect(result.current.data?.account_name).toBe("fevm-cmegdemos"));
  });
});

describe("DBSQL unavailable polling", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    fetchMock.mockReset();
    fetchMock.mockResolvedValue(jsonResponse(200, { available: false }));
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("keeps one timeout window across unmounts instead of restarting on render", async () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const wrapper = ({ children }: { children: ReactNode }) =>
      createElement(QueryClientProvider, { client }, children);

    const first = renderHook(() => useDBSQLQueryCosts(undefined, undefined, true), { wrapper });
    await act(() => vi.advanceTimersByTimeAsync(0));
    expect(first.result.current.data?.available).toBe(false);

    await act(() => vi.advanceTimersByTimeAsync(2000));
    expect(fetchMock.mock.calls.length).toBeGreaterThan(1);
    first.unmount();

    await act(() => vi.advanceTimersByTimeAsync(3 * 60 * 1000));
    const callsBeforeRemount = fetchMock.mock.calls.length;
    const second = renderHook(() => useDBSQLQueryCosts(undefined, undefined, true), { wrapper });
    await act(() => vi.advanceTimersByTimeAsync(5000));

    expect(second.result.current.data?.available).toBe(false);
    expect(fetchMock.mock.calls.length).toBeLessThanOrEqual(callsBeforeRemount + 1);
    const callsAfterRemount = fetchMock.mock.calls.length;
    await act(() => vi.advanceTimersByTimeAsync(10_000));
    expect(fetchMock.mock.calls.length).toBe(callsAfterRemount);
    second.unmount();
  });
});

describe("KPI tab cache", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    fetchMock.mockResolvedValue(jsonResponse(200, { availability: "available" }));
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("does not refetch settled KPI data when the tab is revisited", async () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const wrapper = ({ children }: { children: ReactNode }) =>
      createElement(QueryClientProvider, { client }, children);
    const range = { startDate: "2026-08-01", endDate: "2026-08-30" };
    const hook = renderHook(
      ({ enabled }) => useKPIsBundle(range, undefined, enabled),
      { initialProps: { enabled: true }, wrapper },
    );

    await waitFor(() => expect(hook.result.current.isSuccess).toBe(true));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    hook.rerender({ enabled: false });
    hook.rerender({ enabled: true });
    await waitFor(() => expect(hook.result.current.isSuccess).toBe(true));
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
