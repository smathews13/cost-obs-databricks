import { createElement, type ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  fetchSubmitAndPoll,
  getDefaultDateRange,
  responsePayloadIssue,
  setActiveSourceLabels,
  useAppsDashboardBundle,
  useDashboardBundleFast,
  useDBSQLQueryCosts,
  useDBSQLTopQueries,
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
    const request = fetchSubmitAndPoll("/api/bundle", undefined, { timeoutMs: 5000 });
    const rejection = expect(request).rejects.toThrow(
      "Timed out waiting for /api/bundle after 5 seconds. Please retry.",
    );

    await vi.advanceTimersByTimeAsync(5000);
    await rejection;
  });

  it("settles immediately on a server error", async () => {
    fetchMock.mockResolvedValue(jsonResponse(500, { detail: "warehouse failed" }));

    await expect(fetchSubmitAndPoll("/api/bundle")).rejects.toThrow(
      "500: warehouse failed",
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
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
    vi.unstubAllGlobals();
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
