import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fetchSubmitAndPoll } from "./useBillingData";

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
});
