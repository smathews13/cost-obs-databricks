import type { ReactNode } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useUpdatingIndicator } from "./useUpdatingIndicator";

function createWrapper(queryClient: QueryClient) {
  return function QueryWrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        {children}
      </QueryClientProvider>
    );
  };
}

describe("useUpdatingIndicator", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("stays active until an observed query settles", async () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    let resolveQuery!: (value: string) => void;
    const queryFn = vi.fn(
      () =>
        new Promise<string>((resolve) => {
          resolveQuery = resolve;
        }),
    );
    const { result, unmount } = renderHook(
      () => useUpdatingIndicator(5_000),
      { wrapper: createWrapper(queryClient) },
    );

    act(() => result.current.arm());
    expect(result.current.updating).toBe(true);

    let request!: Promise<string>;
    act(() => {
      request = queryClient.fetchQuery({
        queryKey: ["updating-indicator"],
        queryFn,
      });
    });
    await waitFor(() => expect(queryFn).toHaveBeenCalledOnce());
    expect(result.current.updating).toBe(true);

    await act(async () => {
      resolveQuery("done");
      await request;
    });
    await waitFor(() => expect(result.current.updating).toBe(false));

    unmount();
    queryClient.clear();
  });

  it("clears after the grace period when no query starts", () => {
    vi.useFakeTimers();
    const queryClient = new QueryClient();
    const { result, unmount } = renderHook(
      () => useUpdatingIndicator(),
      { wrapper: createWrapper(queryClient) },
    );

    act(() => result.current.arm());
    expect(result.current.updating).toBe(true);

    act(() => vi.advanceTimersByTime(500));
    expect(result.current.updating).toBe(false);

    unmount();
    queryClient.clear();
  });
});
