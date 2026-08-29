import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_APP_SETTINGS, DEFAULT_VISIBILITY } from "@/utils/settingsHydration";
import { ToastProvider } from "../dubois";
import { GeneralSection, ScheduleGroup } from "../sections";

afterEach(() => {
  vi.unstubAllGlobals();
});

function wrapper(children: React.ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <ToastProvider>{children}</ToastProvider>
    </QueryClientProvider>,
  );
}

describe("immediate settings saves", () => {
  it("checks price-basis response status and rolls back visibly", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/api/settings/pricing-mode") && init?.method === "PUT") {
        return new Response("failed", { status: 503 });
      }
      if (url.endsWith("/api/settings/pricing-mode")) {
        return Response.json({ use_account_prices: false });
      }
      return Response.json({});
    }));
    wrapper(
      <GeneralSection
        localSettings={DEFAULT_APP_SETTINGS}
        updateSetting={vi.fn()}
        tabVisibility={DEFAULT_VISIBILITY}
      />,
    );

    const select = await screen.findByRole("combobox", { name: "Price basis" });
    await userEvent.selectOptions(select, "account");

    expect(await screen.findByRole("alert")).toHaveTextContent("Price basis was not saved: Server returned 503");
    await waitFor(() => expect(select).toHaveValue("list"));
  });

  it("serializes overlapping price-basis writes", async () => {
    let resolveFirst: ((response: Response) => void) | undefined;
    let putCount = 0;
    vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/api/settings/pricing-mode") && init?.method === "PUT") {
        putCount += 1;
        if (putCount === 1) return new Promise<Response>((resolve) => { resolveFirst = resolve; });
        return Promise.resolve(Response.json({ status: "ok" }));
      }
      if (url.endsWith("/api/settings/pricing-mode")) {
        return Promise.resolve(Response.json({ use_account_prices: false }));
      }
      return Promise.resolve(Response.json({}));
    }));
    wrapper(
      <GeneralSection
        localSettings={DEFAULT_APP_SETTINGS}
        updateSetting={vi.fn()}
        tabVisibility={DEFAULT_VISIBILITY}
      />,
    );

    const select = await screen.findByRole("combobox", { name: "Price basis" });
    await userEvent.selectOptions(select, "account");
    await userEvent.selectOptions(select, "list");
    expect(putCount).toBe(1);

    resolveFirst?.(Response.json({ status: "ok" }));
    await waitFor(() => expect(putCount).toBe(2));
  });

  it("checks schedule response status and restores the durable value", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/api/settings/schedule") && init?.method === "POST") {
        return new Response("failed", { status: 500 });
      }
      if (url.endsWith("/api/settings/schedule")) {
        return Response.json({ enabled: true, frequency: "nightly", hour_utc: 5, lookback_days: 180 });
      }
      return Response.json({});
    }));
    wrapper(<ScheduleGroup />);

    const select = await screen.findByRole("combobox", { name: "Refresh frequency" });
    await userEvent.selectOptions(select, "weekly");

    expect(await screen.findByRole("alert")).toHaveTextContent("Refresh schedule was not saved: Server returned 500");
    expect(select).toHaveValue("nightly");
  });

  it("serializes overlapping schedule writes", async () => {
    let resolveFirst: ((response: Response) => void) | undefined;
    let postCount = 0;
    vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/api/settings/schedule") && init?.method === "POST") {
        postCount += 1;
        if (postCount === 1) return new Promise<Response>((resolve) => { resolveFirst = resolve; });
        return Promise.resolve(Response.json({ status: "ok" }));
      }
      if (url.endsWith("/api/settings/schedule")) {
        return Promise.resolve(Response.json({ enabled: true, frequency: "nightly", hour_utc: 5, lookback_days: 180 }));
      }
      return Promise.resolve(Response.json({}));
    }));
    wrapper(<ScheduleGroup />);

    const select = await screen.findByRole("combobox", { name: "Refresh frequency" });
    await userEvent.selectOptions(select, "weekly");
    await userEvent.selectOptions(select, "monthly");
    expect(postCount).toBe(1);

    resolveFirst?.(Response.json({ status: "ok" }));
    await waitFor(() => expect(postCount).toBe(2));
  });
});
