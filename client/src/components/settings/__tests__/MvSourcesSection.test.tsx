import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MvSourcesSection } from "../MvSourcesSection";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("shared source freshness", () => {
  it("checks freshness and keeps refresh/remove actions aligned", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes("/mv-sources/check")) {
        return {
          ok: true,
          json: async () => ({
            ok: true,
            checked_at: "2026-08-28T12:00:00Z",
            share_last_updated: "2026-08-28T11:00:00Z",
          }),
        };
      }
      return {
        ok: true,
        json: async () => ({
          local_label: "local",
          sources: [{
            label: "west",
            catalog: "shared",
            schema: "cost_obs",
            tables: ["daily_usage_summary"],
            share_last_updated: "2026-08-28T11:00:00Z",
          }],
        }),
      };
    });
    vi.stubGlobal("fetch", fetchMock);
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();

    render(
      <QueryClientProvider client={client}>
        <MvSourcesSection />
      </QueryClientProvider>,
    );

    const check = await screen.findByRole("button", { name: "Check freshness" });
    const remove = screen.getByRole("button", { name: "Remove" });
    expect(check.parentElement).toContainElement(remove);

    await user.click(check);

    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
      "/api/settings/mv-sources/check?label=west",
      { method: "POST" },
    ));
    expect(await screen.findByText("Checked just now")).toBeInTheDocument();
  });
});
