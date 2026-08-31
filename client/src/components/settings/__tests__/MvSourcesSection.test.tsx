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
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/mv-sources/check")) {
        return {
          ok: true,
          json: async () => ({
            ok: true,
            checked_at: "2026-08-28T12:00:00Z",
            share_last_updated: "2026-08-28T11:00:00Z",
            matched: 1,
            total: 1,
            tables: [{ table: "daily_usage_summary", status: "match" }],
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
            catalog_explorer_tables: [{
              fqn: "shared.cost_obs.daily_usage_summary",
              url: "https://dbc.example.com/explore/data/shared/cost_obs/daily_usage_summary",
            }],
          }],
          recipient_refresh: {
            supported: false,
            mode: "provider_managed",
            check_action: "metadata_and_local_bindings_only",
          },
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

    const check = await screen.findByRole("button", { name: "Re-check metadata" });
    const remove = screen.getByRole("button", { name: "Remove" });
    expect(check.parentElement).toContainElement(remove);
    expect(screen.getByText(/Provider updates appear automatically/)).toBeVisible();
    expect(screen.getByRole("link", {
      name: "Open shared.cost_obs.daily_usage_summary in Catalog Explorer (opens in a new tab)",
    })).toHaveAttribute(
      "href",
      "https://dbc.example.com/explore/data/shared/cost_obs/daily_usage_summary",
    );

    await user.click(check);

    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
      "/api/settings/mv-sources/check?label=west",
      { method: "POST" },
    ));
    expect(await screen.findByText("Checked just now")).toBeInTheDocument();
  });

  it.each([
    {
      name: "reports a failed rebuild",
      response: { ok: false, build: { error: "view rebuild failed" }, tables: [{ table: "daily_usage_summary", status: "match" }] },
      message: "view rebuild failed",
    },
    {
      name: "reports a missing configured view",
      response: { ok: true, matched: 0, total: 1, tables: [{ table: "daily_usage_summary", status: "absent" }] },
      message: "Freshness check failed for: daily_usage_summary",
    },
  ])("$name instead of showing a successful check", async ({ response, message }) => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      if (String(input).includes("/mv-sources/check")) {
        return { ok: true, json: async () => response };
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
          }],
        }),
      };
    });
    vi.stubGlobal("fetch", fetchMock);
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    render(
      <QueryClientProvider client={client}>
        <MvSourcesSection />
      </QueryClientProvider>,
    );
    await userEvent.click(await screen.findByRole("button", { name: "Re-check metadata" }));

    expect(await screen.findByText(message)).toBeInTheDocument();
    expect(screen.queryByText("Checked just now")).not.toBeInTheDocument();
  });
});
