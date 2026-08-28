/**
 * Regression tests for SettingsConfig destructive action safety.
 *
 * Key invariant: "Drop Tables" must be disabled when the system is degraded
 * (any non-optional table has exists === false). Users must type "CONFIRM"
 * before the final drop button becomes active.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { SettingsConfig } from "../SettingsConfig";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const fetchMock = vi.fn<typeof fetch>();

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  fetchMock.mockReset();
});

const HEALTHY_TABLES = {
  tables: [
    { name: "billing_usage", exists: true, optional: false, row_count: 1000 },
    { name: "query_history", exists: true, optional: true,  row_count: 500  },
  ],
  refresh_status: { status: "ok", stale: false, hours_since_refresh: 0, last_refresh_utc: "2026-05-21T00:00:00Z" },
  auth_error: null,
};

const DEGRADED_TABLES = {
  tables: [
    { name: "billing_usage",  exists: false, optional: false, row_count: 0 },
    { name: "query_history",  exists: true,  optional: true,  row_count: 500 },
  ],
  refresh_status: { status: "error", stale: true, hours_since_refresh: 48, last_refresh_utc: null },
  auth_error: null,
};

function renderSettingsConfig(tablesPayload: object) {
  fetchMock.mockImplementation((input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/api/settings/tables")) {
      return Promise.resolve(
        new Response(JSON.stringify(tablesPayload), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      );
    }
    // All other setup endpoints: return empty OK
    return Promise.resolve(new Response("{}", { status: 200 }));
  });

  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <SettingsConfig />
    </QueryClientProvider>
  );
}

// ---------------------------------------------------------------------------
// Degraded state: Drop Tables button disabled
// ---------------------------------------------------------------------------

describe("SettingsConfig: destructive action disabled in degraded state", () => {
  it("'Drop Tables' button is disabled when a required table is missing", async () => {
    renderSettingsConfig(DEGRADED_TABLES);

    // Wait for the async table-status fetch to resolve; the degraded guard is applied
    // once tablesStatus loads (the button is always present, so findByRole alone would
    // capture the pre-fetch, not-yet-disabled state).
    await waitFor(() =>
      expect(screen.getByRole("button", { name: /drop tables/i })).toBeDisabled()
    );
  });

  it("shows a degraded-state warning when tables are missing", async () => {
    renderSettingsConfig(DEGRADED_TABLES);

    expect(await screen.findByText(/table.*already missing|missing.*table/i)).toBeInTheDocument();
  });

  it("confirm UI never appears in degraded state: hard block, no break-glass path", async () => {
    renderSettingsConfig(DEGRADED_TABLES);

    await screen.findByRole("button", { name: /drop tables/i });

    // The button is disabled so wipePending can never become true.
    // CONFIRM input must not exist in the DOM at all.
    expect(screen.queryByPlaceholderText(/type confirm/i)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /confirm drop/i })).not.toBeInTheDocument();
  });

  it("'Drop Tables' button is enabled when all required tables exist", async () => {
    renderSettingsConfig(HEALTHY_TABLES);

    const dropBtn = await screen.findByRole("button", { name: /drop tables/i });
    expect(dropBtn).not.toBeDisabled();
  });
});

// ---------------------------------------------------------------------------
// CONFIRM gate: must type exact text before final drop button enables
// ---------------------------------------------------------------------------

describe("SettingsConfig: CONFIRM gate before irreversible drop", () => {
  it("shows the CONFIRM input after clicking 'Drop Tables'", async () => {
    renderSettingsConfig(HEALTHY_TABLES);

    const dropBtn = await screen.findByRole("button", { name: /drop tables/i });
    await userEvent.click(dropBtn);

    expect(screen.getByPlaceholderText(/type confirm/i)).toBeInTheDocument();
  });

  it("'Confirm Drop' button is disabled until 'CONFIRM' is typed exactly", async () => {
    renderSettingsConfig(HEALTHY_TABLES);

    const dropBtn = await screen.findByRole("button", { name: /drop tables/i });
    await userEvent.click(dropBtn);

    const confirmBtn = screen.getByRole("button", { name: /confirm drop/i });
    expect(confirmBtn).toBeDisabled();

    const input = screen.getByPlaceholderText(/type confirm/i);
    await userEvent.type(input, "confirm"); // lowercase: must not enable
    expect(confirmBtn).toBeDisabled();

    await userEvent.clear(input);
    await userEvent.type(input, "CONFIRM"); // exact match
    expect(confirmBtn).not.toBeDisabled();
  });

  it("partial text does not enable Confirm Drop", async () => {
    renderSettingsConfig(HEALTHY_TABLES);

    const dropBtn = await screen.findByRole("button", { name: /drop tables/i });
    await userEvent.click(dropBtn);

    const confirmBtn = screen.getByRole("button", { name: /confirm drop/i });
    const input = screen.getByPlaceholderText(/type confirm/i);
    await userEvent.type(input, "CON");
    expect(confirmBtn).toBeDisabled();
  });
});

describe("SettingsConfig: rebuild history recovery state", () => {
  it("shows the storage block reason and disables rebuild", async () => {
    renderSettingsConfig({
      tables: [],
      storage_block_reason: "Multiple app-owned schemas were found.",
      refresh_status: {
        status: "blocked",
        stale: true,
        hours_since_refresh: null,
        last_refresh_utc: null,
        block_reason: "Multiple app-owned schemas were found.",
        refresh_history: [],
      },
    });

    expect(await screen.findByText(/multiple app-owned schemas were found/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /rebuild now/i })).toBeDisabled();
    expect(screen.getAllByText(/rebuild blocked/i)).toHaveLength(2);
  });

  it("renders restored blocked and successful history entries", async () => {
    renderSettingsConfig({
      tables: [{ name: "daily_usage_summary", exists: true, optional: false, row_count: 1 }],
      refresh_status: {
        status: "success",
        stale: false,
        hours_since_refresh: 1,
        last_refresh_utc: "2026-08-28T05:00:00Z",
        refresh_history: [
          {
            id: "blocked-run",
            timestamp: "2026-08-27T05:00:00Z",
            status: "blocked",
            duration_seconds: 0,
            lookback_days: 180,
            trigger: "scheduled",
            block_reason: "Storage was not configured.",
          },
          {
            id: "successful-run",
            timestamp: "2026-08-28T05:00:00Z",
            status: "success",
            duration_seconds: 42,
            lookback_days: 180,
            trigger: "scheduled",
          },
        ],
      },
    });

    expect(await screen.findByText(/^blocked$/i)).toBeInTheDocument();
    expect(screen.getByText(/^success$/i)).toBeInTheDocument();
    expect(screen.getAllByText(/^scheduled$/i)).toHaveLength(2);
  });

  it("surfaces Delta persistence failures", async () => {
    renderSettingsConfig({
      tables: [{ name: "daily_usage_summary", exists: true, optional: false, row_count: 1 }],
      refresh_status: {
        status: "success",
        stale: false,
        hours_since_refresh: 1,
        last_refresh_utc: "2026-08-28T05:00:00Z",
        persistence_error: "warehouse returned 503",
        refresh_history: [],
      },
    });

    expect(await screen.findByText(/history is not durable/i)).toBeInTheDocument();
    expect(screen.getByText(/warehouse returned 503/i)).toBeInTheDocument();
  });
});
