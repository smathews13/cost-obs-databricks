/**
 * Regression tests for SQLWarehousing360 summary display states.
 *
 * Key invariants:
 * 1. isLoading → shared loading panels (first deploy / still computing).
 * 2. available=false after load → guidance that query-cost data is not configured.
 * 3. isError → error, not a spinner.
 * 2. available=true + summary=null → "No summary data returned" gray panel.
 * 3. available=true + all-zero summary → $0 rendered (valid zero activity), not "N/A".
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { SQLWarehousing360 } from "../SQLWarehousing360";
import type { DBSQLDashboardBundle } from "@/types/billing";

// ---------------------------------------------------------------------------
// Mock useFeatureAvailability: tests control grant state without a server
// ---------------------------------------------------------------------------

vi.mock("@/hooks/useFeatureAvailability", () => ({
  useFeatureAvailability: vi.fn(),
  READINESS_QUERY_KEY: ["setup-readiness"],
}));

import { useFeatureAvailability } from "@/hooks/useFeatureAvailability";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const fetchMock = vi.fn<typeof fetch>();

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  fetchMock.mockReset();
  // Default: all grants unknown: never blocks rendering
  vi.mocked(useFeatureAvailability).mockReturnValue({
    warehouseGranted: undefined,
    tableGranted: () => undefined,
    isLoaded: true,
  });
  // Stub warehouse-health and any prefetch calls
  fetchMock.mockResolvedValue(
    new Response(JSON.stringify({ available: false, recommendations: [], warehouses_analyzed: 0 }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    })
  );
});

function renderSQLView(
  queryData: DBSQLDashboardBundle | undefined,
  opts: {
    isLoading?: boolean;
    isError?: boolean;
    topQueriesData?: import("@/types/billing").TopQueriesResponse;
  } = {},
) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <SQLWarehousing360
        sqlBreakdownData={undefined}
        queryData={queryData}
        isLoading={opts.isLoading ?? false}
        isError={opts.isError}
        topQueriesData={opts.topQueriesData}
      />
    </QueryClientProvider>
  );
}

const BASE_BUNDLE_AVAILABLE: DBSQLDashboardBundle = {
  available: true,
  start_date: "2026-01-01",
  end_date: "2026-01-31",
};

describe("SQLWarehousing360: first load shows loading panels", () => {
  it("shows a shared loading indicator for every SQL panel", () => {
    renderSQLView(undefined, { isLoading: true });

    expect(screen.getAllByRole("status", { name: /loading/i })).toHaveLength(6);
    expect(screen.getByText("Top Queries")).toBeInTheDocument();
  });

  it("does not flash the missing-grant guidance on first load", () => {
    renderSQLView(undefined, { isLoading: true });

    expect(screen.queryByText(/query-level cost attribution.*not available/i)).not.toBeInTheDocument();
  });
});

describe("SQLWarehousing360: fetch failure shows an error", () => {
  it("does not keep the loading panels after a failed fetch", () => {
    renderSQLView(undefined, { isError: true });

    expect(screen.getByText(/query cost data could not be loaded/i)).toBeInTheDocument();
    expect(screen.queryByRole("status", { name: /loading/i })).not.toBeInTheDocument();
  });
});

describe("SQLWarehousing360: available=false after load shows setup guidance", () => {
  it("explains that query-level cost attribution is not available", () => {
    renderSQLView({ available: false, start_date: "2026-01-01", end_date: "2026-01-31" });

    expect(screen.getByText(/query-level cost attribution is not available/i)).toBeInTheDocument();
    expect(screen.queryByRole("status", { name: /loading/i })).not.toBeInTheDocument();
  });

  it("does NOT show the KPI summary cards", () => {
    renderSQLView({ available: false, start_date: "2026-01-01", end_date: "2026-01-31" });

    expect(screen.queryByText(/total query spend/i)).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// available=true + summary=null: internal error / no data returned
// ---------------------------------------------------------------------------

describe("SQLWarehousing360: available=true but summary null renders unavailable banner", () => {
  it("shows 'Query summary unavailable' when summary is absent", () => {
    renderSQLView({ ...BASE_BUNDLE_AVAILABLE, summary: undefined });

    expect(screen.getByText(/query summary unavailable/i)).toBeInTheDocument();
  });

  it("shows 'No summary data returned' as the reason", () => {
    renderSQLView({ ...BASE_BUNDLE_AVAILABLE, summary: undefined });

    expect(screen.getByText(/no summary data returned/i)).toBeInTheDocument();
  });

  it("does NOT show currency values (no fake zeros)", () => {
    renderSQLView({ ...BASE_BUNDLE_AVAILABLE, summary: undefined });

    // KPI card headings must not appear when summary is absent
    expect(screen.queryByText(/total query spend/i)).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// available=true + all-zero summary: valid zero activity → $0, not "N/A"
// ---------------------------------------------------------------------------

describe("SQLWarehousing360: available=true with zero-value summary renders $0", () => {
  const zeroSummary: DBSQLDashboardBundle = {
    ...BASE_BUNDLE_AVAILABLE,
    summary: {
      available: true,
      total_spend: 0,
      total_dbus: 0,
      total_queries: 0,
      unique_users: 0,
      unique_warehouses: 0,
      avg_cost_per_query: 0,
      avg_duration_seconds: 0,
      start_date: "2026-01-01",
      end_date: "2026-01-31",
    },
  };

  it("renders the 'Total Query Spend' KPI card", () => {
    renderSQLView(zeroSummary);

    expect(screen.getByText(/total query spend/i)).toBeInTheDocument();
  });

  it("shows a currency value ($0) rather than 'N/A' for zero spend", () => {
    renderSQLView(zeroSummary);

    // formatCurrency(0) → "$0": valid zero activity, not a missing-data dash
    const spendValues = screen.getAllByText(/^\$0/);
    expect(spendValues.length).toBeGreaterThan(0);
  });

  it("does NOT show 'Query summary unavailable' for a zero-value summary", () => {
    renderSQLView(zeroSummary);

    expect(screen.queryByText(/query summary unavailable/i)).not.toBeInTheDocument();
  });

  it("does NOT show 'Query-level Cost Attribution Not Available'", () => {
    renderSQLView(zeroSummary);

    expect(
      screen.queryByText(/query-level cost attribution not available/i)
    ).not.toBeInTheDocument();
  });
});

describe("SQLWarehousing360: dashboard polish", () => {
  const source = {
    query_source_type: "JOB",
    query_count: 2,
    total_spend: 12,
    total_dbus: 3,
    avg_cost_per_query: 6,
    percentage: 100,
  };
  const bundle: DBSQLDashboardBundle = {
    ...BASE_BUNDLE_AVAILABLE,
    by_source: {
      available: true,
      sources: [source],
      total_spend: 12,
      start_date: "2026-01-01",
      end_date: "2026-01-31",
    },
    by_warehouse: {
      available: true,
      warehouses: [
        { warehouse_id: "w1", warehouse_size: "SMALL", workspace_id: "1", query_count: 1, unique_users: 1, total_spend: 6, total_dbus: 1, percentage: 50 },
        { warehouse_id: "w2", warehouse_size: "LARGE", workspace_id: "2", query_count: 1, unique_users: 1, total_spend: 6, total_dbus: 1, percentage: 50 },
      ],
      total_spend: 12,
      start_date: "2026-01-01",
      end_date: "2026-01-31",
    },
  };
  const topQueriesData = {
    available: true,
    queries: [{
      statement_id: "q1",
      query_source_type: "JOB",
      query_source_id: null,
      executed_by: "user@example.com",
      warehouse_id: "w1",
      workspace_id: "1",
      statement_preview: "SELECT 1",
      duration_seconds: 1,
      cost: 12,
      dbus: 3,
      query_profile_url: null,
      source_url: null,
      start_time: "2026-01-15T00:00:00Z",
      end_time: "2026-01-15T00:00:01Z",
    }],
    start_date: "2026-01-01",
    end_date: "2026-01-31",
  };

  it("uses the same full tinted source badge across source tables", () => {
    renderSQLView(bundle, { topQueriesData });

    const badges = screen.getAllByText("JOB");
    expect(badges).toHaveLength(2);
    const badgeStyles = badges.map((badge) => badge.getAttribute("style"));
    badges.forEach((badge) => {
      expect(badge).toHaveClass("rounded-full", "px-2", "py-1");
      expect(badge.getAttribute("style")).toContain("background-color");
      expect(badge.getAttribute("style")).toContain("color-mix");
    });
    expect(new Set(badgeStyles).size).toBe(1);
  });

  it("uses only the top-level workspace scope for warehouse counts", () => {
    renderSQLView(bundle);

    expect(screen.getByText("Warehouse Count by Size")).toBeInTheDocument();
    expect(screen.queryByPlaceholderText("Search workspaces...")).not.toBeInTheDocument();
    expect(screen.queryByText(/Filtered to/i)).not.toBeInTheDocument();
  });
});
