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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  SQLWarehousing360,
  WarehouseIdleTimeView,
  WarehouseRightsizingView,
} from "../SQLWarehousing360";
import type { DBSQLDashboardBundle } from "@/types/billing";
import { setActiveSourceLabels } from "@/hooks/useBillingData";

// ---------------------------------------------------------------------------
// Mock useFeatureAvailability: tests control grant state without a server
// ---------------------------------------------------------------------------

vi.mock("@/hooks/useFeatureAvailability", () => ({
  useFeatureAvailability: vi.fn(),
  READINESS_QUERY_KEY: ["setup-readiness"],
}));
vi.mock("../KPITrendModal", () => ({
  KPITrendModal: ({ kpi }: { kpi: string }) => <div data-testid="sql-selected-kpi">{kpi}</div>,
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

afterEach(() => {
  setActiveSourceLabels([]);
  vi.unstubAllGlobals();
});

function renderSQLView(
  queryData: DBSQLDashboardBundle | undefined,
  opts: {
    isLoading?: boolean;
    isError?: boolean;
    topQueriesData?: import("@/types/billing").TopQueriesResponse;
    workspaceIds?: string[];
    startDate?: string;
    endDate?: string;
  } = {},
) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const view = render(
    <QueryClientProvider client={client}>
      <SQLWarehousing360
        sqlBreakdownData={undefined}
        queryData={queryData}
        isLoading={opts.isLoading ?? false}
        isError={opts.isError}
        topQueriesData={opts.topQueriesData}
        workspaceIds={opts.workspaceIds}
        startDate={opts.startDate}
        endDate={opts.endDate}
      />
    </QueryClientProvider>
  );
  return { ...view, queryClient: client };
}

function renderOptimizeView(view: React.ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      {view}
    </QueryClientProvider>,
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

  it.each([
    ["Total Query Spend", "sql_spend"],
    ["Total Queries", "sql_queries"],
    ["Unique SQL Users", "sql_users"],
    ["Query Duration", "avg_query_duration"],
  ])("opens the %s trend from the full KPI card", async (title, kpi) => {
    renderSQLView(zeroSummary, {
      startDate: "2026-01-01",
      endDate: "2026-01-31",
    });

    const card = screen.getByRole("button", { name: `See ${title} trend` });
    expect(card).toHaveClass("co-kpi-card");
    expect(card.querySelector("button")).toBeNull();
    await userEvent.click(card);
    expect(screen.getByTestId("sql-selected-kpi")).toHaveTextContent(kpi);
  });
});

describe("SQLWarehousing360: magnitude-aware KPI formatting", () => {
  it("compacts million-dollar query spend into a single KPI value", () => {
    renderSQLView({
      ...BASE_BUNDLE_AVAILABLE,
      summary: {
        available: true,
        total_spend: 1_687_075.63,
        total_dbus: 24_560,
        total_queries: 22_008_909,
        unique_users: 5_500,
        unique_warehouses: 149,
        avg_cost_per_query: 0.08,
        avg_duration_seconds: 2.3,
        start_date: "2026-01-01",
        end_date: "2026-01-31",
      },
    });

    const value = screen.getByText("$1.69M");
    expect(value).toHaveClass("co-kpi-card__value");
    expect(value.closest(".co-kpi-grid")).not.toBeNull();
    expect(screen.queryByText("$1,687,075.63")).not.toBeInTheDocument();
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

  it("uses source and workspace scope for source-query drilldowns", async () => {
    setActiveSourceLabels(["shared-west"]);
    const view = renderSQLView(bundle, {
      workspaceIds: ["2", "1"],
      startDate: "2026-01-01",
      endDate: "2026-01-31",
    });

    await waitFor(() => {
      expect(
        fetchMock.mock.calls.some(([input]) =>
          String(input).startsWith("/api/dbsql/top-queries-by-source?")),
      ).toBe(true);
    });
    const input = fetchMock.mock.calls.find(([url]) =>
      String(url).startsWith("/api/dbsql/top-queries-by-source?"))![0];
    const url = new URL(String(input), "https://example.test");

    expect(url.searchParams.get("workspace_ids")).toBe("1,2");
    expect(url.searchParams.getAll("source_labels")).toEqual(["shared-west"]);
    expect(
      view.queryClient.getQueryCache().find({
        queryKey: [
          "dbsql",
          "top-queries-by-source",
          "JOB",
          "2026-01-01",
          "2026-01-31",
          "1,2",
          "shared-west",
        ],
      }),
    ).toBeDefined();
  });
});

describe("Optimize table pagination", () => {
  it("shows 10 rightsizing rows per page with normal counts and disabled states", async () => {
    const recommendations = Array.from({ length: 21 }, (_, index) => ({
      warehouse_id: `warehouse-${index + 1}`,
      warehouse_name: `Warehouse ${index + 1}`,
      warehouse_size: "SMALL",
      workspace_id: "1",
      recommendation_type: "OVER_SCALED",
      recommendation_text: "Reduce max clusters.",
    }));
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({
        available: true,
        recommendations,
        warehouses_analyzed: 21,
      }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    renderOptimizeView(<WarehouseRightsizingView />);

    expect(await screen.findByText("Warehouse 1")).toBeInTheDocument();
    expect(screen.getAllByRole("row")).toHaveLength(11);
    expect(screen.getByLabelText("Showing 1 to 10 of 21 recommendations")).toBeInTheDocument();
    expect(screen.getByText("Page 1 of 3")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Previous" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Next" })).toBeEnabled();

    await userEvent.click(screen.getByRole("button", { name: "Next" }));

    expect(screen.queryByText("Warehouse 1")).not.toBeInTheDocument();
    expect(screen.getByText("Warehouse 11")).toBeInTheDocument();
    expect(screen.getByLabelText("Showing 11 to 20 of 21 recommendations")).toBeInTheDocument();
    expect(screen.getByText("Page 2 of 3")).toBeInTheDocument();
  });

  it("shows 10 idle-time rows per page and disables Next on the last page", async () => {
    const warehouses = Array.from({ length: 12 }, (_, index) => ({
      warehouse_id: `warehouse-${index + 1}`,
      warehouse_name: `Idle Warehouse ${index + 1}`,
      warehouse_size: "SMALL",
      warehouse_type: "CLASSIC",
      workspace_id: "1",
      uptime_source: "events",
      total_running_minutes: 60,
      busy_union_minutes: 10,
      idle_minutes: 50,
      idle_pct: 83.3,
      warm_hold_minutes: 5,
      keep_alive_score: 8.3,
      auto_stop_mins: 10,
      max_num_clusters: 1,
      total_spend: 4,
      estimated_idle_spend: 3,
      low_confidence: false,
    }));
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({
        available: true,
        serverless_detected: false,
        warehouses,
      }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    renderOptimizeView(<WarehouseIdleTimeView />);

    expect(await screen.findByText("Idle Warehouse 1")).toBeInTheDocument();
    expect(screen.getAllByRole("row")).toHaveLength(11);
    expect(screen.getByLabelText("Showing 1 to 10 of 12 warehouses")).toBeInTheDocument();
    expect(screen.getByText("Page 1 of 2")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Next" }));

    expect(screen.queryByText("Idle Warehouse 1")).not.toBeInTheDocument();
    expect(screen.getByText("Idle Warehouse 11")).toBeInTheDocument();
    expect(screen.getAllByRole("row")).toHaveLength(3);
    expect(screen.getByLabelText("Showing 11 to 12 of 12 warehouses")).toBeInTheDocument();
    expect(screen.getByText("Page 2 of 2")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Next" })).toBeDisabled();
  });
});
