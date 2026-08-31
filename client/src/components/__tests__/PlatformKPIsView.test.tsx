/**
 * Regression tests for PlatformKPIsView fake-zero prevention.
 *
 * Core invariant: when a system table grant is explicitly denied (false),
 * the corresponding KPI cards must show the unavailable state (renders "N/A"),
 * NOT zero or a loading spinner.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { PlatformKPIsView } from "../PlatformKPIsView";
import type { PlatformKPIsResponse } from "@/types/billing";

// ---------------------------------------------------------------------------
// Mock useFeatureAvailability so tests control grant state without a server
// ---------------------------------------------------------------------------

vi.mock("@/hooks/useFeatureAvailability", () => ({
  useFeatureAvailability: vi.fn(),
  READINESS_QUERY_KEY: ["setup-readiness"],
}));
vi.mock("../KPITrendModal", () => ({
  KPITrendModal: ({ kpi }: { kpi: string }) => <div data-testid="selected-kpi">{kpi}</div>,
}));

import { useFeatureAvailability } from "@/hooks/useFeatureAvailability";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeTableGranted(overrides: Record<string, boolean | undefined> = {}) {
  return (table: string) => overrides[table];
}

// Complete KPI payload so the card-rendering path is reached. The component
// early-returns an empty state when `data` is undefined; the unavailable-vs-zero
// invariant only applies once data exists and a grant is denied (a denied card
// renders "N/A" regardless of its numeric value).
const SAMPLE_DATA: PlatformKPIsResponse = {
  total_queries: 100,
  unique_query_users: 10,
  total_rows_read: 1000,
  total_bytes_read: 1000000,
  total_compute_seconds: 500,
  total_jobs: 20,
  total_job_runs: 40,
  successful_runs: 38,
  successful_runs_available: true,
  total_job_run_hours: 12,
  unique_job_owners: 5,
  active_workspaces: 3,
  avg_daily_workspaces: 2,
  total_workspace_count: 4,
  active_notebooks: 7,
  total_clusters: 5,
  sql_warehouses: 2,
  models_served: 2,
  total_serving_dbus: 50,
  avg_daily_models: 1,
  avg_daily_query_users: 8,
  stickiness_pct: 60,
  query_users_available: true,
  stickiness_available: true,
  start_date: "2026-01-01",
  end_date: "2026-01-31",
};

function renderView(
  tableOverrides: Record<string, boolean | undefined> = {},
  data: PlatformKPIsResponse = SAMPLE_DATA,
  trendEnabled = false,
) {
  vi.mocked(useFeatureAvailability).mockReturnValue({
    warehouseGranted: true,
    tableGranted: makeTableGranted(tableOverrides),
    isLoaded: true,
  });

  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <PlatformKPIsView
        data={data}
        isLoading={false}
        spendAnomalies={undefined}
        anomaliesLoading={false}
        startDate={trendEnabled ? "2026-01-01" : undefined}
        endDate={trendEnabled ? "2026-01-31" : undefined}
      />
    </QueryClientProvider>
  );
}

// ---------------------------------------------------------------------------
// Core invariant: denied → "N/A" (Unavailable), not 0
// ---------------------------------------------------------------------------

describe("PlatformKPIsView: denied dependency renders unavailable, not 0", () => {
  beforeEach(() => {
    vi.mocked(useFeatureAvailability).mockReset();
  });

  it("shows unavailable state for query.history KPIs when grant is false", () => {
    renderView({ "system.query.history": false });

    // All query.history-dependent KPI cards must show the unavailable text
    const unavailableEls = screen.getAllByText(/Unavailable/i);
    expect(unavailableEls.length).toBeGreaterThan(0);

    // The reason text must be present (grant hint)
    expect(screen.getAllByText(/query\.history grant required/i).length).toBeGreaterThan(0);
  });

  it("does NOT show 0 for query.history KPIs when grant is false", () => {
    renderView({ "system.query.history": false });

    // The KPI value "0" must not appear as a standalone text node in unavailable cards
    const cards = screen.getAllByTitle(/query\.history grant required/i);
    cards.forEach(card => {
      // The card's value cell should show "N/A" not "0"
      expect(card).not.toHaveTextContent(/^\s*0\s*$/);
    });
  });

  it("shows unavailable state for lakeflow KPIs when grant is false", () => {
    renderView({ "system.lakeflow.pipelines": false });

    expect(screen.getAllByText(/lakeflow grants required/i).length).toBeGreaterThan(0);
  });

  it("keeps the billing-backed compute resource KPI available when compute metadata is denied", () => {
    renderView({ "system.compute.clusters": false });

    const title = screen.getByText("Total Compute Resources");
    const card = title.closest(".co-kpi-card");
    expect(title).toHaveClass("co-kpi-card__title--nowrap", "whitespace-nowrap");
    expect(card?.querySelector(".co-kpi-card__layout")).not.toBeNull();
    expect(card?.querySelector(".co-kpi-card__icon")).not.toBeNull();
    expect(screen.getByText("5 clusters · 2 SQL warehouses")).toBeInTheDocument();
    expect(screen.queryByText(/compute\.clusters grant required/i)).not.toBeInTheDocument();
  });

  it("shows unavailable state for serving.served_entities KPI when grant is false", () => {
    renderView({ "system.serving.served_entities": false });

    expect(screen.getAllByText(/serving\.served_entities grant required/i).length).toBeGreaterThan(0);
  });

  it("does NOT show unavailable state when grant is undefined (unknown)", () => {
    // undefined = not yet loaded: must NOT block rendering
    renderView({ "system.query.history": undefined });

    const unavailableEls = screen.queryAllByText(/query\.history grant required/i);
    expect(unavailableEls).toHaveLength(0);
  });

  it("does NOT show unavailable state when grant is true", () => {
    renderView({
      "system.query.history": true,
      "system.lakeflow.pipelines": true,
      "system.compute.clusters": true,
      "system.serving.served_entities": true,
    });

    expect(screen.queryAllByText(/grant required/i)).toHaveLength(0);
    expect(screen.queryAllByText(/Unavailable/i)).toHaveLength(0);
  });
});

describe("PlatformKPIsView: successful run result-state availability", () => {
  beforeEach(() => {
    vi.mocked(useFeatureAvailability).mockReset();
  });

  it("shows a true zero with the normal tile when result states are available", () => {
    renderView({}, { ...SAMPLE_DATA, successful_runs: 0, successful_runs_available: true });

    const title = screen.getByText("Successful Runs");
    const card = title.closest(".co-kpi-card");
    expect(card).toHaveTextContent("0");
    expect(card).toHaveTextContent("0.0% success rate");
    expect(card?.querySelector(".co-kpi-card__icon")).not.toBeNull();
    expect(card?.querySelector(".text-lava")).not.toBeNull();
  });

  it("omits the card when result-state data is unavailable", () => {
    renderView({}, { ...SAMPLE_DATA, successful_runs: 0, successful_runs_available: false });

    expect(screen.queryByText("Successful Runs")).not.toBeInTheDocument();
    expect(screen.queryByText("Result states unavailable")).not.toBeInTheDocument();
  });

  it("does not advertise empty job trends when the period has no jobs or runs", () => {
    renderView({}, {
      ...SAMPLE_DATA,
      total_jobs: 0,
      total_job_runs: 0,
      successful_runs: 0,
      successful_runs_available: false,
    });

    const jobsCard = screen.getByText("Total Active Jobs").closest(".co-kpi-card");
    const runsCard = screen.getByText("Job Runs").closest(".co-kpi-card");
    expect(jobsCard).not.toHaveTextContent("See trend");
    expect(runsCard).not.toHaveTextContent("See trend");
  });
});

describe("PlatformKPIsView: card trend mappings", () => {
  it.each([
    ["Total Queries Executed", "total_queries"],
    ["Rows Processed", "total_rows_read"],
    ["Data Processed", "total_bytes_read"],
    ["Compute Time", "total_compute_seconds"],
    ["Total Active Jobs", "total_jobs"],
    ["Job Runs", "total_job_runs"],
    ["Successful Runs", "successful_runs"],
    ["Total Compute Resources", "active_notebooks"],
    ["Active Workspaces", "active_workspaces"],
    ["Unique Models Served", "models_served"],
    ["Unique Active Users", "total_users"],
    ["Usage Stickiness", "stickiness"],
  ])("maps %s to %s", (title, expectedKpi) => {
    renderView({}, SAMPLE_DATA, true);

    const card = screen.getByText(title).closest(".co-kpi-card")!;
    expect(card.tagName).toBe("BUTTON");
    expect(card).toHaveAccessibleName(`See ${title} trend`);
    expect(card.querySelector("button")).toBeNull();
    fireEvent.click(card);

    expect(screen.getByTestId("selected-kpi")).toHaveTextContent(expectedKpi);
  });
});

describe("PlatformKPIsView: managed query-user population", () => {
  it("does not derive stickiness from a different fallback population", () => {
    renderView({}, {
      ...SAMPLE_DATA,
      stickiness_pct: null,
      avg_daily_query_users: 8,
      unique_query_users: 10,
      stickiness_available: true,
    });

    const card = screen.getByText("Usage Stickiness").closest(".co-kpi-card");
    expect(card).toHaveTextContent("N/A");
    expect(card).not.toHaveTextContent("80%");
    expect(card).not.toHaveTextContent("Unavailable:");
  });

  it("shows both user KPIs as unavailable when the managed source failed", () => {
    renderView({}, {
      ...SAMPLE_DATA,
      query_users_available: false,
      stickiness_available: false,
    });

    for (const title of ["Unique Active Users", "Usage Stickiness"]) {
      const card = screen.getByText(title).closest(".co-kpi-card");
      expect(card).toHaveTextContent("N/A");
      expect(card).toHaveTextContent("managed query-user data is unavailable");
      expect(card).not.toHaveTextContent("See trend");
    }
  });
});

describe("PlatformKPIsView: compact KPI values", () => {
  it("keeps a large compute duration and its unit in one value element", () => {
    renderView({}, {
      ...SAMPLE_DATA,
      total_compute_seconds: 21_924.7 * 24 * 60 * 60,
    });

    const value = screen.getByText("21.9K days");
    expect(value.tagName).toBe("SPAN");
    expect(value).toHaveClass("co-kpi-card__value");
    expect(value.closest(".co-kpi-card")).toHaveTextContent("Compute Time");
  });
});
