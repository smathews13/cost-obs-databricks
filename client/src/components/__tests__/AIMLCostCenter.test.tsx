import { fireEvent, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { describe, expect, it, vi } from "vitest";
import type { AIMLDashboardBundle } from "@/types/billing";
import { AIMLCostCenter } from "../AIMLCostCenter";

vi.mock("../KPITrendModal", () => ({
  KPITrendModal: ({ kpi }: { kpi: string }) => <div data-testid="aiml-selected-kpi">{kpi}</div>,
}));

const DATA: AIMLDashboardBundle = {
  summary: {
    total_dbus: 10,
    total_spend: 20,
    workspace_count: 2,
    endpoint_count: 3,
    days_in_range: 30,
    avg_daily_spend: 2,
    avg_cost_per_endpoint: 4,
    start_date: "2026-08-01",
    end_date: "2026-08-30",
    first_date: "2026-08-01",
    last_date: "2026-08-30",
  },
  providers: { providers: [], total_spend: 0, start_date: "2026-08-01", end_date: "2026-08-30" },
  endpoints: { endpoints: [], total_spend: 0, start_date: "2026-08-01", end_date: "2026-08-30" },
  categories: { categories: [], total_spend: 0, start_date: "2026-08-01", end_date: "2026-08-30" },
  timeseries: { timeseries: [], categories: [], start_date: "2026-08-01", end_date: "2026-08-30" },
  start_date: "2026-08-01",
  end_date: "2026-08-30",
};

describe("AIMLCostCenter error state", () => {
  it.each([
    ["Total AI/ML Spend", "aiml_spend"],
    ["Total DBUs", "aiml_dbus"],
    ["Active Endpoints", "aiml_endpoints"],
    ["Endpoint Cost", "aiml_avg_endpoint_cost"],
  ])("opens the %s trend from the full KPI card", (title, kpi) => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <AIMLCostCenter
          data={DATA}
          isLoading={false}
          startDate="2026-08-01"
          endDate="2026-08-30"
        />
      </QueryClientProvider>,
    );

    const card = screen.getByRole("button", { name: `See ${title} trend` });
    expect(card).toHaveClass("co-kpi-card");
    expect(card.querySelector("button")).toBeNull();
    fireEvent.click(card);
    expect(screen.getByTestId("aiml-selected-kpi")).toHaveTextContent(kpi);
  });

  it("shows a settled error instead of a loading or blank state", () => {
    const onRetry = vi.fn();
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });

    render(
      <QueryClientProvider client={client}>
        <AIMLCostCenter
          data={undefined}
          isLoading
          isError
          error={new Error("Timed out waiting for AI/ML data")}
          onRetry={onRetry}
        />
      </QueryClientProvider>,
    );

    expect(screen.getByText("Failed to load AI/ML data")).toBeInTheDocument();
    expect(screen.getByText("Timed out waiting for AI/ML data")).toBeInTheDocument();
    expect(screen.queryByRole("status", { name: /loading/i })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(onRetry).toHaveBeenCalledOnce();
  });
});
