import { fireEvent, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { describe, expect, it, vi } from "vitest";
import type { AIMLDashboardBundle } from "@/types/billing";
import { AIMLCostCenter } from "../AIMLCostCenter";
import {
  AIML_CATEGORY_COLORS,
  buildAimlCategoryColorMap,
} from "../aimlCategoryColors";

vi.mock("../KPITrendModal", () => ({
  KPITrendModal: ({ kpi }: { kpi: string }) => <div data-testid="aiml-selected-kpi">{kpi}</div>,
}));

it("assigns distinct colors to the AI categories shown together", () => {
  const colors = Object.values(AIML_CATEGORY_COLORS);
  expect(new Set(colors).size).toBe(colors.length);
});

it("allocates stable unique fallback colors beyond the base palette", () => {
  const categories = [
    ...Object.keys(AIML_CATEGORY_COLORS),
    ...Array.from({ length: 30 }, (_, index) => `Future AI category ${index}`),
  ];
  const first = buildAimlCategoryColorMap(categories);
  const second = buildAimlCategoryColorMap([...categories].reverse());

  expect(second).toEqual(first);
  expect(new Set(Object.values(first)).size).toBe(categories.length);
});

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

  it("keeps zero-value KPI cards noninteractive", () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const zeroData = {
      ...DATA,
      summary: {
        ...DATA.summary,
        total_spend: 0,
        total_dbus: 0,
        endpoint_count: 0,
        avg_cost_per_endpoint: 0,
      },
    };
    render(
      <QueryClientProvider client={client}>
        <AIMLCostCenter
          data={zeroData}
          isLoading={false}
          startDate="2026-08-01"
          endDate="2026-08-30"
        />
      </QueryClientProvider>,
    );

    for (const title of [
      "Total AI/ML Spend",
      "Total DBUs",
      "Active Endpoints",
      "Endpoint Cost",
    ]) {
      expect(screen.queryByRole("button", { name: `See ${title} trend` })).toBeNull();
    }
  });

  it("keeps all four KPI cards static when their underlying values are zero", () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const zeroData: AIMLDashboardBundle = {
      ...DATA,
      summary: {
        ...DATA.summary,
        total_dbus: 0,
        total_spend: 0,
        endpoint_count: 0,
        avg_cost_per_endpoint: 0,
      },
    };
    render(
      <QueryClientProvider client={client}>
        <AIMLCostCenter
          data={zeroData}
          isLoading={false}
          startDate="2026-08-01"
          endDate="2026-08-30"
        />
      </QueryClientProvider>,
    );

    for (const title of ["Total AI/ML Spend", "Total DBUs", "Active Endpoints", "Endpoint Cost"]) {
      expect(screen.queryByRole("button", { name: `See ${title} trend` })).not.toBeInTheDocument();
      expect(screen.getByText(title).closest(".co-kpi-card")?.tagName).toBe("DIV");
    }
    expect(screen.queryByText("See trend")).not.toBeInTheDocument();
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
          error={new Error("Failed /api/aiml?workspace_ids=987654321")}
          onRetry={onRetry}
        />
      </QueryClientProvider>,
    );

    expect(screen.getByText("Failed to load AI/ML data")).toBeInTheDocument();
    expect(screen.getByText("AI/ML data is temporarily unavailable. Retry shortly.")).toBeInTheDocument();
    expect(screen.queryByText(/987654321/)).not.toBeInTheDocument();
    expect(screen.queryByRole("status", { name: /loading/i })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(onRetry).toHaveBeenCalledOnce();
  });
});
