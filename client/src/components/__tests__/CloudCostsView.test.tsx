import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it } from "vitest";
import { CloudCostsView } from "../CloudCostsView";

const EMPTY_COSTS = {
  clusters: [],
  instance_families: [],
  total_estimated_cost: 0,
  total_dbu_hours: 0,
  start_date: "2026-08-01",
  end_date: "2026-08-28",
};

function renderView(extraProps: Partial<React.ComponentProps<typeof CloudCostsView>> = {}) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <CloudCostsView
        data={EMPTY_COSTS}
        isLoading={false}
        timeseriesData={undefined}
        timeseriesLoading={false}
        host={null}
        {...extraProps}
      />
    </QueryClientProvider>,
  );
}

describe("CloudCostsView empty-data controls", () => {
  beforeEach(() => localStorage.clear());

  it("opens the integration wizard even when no estimate rows exist", async () => {
    renderView();

    await userEvent.click(screen.getByRole("button", { name: /integrate cloud costs/i }));

    expect(screen.getByText(/choose the cloud environment/i)).toBeInTheDocument();
  });

  it("allows the Actual Costs view to explain its unconfigured state", async () => {
    renderView();

    await userEvent.click(screen.getByRole("button", { name: "Actual Costs" }));

    expect(screen.getByText(/actual cloud costs are not connected yet/i)).toBeInTheDocument();
  });

  it("opens the actual provider selected in the multi-cloud switcher", async () => {
    renderView({
      actualData: { available: true, start_date: "2026-08-01", end_date: "2026-08-28" },
      azureActualData: { available: true, start_date: "2026-08-01", end_date: "2026-08-28" },
    });

    await userEvent.click(screen.getByRole("button", { name: /Azure/ }));
    await userEvent.click(screen.getByRole("button", { name: "Actual Costs" }));

    expect(screen.getByText("Actual Azure Infrastructure Cost")).toBeInTheDocument();
  });
});
