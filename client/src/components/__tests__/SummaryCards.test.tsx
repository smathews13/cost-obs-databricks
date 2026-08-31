import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { SummaryCards } from "../SummaryCards";

describe("SummaryCards KPI formatting", () => {
  it("renders large values compactly in a no-wrap value element", () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <SummaryCards
          data={{
            total_dbus: 22_008_909,
            total_spend: 1_687_075.63,
            workspace_count: 5_500,
            days_in_range: 30,
            avg_daily_spend: 0.63,
            start_date: "2026-08-01",
            end_date: "2026-08-30",
            first_date: "2026-08-01",
            last_date: "2026-08-30",
          }}
          isLoading={false}
        />
      </QueryClientProvider>,
    );

    expect(screen.getByText("$1.69M")).toHaveClass("whitespace-nowrap");
    expect(screen.getByText("22M")).toHaveClass("whitespace-nowrap");
    expect(screen.getByText("$0.63")).toHaveClass("whitespace-nowrap");
  });
});
