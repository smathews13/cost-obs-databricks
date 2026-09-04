import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SummaryCards } from "../SummaryCards";

afterEach(() => {
  vi.unstubAllGlobals();
});

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

  it("fetches a trend on click and reuses the cached result when reopened", async () => {
    const fetchMock = vi.fn(async () => new Response(JSON.stringify({
      kpi: "total_spend",
      granularity: "daily",
      data_points: [],
      summary: {
        period_start_value: 0,
        period_end_value: 0,
        change_amount: 0,
        change_percent: 0,
        min_value: 0,
        max_value: 0,
        avg_value: 0,
        trend: "flat",
      },
    }), { status: 200, headers: { "Content-Type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <SummaryCards
          data={{
            total_dbus: 10,
            total_spend: 20,
            workspace_count: 1,
            days_in_range: 30,
            avg_daily_spend: 0.67,
            start_date: "2026-08-01",
            end_date: "2026-08-30",
            first_date: "2026-08-01",
            last_date: "2026-08-30",
          }}
          isLoading={false}
          startDate="2026-08-01"
          endDate="2026-08-30"
        />
      </QueryClientProvider>,
    );

    expect(fetchMock).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole("button", { name: "See Total Spend trend" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    await userEvent.keyboard("{Escape}");
    await userEvent.click(screen.getByRole("button", { name: "See Total Spend trend" }));
    await waitFor(() => expect(screen.getByText("Trend Analysis")).toBeVisible());
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("does not expose dead trend actions without dates", () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <SummaryCards data={undefined} isLoading={false} />
      </QueryClientProvider>,
    );

    expect(screen.queryByRole("button", { name: /trend$/ })).not.toBeInTheDocument();
    for (const title of ["Total Spend", "Total DBUs", "Average Daily Spend", "Workspaces"]) {
      expect(screen.getByText(title).closest(".co-kpi-card")?.tagName).toBe("DIV");
    }
  });

  it("uses the workspace-picker population instead of latest-day activity", () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <SummaryCards
          data={{
            total_dbus: 10,
            total_spend: 20,
            workspace_count: 2,
            days_in_range: 30,
            avg_daily_spend: 0.67,
            start_date: "2026-08-01",
            end_date: "2026-08-30",
            first_date: "2026-08-01",
            last_date: "2026-08-30",
          }}
          workspaceScopeCount={3}
          isLoading={false}
        />
      </QueryClientProvider>,
    );

    expect(screen.getByText("Workspaces").closest(".co-kpi-card")).toHaveTextContent("3");
    expect(screen.getByText("Average Daily Spend").closest(".co-kpi-card"))
      .toHaveTextContent("across 3 workspaces");
  });

  it("never relabels latest-day activity as workspace filter scope", () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <SummaryCards
          data={{
            total_dbus: 10,
            total_spend: 20,
            workspace_count: 2,
            days_in_range: 30,
            avg_daily_spend: 0.67,
            start_date: "2026-08-01",
            end_date: "2026-08-30",
            first_date: "2026-08-01",
            last_date: "2026-08-30",
          }}
          isLoading={false}
        />
      </QueryClientProvider>,
    );

    expect(screen.getByText("Workspaces").closest(".co-kpi-card")).toHaveTextContent(
      "N/A",
    );
    expect(screen.getByText("Workspaces").closest(".co-kpi-card")).toHaveTextContent(
      "workspace scope unavailable",
    );
    expect(screen.getByText("Average Daily Spend").closest(".co-kpi-card"))
      .toHaveTextContent("daily average");
  });

  it("gives every interactive summary card a metric-specific full-card label", () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <SummaryCards
          data={{
            total_dbus: 10,
            total_spend: 20,
            workspace_count: 1,
            days_in_range: 30,
            avg_daily_spend: 0.67,
            start_date: "2026-08-01",
            end_date: "2026-08-30",
            first_date: "2026-08-01",
            last_date: "2026-08-30",
          }}
          isLoading={false}
          startDate="2026-08-01"
          endDate="2026-08-30"
        />
      </QueryClientProvider>,
    );

    for (const title of ["Total Spend", "Total DBUs", "Average Daily Spend", "Workspaces"]) {
      const card = screen.getByRole("button", { name: `See ${title} trend` });
      expect(card).toHaveClass("co-kpi-card");
      expect(card.querySelector("button")).toBeNull();
    }
  });
});
