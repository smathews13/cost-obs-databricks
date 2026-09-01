import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it, vi } from "vitest";
import type { TaggingDashboardBundle } from "@/types/billing";
import { TaggingHub } from "../TaggingHub";

vi.mock("../KPITrendModal", () => ({
  KPITrendModal: ({ kpi, dataOverride }: { kpi: string; dataOverride?: { data_points?: unknown[] } }) => (
    <div data-testid="tagging-selected-kpi" data-local-points={dataOverride?.data_points?.length ?? 0}>
      {kpi}
    </div>
  ),
}));

const emptyGroup = { items: [], total_spend: 0, count: 0 };
const DATA: TaggingDashboardBundle = {
  summary: {
    tagged_spend: 10,
    untagged_spend: 0,
    total_spend: 10,
    tagged_percentage: 100,
    untagged_percentage: 0,
    tagged_workspaces: 1,
    untagged_workspaces: 0,
    start_date: "2026-08-01",
    end_date: "2026-08-28",
  },
  untagged: {
    clusters: emptyGroup,
    jobs: emptyGroup,
    pipelines: emptyGroup,
    warehouses: emptyGroup,
    endpoints: emptyGroup,
  },
  cost_by_tag: {
    tags: [{
      tag_key: "DataClassification",
      tag_value: "Confidential",
      total_dbus: 5,
      total_spend: 10,
      workspace_count: 1,
      days_active: 2,
      percentage: 100,
    }],
    total_spend: 10,
    start_date: "2026-08-01",
    end_date: "2026-08-28",
  },
  timeseries: { timeseries: [], categories: [], start_date: "2026-08-01", end_date: "2026-08-28" },
  start_date: "2026-08-01",
  end_date: "2026-08-28",
  avg_cost_per_tag: 5,
  total_tag_count: 2,
};

it.each([
  ["Tagged Spend", "tagged_spend"],
  ["Cost Per-Tag", "cost_per_tag"],
  ["Total Tags", "total_tags"],
])("opens the %s trend from the full KPI card", async (title, kpi) => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <TaggingHub
        data={DATA}
        isLoading={false}
        startDate="2026-08-01"
        endDate="2026-08-28"
      />
    </QueryClientProvider>,
  );

  const card = screen.getByRole("button", { name: `See ${title} trend` });
  expect(card).toHaveClass("co-kpi-card");
  expect(card.querySelector("button")).toBeNull();
  await userEvent.click(card);
  expect(screen.getByTestId("tagging-selected-kpi")).toHaveTextContent(kpi);
});

it("keeps zero-value spend cards static", () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <TaggingHub data={DATA} isLoading={false} startDate="2026-08-01" endDate="2026-08-28" />
    </QueryClientProvider>,
  );

  expect(screen.queryByRole("button", { name: "See Untagged Spend trend" })).not.toBeInTheDocument();
});

it("derives spend KPIs from a populated timeseries instead of showing false zeroes", () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const data: TaggingDashboardBundle = {
    ...DATA,
    summary: {
      ...DATA.summary,
      tagged_spend: 0,
      untagged_spend: 0,
      total_spend: 0,
      tagged_percentage: 0,
      untagged_percentage: 0,
    },
    timeseries: {
      ...DATA.timeseries,
      timeseries: [{ date: "2026-08-01", Tagged: 400, Untagged: 20 }],
    },
  };
  render(
    <QueryClientProvider client={client}>
      <TaggingHub data={data} isLoading={false} startDate="2026-08-01" endDate="2026-08-28" />
    </QueryClientProvider>,
  );

  expect(screen.getByText("$400")).toBeVisible();
  expect(screen.getByText("$20")).toBeVisible();
});

it("opens untagged spend from loaded bundle timeseries without a cold query", async () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const data: TaggingDashboardBundle = {
    ...DATA,
    summary: {
      ...DATA.summary,
      tagged_spend: 15,
      untagged_spend: 5,
      total_spend: 20,
      tagged_percentage: 75,
      untagged_percentage: 25,
    },
    timeseries: {
      ...DATA.timeseries,
      timeseries: [
        { date: "2026-08-01", Tagged: 10, Untagged: 2 },
        { date: "2026-08-02", Tagged: 5, Untagged: 3 },
      ],
    },
  };
  render(
    <QueryClientProvider client={client}>
      <TaggingHub data={data} isLoading={false} startDate="2026-08-01" endDate="2026-08-28" />
    </QueryClientProvider>,
  );

  await userEvent.click(screen.getByRole("button", { name: "See Untagged Spend trend" }));

  expect(screen.getByTestId("tagging-selected-kpi")).toHaveAttribute("data-local-points", "2");
});

it("opens a key-wide drilldown when a Spend by Key row is clicked", async () => {
  const fetchMock = vi.fn().mockResolvedValue({
    json: async () => ({ objects: [] }),
  });
  vi.stubGlobal("fetch", fetchMock);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <TaggingHub data={DATA} isLoading={false} />
    </QueryClientProvider>,
  );

  const card = screen.getByRole("heading", { name: "Spend by Key" }).parentElement?.parentElement;
  expect(card).not.toBeNull();
  await userEvent.click(within(card as HTMLElement).getByText("DataClassification"));

  expect(screen.getByText("Top 5 Objects Across All Values")).toBeInTheDocument();
  expect(fetchMock).toHaveBeenCalledWith(expect.stringMatching(/tag_key=DataClassification/));
  expect(fetchMock.mock.calls[0][0]).not.toContain("tag_value=");
  vi.unstubAllGlobals();
});

it("matches Spend by Tag cell sizing and truncation in Spend by Key", () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <TaggingHub data={DATA} isLoading={false} />
    </QueryClientProvider>,
  );

  const card = screen.getByRole("heading", { name: "Spend by Key" }).parentElement?.parentElement;
  const keyBadge = within(card as HTMLElement).getByText("DataClassification");
  expect(keyBadge).toHaveClass("inline-block", "max-w-full", "truncate", "px-1.5", "py-0.5");
  expect(keyBadge.closest("td")).toHaveStyle({ width: "100px", maxWidth: "100px" });
  expect(keyBadge.closest("tr")?.querySelector(".h-1\\.5.w-10")).not.toBeNull();
});

it("uses compact accessible dropdown controls for tag value and key filters", () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <TaggingHub data={DATA} isLoading={false} />
    </QueryClientProvider>,
  );

  const valueFilter = screen.getByRole("button", { name: "Filter spend by tag value" });
  const keyFilter = screen.getByRole("button", { name: "Filter spend by tag key" });

  for (const filter of [valueFilter, keyFilter]) {
    expect(filter).toHaveClass("rounded-full", "justify-between", "text-xs");
    expect(filter).toHaveAttribute("aria-haspopup", "menu");
    expect(filter).toHaveAttribute("aria-expanded", "false");
    expect(filter.querySelector("svg")).not.toBeNull();
  }
});

it("totals the complete filtered untagged table set rather than the summary or visible page", async () => {
  const clusters = Array.from({ length: 12 }, (_, index) => {
    const number = index + 1;
    const id = `cluster-${number}`;
    return {
      cluster_id: id,
      cluster_name: number === 12 ? id : `Cluster ${number}`,
      cluster_source: "UI",
      owner: "owner@example.com",
      workspace_id: "123",
      total_dbus: 1,
      total_spend: 10,
      days_active: 1,
    };
  });
  const data: TaggingDashboardBundle = {
    ...DATA,
    summary: {
      ...DATA.summary,
      untagged_spend: 999,
      total_spend: 1_009,
      untagged_percentage: 99,
    },
    untagged: {
      ...DATA.untagged,
      clusters: { items: clusters, total_spend: 120, count: 12 },
    },
  };
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <TaggingHub data={data} isLoading={false} />
    </QueryClientProvider>,
  );

  expect(screen.getByText("$110 untagged spend")).toBeInTheDocument();
  expect(screen.queryByText("$999 untagged spend")).not.toBeInTheDocument();

  await userEvent.click(screen.getByRole("checkbox", { name: /show historical/i }));
  expect(screen.getByText("$120 untagged spend")).toBeInTheDocument();

  await userEvent.type(screen.getByPlaceholderText("Search resources..."), "Cluster 11");
  expect(screen.getByText("$10 untagged spend")).toBeInTheDocument();
});

it("keeps aggregate spend but never invents zero when shared scope suppresses details", () => {
  const sharedScopeData: TaggingDashboardBundle = {
    ...DATA,
    local_detail_in_scope: false,
    summary: {
      ...DATA.summary,
      untagged_spend: 500,
      total_spend: 510,
      untagged_percentage: 98,
    },
  };
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });

  render(
    <QueryClientProvider client={client}>
      <TaggingHub data={sharedScopeData} isLoading={false} />
    </QueryClientProvider>,
  );

  expect(screen.getByText("Resource details unavailable for this source scope"))
    .toBeInTheDocument();
  expect(screen.getByText("$500 aggregate untagged spend")).toBeInTheDocument();
  expect(screen.queryByText("$0 untagged spend")).not.toBeInTheDocument();
  expect(screen.queryByText(/great job/i)).not.toBeInTheDocument();
});
