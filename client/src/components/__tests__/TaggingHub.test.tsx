import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it, vi } from "vitest";
import type { TaggingDashboardBundle } from "@/types/billing";
import { TaggingHub } from "../TaggingHub";

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
};

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
