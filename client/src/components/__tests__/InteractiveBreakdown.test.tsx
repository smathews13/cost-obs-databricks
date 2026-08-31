import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { InteractiveBreakdownResponse, InteractiveItem } from "@/types/billing";
import { InteractiveBreakdown } from "../InteractiveBreakdown";

function item(overrides: Partial<InteractiveItem>): InteractiveItem {
  return {
    cluster_id: "cluster-1",
    cluster_name: "Cluster One",
    notebook_path: "/Shared/notebook-a",
    user: "alice@example.com",
    workspace_id: "workspace-1",
    cluster_state: "RUNNING",
    total_dbus: 1,
    total_spend: 1,
    days_active: 1,
    notebook_count: 1,
    percentage: 0,
    ...overrides,
  };
}

function data(items: InteractiveItem[]): InteractiveBreakdownResponse {
  return {
    items,
    total_spend: items.reduce((sum, row) => sum + row.total_spend, 0),
    start_date: "2026-08-01",
    end_date: "2026-08-30",
  };
}

describe("InteractiveBreakdown transformations", () => {
  it("keeps grouping, historical filtering, searching, and totals stable across rerenders", () => {
    const rows = [
      item({ total_dbus: 5, total_spend: 10, days_active: 2 }),
      item({
        cluster_id: "cluster-2",
        cluster_name: "Cluster Two",
        notebook_path: "/Shared/notebook-b",
        total_dbus: 8,
        total_spend: 20,
        days_active: 3,
      }),
      item({
        user: "bob@example.com",
        total_dbus: 2,
        total_spend: 5,
      }),
      item({
        cluster_id: "historical-cluster",
        cluster_name: null,
        notebook_path: "/Shared/notebook-c",
        user: "bob@example.com",
        total_dbus: 1,
        total_spend: 3,
      }),
    ];
    const response = data(rows);
    const view = render(
      <InteractiveBreakdown data={response} isLoading={false} host={null} />,
    );

    expect(screen.getByText("2 users · 3 clusters · 3 notebooks")).toBeVisible();
    expect(screen.getByText("Total (2 users)")).toBeVisible();
    expect(screen.getAllByText("$38")).toHaveLength(1);

    fireEvent.click(screen.getByRole("button", { name: "By User" }));
    fireEvent.click(screen.getByRole("button", { name: "By Cluster" }));

    expect(screen.getByText("Show historical (1)")).toBeVisible();
    expect(screen.getByText("Total (2 clusters)")).toBeVisible();
    expect(screen.getAllByText("$35")).toHaveLength(1);

    fireEvent.change(screen.getByPlaceholderText("Search clusters..."), {
      target: { value: "cluster-1" },
    });
    expect(screen.getByText("Total (1 clusters)")).toBeVisible();
    expect(screen.getAllByText("$15")).toHaveLength(2);

    view.rerender(
      <InteractiveBreakdown
        data={{ ...response, items: response.items.map(row => ({ ...row })) }}
        isLoading={false}
        host={null}
      />,
    );
    expect(screen.getByPlaceholderText("Search clusters...")).toHaveValue("cluster-1");
    expect(screen.getByText("Total (1 clusters)")).toBeVisible();
    expect(screen.getAllByText("$15")).toHaveLength(2);
  });

  it("preserves the full 100-row bounded response through pagination", () => {
    const rows = Array.from({ length: 100 }, (_, index) => item({
      cluster_id: `cluster-${index}`,
      cluster_name: `Cluster ${index}`,
      notebook_path: `/Shared/notebook-${index}`,
      user: `user-${index}@example.com`,
    }));

    render(<InteractiveBreakdown data={data(rows)} isLoading={false} host={null} />);

    expect(screen.getByText("100 users · 100 clusters · 100 notebooks")).toBeVisible();
    expect(screen.getByText((_, element) =>
      element?.tagName === "P"
      && element.textContent === "Showing 1 to 10 of 100 users"
    )).toBeVisible();
  });
});
