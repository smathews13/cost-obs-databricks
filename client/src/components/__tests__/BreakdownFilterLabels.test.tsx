import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type {
  ProductBreakdownResponse,
  SKUBreakdownResponse,
  WorkspaceBreakdown,
} from "@/types/billing";
import { ProductBreakdown } from "../ProductBreakdown";
import { SKUBreakdown } from "../SKUBreakdown";

const workspaces: WorkspaceBreakdown[] = ["1", "2"].map((workspace_id) => ({
  workspace_id,
  workspace_name: `Workspace ${workspace_id}`,
  total_dbus: 1,
  total_spend: 1,
  percentage: 50,
  top_products: ["SQL"],
  top_users: [],
}));

const productData: ProductBreakdownResponse = {
  products: [{
    category: "SQL",
    total_dbus: 2,
    total_spend: 2,
    workspace_count: 2,
    percentage: 100,
  }],
  total_spend: 2,
  start_date: "2026-08-01",
  end_date: "2026-08-30",
};

const skuData: SKUBreakdownResponse = {
  skus: [{
    product: "SERVERLESS_SQL_COMPUTE",
    workspaces_using: 2,
    total_dbus: 2,
    total_spend: 2,
    percentage: 100,
  }],
  total_spend: 2,
  start_date: "2026-08-01",
  end_date: "2026-08-30",
};

describe("breakdown workspace filter labels", () => {
  it.each([
    ["product", <ProductBreakdown data={productData} isLoading={false} workspaces={workspaces} />],
    ["SKU", <SKUBreakdown data={skuData} isLoading={false} workspaces={workspaces} />],
  ])("uses singular Workspace for the %s trigger", (_name, component) => {
    render(component);

    expect(screen.getByRole("button", { name: "Workspace" })).toBeVisible();
    expect(screen.queryByRole("button", { name: "Workspaces" })).not.toBeInTheDocument();
  });

  it("keeps an unfiltered product chart account-wide when workspace metadata is absent", () => {
    render(<ProductBreakdown data={productData} isLoading={false} workspaces={undefined} />);

    expect(screen.getByText("Spend by Product")).toBeVisible();
    expect(screen.queryByText(/No product spend matches/i)).not.toBeInTheDocument();
  });

  it("shows no-matches copy only after an explicit workspace filter is cleared", () => {
    render(<ProductBreakdown data={productData} isLoading={false} workspaces={workspaces} />);

    fireEvent.click(screen.getByRole("button", { name: "Workspace" }));
    fireEvent.click(screen.getByRole("button", { name: "Clear" }));

    expect(screen.getByText(/No product spend matches the selected workspace filters/i))
      .toBeVisible();
  });

  it("shows null selection as all checked and first click deselects only that workspace", () => {
    vi.stubGlobal("fetch", vi.fn(() => new Promise<Response>(() => {})));
    render(<ProductBreakdown data={productData} isLoading={false} workspaces={workspaces} />);

    fireEvent.click(screen.getByRole("button", { name: "Workspace" }));
    const first = screen.getByRole("button", { name: "Workspace 1" });
    const second = screen.getByRole("button", { name: "Workspace 2" });
    expect(first.querySelector("svg")).not.toBeNull();
    expect(second.querySelector("svg")).not.toBeNull();

    fireEvent.click(first);

    expect(first.querySelector("svg")).toBeNull();
    expect(second.querySelector("svg")).not.toBeNull();
  });
});
