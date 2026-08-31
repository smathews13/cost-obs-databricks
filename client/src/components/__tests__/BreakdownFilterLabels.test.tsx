import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
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
});
