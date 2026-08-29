import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  WarehouseGuidanceBanner,
  WarehouseHealthCheckBanner,
} from "../WarehouseGuidanceBanner";

const smallWarehouse = {
  status: "warm" as const,
  warehouse_id: "warehouse-123",
  warehouse_size: "X-Small",
  warehouse_type: "SERVERLESS",
};

afterEach(() => {
  cleanup();
  localStorage.clear();
});

describe("WarehouseGuidanceBanner", () => {
  it("shows the exact recommendation and links to the bound warehouse", () => {
    render(
      <WarehouseGuidanceBanner
        warehouse={smallWarehouse}
        workspaceHost="https://dbc.example.com"
      />,
    );

    expect(screen.getByText(
      "The recommended warehouse size is Medium, otherwise query latency may be volatile.",
    )).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Manage warehouse/i })).toHaveAttribute(
      "href",
      "https://dbc.example.com/sql/warehouses/warehouse-123/edit",
    );
    expect(screen.getByText(/X-Small · serverless/i)).toBeInTheDocument();
  });

  it("persists dismissal only after the rendered warning is dismissed", async () => {
    const user = userEvent.setup();
    const key = "coc-warehouse-size-guidance:test-v1:warehouse-123";
    const first = render(
      <WarehouseGuidanceBanner
        warehouse={smallWarehouse}
        workspaceHost="dbc.example.com"
        warningVersion="test-v1"
      />,
    );

    expect(localStorage.getItem(key)).toBeNull();
    await user.click(screen.getByRole("button", { name: /Dismiss warehouse size recommendation/i }));
    expect(localStorage.getItem(key)).toBe("1");
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();

    first.unmount();
    render(
      <WarehouseGuidanceBanner
        warehouse={smallWarehouse}
        workspaceHost="dbc.example.com"
        warningVersion="test-v1"
      />,
    );
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();

    cleanup();
    render(
      <WarehouseGuidanceBanner
        warehouse={smallWarehouse}
        workspaceHost="dbc.example.com"
        warningVersion="test-v2"
      />,
    );
    expect(screen.getByRole("alert")).toBeInTheDocument();
  });

  it("stays hidden for Medium-or-larger warehouses with no manage action", () => {
    render(
      <WarehouseGuidanceBanner
        warehouse={{ ...smallWarehouse, warehouse_size: "Medium" }}
        workspaceHost="dbc.example.com"
      />,
    );

    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /Manage warehouse/i })).not.toBeInTheDocument();
  });
});

describe("WarehouseHealthCheckBanner", () => {
  it("reports an uncertain status without calling the warehouse unavailable", async () => {
    const user = userEvent.setup();
    const onRetry = vi.fn();
    render(<WarehouseHealthCheckBanner isRetrying={false} onRetry={onRetry} />);

    expect(screen.getByRole("alert")).toHaveTextContent(
      "Warehouse status could not be verified. The app will keep loading data normally.",
    );
    expect(screen.queryByText(/SQL Warehouse unavailable/i)).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Retry status check" }));
    expect(onRetry).toHaveBeenCalledOnce();
  });
});
