import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { WorkspaceTable } from "../WorkspaceTable";
import type { WorkspaceBreakdownResponse } from "@/types/billing";

const data = {
  workspaces: [
    {
      workspace_id: "workspace-1",
      workspace_name: "Primary workspace",
      total_dbus: 12,
      total_spend: 42,
      percentage: 100,
      top_products: ["APPS", "SQL"],
      top_users: ["user@example.com", "service-principal"],
      historical: false,
    },
    {
      workspace_id: "workspace-2",
      workspace_name: null,
      total_dbus: 1,
      total_spend: 2,
      percentage: 4,
      top_products: ["APPS"],
      top_users: ["service-principal"],
      historical: true,
    },
  ],
  total_spend: 44,
} as WorkspaceBreakdownResponse;

function renderTable(workspaceNameMap?: Record<string, string>) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={client}>
      <WorkspaceTable
        data={data}
        isLoading={false}
        host="https://workspace.example.com"
        workspaceNameMap={workspaceNameMap}
      />
    </QueryClientProvider>,
  );
}

describe("WorkspaceTable floating controls", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      json: async () => ({ sources: [] }),
    }));
  });

  it.each(["Products", "Users"])("elevates the %s menu above adjacent cards", (label) => {
    renderTable();
    fireEvent.click(screen.getByRole("button", { name: label }));

    const menuHeading = screen.getByText(label, { selector: "span" });
    const menu = menuHeading.closest(".co-floating-menu");
    expect(menu).toBeInTheDocument();
    expect(menu?.parentElement).toBe(document.body);
  });

  it("opens historical help without toggling the historical filter", () => {
    renderTable();
    const checkbox = screen.getByRole("checkbox");

    fireEvent.click(screen.getByRole("button", { name: "About historical workspaces" }));

    expect(checkbox).not.toBeChecked();
    expect(screen.getByRole("tooltip")).toHaveTextContent("decommissioned or inaccessible");
  });

  it("treats an account-resolved workspace name as current even when billing marks it historical", () => {
    renderTable({ "workspace-2": "Recovered workspace" });

    expect(screen.getByText("Recovered workspace")).toBeVisible();
    expect(screen.queryByText("Historical")).not.toBeInTheDocument();
  });
});
