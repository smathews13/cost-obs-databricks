import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SourceLabelFilter } from "../SourceLabelFilter";
import { WorkspaceFilter } from "../WorkspaceFilter";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("chrome filter variants", () => {
  const workspaces = [
    { workspace_id: "1", workspace_name: "Workspace one" },
    { workspace_id: "2", workspace_name: "Workspace two" },
  ];
  const renderWithQueryClient = (component: React.ReactNode) => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    return render(<QueryClientProvider client={client}>{component}</QueryClientProvider>);
  };

  it("keeps the existing header trigger by default", () => {
    renderWithQueryClient(
      <WorkspaceFilter
        workspaces={workspaces}
        selectedIds={[]}
        onChange={vi.fn()}
      />,
    );

    expect(screen.getByRole("button", { name: /All Workspaces/i })).toHaveClass("co-filter");
  });

  it("renders the workspace trigger with the rail skin", () => {
    renderWithQueryClient(
      <WorkspaceFilter
        workspaces={workspaces}
        selectedIds={[]}
        onChange={vi.fn()}
        variant="rail"
      />,
    );

    const trigger = screen.getByRole("button", { name: /All Workspaces/i });
    expect(trigger).toHaveClass(
      "rail-workspace-filter",
      "h-[32px]",
      "max-w-[116px]",
      "min-[1280px]:max-w-[190px]",
      "bg-white/[.07]",
      "border-white/[.16]",
    );
    expect(screen.getByText("Workspaces")).toHaveClass("min-[1180px]:hidden");
    expect(trigger).not.toHaveClass("co-filter");
  });

  it("renders the source trigger with the rail skin", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      json: vi.fn().mockResolvedValue({
        local_label: "This workspace",
        sources: [{ label: "Shared source", catalog: "main", schema: "billing" }],
      }),
    }));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    render(
      <QueryClientProvider client={client}>
        <SourceLabelFilter variant="rail" />
      </QueryClientProvider>,
    );

    const trigger = await screen.findByRole("button", { name: /All sources/i });
    expect(trigger).toHaveClass(
      "rail-source-filter",
      "h-[32px]",
      "max-w-[104px]",
      "min-[1280px]:max-w-[190px]",
      "bg-white/[.07]",
      "border-white/[.16]",
    );
    expect(screen.getByText("Sources")).toHaveClass("min-[1180px]:hidden");
    expect(trigger).not.toHaveClass("co-filter");
  });
});
