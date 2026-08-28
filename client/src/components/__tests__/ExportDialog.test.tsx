import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { TabVisibility } from "../SettingsDialog";
import { ExportDialog } from "../ExportDialog";

const visibility: TabVisibility = {
  dbu: true,
  sql: true,
  infra: true,
  optimizer: true,
  kpis: true,
  aiml: true,
  apps: true,
  tagging: true,
  "users-groups": true,
};

describe("ExportDialog report data loading", () => {
  it("waits for on-demand report queries before exporting", () => {
    render(
      <ExportDialog
        isOpen
        onClose={vi.fn()}
        onExport={vi.fn()}
        tabVisibility={visibility}
        dataLoading
      />,
    );

    const button = screen.getByRole("button", { name: "Preparing report data" });
    expect(button).toBeDisabled();
  });

  it("hides architecture export when the feature is disabled", () => {
    render(
      <ExportDialog
        isOpen
        onClose={vi.fn()}
        onExport={vi.fn()}
        onExportArchitecture={vi.fn()}
        tabVisibility={visibility}
      />,
    );

    expect(screen.queryByText("Architecture PDF")).not.toBeInTheDocument();
  });

  it("shows and invokes the separate architecture PDF action", async () => {
    const onClose = vi.fn();
    const onExportArchitecture = vi.fn().mockResolvedValue(undefined);
    render(
      <ExportDialog
        isOpen
        onClose={onClose}
        onExport={vi.fn()}
        enableArchitectureView
        onExportArchitecture={onExportArchitecture}
        tabVisibility={visibility}
      />,
    );

    expect(screen.getByText("Architecture PDF")).toBeVisible();
    await userEvent.click(screen.getByRole("button", { name: "Download Architecture PDF" }));
    expect(onExportArchitecture).toHaveBeenCalledOnce();
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("shows architecture export progress while generation is pending", async () => {
    let resolveExport: (() => void) | undefined;
    const onExportArchitecture = vi.fn(() => new Promise<void>((resolve) => { resolveExport = resolve; }));
    render(
      <ExportDialog
        isOpen
        onClose={vi.fn()}
        onExport={vi.fn()}
        enableArchitectureView
        onExportArchitecture={onExportArchitecture}
        tabVisibility={visibility}
      />,
    );

    await userEvent.click(screen.getByRole("button", { name: "Download Architecture PDF" }));
    expect(screen.getByRole("button", { name: /Generating architecture PDF/ })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Close export dialog" })).toBeDisabled();
    resolveExport?.();
    await waitFor(() => expect(screen.getByRole("button", { name: "Download Architecture PDF" })).toBeEnabled());
  });

  it("keeps the dialog open and reports architecture export failures", async () => {
    const onClose = vi.fn();
    const onExportArchitecture = vi.fn().mockRejectedValue(new Error("render failed"));
    render(
      <ExportDialog
        isOpen
        onClose={onClose}
        onExport={vi.fn()}
        enableArchitectureView
        onExportArchitecture={onExportArchitecture}
        tabVisibility={visibility}
      />,
    );

    await userEvent.click(screen.getByRole("button", { name: "Download Architecture PDF" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("The architecture PDF could not be generated");
    expect(onClose).not.toHaveBeenCalled();
    expect(screen.getByRole("dialog")).toBeVisible();
  });
});
