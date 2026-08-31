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
        requiredTabs={["dbu", "apps"]}
        tabLoading={{ dbu: true, apps: true }}
        onPrepare={vi.fn()}
      />,
    );

    const button = screen.getByRole("button", { name: "Preparing report data: 0 of 2 tabs ready" });
    expect(button).toBeDisabled();
  });

  it("waits for an explicit prepare action and demands only selected section tabs", async () => {
    const onPrepare = vi.fn();
    render(
      <ExportDialog
        isOpen
        onClose={vi.fn()}
        onExport={vi.fn()}
        tabVisibility={visibility}
        onPrepare={onPrepare}
      />,
    );

    expect(onPrepare).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole("button", { name: "Select none" }));
    await userEvent.click(screen.getByRole("checkbox", { name: /Executive Summary/ }));
    await userEvent.click(screen.getByRole("checkbox", { name: /Apps/ }));
    await userEvent.click(screen.getByRole("button", { name: "Prepare report data" }));

    expect(onPrepare).toHaveBeenCalledOnce();
    expect(onPrepare.mock.calls[0][0]).toMatchObject({
      summary: true,
      apps: true,
      query360: false,
      awsCosts: false,
    });
  });

  it("blocks failed selected sections, annotates them, and retries", async () => {
    const onRetryFailed = vi.fn().mockResolvedValue(undefined);
    const onExport = vi.fn();
    render(
      <ExportDialog
        isOpen
        onClose={vi.fn()}
        onExport={onExport}
        tabVisibility={visibility}
        dataPrepared
        requiredTabs={["apps"]}
        dataErrors={{ apps: "Apps data failed to load." }}
        onRetryFailed={onRetryFailed}
      />,
    );

    expect(screen.getByRole("alert")).toHaveTextContent("Export is blocked");
    expect(screen.getByRole("button", { name: "Export blocked by failed report data" })).toBeDisabled();
    expect(screen.getByText("Data failed to load. Deselect this section or retry.")).toBeVisible();
    await userEvent.click(screen.getByRole("button", { name: "Retry failed sections" }));
    expect(onRetryFailed).toHaveBeenCalledOnce();
    expect(onExport).not.toHaveBeenCalled();
  });

  it("allows an explicit complete export after failed sections are deselected", async () => {
    const onExport = vi.fn();
    render(
      <ExportDialog
        isOpen
        onClose={vi.fn()}
        onExport={onExport}
        tabVisibility={visibility}
        dataPrepared
        dataErrors={{ apps: "Apps data failed to load." }}
      />,
    );

    await userEvent.click(screen.getByRole("checkbox", { name: /Apps/ }));
    await userEvent.click(screen.getByRole("button", { name: /Export 14 sections as PDF/ }));
    expect(onExport).toHaveBeenCalledOnce();
    expect(onExport.mock.calls[0][0].apps).toBe(false);
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
